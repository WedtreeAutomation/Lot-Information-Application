import sys
import asyncio

if sys.platform.startswith("win") and sys.version_info < (3, 14):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

import base64
import hashlib
import hmac
import logging
import threading
import time
import warnings
import html as html_lib
from io import BytesIO
from datetime import date
from dataclasses import dataclass, replace
from urllib.parse import urlparse
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout, wait as wait_futures

import pandas as pd
import polars as pl
import streamlit as st
from streamlit.errors import StreamlitAPIException
from PIL import Image
from azure.identity import ClientSecretCredential
from azure.core.exceptions import ResourceNotFoundError
from azure.core.pipeline.transport import RequestsTransport
from azure.storage.filedatalake import ContentSettings, DataLakeServiceClient, ExponentialRetry
import requests
import requests.adapters

# ===========================================================
# Safe secrets access - st.secrets.get() still raises
# StreamlitSecretNotFoundError if no secrets.toml/Cloud secrets exist
# at all (e.g. on first run before anything is configured), so we
# wrap it to fall back to `default` in that case instead of crashing.
# ===========================================================
def secret(key: str, default: str = "") -> str:
    try:
        return st.secrets.get(key, default)
    except Exception:
        return default


def _safe_identifier(name, default: str = "") -> str:
    name = (name or "").strip()
    return name if name and name.replace("_", "").isalnum() else default


# ===========================================================
# Page / brand config
# ===========================================================
APP_TITLE = secret("APP_TITLE", "Man Mandir Silks and Saris")
APP_SUBTITLE = secret("APP_SUBTITLE", "PRODUCT GALLERY")
PAGE_ICON = secret("APP_ICON", "🧵")

st.set_page_config(page_title=APP_TITLE, page_icon=PAGE_ICON, layout="wide")

# --- Login credentials (single user) - set in secrets.toml / Streamlit Cloud Secrets ---
APP_USERNAME = secret("APP_USERNAME")
APP_PASSWORD = secret("APP_PASSWORD")
LOGIN_CONFIGURED = bool(APP_USERNAME and APP_PASSWORD)
AUTH_TOKEN_DAYS = 7   # how long a login survives browser refreshes

PAGE_SIZE = 12
DEBUG = False
MAX_WORKERS = 20
CARD_IMAGE_MAX_PX = 600   # card thumbnails sent to the browser; originals stay untouched in OneLake
THUMB_DIR = "_gallery_thumbnails"   # saved thumbnails: <lakehouse>/Files/_gallery_thumbnails/w600/...
IMAGE_CACHE_ENTRIES = 2000          # thumbnails kept in memory (~40 KB each)

# ---------------------------------------------------------------
# Data freshness
# ---------------------------------------------------------------
# The inventory is built in a BACKGROUND thread and swapped in when ready
# (stale-while-revalidate): users always get the last good copy instantly
# and never wait for a rebuild. Only the very first load after the app
# starts (e.g. Streamlit Cloud waking from sleep) has to wait.
#
# Rebuild triggers:
#   * a source Delta table got a new version (checked every VERSION_CHECK_SECONDS),
#     so the nightly ETL shows up within minutes instead of up to a day later;
#   * DATA_TTL_SECONDS passed since the last build (safety net).
DATA_TTL_SECONDS = 60 * 60 * 24        # 1 day
VERSION_CHECK_SECONDS = 10 * 60        # how often to look for new table versions
REFRESH_CHECK_SECONDS = 60             # background loop tick

# Images uploaded in the upload app are recorded in this SILVER table (one row
# per lot: id, product_id, image_path, uploaded_at, uploaded_by). It is small,
# so it is re-read far more often than the inventory: new uploads show up in
# the report within this many seconds.
APP_IMAGES_TTL_SECONDS = 5 * 60
APP_IMAGES_WAIT_SECONDS = 3         # first load waits at most this long for the uploads table
NO_IMAGE = "No image available"

WARMUP_PREFETCH_PAGES = 2          # first gallery pages whose images are pre-loaded
PAGE_IMAGE_WAIT_SECONDS = 4        # a page is drawn after at most this long; slower images fill in
PENDING_IMAGE_MAX_WAIT_SECONDS = 60

# Optional pre-joined table built in Fabric (see build_product_gallery_table.py).
# When GALLERY_TABLE is set in secrets, the app reads that ONE small table
# instead of joining five Odoo tables (incl. the large stock_move_line) itself.
GALLERY_TABLE = _safe_identifier(secret("GALLERY_TABLE"))
GALLERY_TABLE_SCHEMA = _safe_identifier(secret("GALLERY_TABLE_SCHEMA"), "Odoo")

log = logging.getLogger("product_gallery")
if not log.handlers:   # the script re-runs on every interaction; add the handler once
    _handler = logging.StreamHandler()
    _handler.setFormatter(logging.Formatter("%(asctime)s %(name)s %(levelname)s: %(message)s"))
    log.addHandler(_handler)
    log.setLevel(logging.INFO)
    log.propagate = False

# ===========================================================
# Fabric / OneLake connection details - read from Streamlit secrets.
# Locally: put these in .streamlit/secrets.toml (see secrets.toml.example).
# On Streamlit Community Cloud: paste the same keys/values into
# App settings -> Secrets in the dashboard. Never hardcode real
# credentials directly in this file.
# ===========================================================
client_id = secret("FABRIC_CLIENT_ID")
client_secret = secret("FABRIC_CLIENT_SECRET")
tenant_id = secret("FABRIC_TENANT_ID")
workspace_id = secret("FABRIC_WORKSPACE_ID")
lakehouse_id_silver = secret("FABRIC_LAKEHOUSE_ID_SILVER")
lakehouse_id_bronze = secret("FABRIC_LAKEHOUSE_ID_BRONZE")

MISSING_CONFIG = not all([
    client_id, client_secret, tenant_id, workspace_id,
    lakehouse_id_silver, lakehouse_id_bronze,
])

storage_options = {
    "azure_storage_client_id": client_id,
    "azure_storage_client_secret": client_secret,
    "azure_storage_tenant_id": tenant_id,
    "use_fabric_endpoint": "true",
}

# Maps the legal-entity names coming out of stock_quant_n1 into the
# short labels used in the UI (mirrors the CASE WHEN in the SQL reference).
COMPANY_LABELS = {
    "Wedtree eStore Private Limited - Coimbatore": "Coimbatore",
    "Wedtree eStore Private Limited - Jayanagar": "Jayanagar",
    "Wedtree eStore Private Limited - Malleshwaram": "Malleshwaram",
    "Wedtree eStore Private Limited - Vizag": "Vizag",
    "Wedtree eStore Private Limited - Hyderabad": "Hyderabad",
    "Wedtree eStore Private Limited - T Nagar": "T Nagar",
    "Wedtree eStore Private Limited - HO": "HO",
}

EXCLUDED_COMPANIES = {"Saree Trails", "Wedtree eStore Private Limited - Online"}
EXCLUDED_LOCATIONS = {
    "Physical Locations/Subcontracting Location",
    "Virtual Locations/Production",
}
EXCLUDED_LOCATION_SUBSTRING = "CONSUMABLE"
EXCLUDED_CATEGORY_PREFIX = "ADMIN"

ODOO_SOURCE_TABLES = (
    "product_product", "product_template", "stock_quant_n1",
    "stock_move_line", "product_images",
)


def table_uri(lakehouse_id: str, schema: str, table: str) -> str:
    return (
        f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/"
        f"{lakehouse_id}/Tables/{schema}/{table}"
    )


def app_images_table_name() -> str:
    """Optional secret PRODUCT_IMAGE_OVERRIDES_TABLE (table in the Odoo schema
    of the silver lakehouse); defaults to product_image_overrides."""
    return _safe_identifier(secret("PRODUCT_IMAGE_OVERRIDES_TABLE"), "product_image_overrides")


def normalize_category_value(value) -> str:
    if value is None or pd.isna(value):
        return ""
    text = str(value).strip()
    text = text.replace(" / ", "/")
    text = text.replace("/ ", "/").replace(" /", "/")
    text = " / ".join(part.strip() for part in text.split("/") if part.strip())
    return text


def expand_category_hierarchy(raw_value):
    cleaned = normalize_category_value(raw_value)
    if not cleaned:
        return []
    parts = [part.strip() for part in cleaned.split("/") if part.strip()]
    hierarchy = []
    current = []
    for part in parts:
        current.append(part)
        hierarchy.append(" / ".join(current))
    return hierarchy


def location_is_excluded(location) -> bool:
    if location is None or pd.isna(location):
        return False
    return (
        location in EXCLUDED_LOCATIONS
        or EXCLUDED_LOCATION_SUBSTRING in str(location).upper()
    )


# ===========================================================
# Branding / CSS
# ===========================================================
def inject_theme():
    theme_css = """
    :root {
        --bg-color: #f5f0f1;
        --panel-bg: #FDF4F6;
        --panel-border: #E3AEBC;
        --panel-text: #2E2126;
        --muted-text: #9C8890;
        --brand-1: #7D1C4A;
        --brand-2: #B5637A;
        --brand-3: #9E4F68;
        --card-bg: #ffffff;
        --card-border: #EFE3E6;
        --input-bg: #ffffff;
        --input-border: #E3AEBC;
        --soft-bg: #FAF3F4;
        --danger-bg: #FBE9E7;
        --danger-text: #C62828;
        --success-bg: #E6F4EA;
        --success-text: #2E7D32;
    }
    @media (prefers-color-scheme: dark) {
        :root {
            --bg-color: #121315;
            --panel-bg: #1d1d1f;
            --panel-border: #7a5764;
            --panel-text: #f3edf0;
            --muted-text: #d0b9c0;
            --brand-1: #3d1a2a;
            --brand-2: #7a4757;
            --brand-3: #a35d71;
            --card-bg: #1b1d1f;
            --card-border: #3b3137;
            --input-bg: #101214;
            --input-border: #85606d;
            --soft-bg: #1e2124;
            --danger-bg: rgba(198,40,40,.15);
            --danger-text: #f4b7b5;
            --success-bg: rgba(46,125,50,.18);
            --success-text: #c0f0c4;
        }
    }
    """

    st.markdown(
        f"""
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Playfair+Display:wght@600;700&family=Inter:wght@400;500;600&display=swap');

        html, body, [class*="css"] {{ font-family: 'Inter', sans-serif; background: var(--bg-color); color: var(--panel-text); }}
        body {{ background: var(--bg-color); }}
        .stApp {{ background: var(--bg-color); }}
        #MainMenu, footer {{ visibility: hidden; }}

        {theme_css}

        /* ---------------------------------------------------------
           Header row: title + theme toggle live INSIDE the same
           Header banner: title sits inside the brand gradient, always
           readable regardless of the visitor's OS light/dark preference
           since it's white-on-gradient rather than themed text.
        --------------------------------------------------------- */
        .st-key-header_row {{
            background: linear-gradient(135deg, var(--brand-1) 0%, var(--brand-2) 55%, var(--brand-3) 100%);
            padding: 22px 32px;
            border-radius: 14px;
            margin-bottom: 22px;
            box-shadow: 0 4px 18px rgba(158, 79, 104, 0.28);
        }}
        .brand-title {{
            font-family: 'Playfair Display', serif;
            font-size: 30px;
            font-weight: 700;
            color: #ffffff;
            margin: 0;
            letter-spacing: 0.3px;
        }}
        .brand-subtitle {{
            font-size: 12px;
            font-weight: 600;
            color: rgba(255,255,255,0.88);
            letter-spacing: 3px;
            margin-top: 2px;
        }}

        /* Spinner text sits directly on the page background by default,
           which reads fine but flat. Give it a small pill so it stands
           out clearly in both light and dark modes, like a native
           loading indicator. */
        [data-testid="stSpinner"] {{
            background: var(--soft-bg);
            border: 1px solid var(--card-border);
            border-radius: 10px;
            padding: 10px 14px;
        }}

        .st-key-filter_panel {{
            background: var(--panel-bg);
            border: 1.5px solid var(--panel-border);
            border-radius: 14px;
            padding: 18px 18px 12px 18px;
            box-shadow: 0 3px 14px rgba(158,79,104,0.10);
        }}
        .st-key-filter_panel h3 {{
            font-family: 'Playfair Display', serif;
            color: var(--brand-3);
            font-size: 18px;
            margin-top: 0;
        }}
        .st-key-filter_panel [data-baseweb="select"],
        .st-key-filter_panel [data-baseweb="select"] input,
        .st-key-mobile_filter_panel [data-baseweb="select"],
        .st-key-mobile_filter_panel [data-baseweb="select"] input {{
            font-size: 13px !important;
        }}
        .st-key-filter_panel [data-baseweb="tag"],
        .st-key-mobile_filter_panel [data-baseweb="tag"] {{
            font-size: 12px !important;
        }}

        /* Streamlit renders widget labels ("Company", "Vendor", etc.)
           with its own low-contrast default gray that was never
           overridden - readable-ish in light mode, nearly invisible in
           dark mode. Force them to the theme's main text color in both
           filter panels (desktop + mobile drawer). */
        .st-key-filter_panel [data-testid="stWidgetLabel"] p,
        .st-key-mobile_filter_panel [data-testid="stWidgetLabel"] p,
        .st-key-filter_panel label,
        .st-key-mobile_filter_panel label {{
            color: var(--panel-text) !important;
            opacity: 1 !important;
            font-weight: 600;
        }}

        .stButton > button {{
            background-color: var(--brand-2);
            color: #ffffff;
            border: none;
            border-radius: 999px;
            padding: 8px 20px;
            font-weight: 600;
            transition: all 0.15s ease-in-out;
        }}
        .stButton > button:hover {{
            background-color: var(--brand-3);
            color: #ffffff;
            transform: translateY(-1px);
            box-shadow: 0 3px 10px rgba(158,79,104,0.35);
        }}
        .stButton > button:disabled {{
            background-color: #E7D3D8;
            color: #ffffff;
        }}

        .product-card {{
            background: var(--card-bg);
            border-radius: 14px;
            overflow: hidden;
            border: 1px solid var(--card-border);
            box-shadow: 0 2px 10px rgba(0,0,0,0.05);
            margin-bottom: 20px;
            transition: transform 0.15s ease, box-shadow 0.15s ease;
        }}
        .product-card:hover {{
            transform: translateY(-3px);
            box-shadow: 0 8px 20px rgba(158,79,104,0.18);
        }}
        .product-card-img {{
            width: 100%;
            aspect-ratio: 1 / 1;
            background: var(--soft-bg);
            display: flex;
            align-items: center;
            justify-content: center;
            overflow: hidden;
        }}
        .product-card-img img {{
            width: 100%;
            height: 100%;
            object-fit: cover;
        }}
        .product-card-noimg {{
            color: var(--muted-text);
            font-size: 13px;
            font-weight: 500;
            text-align: center;
        }}
        .product-card-body {{ padding: 14px 16px 16px 16px; }}
        .product-card-title {{
            font-size: 15px;
            font-weight: 700;
            color: var(--panel-text);
            line-height: 1.3;
            margin-bottom: 2px;
            min-height: 39px;
        }}
        .product-card-meta {{
            font-size: 12px;
            color: var(--muted-text);
            margin-bottom: 10px;
        }}
        .product-card-row {{
            display: flex;
            justify-content: space-between;
            font-size: 12.5px;
            color: var(--panel-text);
            padding: 3px 0;
            border-bottom: 1px dashed var(--card-border);
        }}
        .product-card-row span:first-child {{ color: var(--muted-text); }}
        .product-card-row span:last-child {{ font-weight: 600; color: var(--panel-text); }}
        .product-card-footer {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-top: 12px;
        }}
        .price-block .sp {{
            font-size: 18px;
            font-weight: 700;
            color: var(--brand-3);
        }}
        .price-block .cp {{
            font-size: 11px;
            color: var(--muted-text);
        }}
        .stock-badge {{
            font-size: 10.5px;
            font-weight: 700;
            padding: 4px 10px;
            border-radius: 999px;
            letter-spacing: 0.3px;
        }}
        .stock-in {{ background: var(--success-bg); color: var(--success-text); }}
        .stock-out {{ background: var(--danger-bg); color: var(--danger-text); }}
        .page-indicator {{
            text-align: center;
            font-weight: 600;
            color: var(--brand-3);
            padding-top: 8px;
        }}

        .st-key-login_card {{
            max-width: 380px;
            margin: 48px auto 0 auto;
            background: var(--card-bg);
            border: 1.5px solid var(--panel-border);
            border-radius: 16px;
            padding: 32px 28px 24px 28px;
            box-shadow: 0 6px 24px rgba(158,79,104,0.12);
        }}
        .st-key-login_card h3 {{
            font-family: 'Playfair Display', serif;
            color: var(--brand-3);
            text-align: center;
            margin-top: 0;
            margin-bottom: 18px;
        }}

        /* Sits in normal document flow, right below the header banner -
           NOT position:fixed at a hardcoded viewport coordinate, which
           is what previously caused it to land wherever it happened to
           overlap (banner corner, drawer heading, etc.) regardless of
           actual layout. position:sticky keeps it reachable while
           scrolling through the product grid, but only once the page
           has actually scrolled past its natural spot - so at rest it
           just sits cleanly below the banner like any other element. */
        .st-key-mobile_filters_toggle {{
            display: none;
            position: sticky;
            top: 12px;
            z-index: 1500;
            margin-bottom: 14px;
            width: fit-content;
        }}
        .st-key-mobile_filters_toggle button {{
            width: 46px;
            height: 46px;
            border-radius: 12px;
            background: linear-gradient(135deg, #C9748A 0%, #B5637A 55%, #9E4F68 100%);
            border: none;
            color: #ffffff;
            font-size: 24px;
            line-height: 1;
            padding: 0;
            box-shadow: 0 6px 18px rgba(158,79,104,0.24);
        }}
        .st-key-mobile_filter_panel {{ display: none; }}

        /* Top-right X close icon inline with the "Filters" heading in
           the mobile drawer - the standard placement used by most
           mobile apps for dismissing a slide-in panel, rather than a
           separate full-width button floating above the heading. */
        .st-key-mobile_filter_panel [data-testid="stHorizontalBlock"]:first-of-type {{
            align-items: center;
        }}
        .st-key-mobile_filter_close_x button {{
            width: 32px;
            height: 32px;
            min-width: 32px;
            border-radius: 8px;
            padding: 0;
            font-size: 14px;
            line-height: 1;
            background: var(--soft-bg);
            color: var(--panel-text);
            box-shadow: none;
            border: 1px solid var(--card-border);
        }}
        .st-key-mobile_filter_close_x button:hover {{
            background: var(--card-border);
            color: var(--panel-text);
            box-shadow: none;
            transform: none;
        }}

        /* Apply Filters is the primary action in the mobile drawer -
           give it a solid brand-gradient fill so it visually outranks
           the plain-pink default Logout button below it. */
        .st-key-mobile_filter_apply button {{
            background: linear-gradient(135deg, var(--brand-1) 0%, var(--brand-2) 55%, var(--brand-3) 100%);
            font-weight: 700;
            box-shadow: 0 4px 14px rgba(158,79,104,0.30);
        }}
        .st-key-mobile_filter_apply button:hover {{
            filter: brightness(1.08);
            box-shadow: 0 6px 18px rgba(158,79,104,0.4);
        }}

        @media (max-width: 768px) {{
            .st-key-filter_panel {{ display: none !important; }}
            .st-key-mobile_filters_toggle {{ display: block; }}
            .st-key-mobile_filter_panel {{
                display: block;
                position: fixed;
                top: 0;
                left: 0;
                bottom: 0;
                width: min(88vw, 380px);
                z-index: 2000;
                background: var(--panel-bg);
                border-right: 1.5px solid var(--panel-border);
                box-shadow: 12px 0 30px rgba(58, 28, 35, 0.12);
                padding: 26px 18px 18px 18px;
                overflow-y: auto;
            }}
            .st-key-mobile_filter_panel > div {{ padding-top: 8px; }}
            .st-key-mobile_filter_panel h3 {{
                font-family: 'Playfair Display', serif;
                color: var(--brand-3);
                font-size: 20px;
                margin-top: 4px;
                margin-bottom: 10px;
            }}
            .st-key-mobile_filter_panel .stButton > button {{ margin-top: 8px; }}
        }}

        [data-testid="stHeader"] {{ display: none; }}
        [data-testid="stToolbar"] {{ display: none; }}
        .block-container {{ padding-top: 1.5rem; }}

        [data-testid="stTextInput"] input,
        [data-testid="stNumberInput"] input,
        [data-testid="stTextArea"] textarea {{
            border: 1.5px solid var(--input-border) !important;
            border-radius: 8px !important;
            background-color: var(--input-bg) !important;
            color: var(--panel-text) !important;
            padding: 8px 12px !important;
        }}
        div[data-baseweb="select"] > div,
        div[data-baseweb="select"] > div > div,
        div[data-baseweb="select"] > div > div > div,
        div[data-baseweb="select"] > div > div > div > div {{
            background-color: var(--input-bg) !important;
            color: var(--panel-text) !important;
            border-color: var(--input-border) !important;
        }}
        [data-testid="stTextInput"] input:focus,
        [data-testid="stNumberInput"] input:focus,
        [data-testid="stTextArea"] textarea:focus {{
            border-color: var(--brand-3) !important;
            box-shadow: 0 0 0 1px var(--brand-3) !important;
        }}
        [data-testid="stTextInput"] input::placeholder,
        [data-testid="stTextArea"] textarea::placeholder {{
            color: var(--muted-text) !important;
            opacity: 1 !important;
        }}
        div[data-baseweb="base-input"],
        div[data-baseweb="input"] {{
            border: none !important;
            background-color: transparent !important;
        }}

        div[data-baseweb="select"] > div {{
            border: 1.5px solid var(--input-border) !important;
            border-radius: 8px !important;
            background-color: var(--input-bg) !important;
        }}
        div[data-baseweb="select"]:focus-within > div {{
            border-color: var(--brand-3) !important;
        }}
        /* The text you type to search/filter options lives in a plain
           <input> nested inside the select box above - the background
           rules on the containing divs don't touch it, so it kept its
           own default (dark) text color and stayed unreadable on a
           dark background. Color it and its placeholder explicitly. */
        div[data-baseweb="select"] input {{
            color: var(--panel-text) !important;
            -webkit-text-fill-color: var(--panel-text) !important;
        }}
        div[data-baseweb="select"] input::placeholder {{
            color: var(--muted-text) !important;
            opacity: 1 !important;
        }}

        /* The multiselect/select OPTIONS dropdown renders in a separate
           floating portal (data-baseweb="popover"), outside the input
           box styled above - it was left with Streamlit's built-in
           light-mode-only colors AND its own default sharp-cornered,
           unbordered panel styling, so it read as a mismatched, floating
           box rather than part of the same rounded-pill input below it.
           Theme the popover, its menu list, individual options, and the
           select-all row explicitly, and match the input's 8px radius,
           border color/width, and give it a bit of separation via
           shadow instead of a hard edge. */
        div[data-baseweb="popover"] {{
            background-color: var(--input-bg) !important;
            border: 1.5px solid var(--input-border) !important;
            border-radius: 8px !important;
            box-shadow: 0 8px 20px rgba(0,0,0,0.18) !important;
            width: max-content !important;
            max-width: min(92vw, 720px) !important;
            overflow: hidden;
        }}
        div[data-baseweb="popover"] [role="listbox"],
        div[data-baseweb="popover"] ul[data-baseweb="menu"] {{
            background-color: var(--input-bg) !important;
            border: none !important;
            border-radius: 8px !important;
        }}
        div[data-baseweb="popover"] li[role="option"],
        div[data-baseweb="popover"] [data-baseweb="menu"] > li {{
            background-color: var(--input-bg) !important;
            color: var(--panel-text) !important;
            border-bottom: 1px solid var(--card-border) !important;
            height: auto !important;
            min-height: 40px !important;
            white-space: normal !important;
            overflow: visible !important;
            text-overflow: clip !important;
        }}
        div[data-baseweb="popover"] li[role="option"]:last-child {{
            border-bottom: none !important;
        }}
        div[data-baseweb="popover"] li[role="option"] * {{
            color: var(--panel-text) !important;
            white-space: normal !important;
            overflow: visible !important;
            text-overflow: clip !important;
        }}
        div[data-baseweb="popover"] li[role="option"]:hover,
        div[data-baseweb="popover"] li[aria-selected="true"] {{
            background-color: var(--soft-bg) !important;
        }}
        /* "Select all N matches" row and any helper/empty-state text */
        div[data-baseweb="popover"] div,
        div[data-baseweb="popover"] span {{
            color: var(--panel-text);
        }}

        /* Long selected values are compacted into BaseWeb tags. Expand the
           tag while hovering so the complete category/location is readable. */
        [data-baseweb="tag"] {{
            max-width: 100% !important;
        }}
        [data-baseweb="tag"] span {{
            overflow: hidden !important;
            text-overflow: ellipsis !important;
            white-space: nowrap !important;
        }}
        [data-baseweb="tag"]:hover {{
            position: relative !important;
            z-index: 10 !important;
            max-width: min(90vw, 620px) !important;
        }}
        [data-baseweb="tag"]:hover span {{
            overflow: visible !important;
            text-overflow: clip !important;
            white-space: normal !important;
            word-break: break-word !important;
        }}

        [data-testid="stSpinner"] > div > div {{
            border-top-color: var(--brand-2) !important;
        }}
        [data-testid="stSpinner"] p {{
            color: var(--brand-3) !important;
            font-weight: 600;
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_header():
    """Title banner. Theme is fully automatic - driven by the visitor's
    OS/browser color-scheme preference via CSS `prefers-color-scheme`
    (see inject_theme) - so there's no manual toggle to keep in sync;
    every element on the page is styled with theme CSS variables that
    resolve correctly for either preference."""
    with st.container(key="header_row"):
        st.markdown(
            f"""
            <div class="brand-title">{html_lib.escape(APP_TITLE)}</div>
            <div class="brand-subtitle">{html_lib.escape(APP_SUBTITLE)}</div>
            """,
            unsafe_allow_html=True,
        )


def _auth_signing_key() -> bytes:
    """Key for the signed login token in the URL. Set AUTH_SECRET in secrets to
    control it; otherwise it is derived from the login, so changing the
    password also logs everyone out."""
    material = secret("AUTH_SECRET") or f"{APP_USERNAME}|{APP_PASSWORD}|{client_secret}"
    return hashlib.sha256(material.encode("utf-8")).digest()


def _make_auth_token() -> str:
    expires = str(int(time.time()) + AUTH_TOKEN_DAYS * 24 * 3600)
    signature = hmac.new(_auth_signing_key(), expires.encode("utf-8"), hashlib.sha256).hexdigest()[:32]
    return f"{expires}.{signature}"


def _auth_token_is_valid(token) -> bool:
    """A plain ?auth=1 is no longer accepted: the token is signed and expires,
    so it can't be typed into the URL to skip the login."""
    try:
        expires, signature = str(token).split(".")
        expected = hmac.new(_auth_signing_key(), expires.encode("utf-8"), hashlib.sha256).hexdigest()[:32]
        return hmac.compare_digest(signature, expected) and int(expires) > time.time()
    except Exception:
        return False


def set_authenticated(value: bool):
    """Keep login state across page reloads with a signed, expiring token in
    the URL query string (?auth=...)."""
    st.session_state.authenticated = value

    try:
        if value:
            st.query_params["auth"] = _make_auth_token()
        elif "auth" in st.query_params:
            del st.query_params["auth"]
    except Exception:
        pass


def check_login() -> bool:
    """Gate the app behind a single username/password pair from secrets.
    Returns True once the current session is authenticated."""
    if st.session_state.get("authenticated"):
        return True

    try:
        if _auth_token_is_valid(st.query_params.get("auth")):
            st.session_state.authenticated = True
            return True
    except Exception:
        pass

    render_header()

    if not LOGIN_CONFIGURED:
        st.error(
            "Login is not configured yet. Add **APP_USERNAME** and **APP_PASSWORD** "
            "under App settings → Secrets (or `.streamlit/secrets.toml` locally) — "
            "see `secrets.toml.example`."
        )
        return False

    with st.container(key="login_card"):
        st.markdown("<h3>Sign in</h3>", unsafe_allow_html=True)
        with st.form("login_form", clear_on_submit=False):
            username = st.text_input("Username", placeholder="Enter your username")
            password = st.text_input("Password", type="password", placeholder="Enter your password")
            submitted = st.form_submit_button("Login", width="stretch")

        if submitted:
            if hmac.compare_digest(username.encode("utf-8"), APP_USERNAME.encode("utf-8")) and hmac.compare_digest(
                password.encode("utf-8"), APP_PASSWORD.encode("utf-8")
            ):
                set_authenticated(True)
                st.rerun()
            else:
                st.error("Incorrect username or password.")

    return False


# ===========================================================
# Azure Data Lake client (for fetching image bytes). One credential and
# client are shared by all sessions: the credential renews its token by
# itself before it expires, and the client keeps HTTPS connections open, so
# image downloads skip a fresh sign-in and TLS handshake on every rerun.
# (After rotating FABRIC_CLIENT_SECRET, reboot the app.)
# ===========================================================
@st.cache_resource(show_spinner=False)
def get_datalake_service_client():
    credential = ClientSecretCredential(tenant_id, client_id, client_secret)
    # The SDK defaults (20 s to connect, 60 s to read, first retry after 15 s)
    # let one hiccup hold a card image for 40 s+. Fail fast and retry quickly
    # instead, and keep enough pooled connections for the parallel downloads.
    session = requests.Session()
    session.mount("https://", requests.adapters.HTTPAdapter(pool_connections=4, pool_maxsize=MAX_WORKERS * 2))
    return DataLakeServiceClient(
        account_url="https://onelake.dfs.fabric.microsoft.com",
        credential=credential,
        transport=RequestsTransport(session=session, connection_timeout=5, read_timeout=20),
        retry_policy=ExponentialRetry(initial_backoff=1, increment_base=2, retry_total=3),
    )


def _parse_abfss_path(abfss_path: str):
    parsed = urlparse(abfss_path)
    filesystem = parsed.username
    file_path = parsed.path.lstrip("/")
    return filesystem, file_path


# ===========================================================
# Card images
# Every card shows a small JPEG thumbnail (longest side CARD_IMAGE_MAX_PX,
# ~30-60 KB). Thumbnails come from the first of:
#   1. memory (this app process)                        instant
#   2. OneLake, saved next to the originals under       one small download;
#      Files/_gallery_thumbnails/w<px>/<original path>.jpg  survives restarts
#   3. the original image: full download + resize,      slow, but only once
#      then saved to (2) for every later session        per image
# A saved thumbnail is used only if it is newer than its original, so an
# image replaced at the same path gets a fresh thumbnail automatically.
# ===========================================================
def _thumbnail_location(abfss_path: str):
    filesystem, file_path = _parse_abfss_path(abfss_path)
    lakehouse, sep, rest = file_path.partition("/Files/")
    if not sep or not rest or rest.startswith(THUMB_DIR + "/"):
        return None
    return filesystem, f"{lakehouse}/Files/{THUMB_DIR}/w{CARD_IMAGE_MAX_PX}/{rest}.jpg"


def _file_client(client, filesystem: str, path: str):
    return client.get_file_system_client(filesystem).get_file_client(path)


def fetch_image_bytes(client, abfss_path: str):
    return _file_client(client, *_parse_abfss_path(abfss_path)).download_file().readall()


def make_thumbnail_jpeg(image_bytes: bytes) -> bytes:
    img = Image.open(BytesIO(image_bytes))
    if img.format == "JPEG":
        # Decode straight at (about) the thumbnail size - several times faster
        # than decoding the full 1920px image and shrinking it afterwards.
        img.draft("RGB", (CARD_IMAGE_MAX_PX, CARD_IMAGE_MAX_PX))
    img.thumbnail((CARD_IMAGE_MAX_PX, CARD_IMAGE_MAX_PX))
    if img.mode in ("RGBA", "LA") or (img.mode == "P" and "transparency" in img.info):
        rgba = img.convert("RGBA")
        img = Image.new("RGB", rgba.size, (255, 255, 255))
        img.paste(rgba, mask=rgba.split()[-1])
    buf = BytesIO()
    img.convert("RGB").save(buf, format="JPEG", quality=82, optimize=True)
    return buf.getvalue()


def _jpeg_data_uri(jpeg: bytes) -> str:
    return "data:image/jpeg;base64," + base64.b64encode(jpeg).decode("ascii")


class _ImageStore:
    """Shared by all sessions: in-memory thumbnails, downloads in progress
    (so two sessions or the prefetcher never fetch the same image twice at
    once) and the worker threads."""

    def __init__(self):
        self.lock = threading.Lock()
        self.cache = OrderedDict()      # ref -> (expires_at, (data_uri | None, error | None))
        self.inflight = {}              # ref -> Future
        self.pool = ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="card-img")
        self.io_pool = ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="card-img-io")
        self.can_save = True            # set False if OneLake refuses thumbnail writes


@st.cache_resource(show_spinner=False)
def image_store() -> _ImageStore:
    return _ImageStore()


def _save_thumbnail(store: _ImageStore, client, location, jpeg: bytes):
    try:
        _file_client(client, *location).upload_data(
            jpeg, overwrite=True, content_settings=ContentSettings(content_type="image/jpeg"),
        )
    except Exception as e:
        if getattr(e, "status_code", None) in (401, 403):
            store.can_save = False
            log.warning("Can't save thumbnails to OneLake (%s). They are kept in memory only, "
                        "so each app restart re-downloads the originals.", type(e).__name__)
        else:
            log.info("thumbnail save failed: %s: %s", type(e).__name__, e)


def _load_card_image(store: _ImageStore, client, ref: str):
    """(data_uri, None) or (None, error). Never raises."""
    started = time.perf_counter()
    try:
        filesystem, path = _parse_abfss_path(ref)
        original = _file_client(client, filesystem, path)
        location = _thumbnail_location(ref)

        if location:
            # Look up the original's date while the saved thumbnail downloads.
            original_props = store.io_pool.submit(original.get_file_properties)
            thumb_bytes = thumb_modified = None
            try:
                downloader = _file_client(client, *location).download_file()
                thumb_bytes = downloader.readall()
                thumb_modified = downloader.properties.last_modified
            except ResourceNotFoundError:
                pass                                  # not made yet
            except Exception as e:
                log.info("saved thumbnail unreadable (%s); using the original", type(e).__name__)
            try:
                original_modified = original_props.result().last_modified
            except Exception:
                original_modified = None
            if thumb_bytes and thumb_modified and original_modified and thumb_modified >= original_modified:
                return _jpeg_data_uri(thumb_bytes), None

        raw = original.download_file().readall()
        if not raw:
            return None, "Empty file (0 bytes)"
        jpeg = make_thumbnail_jpeg(raw)
        if location and store.can_save:
            store.io_pool.submit(_save_thumbnail, store, client, location, jpeg)
        log.info("thumbnail made from original: %.1f MB -> %d KB in %.1fs",
                 len(raw) / 1e6, len(jpeg) // 1024, time.perf_counter() - started)
        return _jpeg_data_uri(jpeg), None
    except Exception as e:
        log.info("image load failed in %.1fs: %s: %s", time.perf_counter() - started, type(e).__name__, e)
        return None, f"{type(e).__name__}: {e}"


def _load_and_remember(store: _ImageStore, client, ref: str):
    result = _load_card_image(store, client, ref)
    # Failures are retried after a few minutes instead of on every rerun.
    ttl = DATA_TTL_SECONDS if result[0] is not None else 5 * 60
    with store.lock:
        store.cache[ref] = (time.time() + ttl, result)
        store.cache.move_to_end(ref)
        while len(store.cache) > IMAGE_CACHE_ENTRIES:
            store.cache.popitem(last=False)
        store.inflight.pop(ref, None)
    return result


def _is_image_ref(ref) -> bool:
    return isinstance(ref, str) and ref.startswith("abfss://")


def request_card_images(refs):
    """Starts loading every ref that isn't in memory yet. Returns
    (ready: {ref: data_uri | None}, pending: {ref: Future})."""
    store = image_store()
    client = get_datalake_service_client()
    now = time.time()
    ready, pending = {}, {}
    with store.lock:
        for ref in dict.fromkeys(r for r in refs if _is_image_ref(r)):
            hit = store.cache.get(ref)
            if hit is not None and hit[0] > now:
                store.cache.move_to_end(ref)
                ready[ref] = hit[1][0]
                continue
            future = store.inflight.get(ref)
            if future is None:
                future = store.pool.submit(_load_and_remember, store, client, ref)
                store.inflight[ref] = future
            pending[ref] = future
    return ready, pending


def get_card_images(refs, timeout=None):
    """({ref: data_uri | None}, {ref: Future still loading}). Waits up to
    `timeout` seconds (None = until all are loaded)."""
    ready, pending = request_card_images(refs)
    if pending:
        wait_futures(list(pending.values()), timeout=timeout)
        for ref, future in list(pending.items()):
            if future.done():
                ready[ref] = future.result()[0]
                del pending[ref]
    return ready, pending


def prefetch_images(refs):
    """Start loading the thumbnails the user is likely to open next (the next
    page) without blocking the current render."""
    request_card_images(refs)


# ===========================================================
# Data loading
# ===========================================================
def _scan(lakehouse_id, schema, table, columns) -> pl.LazyFrame:
    # scan_delta (lazy): nothing is read until collect(), so only the needed
    # columns are fetched and filters can skip files / row groups.
    return pl.scan_delta(
        table_uri(lakehouse_id, schema, table),
        storage_options=storage_options,
    ).select(columns)


def _as_date(lf: pl.LazyFrame, column: str) -> pl.Expr:
    """Column as a Date whether the Delta table stores it as a timestamp/date
    or as text (Polars 2 no longer casts text to Date). Reads only the schema."""
    dtype = lf.collect_schema()[column]
    if dtype == pl.Utf8:
        return pl.col(column).str.to_datetime(strict=False).dt.date()
    return pl.col(column).cast(pl.Date, strict=False)


def _inventory_plan_from_odoo() -> pl.LazyFrame:
    """Replicates the reference SQL as Polars lazy joins over the Odoo silver
    tables. Used when no pre-joined GALLERY_TABLE is configured."""
    # --- product_product (p) ---
    product_product = _scan(
        lakehouse_id_silver, "Odoo", "product_product",
        ["id", "display_name", "categ_id_name", "product_variant_id",
         "sku", "lst_price", "standard_price"],
    ).rename({
        "id": "product_id",
        "display_name": "product_name",
        "categ_id_name": "category",
        "lst_price": "sp",
        "standard_price": "cp",
    }).with_columns([
        pl.col("product_id").cast(pl.Utf8),
        pl.col("sku").cast(pl.Utf8),
    ])

    # --- product_template (pt) --- .unique() guards against join explosion
    product_template = _scan(
        lakehouse_id_silver, "Odoo", "product_template",
        ["product_variant_id", "vendor_id_name"],
    ).rename({"vendor_id_name": "vendor"}).unique(
        subset=["product_variant_id"], keep="first"
    )

    product_base = product_product.join(product_template, on="product_variant_id", how="left")

    # --- stock_quant_n1 (sq) ---
    # Company / location exclusions are applied HERE, before any join, so the
    # joins only process rows that can reach the UI.
    stock_quant = _scan(
        lakehouse_id_silver, "Odoo", "stock_quant_n1",
        ["company_id_name", "product_id", "lot_id_name", "location_id_name", "quantity"],
    ).rename({
        "lot_id_name": "lot_number",
        "location_id_name": "location",
        "quantity": "available_inventory",
    }).with_columns([
        pl.col("product_id").cast(pl.Utf8),
        pl.col("company_id_name").replace(COMPANY_LABELS).alias("company"),
    ]).filter(
        ~pl.col("company").is_in(sorted(EXCLUDED_COMPANIES))
        & ~pl.col("location").is_in(sorted(EXCLUDED_LOCATIONS))
        & ~pl.col("location").cast(pl.Utf8).fill_null("").str.to_uppercase()
            .str.contains(EXCLUDED_LOCATION_SUBSTRING, literal=True)
    )

    inventory = stock_quant.join(product_base, on="product_id", how="left").with_columns(
        (pl.col("available_inventory") * pl.col("sp")).alias("available_selling_price")
    )

    # --- first stock movement date per product+lot (for "overall age") ---
    move_lines = _scan(
        lakehouse_id_silver, "Odoo", "stock_move_line",
        ["product_id", "lot_id_name", "date", "company_id_name", "location_id_name"],
    )
    stock_min_date = move_lines.filter(
        (pl.col("company_id_name") == "Wedtree eStore Private Limited - HO")
        & (pl.col("location_id_name") == "Partners/Vendors")
    ).select(
        pl.col("product_id").cast(pl.Utf8),
        pl.col("lot_id_name").alias("lot_number"),
        _as_date(move_lines, "date"),
    ).group_by(["product_id", "lot_number"]).agg(
        pl.col("date").min().alias("stock_move_date")
    )

    inventory = inventory.join(stock_min_date, on=["product_id", "lot_number"], how="left")

    # --- product_images (silver): one image per lot, plus all its row ids ---
    product_images = _scan(
        lakehouse_id_silver, "Odoo", "product_images",
        ["id", "name", "product_id", "image_1920"],
    ).select(
        pl.col("id").cast(pl.Utf8).str.strip_chars().alias("pi_id"),
        pl.col("product_id").cast(pl.Utf8).str.strip_chars().alias("product_id"),
        pl.col("name").cast(pl.Utf8).str.strip_chars().str.to_uppercase().alias("_lot_key"),
        pl.col("image_1920").cast(pl.Utf8).str.strip_chars().alias("image_1920"),
    ).filter(
        pl.col("pi_id").is_not_null() & pl.col("_lot_key").is_not_null() & (pl.col("_lot_key") != "")
    ).with_columns(
        pl.when(
            pl.col("image_1920").is_null()
            | pl.col("image_1920").str.to_lowercase().is_in(["", "false", "none", "null"])
        ).then(None).otherwise(pl.col("image_1920")).alias("image_1920")
    )

    lot_images = product_images.group_by(["product_id", "_lot_key"]).agg(
        pl.col("image_1920").drop_nulls().first().alias("image_1920"),
        pl.col("pi_id").unique().sort().str.join(",").alias("pi_ids"),
    )

    final = inventory.with_columns(
        pl.col("lot_number").cast(pl.Utf8).str.strip_chars().str.to_uppercase().alias("_lot_key")
    ).join(lot_images, on=["product_id", "_lot_key"], how="left")

    # Category comes from the right side of a left join, so it can only be
    # filtered after the join.
    final = final.filter(
        ~pl.col("category").cast(pl.Utf8).fill_null("").str.strip_chars()
            .str.to_uppercase().str.starts_with(EXCLUDED_CATEGORY_PREFIX)
    )

    return final.select([
        "company", "category", "vendor", "sku", "lot_number", "location",
        "product_name", "cp", "sp", "available_inventory",
        "available_selling_price", "product_id", "stock_move_date",
        "image_1920", "pi_ids",
    ])


def _inventory_plan_from_gold() -> pl.LazyFrame:
    """Reads the pre-joined table written by build_product_gallery_table.py.
    The exclusions are already applied there."""
    return _scan(
        lakehouse_id_silver, GALLERY_TABLE_SCHEMA, GALLERY_TABLE,
        ["company", "category", "vendor", "sku", "lot_number", "location",
         "product_name", "cp", "sp", "available_inventory",
         "available_selling_price", "product_id", "stock_move_date",
         "image_1920", "pi_ids"],
    ).with_columns(
        pl.col("product_id").cast(pl.Utf8),
        pl.col("sku").cast(pl.Utf8),
    )


def _source_tables():
    if GALLERY_TABLE:
        return [(GALLERY_TABLE_SCHEMA, GALLERY_TABLE)]
    return [("Odoo", name) for name in ODOO_SOURCE_TABLES]


def delta_version(uri: str) -> int:
    """Current Delta version of one table - reads only the transaction log."""
    from deltalake import DeltaTable
    # without_files skips loading the file list on deltalake 0.x; newer
    # versions ignore it (with a DeprecationWarning) or don't accept it.
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        try:
            return DeltaTable(uri, storage_options=storage_options, without_files=True).version()
        except TypeError:
            return DeltaTable(uri, storage_options=storage_options).version()


def source_version():
    """Delta version of each source table. Returns None if it can't be read
    (then only the TTL applies)."""
    try:
        return tuple(
            delta_version(table_uri(lakehouse_id_silver, schema, table))
            for schema, table in _source_tables()
        )
    except Exception:
        log.warning("Couldn't read source table versions", exc_info=True)
        return None


def build_base_inventory() -> pd.DataFrame:
    """Runs the query and returns the inventory, sorted "products with images
    first", with helper columns the filters use (computed once here instead
    of on every click)."""
    started = time.perf_counter()
    plan = _inventory_plan_from_gold() if GALLERY_TABLE else _inventory_plan_from_odoo()

    plan = plan.with_columns(
        pl.col("stock_move_date").cast(pl.Date, strict=False),
        pl.col("image_1920").fill_null(NO_IMAGE),
        pl.col("pi_ids").fill_null(""),
    ).with_columns(
        (pl.lit(date.today()) - pl.col("stock_move_date")).dt.total_days().alias("overall_age"),
        (pl.col("image_1920") == NO_IMAGE).alias("_no_image"),
        # Lower-cased search text, so a search is one vectorised .contains()
        pl.concat_str(
            [
                pl.col("product_name").cast(pl.Utf8).fill_null("").str.to_lowercase(),
                pl.col("sku").cast(pl.Utf8).fill_null("").str.to_lowercase(),
            ],
            separator="\n",
        ).alias("_search"),
        pl.col("lot_number").cast(pl.Utf8).fill_null("").str.to_lowercase().alias("_lot_lc"),
    ).sort(
        # Products with a real image always come first; filters keep this order.
        # location is a final tie-breaker and maintain_order makes the sort
        # stable, so the same data always gives the same pages.
        ["_no_image", "company", "category", "vendor", "sku", "lot_number", "location"],
        nulls_last=True,
        maintain_order=True,
    ).drop(["_no_image", "stock_move_date"])

    df = plan.collect(engine="streaming").to_pandas()

    # Normalised category for the category filter - computed per DISTINCT
    # category (a few hundred), not per row.
    categories = df["category"]
    mapping = {value: normalize_category_value(value) for value in categories.dropna().unique()}
    df["_cat_norm"] = categories.map(mapping).fillna("")

    log.info(
        "inventory built from %s: %d rows in %.1fs",
        f"{GALLERY_TABLE_SCHEMA}.{GALLERY_TABLE}" if GALLERY_TABLE else "Odoo tables",
        len(df), time.perf_counter() - started,
    )
    return df


# ===========================================================
# Images uploaded in the app
# The report shows coalesce(app image, Odoo image) PER LOT. Uploads are
# matched on the lot's product_images id. Only this small table is re-read
# every APP_IMAGES_TTL_SECONDS, so new uploads appear without a rebuild.
# ===========================================================
def load_app_images() -> pd.DataFrame:
    """Newest app upload per lot: columns pi_id (text), image_path, uploaded_at.
    Empty if the table doesn't exist yet or can't be read (the report then
    simply shows the Odoo images)."""
    empty = pd.DataFrame({"pi_id": pd.Series(dtype=str), "image_path": pd.Series(dtype=str),
                          "uploaded_at": pd.Series(dtype="datetime64[ns, UTC]")})
    try:
        uploads = (
            pl.scan_delta(
                table_uri(lakehouse_id_silver, "Odoo", app_images_table_name()),
                storage_options=storage_options,
            )
            .select(
                pl.col("id").cast(pl.Utf8).str.strip_chars().alias("pi_id"),
                pl.col("image_path").cast(pl.Utf8).str.strip_chars().alias("image_path"),
                pl.col("uploaded_at"),
            )
            .filter(
                pl.col("pi_id").is_not_null()
                & (pl.col("pi_id") != "")
                & pl.col("image_path").str.starts_with("abfss://")
            )
            .sort("uploaded_at", nulls_last=False)
            .unique(subset=["pi_id"], keep="last")
            .collect()
        )
    except Exception:
        log.warning("Couldn't read the app image table; showing Odoo images only", exc_info=True)
        return empty
    return uploads.to_pandas() if uploads.height else empty


def apply_app_images(df: pd.DataFrame, uploads: pd.DataFrame) -> pd.DataFrame:
    """image_1920 = the lot's app image where it has one, else its Odoo image.
    Keeps "products with images first", so a lot that only got its image in
    the app moves up with the others (the rest of the order is unchanged)."""
    if uploads.empty or "pi_ids" not in df.columns:
        return df
    # Each stock row lists its lot's product_images ids ("123" or "123,456").
    row_ids = df["pi_ids"].astype(str).str.split(",").explode()
    row_ids = row_ids[row_ids.isin(set(uploads["pi_id"]))]
    if row_ids.empty:
        return df
    matches = (
        pd.DataFrame({"row": row_ids.index, "pi_id": row_ids.to_numpy()})
        .merge(uploads, on="pi_id", how="inner")
        .sort_values("uploaded_at", kind="stable")
        .drop_duplicates("row", keep="last")         # newest upload if several rows of the lot have one
    )
    df = df.copy()
    df.loc[matches["row"].to_numpy(), "image_1920"] = matches["image_path"].to_numpy()
    no_image = (df["image_1920"] == NO_IMAGE).to_numpy()
    return df.iloc[no_image.argsort(kind="stable")].reset_index(drop=True)


def compute_filter_options(df: pd.DataFrame) -> dict:
    """Option lists for every filter dropdown, computed once per data build
    and shared by all sessions."""
    company_values = [
        value for value in sorted(df["company"].dropna().unique())
        if value not in EXCLUDED_COMPANIES
    ]

    pairs = df[["company", "location"]].dropna(subset=["location"]).drop_duplicates()
    pairs = pairs[~pairs["location"].map(location_is_excluded)]
    location_by_company = {
        company: sorted(group["location"].unique())
        for company, group in pairs.dropna(subset=["company"]).groupby("company")
    }

    category_values = sorted({
        value
        for raw in df["category"].dropna().unique()          # distinct values only
        for value in expand_category_hierarchy(raw)
        if not str(value).strip().upper().startswith(EXCLUDED_CATEGORY_PREFIX)
    })

    return {
        "company": company_values,
        "category": category_values,
        "vendor": sorted(df["vendor"].dropna().unique()),
        "location": sorted(pairs["location"].unique()),
        "location_by_company": location_by_company,
    }


# ===========================================================
# Shared data snapshot (stale-while-revalidate)
# ===========================================================
@dataclass(frozen=True, eq=False)
class GallerySnapshot:
    df: pd.DataFrame          # inventory with app images applied - READ-ONLY
    base_df: pd.DataFrame     # inventory as built from Fabric - READ-ONLY
    options: dict
    version: tuple            # source Delta versions the build was made from
    built_at: float
    images_at: float
    checked_at: float


class _DataStore:
    def __init__(self):
        self.lock = threading.Lock()
        self.snapshot = None
        # App-uploaded images are read on their own thread so a slow read of
        # that table never holds up the gallery (see refresh_data).
        self.uploads_pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="app-images")
        self.uploads_job = None          # Future of the read in progress
        self.uploads_df = None           # last uploads table that was read
        self.uploads_version = None      # its Delta version


@st.cache_resource(show_spinner=False)
def data_store() -> _DataStore:
    # One store per app process, shared by every session and the background
    # thread (module globals don't work: Streamlit re-executes this file on
    # every rerun).
    return _DataStore()


def _timed(timings: dict, name: str, fn, *args):
    """Runs fn(*args) and records how long it took (shown in the refresh log)."""
    started = time.perf_counter()
    try:
        return fn(*args)
    finally:
        timings[name] = time.perf_counter() - started


def _read_uploads(store: _DataStore):
    """(uploads DataFrame, version, changed). Skips the read when the table's
    Delta version is the same as last time."""
    started = time.perf_counter()
    try:
        version = delta_version(table_uri(lakehouse_id_silver, "Odoo", app_images_table_name()))
    except Exception:
        version = None
    if version is not None and version == store.uploads_version and store.uploads_df is not None:
        return store.uploads_df, version, False
    uploads = load_app_images()
    log.info("app image table read: %d uploads in %.1fs", len(uploads), time.perf_counter() - started)
    return uploads, version, True


def _start_uploads_read(store: _DataStore):
    if store.uploads_job is None:
        store.uploads_job = store.uploads_pool.submit(_read_uploads, store)


def _take_uploads(store: _DataStore, wait: float = 0):
    """The finished uploads read as (DataFrame, changed), or None if it is
    still running (waits up to `wait` seconds for it)."""
    job = store.uploads_job
    if job is None:
        return None
    try:
        uploads, version, changed = job.result(timeout=wait)
    except FutureTimeout:
        return None
    except Exception:
        log.warning("App image table read failed", exc_info=True)
        uploads, version, changed = store.uploads_df, store.uploads_version, False
    store.uploads_job = None
    if uploads is None:
        return None
    store.uploads_df, store.uploads_version = uploads, version
    return uploads, changed


def refresh_data() -> GallerySnapshot:
    """Brings the shared snapshot up to date and returns it. Only one thread
    rebuilds at a time; readers keep using the previous snapshot meanwhile."""
    store = data_store()
    with store.lock:
        snap = store.snapshot
        now = time.time()

        inventory_due = snap is None or now - snap.built_at >= DATA_TTL_SECONDS
        version = snap.version if snap is not None else None
        checked_at = snap.checked_at if snap is not None else 0.0

        if not inventory_due and now - checked_at >= VERSION_CHECK_SECONDS:
            checked_at = now
            latest = source_version()
            if latest is not None and latest != snap.version:
                log.info("source tables changed %s -> %s; rebuilding", snap.version, latest)
                inventory_due = True

        # App uploads: re-read every APP_IMAGES_TTL_SECONDS in the background;
        # a finished read is applied on the next pass (at most a minute later).
        if snap is None or now - snap.images_at >= APP_IMAGES_TTL_SECONDS:
            _start_uploads_read(store)
        taken = _take_uploads(store)

        if not inventory_due and taken is None:
            if snap.checked_at != checked_at:
                store.snapshot = replace(snap, checked_at=checked_at)
            return store.snapshot
        if not inventory_due and not taken[1]:
            # Uploads table unchanged: nothing to re-apply.
            store.snapshot = replace(snap, images_at=now, checked_at=checked_at)
            return store.snapshot

        base_df = options = None
        built_at = now
        timings = {}
        started = time.perf_counter()
        if inventory_due:
            # The version read runs alongside the build and starts with it, so
            # if the ETL writes during the build the next check sees a newer
            # version and rebuilds again.
            with ThreadPoolExecutor(max_workers=1, thread_name_prefix="gallery-version") as ex:
                version_job = ex.submit(_timed, timings, "version check", source_version)
                try:
                    base_df = _timed(timings, "inventory", build_base_inventory)
                    options = _timed(timings, "filter options", compute_filter_options, base_df)
                    version = version_job.result() or ()
                    checked_at = now
                except Exception:
                    if snap is None:
                        raise
                    log.warning("Inventory refresh failed; still serving the previous data", exc_info=True)
            if taken is None and store.uploads_df is None:
                # First load: give the uploads table a few more seconds, but
                # never let it hold up the gallery.
                taken = _timed(timings, "waiting for app image table", _take_uploads,
                               store, APP_IMAGES_WAIT_SECONDS)
        if base_df is None:
            base_df, options, built_at = snap.base_df, snap.options, snap.built_at

        if taken is not None:
            uploads, images_at = taken[0], now
        else:
            uploads = store.uploads_df          # last known uploads (None on a cold start)
            images_at = snap.images_at if snap is not None else 0.0
            if uploads is None:
                log.warning(
                    "App image table is still loading; showing Odoo images until it is ready. "
                    "If this keeps happening, compact the table (see build_product_gallery_table.py)."
                )
        df = (_timed(timings, "apply app images", apply_app_images, base_df, uploads)
              if uploads is not None else base_df)
        log.info(
            "data refresh done in %.1fs (%s)", time.perf_counter() - started,
            ", ".join(f"{name} {secs:.1f}s" for name, secs in timings.items()),
        )
        store.snapshot = GallerySnapshot(
            df=df, base_df=base_df, options=options, version=version,
            built_at=built_at, images_at=images_at, checked_at=checked_at,
        )
        return store.snapshot


def get_snapshot() -> GallerySnapshot:
    """Instant once anything is loaded; refreshes happen in the background."""
    snap = data_store().snapshot
    return snap if snap is not None else refresh_data()


# The background thread uses Streamlit's caches outside a page run, which makes
# Streamlit log a harmless "missing ScriptRunContext" warning each time.
logging.getLogger("streamlit.runtime.scriptrunner_utils.script_run_context").setLevel(logging.ERROR)


@st.cache_resource
def start_background_refresh() -> bool:
    """One refresh thread per app process, started on the first visit (the
    login page), so data loads while people sign in and later rebuilds never
    block a user."""
    if GALLERY_TABLE:
        log.info("data source: pre-joined table %s.%s", GALLERY_TABLE_SCHEMA, GALLERY_TABLE)
    else:
        log.warning(
            "data source: GALLERY_TABLE is not set in secrets, so the app joins the five "
            "Odoo tables itself (slow). Add GALLERY_TABLE = \"product_gallery_inventory\"."
        )

    def loop():
        while True:
            try:
                snap = refresh_data()
                refs = snap.df["image_1920"].iloc[:PAGE_SIZE * WARMUP_PREFETCH_PAGES].tolist()
                if refs:
                    get_card_images(refs)            # waits until loaded
            except Exception:
                log.warning("Background refresh failed; will retry", exc_info=True)
            time.sleep(REFRESH_CHECK_SECONDS)

    threading.Thread(target=loop, name="gallery-refresh", daemon=True).start()
    return True


# ===========================================================
# Filtering (vectorised - no per-row Python functions)
# ===========================================================
FILTER_KEYS = ("company", "category", "vendor", "location", "search",
               "lot_number", "in_stock_only", "without_images")


def apply_filters(df, f):
    mask = pd.Series(True, index=df.index)
    if f["company"]:
        mask &= df["company"].isin(f["company"])
    if f["category"]:
        selected = [s for s in (normalize_category_value(v) for v in f["category"]) if s]
        category_mask = pd.Series(False, index=df.index)
        if selected:
            cat = df["_cat_norm"]
            category_mask = cat.isin(selected)
            for value in selected:      # a category also matches its sub-categories
                category_mask |= cat.str.startswith(value + " / ")
        mask &= category_mask
    if f["vendor"]:
        mask &= df["vendor"].isin(f["vendor"])
    if f["location"]:
        mask &= df["location"].isin(f["location"])
    needle = (f["search"] or "").strip().lower()
    if needle:
        mask &= df["_search"].str.contains(needle, regex=False)
    lot_needle = (f["lot_number"] or "").strip().lower()
    if lot_needle:
        mask &= df["_lot_lc"].str.contains(lot_needle, regex=False)
    if f["in_stock_only"]:
        mask &= df["available_inventory"].fillna(0) > 0
    if f["without_images"]:
        mask &= df["image_1920"] == NO_IMAGE
    return df[mask]


def render_filters(options: dict, panel_key: str = "filter_panel") -> dict:
    filter_state = st.session_state.setdefault("filter_state", {
        "company": [],
        "category": [],
        "vendor": [],
        "location": [],
        "search": "",
        "lot_number": "",
        "in_stock_only": False,
        "without_images": False,
        "cols_per_row": 4,
    })

    with st.container(key=panel_key):
        if panel_key == "mobile_filter_panel":
            head_col, close_col = st.columns([5, 1])
            with head_col:
                st.markdown("<h3>Filters</h3>", unsafe_allow_html=True)
            with close_col:
                if st.button("✕", key="mobile_filter_close_x", help="Close filters"):
                    st.session_state.mobile_filters_open = False
                    st.rerun()
        else:
            st.markdown("<h3>Filters</h3>", unsafe_allow_html=True)

        company = st.multiselect(
            "Company",
            options["company"],
            default=[v for v in filter_state.get("company", []) if v in options["company"]],
            key="filters_company",
        )
        filter_state["company"] = company

        # --- Category with shortened display labels ---
        category_options = options["category"]

        def make_display_label(full_path):
            if not full_path:
                return full_path
            parts = full_path.split(" / ")
            if len(parts) <= 2:
                return full_path
            return "… / " + " / ".join(parts[-2:])

        display_map = {cat: make_display_label(cat) for cat in category_options}
        display_options = [display_map[cat] for cat in category_options]
        current_selected = filter_state.get("category", [])
        selected_display = [display_map.get(cat, cat) for cat in current_selected if cat in display_map]

        selected_display_labels = st.multiselect(
            "Category",
            display_options,
            default=selected_display,
            key=f"filters_category_{panel_key}",
            format_func=lambda x: x,
        )

        reverse_map = {}
        for full_path, display_label in display_map.items():
            reverse_map.setdefault(display_label, []).append(full_path)

        selected_full_paths = []
        for display_label in selected_display_labels:
            matches = reverse_map.get(display_label, [])
            if matches:
                selected_full_paths.append(matches[0])
            elif display_label in category_options:
                selected_full_paths.append(display_label)

        for cat in current_selected:
            if (cat not in selected_full_paths and cat in category_options
                    and display_map.get(cat) not in selected_display_labels):
                selected_full_paths.append(cat)

        selected_full_paths = list(dict.fromkeys(selected_full_paths))
        filter_state["category"] = selected_full_paths

        vendor = st.multiselect(
            "Vendor",
            options["vendor"],
            default=[v for v in filter_state.get("vendor", []) if v in options["vendor"]],
            key="filters_vendor",
        )
        filter_state["vendor"] = vendor

        if company:
            by_company = options["location_by_company"]
            location_options = sorted(set().union(*(by_company.get(c, []) for c in company)))
        else:
            location_options = options["location"]
        filter_state["location"] = [
            value for value in filter_state.get("location", [])
            if value in location_options
        ]
        location = st.multiselect(
            "Location",
            location_options,
            default=filter_state.get("location", []),
            key="filters_location",
        )
        filter_state["location"] = location

        search = st.text_input("Product/SKU", value=filter_state.get("search", ""), key="filters_search")
        filter_state["search"] = search

        lot_number = st.text_input("Lot Number", value=filter_state.get("lot_number", ""), key="filters_lot")
        filter_state["lot_number"] = lot_number

        in_stock_only = st.checkbox(
            "In stock only",
            value=filter_state.get("in_stock_only", False),
            key="filters_stock",
        )
        filter_state["in_stock_only"] = in_stock_only

        without_images = st.checkbox(
            "Without images",
            value=filter_state.get("without_images", False),
            key="filters_without_images",
        )
        filter_state["without_images"] = without_images

        cols_per_row = st.slider(
            "Columns per row",
            min_value=2,
            max_value=6,
            value=filter_state.get("cols_per_row", 4),
            key="filters_cols",
        )
        filter_state["cols_per_row"] = cols_per_row

        st.markdown("<br>", unsafe_allow_html=True)

        if panel_key == "mobile_filter_panel":
            if st.button("✅ Apply Filters", key="mobile_filter_apply", width="stretch"):
                st.session_state.mobile_filters_open = False
                st.rerun()

        if st.button("🚪 Logout", key=f"{panel_key}_logout", width="stretch"):
            set_authenticated(False)
            st.rerun()

    return {
        "company": company,
        "category": filter_state["category"],
        "vendor": vendor,
        "location": location,
        "search": search,
        "lot_number": lot_number,
        "in_stock_only": in_stock_only,
        "without_images": without_images,
        "cols_per_row": cols_per_row,
    }


def reset_page_if_filters_changed(filters: dict):
    """Go back to page 1 when the filters change (not on paging/column changes)."""
    key = tuple(
        tuple(filters[k]) if isinstance(filters[k], list) else filters[k]
        for k in FILTER_KEYS
    )
    if st.session_state.get("_filter_key") != key:
        st.session_state._filter_key = key
        st.session_state.page = 1


# ===========================================================
# Card rendering
# ===========================================================
def _fmt_money(v):
    if pd.isna(v):
        return "—"
    return f"₹{v:,.0f}"


def _fmt(v):
    if pd.isna(v) or v == "":
        return "—"
    return html_lib.escape(str(v))


def image_to_data_uri(img: Image.Image) -> str:
    buf = BytesIO()
    fmt = (img.format or "JPEG").upper()
    if fmt not in ("JPEG", "PNG", "WEBP"):
        fmt = "JPEG"
    save_img = img.convert("RGB") if fmt == "JPEG" else img
    save_img.save(buf, format=fmt)
    b64 = base64.b64encode(buf.getvalue()).decode("ascii")
    return f"data:image/{fmt.lower()};base64,{b64}"


def render_card_html(item, img, loading: bool = False) -> str:
    if isinstance(img, str):                       # cached card thumbnail (data URI)
        img_html = f'<img src="{img}" alt="" />'
    elif img is not None:
        img_html = f'<img src="{image_to_data_uri(img)}" alt="" />'
    elif loading:
        img_html = '<div class="product-card-noimg">Loading image…</div>'
    else:
        img_html = '<div class="product-card-noimg">No image<br>available</div>'

    image_ref = item.get("image_1920")
    if isinstance(image_ref, str) and image_ref in ("", "False", "None"):
        img_html = '<div class="product-card-noimg">No image<br>available</div>'

    qty = item.get("available_inventory")
    in_stock = pd.notna(qty) and qty > 0
    badge_class = "stock-in" if in_stock else "stock-out"
    badge_text = f"In stock · {int(qty)}" if in_stock else "Out of stock"

    return f"""
    <div class="product-card">
        <div class="product-card-img">{img_html}</div>
        <div class="product-card-body">
            <div class="product-card-title">{_fmt(item.get('product_name'))}</div>
            <div class="product-card-meta">{_fmt(item.get('category'))} · {_fmt(item.get('vendor'))}</div>
            <div class="product-card-row"><span>SKU</span><span>{_fmt(item.get('sku'))}</span></div>
            <div class="product-card-row"><span>Lot No.</span><span>{_fmt(item.get('lot_number'))}</span></div>
            <div class="product-card-row"><span>Location</span><span>{_fmt(item.get('location'))}</span></div>
            <div class="product-card-row"><span>Age (days)</span><span>{_fmt(item.get('overall_age'))}</span></div>
            <div class="product-card-row"><span>Stock value</span><span>{_fmt_money(item.get('available_selling_price'))}</span></div>
            <div class="product-card-footer">
                <div class="price-block">
                    <div class="sp">{_fmt_money(item.get('sp'))}</div>
                    <div class="cp">Cost: {_fmt_money(item.get('cp'))}</div>
                </div>
                <div class="stock-badge {badge_class}">{badge_text}</div>
            </div>
        </div>
    </div>
    """


def _change_page(delta: int):
    st.session_state.page = st.session_state.get("page", 1) + delta


@st.fragment
def render_results(df: pd.DataFrame, cols_per_row: int):
    """Product grid + pager. A fragment: Previous/Next re-run only this part,
    not the filters, data lookup and the rest of the page."""
    total = len(df)
    total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
    page = min(max(1, st.session_state.get("page", 1)), total_pages)
    st.session_state.page = page

    start = (page - 1) * PAGE_SIZE
    end = start + PAGE_SIZE
    page_df = df.iloc[start:end]

    refs = page_df["image_1920"].tolist()
    images_started = time.perf_counter()
    with st.spinner("Loading images..."):
        image_lookup, pending = get_card_images(refs, timeout=PAGE_IMAGE_WAIT_SECONDS)
    log.info("page %d: %d cards, images ready in %.1fs%s", page, len(refs),
             time.perf_counter() - images_started,
             f" ({len(pending)} still loading, shown as they arrive)" if pending else "")

    st.markdown(
        f"<div style='color: var(--muted-text); font-size:13px; margin-bottom:8px;'>"
        f"{total} product{'s' if total != 1 else ''} found</div>",
        unsafe_allow_html=True,
    )
    status = st.empty()

    if total == 0:
        st.info("No products match the current filters.")
    else:
        for row_start in range(0, len(page_df), cols_per_row):
            row_items = page_df.iloc[row_start:row_start + cols_per_row]
            cols = st.columns(cols_per_row)
            for col, (_, item) in zip(cols, row_items.iterrows()):
                with col:
                    img = image_lookup.get(item["image_1920"])
                    if DEBUG:
                        st.caption(str(item["image_1920"]))
                    loading = item["image_1920"] in pending
                    st.markdown(render_card_html(item, img, loading), unsafe_allow_html=True)

    st.divider()
    c1, c2, c3 = st.columns([1, 2, 1])
    with c1:
        st.button("⬅ Previous", key="page_prev", disabled=page <= 1,
                  on_click=_change_page, args=(-1,))
    with c2:
        st.markdown(
            f"<div class='page-indicator'>Page {page} of {total_pages} "
            f"({total} products)</div>",
            unsafe_allow_html=True,
        )
    with c3:
        st.button("Next ➡", key="page_next", disabled=page >= total_pages,
                  on_click=_change_page, args=(1,))

    # Load the next page's images in the background so "Next" is instant.
    prefetch_images(df["image_1920"].iloc[end:end + PAGE_SIZE].tolist())

    if pending:
        _show_images_as_they_arrive(status, pending)


def _show_images_as_they_arrive(status, pending: dict):
    """The page is already on screen with placeholders for slow images. Wait
    for them here and redraw the grid once they are in. Updating the status
    line every half second also lets Streamlit stop this wait as soon as the
    user clicks something."""
    futures = list(pending.values())
    deadline = time.monotonic() + PENDING_IMAGE_MAX_WAIT_SECONDS
    while time.monotonic() < deadline:
        left = sum(not f.done() for f in futures)
        if not left:
            break
        status.caption(f"Loading {left} more image{'s' if left != 1 else ''}…")
        wait_futures(futures, timeout=0.5)
    status.empty()
    if not any(f.done() for f in futures):
        return
    try:
        st.rerun(scope="fragment")
    except StreamlitAPIException:
        st.rerun()          # first render is part of a full page run, where a fragment rerun isn't allowed


# ===========================================================
# Main
# ===========================================================
def main():
    run_started = time.perf_counter()
    inject_theme()

    if not MISSING_CONFIG:
        start_background_refresh()          # data loads while the user signs in

    if not check_login():
        return

    render_header()

    if MISSING_CONFIG:
        st.error(
            "Fabric connection details are missing. Add them under **App settings → "
            "Secrets** (or `.streamlit/secrets.toml` locally) — see `secrets.toml.example` "
            "for the required keys."
        )
        return

    try:
        snap = data_store().snapshot
        if snap is None:
            # Only after an app (re)start, before the first build has finished.
            with st.spinner("Loading your product gallery..."):
                snap = get_snapshot()
    except Exception as e:
        st.error(f"Failed to connect / fetch data: {e}")
        return

    full_df = snap.df
    filter_options = snap.options

    if "mobile_filters_open" not in st.session_state:
        st.session_state.mobile_filters_open = False

    if not st.session_state.mobile_filters_open:
        if st.button("☰", key="mobile_filters_toggle", help="Open filters"):
            st.session_state.mobile_filters_open = True
            st.rerun()

    if st.session_state.mobile_filters_open:
        filters = render_filters(filter_options, "mobile_filter_panel")
        reset_page_if_filters_changed(filters)
        with st.container():
            render_results(apply_filters(full_df, filters), filters["cols_per_row"])
    else:
        filter_col, results_col = st.columns([1.25, 3])
        with filter_col:
            filters = render_filters(filter_options, "filter_panel")
        reset_page_if_filters_changed(filters)
        with results_col:
            render_results(apply_filters(full_df, filters), filters["cols_per_row"])

    # Time the server spent building this page. If the browser takes much
    # longer than this to show it, the delay is in the browser / network.
    log.info("page built on the server in %.1fs", time.perf_counter() - run_started)


if __name__ == "__main__":
    main()
