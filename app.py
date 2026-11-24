import sys
import asyncio

if sys.platform.startswith("win") and sys.version_info < (3, 14):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

import atexit
import base64
import faulthandler
import json
import os
import re
import hashlib
import hmac
import inspect
import logging
import threading
import time
import uuid
import zipfile
import html as html_lib
from io import BytesIO
from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo
from dataclasses import dataclass, replace
from urllib.parse import unquote, urlparse
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, wait as wait_futures

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
IMAGE_CACHE_TTL_SECONDS = 60 * 60 * 24

# ---------------------------------------------------------------
# Data freshness - the gallery refreshes whenever something changes
# ---------------------------------------------------------------
# One background thread checks every CHANGE_CHECK_SECONDS whether any source
# table has a new commit. That check is one tiny "does the next log file
# exist?" request per table - it never opens the table - so it is cheap even
# for tables with a very long history. Only a table that changed is re-read:
#   gallery table / Odoo tables (new or updated records) -> full rebuild
#   product_images (Odoo images, photo deletes)          -> lot images re-read
#   product_image_overrides (upload app)                 -> just the new uploads read
#   product_image_deletions (photo delete, any computer) -> deletions re-read
# Users keep seeing the last good data while a refresh runs, and open pages
# redraw by themselves within LIVE_UPDATE_SECONDS.
CHANGE_CHECK_SECONDS = 5
LIVE_UPDATE_SECONDS = 5
FULL_REBUILD_SECONDS = 60 * 60 * 24     # safety net: a full rebuild at least once a day
HEARTBEAT_SECONDS = 5 * 60              # "still running" line (memory, rows) in the terminal
HIDE_GRACE_SECONDS = 60                 # a deleted photo stays hidden this long after it was saved
INCREMENTAL_MAX_COMMITS = 200           # more new upload commits than this -> re-read the whole table
FIRST_LOAD_TIMEOUT_SECONDS = 15 * 60
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
# Crash diagnostics - everything goes to the terminal the app runs in
#   * a native crash (in polars / deltalake / Azure code) prints the Python
#     stack of every thread (faulthandler) instead of the app just vanishing
#   * an error in a background thread is logged with its traceback
#   * a normal shutdown logs "app process exiting" - if the app disappears
#     WITHOUT that line and without a faulthandler dump, the process was
#     killed from outside (e.g. out of memory)
#   * a heartbeat every HEARTBEAT_SECONDS logs memory use and data size
# ===========================================================
def _process_memory_mb():
    try:
        import psutil
        return psutil.Process().memory_info().rss / 2**20
    except Exception:
        pass
    if sys.platform.startswith("win"):
        try:
            import ctypes
            from ctypes import wintypes

            class _Counters(ctypes.Structure):
                _fields_ = [("cb", wintypes.DWORD), ("PageFaultCount", wintypes.DWORD),
                            ("PeakWorkingSetSize", ctypes.c_size_t), ("WorkingSetSize", ctypes.c_size_t),
                            ("QuotaPeakPagedPoolUsage", ctypes.c_size_t), ("QuotaPagedPoolUsage", ctypes.c_size_t),
                            ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                            ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                            ("PagefileUsage", ctypes.c_size_t), ("PeakPagefileUsage", ctypes.c_size_t)]

            counters = _Counters()
            counters.cb = ctypes.sizeof(_Counters)
            kernel32 = ctypes.windll.kernel32
            kernel32.GetCurrentProcess.restype = wintypes.HANDLE
            ctypes.windll.psapi.GetProcessMemoryInfo(kernel32.GetCurrentProcess(), ctypes.byref(counters),
                                                     counters.cb)
            return counters.WorkingSetSize / 2**20
        except Exception:
            return None
    try:
        import resource
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024     # peak, Linux
    except Exception:
        return None


def _memory_text() -> str:
    mb = _process_memory_mb()
    return f"{mb:,.0f} MB" if mb is not None else "n/a"


if not getattr(sys, "_product_gallery_diagnostics", False):    # this file re-runs on every click
    sys._product_gallery_diagnostics = True
    faulthandler.enable(all_threads=True)

    def _log_thread_exception(args):
        log.error("Uncaught error in thread %s", getattr(args.thread, "name", "?"),
                  exc_info=(args.exc_type, args.exc_value, args.exc_traceback))

    threading.excepthook = _log_thread_exception
    atexit.register(lambda: log.info("app process exiting normally (memory %s)", _memory_text()))
    log.info("app process started - pid %s, Python %s, polars %s, memory %s",
             os.getpid(), sys.version.split()[0], pl.__version__, _memory_text())

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

# Same list as the lot-coverage query. The pre-joined gallery table may have been
# built with an older list, so load_inventory_rows re-applies this one.
EXCLUDED_COMPANIES = {
    "Saree Trails",
    "Wedtree eStore Private Limited - Online",
    "Wedtree Estore private Limited - Saree Trails",
}
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
        [class*="st-key-del_"] {{ margin-top: -14px; margin-bottom: 18px; }}
        [class*="st-key-del_"] button {{
            background: transparent;
            color: var(--danger-text);
            border: 1px solid var(--card-border);
            padding: 4px 12px;
            min-height: 0;
            font-size: 12.5px;
            font-weight: 600;
        }}
        [class*="st-key-del_"] button:hover {{
            background: var(--danger-bg);
            color: var(--danger-text);
            box-shadow: none;
        }}
        .stDownloadButton > button {{
            background-color: var(--brand-3);
            color: #ffffff;
            border: none;
            border-radius: 999px;
            padding: 8px 20px;
            font-weight: 600;
        }}
        .stDownloadButton > button:hover {{
            background-color: var(--brand-1);
            color: #ffffff;
            box-shadow: 0 3px 10px rgba(158,79,104,0.35);
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
    ttl = IMAGE_CACHE_TTL_SECONDS if result[0] is not None else 5 * 60
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
def _id_text(expr: pl.Expr) -> pl.Expr:
    """An id / product id as plain text, the way T-SQL CAST(... AS VARCHAR)
    writes it: 12345, never 12345.0. Delta tables store these ids as bigint in
    some places and as double/decimal or text in others; without this,
    "12345" and "12345.0" would never match."""
    return expr.cast(pl.Utf8).str.strip_chars().str.replace(r"\.0+$", "")


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
        _id_text(pl.col("id")).alias("pi_id"),
        _id_text(pl.col("product_id")).alias("product_id"),
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


# ===========================================================
# Change detection - plain HTTP requests, the Delta library isn't involved.
# A Delta table's history is a folder of numbered commit files
# (_delta_log/00000000000000000042.json). "Has the table changed since
# version N?" is simply "does file N+1 exist?" - one small request, however
# long the history is.
# ===========================================================
def _log_location(schema: str, table: str):
    return workspace_id, f"{lakehouse_id_silver}/Tables/{schema}/{table}/_delta_log"


def _commit_exists(location, version: int) -> bool:
    filesystem, log_dir = location
    try:
        _file_client(get_datalake_service_client(), filesystem,
                     f"{log_dir}/{version:020d}.json").get_file_properties()
        return True
    except ResourceNotFoundError:
        return False


def _last_checkpoint_version(location):
    filesystem, log_dir = location
    try:
        raw = _file_client(get_datalake_service_client(), filesystem,
                           f"{log_dir}/_last_checkpoint").download_file().readall()
        return int(json.loads(raw)["version"])
    except ResourceNotFoundError:
        return None


def latest_version(location, known=None):
    """Latest commit version of a Delta table, or None if the table doesn't
    exist (yet). With `known` - the version seen last time - an unchanged
    table costs exactly one request."""
    if known is not None:
        if not _commit_exists(location, known + 1):
            return known
        low = known + 1
    else:
        low = _last_checkpoint_version(location)      # a version that certainly exists
        if low is None:
            if not _commit_exists(location, 0):
                return None
            low = 0
    step = 1                                          # gallop forward, then halve
    while _commit_exists(location, low + step):
        low += step
        step *= 2
    high = low + step
    while high - low > 1:
        mid = (low + high) // 2
        if _commit_exists(location, mid):
            low = mid
        else:
            high = mid
    return low


def watched_tables() -> dict:
    """name -> (kind of change, log location)."""
    tables = {}
    if GALLERY_TABLE:
        tables[GALLERY_TABLE] = ("inventory", _log_location(GALLERY_TABLE_SCHEMA, GALLERY_TABLE))
    else:
        for name in ODOO_SOURCE_TABLES:
            if name != "product_images":
                tables[name] = ("inventory", _log_location("Odoo", name))
    tables["product_images"] = ("images", _log_location("Odoo", "product_images"))
    tables["uploads"] = ("uploads", _log_location("Odoo", app_images_table_name()))
    tables["deletions"] = ("deletions", _log_location("Odoo", deletions_table_name()))
    return tables


# ===========================================================
# Reading the data
# ===========================================================
def load_inventory_rows() -> pd.DataFrame:
    """The stock rows (no images yet) with the helper columns the filters use
    (computed once here instead of on every click)."""
    plan = _inventory_plan_from_gold() if GALLERY_TABLE else _inventory_plan_from_odoo()

    # Re-apply the company exclusions: the gallery table may have been built
    # with an older EXCLUDED_COMPANIES list. Only the companies in
    # COMPANY_LABELS are shown anywhere in the app (cards, filters, counts,
    # Excel export) - add a company there to show it.
    plan = plan.filter(~pl.col("company").is_in(sorted(EXCLUDED_COMPANIES)))
    plan = plan.with_columns(pl.col("company").replace(COMPANY_LABELS)).filter(
        pl.col("company").is_in(sorted(set(COMPANY_LABELS.values())))
    )

    lot_text = pl.col("lot_number").cast(pl.Utf8).fill_null("").str.strip_chars()
    product_text = _id_text(pl.col("product_id")).fill_null("")
    plan = plan.with_columns(
        pl.col("stock_move_date").cast(pl.Date, strict=False),
    ).with_columns(
        # The lot across companies (product + lot): the key a lot's image is
        # decided by, as in the lot-coverage query. No lot number -> no key.
        pl.when(lot_text == "").then(None)
        .otherwise(pl.concat_str([product_text, lot_text.str.to_uppercase()], separator="|"))
        .alias("_img_key"),
        # One key per lot for counting (company + product + lot number).
        pl.when(lot_text == "").then(None)
        .otherwise(pl.concat_str([pl.col("company").cast(pl.Utf8).fill_null(""), product_text,
                                  lot_text.str.to_uppercase()], separator="|"))
        .alias("_lot_count_key"),
        (pl.lit(date.today()) - pl.col("stock_move_date")).dt.total_days().alias("overall_age"),
        # Lower-cased search text, so a search is one vectorised .contains()
        pl.concat_str([pl.col("product_name").cast(pl.Utf8).fill_null("").str.to_lowercase(),
                       pl.col("sku").cast(pl.Utf8).fill_null("").str.to_lowercase()],
                      separator="\n").alias("_search"),
        pl.col("lot_number").cast(pl.Utf8).fill_null("").str.to_lowercase().alias("_lot_lc"),
    ).drop(["stock_move_date", "image_1920", "pi_ids"])

    rows = plan.collect(engine="streaming").to_pandas()
    # Normalised category for the category filter - per DISTINCT category.
    mapping = {value: normalize_category_value(value) for value in rows["category"].dropna().unique()}
    rows["_cat_norm"] = rows["category"].map(mapping).fillna("")
    return rows


def product_images_lots() -> pl.LazyFrame:
    """Odoo.product_images rows, same rules as the lot-coverage query:
      pi_id       the product_images id as text (12345, never 12345.0)
      _raw_id     the id exactly as stored - used to address rows when writing
      _img_key    product_id|UPPER(TRIM(name)) - the lot, across companies
      odoo_image  the Odoo image path, or null when blank/False/None/null"""
    lot_name = pl.col("name").cast(pl.Utf8).str.strip_chars()
    return (
        _scan(lakehouse_id_silver, "Odoo", "product_images", ["id", "name", "product_id", "image_1920"])
        .filter(pl.col("id").is_not_null() & lot_name.is_not_null() & (lot_name != ""))
        .select(
            _id_text(pl.col("id")).alias("pi_id"),
            pl.col("id").cast(pl.Utf8).alias("_raw_id"),
            pl.concat_str([_id_text(pl.col("product_id")), lot_name.str.to_uppercase()],
                          separator="|").alias("_img_key"),
            pl.col("image_1920").cast(pl.Utf8).str.strip_chars().alias("odoo_image"),
        )
        .filter(pl.col("_img_key").is_not_null())
        .with_columns(
            pl.when(pl.col("odoo_image").is_null()
                    | pl.col("odoo_image").str.to_lowercase().is_in(["", "false", "none", "null"]))
            .then(None).otherwise(pl.col("odoo_image")).alias("odoo_image")
        )
    )


def load_product_images_index() -> pl.DataFrame:
    return product_images_lots().collect()


SORT_COLUMNS = ["_no_image", "company", "category", "vendor", "sku", "lot_number", "location"]


def attach_odoo_images(rows: pd.DataFrame, pi_index: pl.DataFrame, deleted_ids):
    """(base_df, lot_ids, shared_paths).
    base_df: the rows with image_1920 = the lot's Odoo image (or NO_IMAGE),
        products with images first. Photos deleted in the app never count.
    lot_ids: every product_images id of the lots shown (pi_id, _raw_id,
        _img_key, odoo_image) - matches app uploads to lots, and photo delete
        uses it so it needn't read product_images again.
    shared_paths: Odoo image path -> ids, for paths more than one id uses
        (photo delete keeps those files)."""
    if deleted_ids:
        pi_index = pi_index.with_columns(
            pl.when(pl.col("pi_id").is_in(sorted(deleted_ids))).then(None)
            .otherwise(pl.col("odoo_image")).alias("odoo_image"))
    keys = pl.Series("_img_key", rows["_img_key"].dropna().unique().tolist(), dtype=pl.Utf8)
    lots = pi_index.filter(pl.col("_img_key").is_in(keys)).unique(subset=["pi_id"], keep="first")
    by_lot = (lots.filter(pl.col("odoo_image").is_not_null()).sort("pi_id")
              .group_by("_img_key").agg(pl.col("odoo_image").first()))
    image_of = pd.Series(by_lot["odoo_image"].to_list(), index=by_lot["_img_key"].to_list(), dtype=object)

    base = rows.copy()
    base["image_1920"] = base["_img_key"].map(image_of).fillna(NO_IMAGE)
    base["_no_image"] = base["image_1920"] == NO_IMAGE
    base = (base.sort_values(SORT_COLUMNS, kind="stable", na_position="last")
            .drop(columns="_no_image").reset_index(drop=True))

    shared = (pi_index.filter(pl.col("odoo_image").is_not_null())
              .group_by("odoo_image").agg(pl.col("pi_id").unique())
              .filter(pl.col("pi_id").list.len() > 1))
    shared_paths = {path: set(ids) for path, ids in shared.iter_rows()}
    lot_ids = lots.select("pi_id", "_raw_id", "_img_key", "odoo_image").to_pandas()
    return base, lot_ids, shared_paths


def _apply_logged_deletions(base_df: pd.DataFrame, lot_ids: pd.DataFrame, deleted_ids):
    """Removes the Odoo images of ids in the deletions log from base_df /
    lot_ids without re-reading anything (a lot keeps an Odoo image from one of
    its other ids if it has one)."""
    if not deleted_ids:
        return base_df, lot_ids
    hit = lot_ids["pi_id"].isin(list(deleted_ids)) & lot_ids["odoo_image"].notna()
    if not hit.any():
        return base_df, lot_ids
    lot_ids = lot_ids.copy()
    lot_ids.loc[hit, "odoo_image"] = None
    keys = set(lot_ids.loc[hit, "_img_key"])
    remaining = (lot_ids[lot_ids["_img_key"].isin(keys) & lot_ids["odoo_image"].notna()]
                 .sort_values("pi_id").groupby("_img_key")["odoo_image"].first())
    mask = base_df["_img_key"].isin(keys).to_numpy()
    base_df = base_df.copy()
    base_df.loc[mask, "image_1920"] = base_df.loc[mask, "_img_key"].map(remaining).fillna(NO_IMAGE).to_numpy()
    no_image = (base_df["image_1920"] == NO_IMAGE).to_numpy()
    return base_df.iloc[no_image.argsort(kind="stable")].reset_index(drop=True), lot_ids


# --- Images uploaded in the upload app (Odoo.product_image_overrides) -------
EMPTY_UPLOADS = pd.DataFrame({"pi_id": pd.Series(dtype=str), "image_path": pd.Series(dtype=str),
                              "uploaded_at": pd.Series(dtype="datetime64[us, UTC]")})


def _clean_uploads(lf: pl.LazyFrame) -> pl.LazyFrame:
    return (
        lf.select(
            _id_text(pl.col("id")).alias("pi_id"),
            pl.col("image_path").cast(pl.Utf8).str.strip_chars().alias("image_path"),
            pl.col("uploaded_at").cast(pl.Datetime("us", "UTC"), strict=False).alias("uploaded_at"),
        )
        .filter(pl.col("pi_id").is_not_null() & (pl.col("pi_id") != "")
                & pl.col("image_path").str.starts_with("abfss://"))
    )


def _newest_per_lot(uploads: pd.DataFrame) -> pd.DataFrame:
    return (uploads.sort_values("uploaded_at", kind="stable", na_position="first")
            .drop_duplicates("pi_id", keep="last").reset_index(drop=True))


def load_app_images() -> pd.DataFrame:
    """The whole uploads table: newest upload per product_images id."""
    uploads = _clean_uploads(pl.scan_delta(table_uri(lakehouse_id_silver, "Odoo", app_images_table_name()),
                                           storage_options=storage_options)).collect()
    return _newest_per_lot(uploads.to_pandas()) if uploads.height else EMPTY_UPLOADS.copy()


def read_new_uploads(location, from_version: int, to_version: int):
    """The rows added by commits from_version+1 .. to_version, read straight
    from the new data files - the rest of the table isn't touched. Returns
    None when a commit did more than add files (a delete, a compaction, a
    schema change, too many commits): then the whole table is re-read."""
    if to_version - from_version > INCREMENTAL_MAX_COMMITS:
        return None
    filesystem, log_dir = location
    table_dir = log_dir.rsplit("/_delta_log", 1)[0]
    client = get_datalake_service_client()
    paths = []
    for version in range(from_version + 1, to_version + 1):
        raw = _file_client(client, filesystem, f"{log_dir}/{version:020d}.json").download_file().readall()
        for line in raw.decode("utf-8").splitlines():
            if not line.strip():
                continue
            action = json.loads(line)
            if "add" in action:
                add = action["add"]
                if add.get("deletionVector") or add.get("partitionValues") or "://" in add["path"]:
                    return None
                paths.append(unquote(add["path"]))
            elif any(key in action for key in ("remove", "metaData", "protocol", "cdc")):
                return None
    if not paths:
        return EMPTY_UPLOADS.copy()
    frames = [pl.read_parquet(BytesIO(_file_client(client, filesystem, f"{table_dir}/{p}")
                                      .download_file().readall())) for p in paths]
    new = _clean_uploads(pl.concat(frames, how="diagonal_relaxed").lazy()).collect()
    return new.to_pandas() if new.height else EMPTY_UPLOADS.copy()


def load_deleted_ids() -> set:
    """product_images ids whose photo was deleted in the app (any computer)."""
    ids = (pl.scan_delta(table_uri(lakehouse_id_silver, "Odoo", deletions_table_name()),
                         storage_options=storage_options)
           .filter(pl.col("source") == "product_images")
           .select(_id_text(pl.col("pi_id")).alias("pi_id")).unique().collect())
    return set(ids["pi_id"].to_list())


def apply_app_images(df: pd.DataFrame, uploads: pd.DataFrame, lot_ids: pd.DataFrame):
    """image_1920 = the lot's app image where it has one, else its Odoo image
    (a lot has an app image if ANY of its product_images ids has an upload).
    Keeps "products with images first". Returns (df, lots with an app image)."""
    if uploads is None or uploads.empty or lot_ids is None or lot_ids.empty:
        return df, 0
    matches = (uploads.merge(lot_ids[["pi_id", "_img_key"]], on="pi_id", how="inner")
               .sort_values("uploaded_at", kind="stable", na_position="first")
               .drop_duplicates("_img_key", keep="last"))
    if matches.empty:
        return df, 0
    paths = pd.Series(matches["image_path"].to_numpy(), index=matches["_img_key"].to_numpy())
    new_image = df["_img_key"].map(paths)
    has_upload = new_image.notna().to_numpy()
    if not has_upload.any():
        return df, 0
    df = df.copy()
    df.loc[has_upload, "image_1920"] = new_image[has_upload].to_numpy()
    no_image = (df["image_1920"] == NO_IMAGE).to_numpy()
    return df.iloc[no_image.argsort(kind="stable")].reset_index(drop=True), len(matches)


def compute_filter_options(df: pd.DataFrame) -> dict:
    """Option lists for every filter dropdown, computed once per rebuild and
    shared by all sessions."""
    company_values = [value for value in sorted(df["company"].dropna().unique())
                      if value not in EXCLUDED_COMPANIES]
    pairs = df[["company", "location"]].dropna(subset=["location"]).drop_duplicates()
    pairs = pairs[~pairs["location"].map(location_is_excluded)]
    location_by_company = {
        company: sorted(group["location"].unique())
        for company, group in pairs.dropna(subset=["company"]).groupby("company")
    }
    category_values = sorted({
        value
        for raw in df["category"].dropna().unique()
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
# Shared data (one copy per app process, used by every session)
# ===========================================================
@dataclass(frozen=True, eq=False)
class GallerySnapshot:
    df: pd.DataFrame          # what the cards show (app uploads applied) - READ-ONLY
    base_df: pd.DataFrame     # same rows with Odoo images only - READ-ONLY
    lot_ids: pd.DataFrame     # product_images ids per lot (pi_id, _raw_id, _img_key, odoo_image)
    options: dict
    shared_paths: dict        # Odoo image paths used by more than one id
    uploads_info: dict
    built_at: float
    generation: int           # +1 whenever what the cards show changes


class _DataStore:
    def __init__(self):
        self.lock = threading.Lock()       # guards swapping the snapshot
        self.snapshot = None
        self.uploads_df = None             # newest upload per product_images id
        self.deleted_ids = None            # ids whose photo was deleted in the app
        self.versions = {}                 # table -> commit version last read
        self.last_error = None             # last refresh error (text), None once a refresh works
        # Photo deletes are saved to Fabric by one background worker, one at a time.
        self.delete_pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="photo-delete")
        self.hidden = {}                   # img_key -> {"pi_ids", "until"}: photos deleted, not shown
        self.pending_deletes = 0
        self.delete_notes = []             # (session id, message) for deletes that went wrong
        self.force_token = 0               # bumped when the tables must be re-read
        self.forced_done = 0


@st.cache_resource(show_spinner=False)
def data_store() -> _DataStore:
    return _DataStore()


def _publish(store: _DataStore, base_df, lot_ids, shared_paths, uploads, options=None, built_at=None):
    with store.lock:
        now = time.time()
        for key in [k for k, v in store.hidden.items() if v["until"] is not None and v["until"] <= now]:
            del store.hidden[key]
        hidden = {k: set(v["pi_ids"]) for k, v in store.hidden.items()}
    hidden_ids = set().union(*hidden.values()) if hidden else set()
    visible_uploads = uploads[~uploads["pi_id"].isin(hidden_ids)] if hidden_ids else uploads
    df, matched = apply_app_images(base_df, visible_uploads, lot_ids)
    if hidden:     # deletes still being saved: never shown again, whatever a table still says
        df = _without_lot_images(df, set(hidden))
    with store.lock:
        old = store.snapshot
        store.snapshot = GallerySnapshot(
            df=df, base_df=base_df, lot_ids=lot_ids,
            options=options if options is not None else old.options,
            shared_paths=shared_paths,
            uploads_info={"read": len(uploads), "matched_lots": matched},
            built_at=built_at if built_at is not None else old.built_at,
            generation=(old.generation + 1) if old is not None else 1,
        )


def refresh_if_changed(store: _DataStore):
    """One pass of the background thread: look for changes, re-read only what
    changed, publish a new snapshot. Raises on failure - the versions are then
    not moved on, so the next pass tries again."""
    tables = watched_tables()
    seen = store.versions
    latest = {name: latest_version(location, seen.get(name)) for name, (_, location) in tables.items()}
    changed = {name for name in tables if latest[name] != seen.get(name)}
    kinds = {tables[name][0] for name in changed}
    force_token = store.force_token
    forced = force_token != store.forced_done          # a delete failed: re-read the tables it touches
    if forced:
        changed |= {"product_images", "uploads", "deletions"}
        kinds |= {"images", "uploads", "deletions"}

    snap = store.snapshot
    full = (snap is None or "inventory" in kinds
            or time.time() - snap.built_at >= FULL_REBUILD_SECONDS)
    if not full and not kinds:
        return
    started = time.perf_counter()
    steps = []

    def step(name, fn, *args):
        t = time.perf_counter()
        value = fn(*args)
        steps.append(f"{name} {time.perf_counter() - t:.1f}s")
        return value

    log.info("refresh: %s (changed: %s)", "full rebuild" if full else ", ".join(sorted(kinds)),
             ", ".join(f"{n} v{seen.get(n)}->v{latest[n]}" for n in sorted(changed)) or "first load / daily")

    deleted = store.deleted_ids
    if full or "deletions" in kinds or deleted is None:
        deleted = step("deletions log", load_deleted_ids) if latest["deletions"] is not None else set()

    uploads = store.uploads_df
    if full or "uploads" in kinds or uploads is None:
        new = None
        if (not full and not forced and uploads is not None and seen.get("uploads") is not None
                and latest["uploads"] is not None):
            try:
                new = step("new uploads", read_new_uploads, tables["uploads"][1],
                           seen["uploads"], latest["uploads"])
            except Exception as exc:
                log.warning("reading only the new uploads failed (%s: %s); reading the whole table",
                            type(exc).__name__, exc)
        if new is not None:
            uploads = _newest_per_lot(pd.concat([uploads, new], ignore_index=True)) if len(new) else uploads
            steps.append(f"+{len(new)} upload rows")
        elif latest["uploads"] is not None:
            uploads = step("uploads table", load_app_images)
        else:
            uploads = EMPTY_UPLOADS.copy()

    options = built_at = None
    if full:
        rows = step("inventory", load_inventory_rows)
        pi_index = step("product_images", load_product_images_index)
        base_df, lot_ids, shared = step("lot images", attach_odoo_images, rows, pi_index, deleted)
        options = step("filter options", compute_filter_options, base_df)
        built_at = time.time()
    elif "images" in kinds:
        pi_index = step("product_images", load_product_images_index)
        base_df, lot_ids, shared = step("lot images", attach_odoo_images,
                                        snap.base_df.drop(columns="image_1920"), pi_index, deleted)
    else:
        base_df, lot_ids, shared = snap.base_df, snap.lot_ids, snap.shared_paths
        if "deletions" in kinds:
            base_df, lot_ids = _apply_logged_deletions(base_df, lot_ids, deleted)

    store.uploads_df, store.deleted_ids = uploads, deleted
    step("publish", _publish, store, base_df, lot_ids, shared, uploads, options, built_at)
    store.versions = latest
    store.forced_done = force_token
    snap = store.snapshot
    log.info("refresh done in %.1fs (%s) - %d cards, %d with an app image, generation %d, memory %s",
             time.perf_counter() - started, ", ".join(steps), len(snap.df),
             snap.uploads_info["matched_lots"], snap.generation, _memory_text())


# The background thread uses Streamlit's caches outside a page run, which makes
# Streamlit log a harmless "missing ScriptRunContext" warning each time.
logging.getLogger("streamlit.runtime.scriptrunner_utils.script_run_context").setLevel(logging.ERROR)


@st.cache_resource
def start_background_refresh() -> bool:
    """One refresh thread per app process, started on the first visit (the
    login page), so data loads while people sign in."""
    store = data_store()
    log.info("data source: %s", f"{GALLERY_TABLE_SCHEMA}.{GALLERY_TABLE}" if GALLERY_TABLE
             else "the Odoo tables (set GALLERY_TABLE in secrets for a faster start)")

    def loop():
        failures, last_heartbeat, prefetched = 0, time.time(), None
        while True:
            try:
                refresh_if_changed(store)
                failures, store.last_error = 0, None
            except Exception as exc:
                failures += 1
                store.last_error = f"{type(exc).__name__}: {str(exc).splitlines()[0][:300] if str(exc) else ''}"
                log.exception("refresh failed (attempt %d) - will try again", failures)
            snap = store.snapshot
            if snap is not None and snap.generation != prefetched:
                prefetched = snap.generation          # warm the first pages' thumbnails
                prefetch_images(snap.df["image_1920"].iloc[:PAGE_SIZE * WARMUP_PREFETCH_PAGES].tolist())
            if time.time() - last_heartbeat >= HEARTBEAT_SECONDS:
                last_heartbeat = time.time()
                log.info("heartbeat: running, memory %s, %s cards, generation %s, threads %d",
                         _memory_text(), len(snap.df) if snap is not None else "-",
                         snap.generation if snap is not None else "-", threading.active_count())
            time.sleep(CHANGE_CHECK_SECONDS if not failures else min(60, CHANGE_CHECK_SECONDS * 2 ** failures))

    threading.Thread(target=loop, name="gallery-refresh", daemon=True).start()
    return True


def wait_for_first_snapshot() -> GallerySnapshot:
    """Only after an app (re)start: wait for the background thread's first load."""
    store = data_store()
    deadline = time.time() + FIRST_LOAD_TIMEOUT_SECONDS
    while store.snapshot is None:
        if store.last_error:
            raise RuntimeError(store.last_error)
        if time.time() > deadline:
            raise TimeoutError("the first data load is taking too long - see the terminal log")
        time.sleep(0.5)
    return store.snapshot


# ===========================================================
# Filtering (vectorised - no per-row Python functions)
# ===========================================================
FILTER_KEYS = ("company", "category", "vendor", "location", "search",
               "lot_number", "in_stock_only", "with_images", "without_images")


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
    if f.get("with_images"):
        mask &= df["image_1920"] != NO_IMAGE
    if f["without_images"]:
        mask &= df["image_1920"] == NO_IMAGE
    return df[mask]


def lot_counts(df: pd.DataFrame) -> dict:
    """Lot counts for the rows the filters left: a lot is company + product +
    lot number, counted once however many locations it is in. No stock
    condition here - the "In stock only" filter decides that, so with it
    ticked the counts match the lot-coverage query (quantity > 0). A lot's
    image is the same on all its rows (it is decided per product + lot), so
    "with photo" + "pending" always add up to "lots"."""
    lots = df[df["_lot_count_key"].notna()]
    has_image = lots["image_1920"] != NO_IMAGE
    return {
        "lots": int(lots["_lot_count_key"].nunique()),
        "with_photo": int(lots.loc[has_image, "_lot_count_key"].nunique()),
        "pending": int(lots.loc[~has_image, "_lot_count_key"].nunique()),
    }


def _untick_other_image_filter(changed_key: str, other_key: str):
    if st.session_state.get(changed_key):
        st.session_state[other_key] = False


def render_filters(options: dict, panel_key: str = "filter_panel") -> dict:
    filter_state = st.session_state.setdefault("filter_state", {
        "company": [],
        "category": [],
        "vendor": [],
        "location": [],
        "search": "",
        "lot_number": "",
        "in_stock_only": False,
        "with_images": False,
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

        # "With images" and "Without images" exclude each other: ticking one
        # unticks the other. The widgets are driven by their session-state keys
        # (no value=), so the callback can change the other box safely.
        for state_key, widget_key in (("with_images", "filters_with_images"),
                                      ("without_images", "filters_without_images")):
            if widget_key not in st.session_state:
                st.session_state[widget_key] = filter_state.get(state_key, False)
        with_images = st.checkbox(
            "With images",
            key="filters_with_images",
            on_change=_untick_other_image_filter,
            args=("filters_with_images", "filters_without_images"),
        )
        without_images = st.checkbox(
            "Without images",
            key="filters_without_images",
            on_change=_untick_other_image_filter,
            args=("filters_without_images", "filters_with_images"),
        )
        filter_state["with_images"] = with_images
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
        "with_images": with_images,
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
# Deleting a lot's photo (anyone signed in - the gallery login is the admin login)
#
# For the clicked card's lot - product + lot number, across companies, the
# same lot the card's image is decided for:
#   product_image_overrides  every app-upload row of the lot's product_images
#                            ids is DELETED, and its image file removed
#   product_images           image_1920 is set to NULL on the lot's rows that
#                            have a path - the records themselves are KEPT -
#                            and the image file removed
#   product_image_deletions  one row per change: who, when, what (audit log;
#                            other computers pick deletes up from it, and the
#                            Source->Bronze notebook reads it so the nightly
#                            load doesn't bring the Odoo image back)
# A file is only removed when no other record still points to it. Saved
# thumbnails of the removed files are deleted too. One confirmation, then the
# photo disappears at once and Fabric is updated in the background
# (see start_photo_delete).
# ===========================================================
def deletions_table_name() -> str:
    """Optional secret PRODUCT_IMAGE_DELETIONS_TABLE (Odoo schema of the silver
    lakehouse); defaults to product_image_deletions."""
    return _safe_identifier(secret("PRODUCT_IMAGE_DELETIONS_TABLE"), "product_image_deletions")


DELETIONS_SCHEMA = {
    "deleted_at": pl.Datetime("us", "UTC"),
    "deleted_by": pl.Utf8,
    "company": pl.Utf8,
    "product_id": pl.Utf8,
    "sku": pl.Utf8,
    "lot_number": pl.Utf8,
    "pi_id": pl.Utf8,             # product_images id, plain text (e.g. 12345)
    "source": pl.Utf8,            # "product_images" or the overrides table name
    "action": pl.Utf8,            # "image_1920 cleared" / "record deleted"
    "image_path": pl.Utf8,
}


def _id_predicate(id_dtype, rows: pl.DataFrame) -> str:
    """`id IN (...)` for rows (pi_id, _raw_id), written to match the column's
    own type: numbers for a numeric id column, the stored text otherwise."""
    if id_dtype.is_numeric():
        values = sorted({v for v in rows["pi_id"].to_list() if v and v.isdigit()})
    else:
        values = sorted({"'" + v.replace("'", "''") + "'" for v in rows["_raw_id"].to_list() if v is not None})
    if not values:
        raise ValueError("no usable ids to address the rows")
    return f"id IN ({', '.join(values)})"


def _deletable_path(path) -> bool:
    """Only image files inside a lakehouse Files folder on OneLake."""
    if not _is_image_ref(path):
        return False
    parsed = urlparse(path)
    return ((parsed.hostname or "").endswith("onelake.dfs.fabric.microsoft.com")
            and "/Files/" in parsed.path and f"/Files/{THUMB_DIR}/" not in parsed.path)


def _still_used_paths(paths, lot_pi_ids, shared_paths: dict, uploads: pd.DataFrame) -> set:
    """Paths another record (outside this lot) still points to - from memory."""
    mine = set(lot_pi_ids)
    used = {p for p in paths if shared_paths.get(p, set()) - mine}
    if uploads is not None and len(uploads):
        others = uploads.loc[~uploads["pi_id"].isin(mine), "image_path"]
        used |= set(others[others.isin(list(paths))])
    return used


def _remove_image_file(path: str) -> bool:
    """Deletes the file and its saved thumbnail and forgets the cached card
    image. False if the file was already gone."""
    client = get_datalake_service_client()
    existed = True
    try:
        _file_client(client, *_parse_abfss_path(path)).delete_file()
    except ResourceNotFoundError:
        existed = False
    location = _thumbnail_location(path)
    if location:
        try:
            _file_client(client, *location).delete_file()
        except ResourceNotFoundError:
            pass
        except Exception as exc:
            log.info("thumbnail delete failed: %s", type(exc).__name__)
    store = image_store()
    with store.lock:
        store.cache.pop(path, None)
    return existed


def _without_lot_images(frame: pd.DataFrame, img_keys) -> pd.DataFrame:
    """The cards of these lots shown without a photo (images-first order kept)."""
    hit = frame["_img_key"].isin(list(img_keys)).to_numpy()
    if not hit.any():
        return frame
    frame = frame.copy()
    frame.loc[hit, "image_1920"] = NO_IMAGE
    no_image = (frame["image_1920"] == NO_IMAGE).to_numpy()
    return frame.iloc[no_image.argsort(kind="stable")].reset_index(drop=True)


def _session_id() -> str:
    return st.session_state.setdefault("_sid", uuid.uuid4().hex)


def start_photo_delete(item: dict, session_id: str) -> list:
    """Checks the lot (from memory), hides its photo at once for everyone
    using the app, and queues the actual delete - saved to Fabric in the
    background, one delete at a time. Returns error messages (empty = done)."""
    store = data_store()
    snap = store.snapshot
    img_key = item.get("_img_key")
    if not isinstance(img_key, str) or not img_key or snap is None:
        return ["This card has no lot number, so there is no photo record to delete."]
    with store.lock:
        if img_key in store.hidden:
            return ["This photo is already being deleted."]
    lot = snap.lot_ids[snap.lot_ids["_img_key"] == img_key]
    if lot.empty:
        return ["No product_images record found for this lot."]
    pi_ids = sorted(set(lot["pi_id"].astype(str)))
    odoo = [(str(i), p) for i, p in zip(lot["pi_id"], lot["odoo_image"]) if isinstance(p, str)]
    uploads = store.uploads_df if store.uploads_df is not None else EMPTY_UPLOADS
    lot_uploads = uploads[uploads["pi_id"].isin(pi_ids)]
    if not odoo and lot_uploads.empty:
        return ["This lot has no photo any more."]

    plan = {
        "img_key": img_key, "pi_ids": pi_ids,
        "id_rows": list(zip(lot["pi_id"].astype(str), lot["_raw_id"].astype(str))), "odoo": odoo,
        "uploads": list(zip(lot_uploads["pi_id"].astype(str), lot_uploads["image_path"])),
        "shared_paths": snap.shared_paths or {},
        "log_base": {"deleted_by": APP_USERNAME or "admin", "company": item.get("company"),
                     "product_id": _id_text_value(item.get("product_id")), "sku": _text_or_none(item.get("sku")),
                     "lot_number": _text_or_none(item.get("lot_number"))},
    }
    with store.lock:
        store.hidden[img_key] = {"pi_ids": set(pi_ids), "until": None}
        store.pending_deletes += 1
        snap = store.snapshot
        store.snapshot = replace(snap, df=_without_lot_images(snap.df, {img_key}), generation=snap.generation + 1)
    log.info("photo delete %s: queued - %d product_images ids, %d with an Odoo image, %d upload(s)",
             img_key, len(pi_ids), len(odoo), len(plan["uploads"]))
    store.delete_pool.submit(_run_photo_delete, plan, session_id)
    return []


def _run_photo_delete(plan: dict, session_id: str):
    """Background worker: saves one delete to Fabric. If a table couldn't be
    changed, the photo is shown again (its data is re-read) and the user who
    deleted it gets an error message."""
    store = data_store()
    img_key = plan["img_key"]
    result = {"errors": [], "table_failed": False}
    try:
        result = _save_photo_delete(plan)
    except BaseException as exc:          # never let the worker die silently
        log.exception("photo delete %s: unexpected error", img_key)
        result = {"errors": [f"{type(exc).__name__}: {exc}"], "table_failed": True}
    finally:
        with store.lock:
            store.pending_deletes -= 1
            if result.get("table_failed"):
                store.hidden.pop(img_key, None)
                store.force_token += 1        # re-read the tables: show what is really there
            else:
                store.hidden[img_key] = {"pi_ids": set(plan["pi_ids"]), "until": time.time() + HIDE_GRACE_SECONDS}
                if result.get("cleared_ids"):
                    store.deleted_ids = set(store.deleted_ids or ()) | set(result["cleared_ids"])
            if result.get("errors"):
                lot = plan["log_base"].get("lot_number") or img_key
                store.delete_notes.append((session_id, f"Photo delete for lot {lot}: " + " ".join(result["errors"])))
            if store.snapshot is not None:       # redraw open pages (the "saving" note goes away)
                store.snapshot = replace(store.snapshot, generation=store.snapshot.generation + 1)


def _save_photo_delete(plan: dict) -> dict:
    """Deletes the lot's upload records and clears its Odoo image path
    (records kept), logs the delete and removes the files. Each step is
    logged with its time."""
    from deltalake import DeltaTable

    img_key = plan["img_key"]
    started = time.perf_counter()
    steps = []
    result = {"overrides": 0, "odoo_cleared": 0, "files_deleted": 0, "files_kept": 0, "logged": False,
              "errors": [], "table_failed": False, "cleared_ids": []}
    lot_rows = pl.DataFrame(plan["id_rows"], schema=["pi_id", "_raw_id"], orient="row")

    def timed(name, fn):
        t = time.perf_counter()
        log.info("photo delete %s: %s ...", img_key, name)
        value = fn()
        steps.append(f"{name} {time.perf_counter() - t:.1f}s")
        return value

    ov_table = app_images_table_name()
    log_base = {**plan["log_base"], "deleted_at": datetime.now(timezone.utc)}
    log_rows, removed_paths = [], set()

    # 1. App uploads: delete the records (all rows of the lot's ids).
    if plan["uploads"]:
        try:
            dt = timed("open uploads table", lambda: DeltaTable(
                table_uri(lakehouse_id_silver, "Odoo", ov_table), storage_options=storage_options))
            predicate = _id_predicate(pl.scan_delta(dt).collect_schema()["id"], lot_rows)
            timed("delete upload records", lambda: dt.delete(predicate))
            result["overrides"] = len(plan["uploads"])
            for pi_id, path in plan["uploads"]:
                log_rows.append({**log_base, "pi_id": pi_id, "source": ov_table,
                                 "action": "record deleted", "image_path": path})
                removed_paths.add(path)
        except Exception as exc:
            log.exception("photo delete %s: deleting upload records failed", img_key)
            result["errors"].append(f"Couldn't delete from {ov_table}: {type(exc).__name__}: {exc}")
            result["table_failed"] = True

    # 2. Odoo image: clear the path on every row of the lot that has one (records kept).
    if plan["odoo"]:
        try:
            dt = timed("open product_images", lambda: DeltaTable(
                table_uri(lakehouse_id_silver, "Odoo", "product_images"), storage_options=storage_options))
            predicate = (f"{_id_predicate(pl.scan_delta(dt).collect_schema()['id'], lot_rows)} "
                         "AND image_1920 IS NOT NULL")
            timed("clear image_1920", lambda: dt.update(updates={"image_1920": "NULL"}, predicate=predicate))
            result["odoo_cleared"] = len(plan["odoo"])
            for pi_id, path in plan["odoo"]:
                log_rows.append({**log_base, "pi_id": pi_id, "source": "product_images",
                                 "action": "image_1920 cleared", "image_path": path})
                removed_paths.add(path)
                result["cleared_ids"].append(pi_id)
        except Exception as exc:
            log.exception("photo delete %s: clearing product_images.image_1920 failed", img_key)
            result["errors"].append(f"Couldn't clear the Odoo image: {type(exc).__name__}: {exc}")
            result["table_failed"] = True

    # 3. Deletions log: other computers pick the delete up from it, and the
    #    Source->Bronze notebook reads it so the nightly load doesn't bring the
    #    Odoo image back.
    if log_rows:
        try:
            timed("write deletions log", lambda: pl.DataFrame(log_rows, schema=DELETIONS_SCHEMA).write_delta(
                table_uri(lakehouse_id_silver, "Odoo", deletions_table_name()),
                mode="append", storage_options=storage_options))
            result["logged"] = True
        except Exception as exc:
            log.exception("photo delete %s: writing the deletions log failed", img_key)
            result["errors"].append(f"The photo was removed, but the deletions log couldn't be written "
                                    f"({type(exc).__name__}) - the nightly load may bring the Odoo image back.")

    # 4. Image files - only those no other record still uses.
    still_used = _still_used_paths(removed_paths, plan["pi_ids"], plan["shared_paths"], data_store().uploads_df)
    for path in sorted(removed_paths):
        if path in still_used or not _deletable_path(path):
            result["files_kept"] += 1
            continue
        try:
            timed(f"delete file {path.rsplit('/', 1)[-1]}", lambda: _remove_image_file(path))
            result["files_deleted"] += 1
        except Exception as exc:
            log.exception("photo delete %s: deleting %s failed", img_key, path)
            result["errors"].append(f"Couldn't delete an image file ({type(exc).__name__}).")

    log.info("photo delete %s saved in %.1fs (%s): %s", img_key, time.perf_counter() - started, ", ".join(steps),
             {k: v for k, v in result.items() if k not in ("cleared_ids",)})
    return result


def _text_or_none(value):
    if value is None or (not isinstance(value, str) and pd.isna(value)):
        return None
    return str(value)


def _id_text_value(value):
    text = _text_or_none(value)
    return re.sub(r"\.0+$", "", text.strip()) if text else None


def _clear_delete_state():
    st.session_state.pop("_delete", None)


def _set_delete_step(step):
    if "_delete" in st.session_state:
        st.session_state["_delete"]["step"] = step


# Newer Streamlit tells the app when the dialog is closed with X / Esc. Then an
# open delete can safely be re-shown after any rerun; older versions open it
# on the button click only.
DIALOG_REOPENS = "on_dismiss" in inspect.signature(st.dialog).parameters


@st.dialog("Delete photo", **({"on_dismiss": _clear_delete_state} if DIALOG_REOPENS else {}))
def delete_photo_dialog(item: dict):
    """One confirmation, then the photo disappears at once and the pop-up
    closes; Fabric is updated in the background (see start_photo_delete). The
    buttons only change the step (callbacks), so this works the same in a full
    page run and in a dialog rerun."""
    state = st.session_state.get("_delete")
    if not state:                      # Cancel / Close was clicked
        st.rerun()

    ref = item.get("image_1920")
    thumb = None
    if state["step"] == 1 and _is_image_ref(ref):
        images, _ = get_card_images([ref], timeout=2)
        thumb = images.get(ref)
    img_col, info_col = st.columns([1, 1.5])
    with img_col:
        if thumb:
            st.markdown(f'<img src="{thumb}" style="width:100%;border-radius:10px;" alt="" />',
                        unsafe_allow_html=True)
    with info_col:
        st.markdown(
            f"<div style='font-size:14px;line-height:1.7'><b>{_fmt(item.get('product_name'))}</b><br>"
            f"SKU: {_fmt(item.get('sku'))}<br>Lot No.: {_fmt(item.get('lot_number'))}<br>"
            f"Company: {_fmt(item.get('company'))}<br>Location: {_fmt(item.get('location'))}</div>",
            unsafe_allow_html=True,
        )

    if state["step"] == 1:
        st.warning(
            "**Delete this lot's photo?** It is removed permanently, in every company: "
            "the uploaded photo (record and file) and the Odoo image (path and file). "
            "The product_images record itself is kept. This can't be undone."
        )
        c1, c2 = st.columns(2)
        c1.button("🗑 Yes, delete", key="delete_confirm", width="stretch",
                  on_click=_set_delete_step, args=("run",))
        c2.button("Cancel", key="delete_cancel", width="stretch", on_click=_clear_delete_state)
        return
    if state["step"] == "run":
        try:
            errors = start_photo_delete(item, _session_id())
        except Exception as exc:
            log.exception("photo delete failed to start")
            errors = [f"{type(exc).__name__}: {exc}"]
        if not errors:
            # Gone from every card already; Fabric is updated in the background.
            st.session_state.pop("_delete", None)
            st.session_state["_notice"] = "🗑 Photo deleted"
            st.rerun()
        state["step"] = 3
        state["errors"] = errors

    for error in state.get("errors", []):
        st.error(error)
    st.button("Close", key="delete_close", width="stretch", on_click=_clear_delete_state)


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


def _uploads_status_text(info: dict) -> str:
    """One line about the app-upload table, so it is visible on screen whether
    uploads were read and matched (and why not, if a read failed)."""
    if not info:
        return ""
    error = info.get("error")
    if error:   # never show storage locations
        error = re.sub(r"(?:abfss|https?)://\S+|\S*@onelake\.\S+", "", error).strip(" :,-") or "read error"
    if info.get("read") is None:
        return "App uploads: still loading" + (f" · last read failed: {error}" if error else "")
    text = f"App uploads: {info['read']:,} read · {info.get('matched_lots') or 0:,} lots matched"
    if info.get("checked_at"):
        try:
            tz = ZoneInfo(secret("APP_TIMEZONE", "Asia/Kolkata") or "Asia/Kolkata")
        except Exception:
            tz = ZoneInfo("Asia/Kolkata")
        text += f" · checked {datetime.fromtimestamp(info['checked_at'], tz):%d %b %H:%M}"
    if error:
        text += f" · last read failed: {error}"
    return text


def _change_page(delta: int):
    st.session_state.page = st.session_state.get("page", 1) + delta


# ===========================================================
# Excel export of the filtered rows - two sheets:
#   "Lots by Location": Company, Location, Total Lots, With Photo, Pending
#   "Lot Details":      Lot No, Company, Location, SKU, Product Name, Qty, Value, Age, Has Photo
# Lots are counted like the line above the cards (company + product + lot
# number); rows without a lot number are left out. The file is written with
# the Python standard library only (see write_xlsx) - no Excel package needed.
# ===========================================================
XLSX_MIME = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
# Newer Streamlit runs a callable `data` only when the button is clicked.
DEFERRED_DOWNLOAD = "callable" in (st.download_button.__doc__ or "")


def location_lot_summary(df: pd.DataFrame) -> pd.DataFrame:
    lots = df[df["_lot_count_key"].notna()]
    key = lots["_lot_count_key"]
    has_photo = lots["image_1920"] != NO_IMAGE
    return (
        pd.DataFrame({
            "Company": lots["company"].fillna("(no company)"),
            "Location": lots["location"].fillna("(no location)"),
            "lot": key,
            "photo": key.where(has_photo),
            "pending": key.where(~has_photo),
        })
        .groupby(["Company", "Location"], sort=True)
        .agg(**{"Total Lots": ("lot", "nunique"), "With Photo": ("photo", "nunique"),
                "Pending": ("pending", "nunique")})
        .reset_index()
    )


def lot_details(df: pd.DataFrame) -> pd.DataFrame:
    lots = df[df["_lot_count_key"].notna()]
    return pd.DataFrame({
        "Lot No": lots["lot_number"],
        "Company": lots["company"],
        "Location": lots["location"],
        "SKU": lots["sku"],
        "Product Name": lots["product_name"],
        "Qty": lots["available_inventory"],
        "Value": lots["available_selling_price"],
        "Age": lots["overall_age"],
        "Has Photo": (lots["image_1920"] != NO_IMAGE).map({True: "Yes", False: "No"}),
    }).sort_values(["Company", "Location", "SKU", "Lot No"], kind="stable", na_position="last")


NUMERIC_EXPORT_COLS = {"Total Lots", "With Photo", "Pending", "Qty", "Value", "Age"}

# --- Minimal .xlsx writer (Python standard library only) --------------------
# An .xlsx file is a zip of a few XML files. Writing them directly needs no
# extra package (nothing to pip install) and builds a whole column at a time,
# which is faster than xlsxwriter/openpyxl writing cell by cell.
_XML_HEAD = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n'
_NS_MAIN = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
_NS_REL = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
_XML_BAD_CHARS = r"[\x00-\x08\x0b\x0c\x0e-\x1f￾￿]"
# cell styles (see _XLSX_STYLES): 0 normal, 1 bold, 2 "#,##0", 3 bold "#,##0"
_XLSX_STYLES = (
    _XML_HEAD + f'<styleSheet xmlns="{_NS_MAIN}">'
    '<fonts count="2"><font><sz val="11"/><name val="Calibri"/><family val="2"/></font>'
    '<font><b/><sz val="11"/><name val="Calibri"/><family val="2"/></font></fonts>'
    '<fills count="2"><fill><patternFill patternType="none"/></fill>'
    '<fill><patternFill patternType="gray125"/></fill></fills>'
    '<borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders>'
    '<cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>'
    '<cellXfs count="4">'
    '<xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/>'
    '<xf numFmtId="0" fontId="1" fillId="0" borderId="0" xfId="0" applyFont="1"/>'
    '<xf numFmtId="3" fontId="0" fillId="0" borderId="0" xfId="0" applyNumberFormat="1"/>'
    '<xf numFmtId="3" fontId="1" fillId="0" borderId="0" xfId="0" applyFont="1" applyNumberFormat="1"/>'
    '</cellXfs><cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles>'
    '</styleSheet>'
)


def _col_letter(index: int) -> str:
    letters = ""
    index += 1
    while index:
        index, rem = divmod(index - 1, 26)
        letters = chr(65 + rem) + letters
    return letters


def _xml_text(values: pd.Series) -> pd.Series:
    return (values.astype(str).str.replace(_XML_BAD_CHARS, "", regex=True)
            .str.replace("&", "&amp;", regex=False).str.replace("<", "&lt;", regex=False)
            .str.replace(">", "&gt;", regex=False))


def _xlsx_cell_column(values: pd.Series, letter: str, row_numbers: pd.Series, numeric: bool) -> pd.Series:
    """The <c> element of every row of one column (empty text for a blank cell)."""
    if numeric:
        nums = pd.to_numeric(values, errors="coerce").astype("float64")
        nums = nums.where(nums.abs() != float("inf"))
        missing = nums.isna()
        body = nums.map(repr)          # shortest exact form, e.g. 12.5, 3.0, 1e+20
        cells = f'<c r="{letter}' + row_numbers + '" s="2"><v>' + body + "</v></c>"
    else:
        missing = values.isna()
        cells = (f'<c r="{letter}' + row_numbers + '" t="inlineStr"><is><t xml:space="preserve">'
                 + _xml_text(values.astype(object)) + "</t></is></c>")
    return cells.mask(missing, "")


def _xlsx_inline_row(row: int, values, numeric_flags, styles=(1, 3)) -> str:
    """One small row (header / TOTAL) written cell by cell."""
    text_style, num_style = styles
    cells = []
    for i, (value, numeric) in enumerate(zip(values, numeric_flags)):
        ref = f"{_col_letter(i)}{row}"
        if value is None or value == "":
            continue
        if numeric and isinstance(value, (int, float)):
            cells.append(f'<c r="{ref}" s="{num_style}"><v>{value!r}</v></c>')
        else:
            text = html_lib.escape(re.sub(_XML_BAD_CHARS, "", str(value)), quote=False)
            cells.append(f'<c r="{ref}" s="{text_style}" t="inlineStr"><is><t xml:space="preserve">{text}</t></is></c>')
    return f'<row r="{row}">{"".join(cells)}</row>'


def _xlsx_sheet(frame: pd.DataFrame, widths, total_row=None) -> str:
    frame = frame.reset_index(drop=True)
    n_rows, n_cols = frame.shape
    numeric = [c in NUMERIC_EXPORT_COLS for c in frame.columns]
    parts = [_xlsx_inline_row(1, list(frame.columns), [False] * n_cols)]
    if n_rows:
        row_numbers = pd.Series(range(2, n_rows + 2)).astype(str)
        rows = '<row r="' + row_numbers + '">'
        for i, col in enumerate(frame.columns):
            rows = rows + _xlsx_cell_column(frame[col], _col_letter(i), row_numbers, numeric[i])
        parts.append("".join((rows + "</row>").tolist()))
    if total_row:
        parts.append(_xlsx_inline_row(n_rows + 2, total_row, numeric))
    cols = "".join(f'<col min="{i}" max="{i}" width="{w}" customWidth="1"/>'
                   for i, w in enumerate(widths, start=1))
    last = f"{_col_letter(n_cols - 1)}{max(n_rows, 1) + 1}"
    return (
        _XML_HEAD + f'<worksheet xmlns="{_NS_MAIN}" xmlns:r="{_NS_REL}">'
        '<sheetViews><sheetView workbookViewId="0">'
        '<pane ySplit="1" topLeftCell="A2" activePane="bottomLeft" state="frozen"/>'
        '<selection pane="bottomLeft" activeCell="A2" sqref="A2"/></sheetView></sheetViews>'
        f'<sheetFormatPr defaultRowHeight="15"/><cols>{cols}</cols>'
        f'<sheetData>{"".join(parts)}</sheetData><autoFilter ref="A1:{last}"/></worksheet>'
    ), last


def write_xlsx(sheets) -> bytes:
    """sheets: [(name, DataFrame, column widths, total row or None)] -> .xlsx bytes."""
    sheet_entries, defined_names, rels, overrides = [], [], [], []
    buf = BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED, compresslevel=3) as zf:
        for i, (name, frame, widths, total_row) in enumerate(sheets, start=1):
            xml, last = _xlsx_sheet(frame, widths, total_row)
            zf.writestr(f"xl/worksheets/sheet{i}.xml", xml)
            safe = html_lib.escape(name)
            sheet_entries.append(f'<sheet name="{safe}" sheetId="{i}" r:id="rId{i}"/>')
            ref = "$" + "$".join(re.match(r"([A-Z]+)(\d+)", last).groups())
            defined_names.append(f'<definedName name="_xlnm._FilterDatabase" localSheetId="{i - 1}" '
                                 f'hidden="1">\'{safe}\'!$A$1:{ref}</definedName>')
            rels.append(f'<Relationship Id="rId{i}" Type="{_NS_REL}/worksheet" '
                        f'Target="worksheets/sheet{i}.xml"/>')
            overrides.append(f'<Override PartName="/xl/worksheets/sheet{i}.xml" ContentType='
                             '"application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>')
        rels.append(f'<Relationship Id="rIdStyles" Type="{_NS_REL}/styles" Target="styles.xml"/>')
        zf.writestr("xl/styles.xml", _XLSX_STYLES)
        zf.writestr("xl/workbook.xml", _XML_HEAD + f'<workbook xmlns="{_NS_MAIN}" xmlns:r="{_NS_REL}">'
                    f'<sheets>{"".join(sheet_entries)}</sheets>'
                    f'<definedNames>{"".join(defined_names)}</definedNames></workbook>')
        zf.writestr("xl/_rels/workbook.xml.rels", _XML_HEAD +
                    '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
                    + "".join(rels) + "</Relationships>")
        zf.writestr("_rels/.rels", _XML_HEAD +
                    '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
                    f'<Relationship Id="rId1" Type="{_NS_REL}/officeDocument" Target="xl/workbook.xml"/>'
                    "</Relationships>")
        zf.writestr("[Content_Types].xml", _XML_HEAD +
                    '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
                    '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
                    '<Default Extension="xml" ContentType="application/xml"/>'
                    '<Override PartName="/xl/workbook.xml" ContentType='
                    '"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
                    '<Override PartName="/xl/styles.xml" ContentType='
                    '"application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>'
                    + "".join(overrides) + "</Types>")
    return buf.getvalue()


def build_lot_export(df: pd.DataFrame) -> bytes:
    started = time.perf_counter()
    summary, details, counts = location_lot_summary(df), lot_details(df), lot_counts(df)
    total_row = ["TOTAL", "", counts["lots"], counts["with_photo"], counts["pending"]]
    data = write_xlsx([  # name, frame, column widths, total row (TOTAL counts each lot once)
        ("Lots by Location", summary, [18, 34, 12, 12, 10], total_row),
        ("Lot Details", details, [18, 16, 34, 18, 44, 8, 12, 8, 10], None),
    ])
    log.info("Excel export: %d locations, %d lots, %d KB in %.1fs", len(summary),
             len(details), len(data) // 1024, time.perf_counter() - started)
    return data


def render_excel_export(df: pd.DataFrame):
    try:
        tz = ZoneInfo(secret("APP_TIMEZONE", "Asia/Kolkata") or "Asia/Kolkata")
    except Exception:
        tz = ZoneInfo("Asia/Kolkata")
    file_name = f"lot_photo_report_{datetime.now(tz):%Y%m%d_%H%M}.xlsx"
    # A plain dict kept in session state: the export runs outside the page
    # script, so a failure is recorded here and shown on the next rerun
    # (Streamlit itself only says "Failed to generate file for download").
    status = st.session_state.setdefault("_export_status", {})

    def make_file():
        try:
            return build_lot_export(df)
        except Exception as exc:
            log.exception("Excel export failed")
            status["error"] = f"{type(exc).__name__}: {exc}"
            raise

    if DEFERRED_DOWNLOAD:
        # Built only on click, with the filters on screen; no page rerun after.
        st.download_button("⬇ Export Excel", data=make_file, file_name=file_name,
                           mime=XLSX_MIME, key="export_xlsx", on_click="ignore",
                           width="stretch", disabled=df.empty)
        return
    # Older Streamlit: build on click, then offer the file.
    filter_key = st.session_state.get("_filter_key")
    if st.button("⬇ Export Excel", key="export_build", width="stretch", disabled=df.empty):
        with st.spinner("Preparing…"):
            try:
                st.session_state["_export_xlsx"] = (filter_key, make_file(), file_name)
            except Exception:
                pass          # shown by render_summary_bar
    ready = st.session_state.get("_export_xlsx")
    if ready and ready[0] == filter_key:
        st.download_button("Download file", ready[1], file_name=ready[2], mime=XLSX_MIME,
                           key="export_xlsx", width="stretch")


def render_summary_bar(df: pd.DataFrame, in_stock_only: bool = False):
    """Lot counts + Excel export, above the cards. Kept OUTSIDE the
    render_results fragment, which re-runs while slow images arrive."""
    # Lots, not rows (company + product + lot). With "In stock only" ticked
    # these match the lot-coverage query, which counts quantity > 0 only.
    counts = lot_counts(df)
    total = len(df)
    lots_label = "in stock" if in_stock_only else "(in stock and out of stock)"
    counts_col, export_col = st.columns([4, 1], vertical_alignment="center")
    with counts_col:
        st.markdown(
            f"<div style='color: var(--muted-text); font-size:13px; margin-bottom:8px;'>"
            f"<b style='color: var(--panel-text);'>{counts['lots']:,} lot{'s' if counts['lots'] != 1 else ''}</b> {lots_label}"
            f" · {counts['with_photo']:,} with photo · {counts['pending']:,} pending"
            f" · {total:,} card{'s' if total != 1 else ''}</div>",
            unsafe_allow_html=True,
        )
    with export_col:
        render_excel_export(df)
    error = st.session_state.get("_export_status", {}).pop("error", None)
    if error:
        st.error(f"Excel export failed: {error}")

    notice = st.session_state.pop("_notice", None)
    if notice:
        st.toast(notice)
    store = data_store()
    sid = _session_id()
    with store.lock:
        pending = store.pending_deletes
        mine = [message for owner, message in store.delete_notes if owner == sid]
        store.delete_notes = [(owner, message) for owner, message in store.delete_notes if owner != sid]
    if pending:
        st.caption(f"Saving {pending} photo delete{'s' if pending != 1 else ''} to Fabric in the background…")
    for message in mine:
        st.error(message)


@st.fragment
def render_results(df: pd.DataFrame, cols_per_row: int, uploads_info: dict = None,
                   in_stock_only: bool = False):
    """Product grid + pager. A fragment: Previous/Next re-run only this part,
    not the filters, data lookup and the rest of the page. Each card with a
    photo gets a delete button (see delete_photo_dialog)."""
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

    uploads_line = "" #_uploads_status_text(uploads_info)
    if uploads_line:
        st.markdown(
            f"<div style='color: var(--muted-text); font-size:11.5px; margin:-4px 0 8px 0;'>"
            f"{html_lib.escape(uploads_line)}</div>",
            unsafe_allow_html=True,
        )
    status = st.empty()

    delete_item = None
    if total == 0:
        st.info("No products match the current filters.")
    else:
        for row_start in range(0, len(page_df), cols_per_row):
            row_items = page_df.iloc[row_start:row_start + cols_per_row]
            cols = st.columns(cols_per_row)
            for col, (idx, item) in zip(cols, row_items.iterrows()):
                with col:
                    img = image_lookup.get(item["image_1920"])
                    if DEBUG:
                        st.caption(str(item["image_1920"]))
                    loading = item["image_1920"] in pending
                    st.markdown(render_card_html(item, img, loading), unsafe_allow_html=True)
                    if item["image_1920"] != NO_IMAGE and isinstance(item.get("_img_key"), str):
                        if st.button("🗑 Delete photo", key=f"del_{idx}", width="stretch"):
                            delete_item = item.to_dict()

    st.divider()
    c1, c2, c3 = st.columns([1, 2, 1])
    with c1:
        st.button("⬅ Previous", key="page_prev", disabled=page <= 1,
                  on_click=_change_page, args=(-1,))
    with c2:
        st.markdown(
            f"<div class='page-indicator'>Page {page} of {total_pages} "
            f"({total:,} cards)</div>",
            unsafe_allow_html=True,
        )
    with c3:
        st.button("Next ➡", key="page_next", disabled=page >= total_pages,
                  on_click=_change_page, args=(1,))

    # Load the next page's images in the background so "Next" is instant.
    prefetch_images(df["image_1920"].iloc[end:end + PAGE_SIZE].tolist())

    if delete_item is not None:
        st.session_state["_delete"] = {"step": 1, "item": delete_item}
    open_delete = st.session_state.get("_delete")
    if open_delete and open_delete.get("item") and (delete_item is not None or DIALOG_REOPENS):
        delete_photo_dialog(open_delete["item"])
        return      # no grid rerun for slow images while the dialog is open

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
# Live updates for open pages
# Streamlit only redraws a page when its user does something. This small
# fragment re-runs every LIVE_UPDATE_SECONDS and redraws the page when the
# shared data changed (an upload or a delete, from any screen or computer) -
# but never while a delete pop-up is open.
# ===========================================================
FRAGMENT_RUN_EVERY = "run_every" in inspect.signature(st.fragment).parameters


def _redraw_if_data_changed():
    snap = data_store().snapshot
    seen = st.session_state.get("_seen_generation")
    if snap is None or seen is None or snap.generation == seen:
        return
    if st.session_state.get("_delete"):
        return
    st.rerun()


live_updates = st.fragment(run_every=LIVE_UPDATE_SECONDS)(_redraw_if_data_changed) if FRAGMENT_RUN_EVERY else None


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
            # Only after an app (re)start, before the first load has finished.
            with st.spinner("Loading your product gallery..."):
                snap = wait_for_first_snapshot()
    except Exception as e:
        st.error(f"Failed to connect / fetch data: {e}")
        return

    full_df = snap.df
    filter_options = snap.options
    st.session_state["_seen_generation"] = snap.generation

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
            filtered = apply_filters(full_df, filters)
            render_summary_bar(filtered, filters["in_stock_only"])
            render_results(filtered, filters["cols_per_row"], snap.uploads_info,
                           filters["in_stock_only"])
    else:
        filter_col, results_col = st.columns([1.25, 3])
        with filter_col:
            filters = render_filters(filter_options, "filter_panel")
        reset_page_if_filters_changed(filters)
        with results_col:
            filtered = apply_filters(full_df, filters)
            render_summary_bar(filtered, filters["in_stock_only"])
            render_results(filtered, filters["cols_per_row"], snap.uploads_info,
                           filters["in_stock_only"])

    if live_updates is not None:
        live_updates()

    # Time the server spent building this page. If the browser takes much
    # longer than this to show it, the delay is in the browser / network.
    log.info("page built on the server in %.1fs", time.perf_counter() - run_started)


if __name__ == "__main__":
    main()
