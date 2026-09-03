import sys
import asyncio

if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

import base64
import html as html_lib
from io import BytesIO
from datetime import date
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import polars as pl
import streamlit as st
from PIL import Image
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient

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

PAGE_SIZE = 12
DEBUG = False
MAX_WORKERS = 20

# How long cached data/images stay valid before being refetched from
# OneLake automatically. Lower this (e.g. 60 * 60 for hourly) if you
# need fresher data; raise it to cut down on OneLake calls.
DATA_TTL_SECONDS =  60 * 60 * 24 # 1 day
DATA_FILTER_VERSION = 2

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


def table_uri(lakehouse_id: str, schema: str, table: str) -> str:
    return (
        f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/"
        f"{lakehouse_id}/Tables/{schema}/{table}"
    )


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


def category_matches_selected(category_value, selected_values):
    category_value = normalize_category_value(category_value)
    if not category_value:
        return False
    for selected in selected_values:
        selected_value = normalize_category_value(selected)
        if not selected_value:
            continue
        if category_value == selected_value or category_value.startswith(selected_value + " / "):
            return True
    return False


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
            <div class="brand-subtitle">{html_lib.escape(APP_SUBTITLE)}</div>f
            """,
            unsafe_allow_html=True,
        )


def set_authenticated(value: bool):
    """Keep login state across page reloads by using the URL query string.
    The single-user app only needs a lightweight persistence mechanism; this
    avoids prompting again after a browser refresh while the user remains in the
    same authenticated session."""
    st.session_state.authenticated = value

    try:
        if value:
            st.query_params["auth"] = "1"
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
        if st.query_params.get("auth") == "1":
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
            if username == APP_USERNAME and password == APP_PASSWORD:
                set_authenticated(True)
                st.rerun()
            else:
                st.error("Incorrect username or password.")

    return False


# ===========================================================
# Azure Data Lake client (for fetching image bytes) - created fresh
# each time, no cache_resource, to avoid stale/expired credentials.
# ===========================================================
def get_datalake_service_client():
    credential = ClientSecretCredential(tenant_id, client_id, client_secret)
    return DataLakeServiceClient(
        account_url="https://onelake.dfs.fabric.microsoft.com",
        credential=credential,
    )


def _parse_abfss_path(abfss_path: str):
    parsed = urlparse(abfss_path)
    filesystem = parsed.username
    file_path = parsed.path.lstrip("/")
    return filesystem, file_path


def fetch_image_bytes(client, abfss_path: str):
    filesystem, file_path = _parse_abfss_path(abfss_path)
    fs_client = client.get_file_system_client(filesystem)
    file_client = fs_client.get_file_client(file_path)
    downloader = file_client.download_file()
    return downloader.readall()


def load_image(client, image_ref: str):
    """Returns (PIL.Image or None, raw_bytes or None, error_message or None). Never raises.
    image_1920 is always an abfss:// path pointing at the image file on OneLake."""
    if image_ref is None or (isinstance(image_ref, str) and image_ref in ("", "False")):
        return None, None, "No image reference"

    if not (isinstance(image_ref, str) and image_ref.startswith("abfss://")):
        return None, None, f"Unexpected image reference format: {image_ref!r}"

    try:
        image_bytes = fetch_image_bytes(client, image_ref)
    except Exception as e:
        return None, None, f"{type(e).__name__}: {e}"

    if not image_bytes:
        return None, image_bytes, "Empty file (0 bytes)"

    try:
        img = Image.open(BytesIO(image_bytes))
        img.load()
        return img, image_bytes, None
    except Exception as e:
        return None, image_bytes, f"{type(e).__name__}: {e}"


@st.cache_data(ttl=DATA_TTL_SECONDS, show_spinner=False)
def load_image_cached(_client, image_ref):
    # Leading underscore on _client tells Streamlit not to try to hash
    # the client object (it isn't hashable / shouldn't be part of the key).
    return load_image(_client, image_ref)


def load_images_parallel(client, refs):
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        results = list(ex.map(lambda r: load_image_cached(client, r), refs))
    return results


# ===========================================================
# Data loading - replicates the reference SQL as Polars lazy joins
# over the underlying Delta tables. Polars handles large-table joins
# with far less memory/time than pandas .merge(), which matters once
# product_product / stock_quant_n1 get into the hundreds of thousands
# of rows. We only convert to pandas at the very end, for the final
# (much smaller) result set that Streamlit actually renders.
# ===========================================================
def _read_delta_lazy(lakehouse_id, schema, table, columns) -> pl.LazyFrame:
    return pl.read_delta(
        table_uri(lakehouse_id, schema, table),
        storage_options=storage_options,
        columns=columns,
    ).lazy()


def _read_delta_lazy_fallback(lakehouse_id, schema, table, candidates):
    last_error = None
    for columns in candidates:
        try:
            return _read_delta_lazy(lakehouse_id, schema, table, columns)
        except Exception as exc:
            last_error = exc
    if last_error is not None:
        raise last_error
    return _read_delta_lazy(lakehouse_id, schema, table, [])


@st.cache_data(ttl=DATA_TTL_SECONDS, show_spinner="Loading product & inventory data...")
def build_inventory_df(data_filter_version: int = DATA_FILTER_VERSION) -> pd.DataFrame:
    """Cached across all users of this deployed app for DATA_TTL_SECONDS.
    After the TTL expires, the next page load re-fetches from OneLake and
    picks up any new/changed rows. Use the Refresh button for an immediate
    update instead of waiting on the TTL."""
    # --- product_product (p) ---
    product_product = _read_delta_lazy(
        lakehouse_id_silver, "Odoo", "product_product",
        columns=["id", "display_name", "categ_id_name", "tracking",
                 "product_variant_id", "sku", "lst_price", "standard_price"],
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

    # --- product_template (pt) ---
    # .unique() guards against join-explosion: if this "lookup" table has
    # more than one row per product_variant_id (duplicates, historical
    # rows, etc.), a left join multiplies every matching row on the left
    # by however many duplicates exist here - which is exactly the kind
    # of thing that turns a modest join into an 18GB allocation.
    product_template = _read_delta_lazy(
        lakehouse_id_silver, "Odoo", "product_template",
        columns=["product_variant_id", "vendor_id_name"],
    ).rename({"vendor_id_name": "vendor"}).unique(
        subset=["product_variant_id"], keep="first"
    )

    product_base = product_product.join(
        product_template, on="product_variant_id", how="left"
    )

    # --- stock_quant_n1 (sq) ---
    # NOTE: product_id here is the internal join key to product_base, not
    # the SKU - the real sku now comes from product_product.sku (p.sku).
    stock_quant = _read_delta_lazy(
        lakehouse_id_silver, "Odoo", "stock_quant_n1",
        columns=["company_id_name", "product_id", "lot_id_name",
                 "location_id_name", "quantity"],
    ).rename({
        "lot_id_name": "lot_number",
        "location_id_name": "location",
        "quantity": "available_inventory",
    }).with_columns([
        pl.col("product_id").cast(pl.Utf8),
        pl.col("company_id_name").replace(COMPANY_LABELS).alias("company"),
    ])

    inventory = stock_quant.join(
        product_base, on="product_id", how="left"
    ).with_columns(
        (pl.col("available_inventory") * pl.col("sp")).alias("available_selling_price")
    )

    # --- stock_min_date (first stock movement date, for "overall age") ---
    stock_min_date = _read_delta_lazy(
        lakehouse_id_silver, "Odoo", "stock_move_line",
        columns=["product_id", "lot_id_name", "date",
                 "company_id_name", "location_id_name"],
    ).filter(
        (pl.col("company_id_name") == "Wedtree eStore Private Limited - HO")
        & (pl.col("location_id_name") == "Partners/Vendors")
    ).with_columns([
        pl.col("product_id").cast(pl.Utf8),
        # strict=False: rows with an unparseable/blank date become null
        # instead of failing the whole query.
        pl.col("date").cast(pl.Date, strict=False),
    ]).rename({
        "lot_id_name": "lot_number",
    }).group_by(["product_id", "lot_number"]).agg(
        pl.col("date").min().alias("stock_move_date")
    )

    inventory = inventory.join(
        stock_min_date, on=["product_id", "lot_number"], how="left"
    ).with_columns(
        (pl.lit(date.today()) - pl.col("stock_move_date"))
        .dt.total_days()
        .alias("overall_age")
    )

    # --- product_images (pi, Bronze) ---
    # Same reasoning: dedupe on product_id so this join can't multiply rows.
    # --- product_images (pi, Bronze) ---
    # A single product_id can have multiple rows here, some with a real
    # image path and some blank/null (e.g. placeholder rows). Filtering
    # to valid images FIRST, then deduping, guarantees that if any valid
    # image exists for a product it's the one kept - doing it in the
    # opposite order risks .unique(keep="first") locking in a blank row
    # before the real image ever gets a chance to survive.
    image_columns = ["image_1920"]
    image_candidates = [
        ["product_id", "image_1920"],
        # ["product_id", "image_1920", "image_1024"],
        # ["product_id", "image_1920", "image_1024", "image_512"],
        # ["product_id", "image_1920", "image_1024", "image_512", "image"],
        # ["product_id", "image_1920", "image", "image_url", "image_link"],
        # ["product_id", "image_1920", "image_url", "image_link"],
        ["product_id"],
    ]
    product_images = _read_delta_lazy_fallback(
        lakehouse_id_bronze, "Odoo", "product_images", image_candidates
    ).with_columns(
        pl.col("product_id").cast(pl.Utf8)
    )

    available_image_columns = [col for col in image_columns if col in product_images.columns]
    if not available_image_columns:
        product_images = product_images.with_columns(
            pl.lit(None).cast(pl.Utf8).alias("image_1920")
        )
    else:
        product_images = product_images.with_columns(
            pl.coalesce([
                pl.col(col).cast(pl.Utf8, strict=False) for col in available_image_columns
            ]).alias("image_1920")
        )

    product_images = product_images.filter(
        pl.col("image_1920").is_not_null()
        & (pl.col("image_1920") != "")
        & (pl.col("image_1920") != "False")
    ).unique(subset=["product_id"], keep="first")

    # Keep all products even when no image exists. The UI renders a
    # "No image available" placeholder instead of dropping the row.
    final = inventory.join(
        product_images, on="product_id",
        how="left", suffix="_img",
    )

    final = final.select([
        "company", "category", "vendor", "sku", "lot_number", "location",
        "product_name", "cp", "sp", "available_inventory",
        "available_selling_price", "product_id", "overall_age",
        pl.col("image_1920").fill_null("No image available").alias("image_1920"),
    ])

    # Remove excluded records before the dataframe reaches the UI. This
    # keeps counts, pagination, searches, and all filter options based on
    # the same dataset.
    final = final.filter(
        ~pl.col("company").is_in(EXCLUDED_COMPANIES)
        & ~pl.col("location").is_in(EXCLUDED_LOCATIONS)
        & ~pl.col("location")
            .cast(pl.Utf8)
            .fill_null("")
            .str.to_uppercase()
            .str.contains(EXCLUDED_LOCATION_SUBSTRING, literal=True)
        & ~pl.col("category")
            .cast(pl.Utf8)
            .fill_null("")
            .str.strip_chars()
            .str.to_uppercase()
            .str.starts_with(EXCLUDED_CATEGORY_PREFIX)
    )

    # Products with a real image should surface first, always - this is
    # the PRIMARY sort key (evaluated before company/category/etc.), and
    # it's baked into the base dataframe rather than applied later, so
    # it holds no matter which filters (if any) get applied downstream:
    # pandas boolean-mask filtering preserves row order, so a filtered
    # view is just a subset of this same ordering, not a re-sort.
    final = final.with_columns(
        (pl.col("image_1920") == "No image available").alias("_no_image")
    ).sort(
        ["_no_image", "company", "category", "vendor", "sku", "lot_number"],
        nulls_last=True,
    ).drop("_no_image")

    # .collect(engine="streaming") runs the whole lazy join plan in
    # chunks rather than materializing everything in one block - this
    # matters on a machine with limited RAM once the underlying tables
    # get large, since it avoids single huge allocations.
    final_df = final.collect(engine="streaming").to_pandas()
    return final_df.loc[
        ~final_df["company"].isin(EXCLUDED_COMPANIES)
        & ~final_df["location"].map(location_is_excluded)
        & ~final_df["category"].fillna("").astype(str).str.strip().str.upper().str.startswith(
            EXCLUDED_CATEGORY_PREFIX
        )
    ].reset_index(drop=True)


# ===========================================================
# Filtering
# ===========================================================
def apply_filters(df, f):
    out = df
    if f["company"]:
        out = out[out["company"].isin(f["company"])]
    if f["category"]:
        category_values = f["category"]
        out = out[
            out["category"].fillna("").map(
                lambda value: category_matches_selected(value, category_values)
            )
        ]
    if f["vendor"]:
        out = out[out["vendor"].isin(f["vendor"])]
    if f["location"]:
        out = out[out["location"].isin(f["location"])]
    if f["search"]:
        needle = f["search"].strip().lower()
        out = out[
            out["product_name"].astype(str).str.lower().str.contains(needle, na=False)
            | out["sku"].astype(str).str.lower().str.contains(needle, na=False)
        ]
    if f["lot_number"]:
        needle = f["lot_number"].strip().lower()
        out = out[out["lot_number"].astype(str).str.lower().str.contains(needle, na=False)]
    if f["in_stock_only"]:
        out = out[out["available_inventory"].fillna(0) > 0]
    return out


@st.cache_data(ttl=DATA_TTL_SECONDS, show_spinner=False)
def compute_filter_options(df: pd.DataFrame) -> dict:
    """Precompute the option lists for every filter dropdown once, from
    the full base dataframe. Previously this work (including the
    category-hierarchy expansion, a Python-level loop) ran inline inside
    render_filters() on every single Streamlit rerun, AND ran a second
    time whenever the mobile drawer was open, since render_filters() was
    called once for the (CSS-hidden but still executed) desktop panel and
    again for the mobile panel. Cached here and computed once per data
    refresh, then shared by both panels - that's what made opening the
    mobile filter drawer feel slow."""

    company_values = [
        value for value in sorted(df["company"].dropna().unique())
        if value not in EXCLUDED_COMPANIES
    ]
    valid_locations = {
        value for value in df["location"].dropna().unique()
        if not location_is_excluded(value)
    }
    category_values = sorted({
        value
        for raw in df["category"].dropna().tolist()
        for value in expand_category_hierarchy(raw)
        if not str(value).strip().upper().startswith(EXCLUDED_CATEGORY_PREFIX)
    })

    return {
        "company": company_values,
        "category": category_values,
        "vendor": sorted(df["vendor"].dropna().unique()),
        "location": sorted(valid_locations),
    }


def render_filters(df, options: dict, panel_key: str = "filter_panel") -> dict:
    filter_state = st.session_state.setdefault("filter_state", {
        "company": [],
        "category": [],
        "vendor": [],
        "location": [],
        "search": "",
        "lot_number": "",
        "in_stock_only": False,
        "cols_per_row": 4,
    })

    with st.container(key=panel_key):
        if panel_key == "mobile_filter_panel":
            # Top-right X, inline with the "Filters" heading - the
            # standard placement for dismissing a slide-in panel in most
            # mobile apps, rather than a separate full-width button
            # floating above the heading.
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
            default=filter_state.get("company", []),
            key="filters_company",
        )
        filter_state["company"] = company

        # --- Category with shortened display labels ---
        category_options = options["category"]
        
        # Create display labels: show last 2-3 parts of the path, or
        # truncate to a reasonable length
        def make_display_label(full_path):
            if not full_path:
                return full_path
            parts = full_path.split(" / ")
            if len(parts) <= 2:
                return full_path
            # Show last 2 parts with ellipsis
            return "… / " + " / ".join(parts[-2:])
        
        # Build mapping from full path to display label
        display_map = {cat: make_display_label(cat) for cat in category_options}
        
        # Create options with display labels
        display_options = [display_map[cat] for cat in category_options]
        
        # Get currently selected values (full paths)
        current_selected = filter_state.get("category", [])
        
        # Map selected full paths to display labels for the widget
        selected_display = [display_map.get(cat, cat) for cat in current_selected if cat in display_map]
        
        # Create a custom multiselect using format_func
        selected_display_labels = st.multiselect(
            "Category",
            display_options,
            default=selected_display,
            key=f"filters_category_{panel_key}",
            format_func=lambda x: x,
        )
        
        # Convert selected display labels back to full paths
        # Find which full paths match the selected display labels
        reverse_map = {}
        for full_path, display_label in display_map.items():
            if display_label not in reverse_map:
                reverse_map[display_label] = []
            reverse_map[display_label].append(full_path)
        
        selected_full_paths = []
        for display_label in selected_display_labels:
            # If multiple full paths share the same display label, 
            # select the first one (shouldn't happen with our mapping)
            matches = reverse_map.get(display_label, [])
            if matches:
                selected_full_paths.append(matches[0])
            else:
                # Fallback: try to find by display_label as full path
                if display_label in category_options:
                    selected_full_paths.append(display_label)
        
        # Preserve any selected values that might not match display labels
        # (for backward compatibility)
        for cat in current_selected:
            if cat not in selected_full_paths and cat in category_options:
                # Check if this category's display label is selected
                if display_map.get(cat) in selected_display_labels:
                    pass  # Already handled
                elif display_map.get(cat) not in selected_display_labels:
                    # Only add if not already there
                    selected_full_paths.append(cat)
        
        # Remove duplicates
        selected_full_paths = list(dict.fromkeys(selected_full_paths))
        filter_state["category"] = selected_full_paths

        vendor = st.multiselect(
            "Vendor",
            options["vendor"],
            default=filter_state.get("vendor", []),
            key="filters_vendor",
        )
        filter_state["vendor"] = vendor

        if company:
            location_options = [
                value for value in sorted(
                    df.loc[df["company"].isin(company), "location"].dropna().unique()
                )
                if not location_is_excluded(value)
            ]
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
        price_range = None

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
            # "Apply Filters" - the explicit commit-and-close action a
            # mobile filter drawer is expected to have, in the spot the
            # old Refresh button used to occupy. Every filter widget
            # above already writes into filter_state / session_state as
            # soon as it's touched, so there's nothing extra to compute
            # here - this button's job is purely to close the drawer and
            # reveal the (already updated) results, which is what
            # "Apply" means from the person's point of view. The X at
            # the top is a plain dismiss for the same underlying state.
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
        "price_range": price_range,
        "cols_per_row": cols_per_row,
    }


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


def render_card_html(item, img) -> str:
    if img is not None:
        img_html = f'<img src="{image_to_data_uri(img)}" alt="" />'
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


# ===========================================================
# Main
# ===========================================================
def main():
    inject_theme()

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
        with st.spinner("Loading your product gallery..."):
            full_df = build_inventory_df(DATA_FILTER_VERSION)
    except Exception as e:
        st.error(f"Failed to connect / fetch data: {e}")
        return

    if "mobile_filters_open" not in st.session_state:
        st.session_state.mobile_filters_open = False

    # Hamburger button for mobile users: opens a left-aligned filter drawer
    # while leaving the desktop two-panel layout completely unchanged.
    # Only rendered while the drawer is CLOSED - the open drawer gets its
    # own dedicated close (✕) button inside render_filters() instead of
    # reusing this one, so there's never a floating icon that can end up
    # sitting on top of the drawer's own heading or the banner above it.
    if not st.session_state.mobile_filters_open:
        if st.button("☰", key="mobile_filters_toggle", help="Open filters"):
            st.session_state.mobile_filters_open = True
            st.rerun()

    # Render exactly one filter layout at a time to keep the widget state
    # consistent and ensure the selected filters are applied once.
    filter_options = compute_filter_options(full_df)

    if st.session_state.mobile_filters_open:
        filters = render_filters(full_df, filter_options, "mobile_filter_panel")
        with st.container():
            df = apply_filters(full_df, filters)
            total = len(df)
            total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)

            if "page" not in st.session_state:
                st.session_state.page = 1
            st.session_state.page = min(max(1, st.session_state.page), total_pages)

            start = (st.session_state.page - 1) * PAGE_SIZE
            end = start + PAGE_SIZE
            page_df = df.iloc[start:end]

            client = get_datalake_service_client()
            refs = page_df["image_1920"].tolist()
            with st.spinner("Loading images..."):
                results = load_images_parallel(client, refs) if refs else []
            image_lookup = dict(zip(refs, results))

            cols_per_row = filters["cols_per_row"]

            st.markdown(
                f"<div style='color: var(--muted-text); font-size:13px; margin-bottom:8px;'>"
                f"{total} product{'s' if total != 1 else ''} found</div>",
                unsafe_allow_html=True,
            )

            if total == 0:
                st.info("No products match the current filters.")
            else:
                for row_start in range(0, len(page_df), cols_per_row):
                    row_items = page_df.iloc[row_start:row_start + cols_per_row]
                    cols = st.columns(cols_per_row)
                    for col, (_, item) in zip(cols, row_items.iterrows()):
                        with col:
                            img, raw, err = image_lookup.get(item["image_1920"], (None, None, "No image reference"))
                            if DEBUG:
                                st.caption(str(item["image_1920"]))
                            st.markdown(render_card_html(item, img), unsafe_allow_html=True)

            st.divider()
            c1, c2, c3 = st.columns([1, 2, 1])
            with c1:
                if st.button("⬅ Previous", disabled=st.session_state.page <= 1):
                    st.session_state.page -= 1
                    st.rerun()
            with c2:
                st.markdown(
                    f"<div class='page-indicator'>Page {st.session_state.page} of {total_pages} "
                    f"({total} products)</div>",
                    unsafe_allow_html=True,
                )
            with c3:
                if st.button("Next ➡", disabled=st.session_state.page >= total_pages):
                    st.session_state.page += 1
                    st.rerun()
    else:
        filter_col, results_col = st.columns([1.25, 3])
        with filter_col:
            filters = render_filters(full_df, filter_options, "filter_panel")
        with results_col:
            df = apply_filters(full_df, filters)
            total = len(df)
            total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)

            if "page" not in st.session_state:
                st.session_state.page = 1
            st.session_state.page = min(max(1, st.session_state.page), total_pages)

            start = (st.session_state.page - 1) * PAGE_SIZE
            end = start + PAGE_SIZE
            page_df = df.iloc[start:end]

            client = get_datalake_service_client()
            refs = page_df["image_1920"].tolist()
            with st.spinner("Loading images..."):
                results = load_images_parallel(client, refs) if refs else []
            image_lookup = dict(zip(refs, results))

            cols_per_row = filters["cols_per_row"]

            st.markdown(
                f"<div style='color: var(--muted-text); font-size:13px; margin-bottom:8px;'>"
                f"{total} product{'s' if total != 1 else ''} found</div>",
                unsafe_allow_html=True,
            )

            if total == 0:
                st.info("No products match the current filters.")
            else:
                for row_start in range(0, len(page_df), cols_per_row):
                    row_items = page_df.iloc[row_start:row_start + cols_per_row]
                    cols = st.columns(cols_per_row)
                    for col, (_, item) in zip(cols, row_items.iterrows()):
                        with col:
                            img, raw, err = image_lookup.get(item["image_1920"], (None, None, "No image reference"))
                            if DEBUG:
                                st.caption(str(item["image_1920"]))
                            st.markdown(render_card_html(item, img), unsafe_allow_html=True)

            st.divider()
            c1, c2, c3 = st.columns([1, 2, 1])
            with c1:
                if st.button("⬅ Previous", disabled=st.session_state.page <= 1):
                    st.session_state.page -= 1
                    st.rerun()
            with c2:
                st.markdown(
                    f"<div class='page-indicator'>Page {st.session_state.page} of {total_pages} "
                    f"({total} products)</div>",
                    unsafe_allow_html=True,
                )
            with c3:
                if st.button("Next ➡", disabled=st.session_state.page >= total_pages):
                    st.session_state.page += 1
                    st.rerun()


if __name__ == "__main__":
    main()
