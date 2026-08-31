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
DATA_TTL_SECONDS = 24 * 60 * 60  # 1 day

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


def table_uri(lakehouse_id: str, schema: str, table: str) -> str:
    return (
        f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/"
        f"{lakehouse_id}/Tables/{schema}/{table}"
    )


# ===========================================================
# Branding / CSS
# ===========================================================
def inject_theme():
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Playfair+Display:wght@600;700&family=Inter:wght@400;500;600&display=swap');

        html, body, [class*="css"]  { font-family: 'Inter', sans-serif; }

        /* Hide default Streamlit chrome for a cleaner, branded look */
        #MainMenu, footer { visibility: hidden; }

        .brand-header {
            background: linear-gradient(135deg, #C9748A 0%, #B5637A 55%, #9E4F68 100%);
            padding: 22px 32px;
            border-radius: 14px;
            margin-bottom: 22px;
            box-shadow: 0 4px 18px rgba(158, 79, 104, 0.28);
        }
        .brand-title {
            font-family: 'Playfair Display', serif;
            font-size: 30px;
            font-weight: 700;
            color: #ffffff;
            margin: 0;
            letter-spacing: 0.3px;
        }
        .brand-subtitle {
            font-size: 12px;
            font-weight: 600;
            color: rgba(255,255,255,0.88);
            letter-spacing: 3px;
            margin-top: 2px;
        }

        /* Filter panel card */
        .st-key-filter_panel {
            background: #FDF4F6;
            border: 1.5px solid #E3AEBC;
            border-radius: 14px;
            padding: 18px 18px 12px 18px;
            box-shadow: 0 3px 14px rgba(158,79,104,0.10);
        }
        .st-key-filter_panel h3 {
            font-family: 'Playfair Display', serif;
            color: #9E4F68;
            font-size: 18px;
            margin-top: 0;
        }

        /* Buttons - pill, brand-colored */
        .stButton > button {
            background-color: #B5637A;
            color: #ffffff;
            border: none;
            border-radius: 999px;
            padding: 8px 20px;
            font-weight: 600;
            transition: all 0.15s ease-in-out;
        }
        .stButton > button:hover {
            background-color: #9E4F68;
            color: #ffffff;
            transform: translateY(-1px);
            box-shadow: 0 3px 10px rgba(158,79,104,0.35);
        }
        .stButton > button:disabled {
            background-color: #E7D3D8;
            color: #ffffff;
        }

        /* Product card */
        .product-card {
            background: #ffffff;
            border-radius: 14px;
            overflow: hidden;
            border: 1px solid #EFE3E6;
            box-shadow: 0 2px 10px rgba(0,0,0,0.05);
            margin-bottom: 20px;
            transition: transform 0.15s ease, box-shadow 0.15s ease;
        }
        .product-card:hover {
            transform: translateY(-3px);
            box-shadow: 0 8px 20px rgba(158,79,104,0.18);
        }
        .product-card-img {
            width: 100%;
            aspect-ratio: 1 / 1;
            background: #FAF3F4;
            display: flex;
            align-items: center;
            justify-content: center;
            overflow: hidden;
        }
        .product-card-img img {
            width: 100%;
            height: 100%;
            object-fit: cover;
        }
        .product-card-noimg {
            color: #C9A3AF;
            font-size: 13px;
            font-weight: 500;
            text-align: center;
        }
        .product-card-body { padding: 14px 16px 16px 16px; }
        .product-card-title {
            font-size: 15px;
            font-weight: 700;
            color: #2E2126;
            line-height: 1.3;
            margin-bottom: 2px;
            min-height: 39px;
        }
        .product-card-meta {
            font-size: 12px;
            color: #9C8890;
            margin-bottom: 10px;
        }
        .product-card-row {
            display: flex;
            justify-content: space-between;
            font-size: 12.5px;
            color: #6B5A61;
            padding: 3px 0;
            border-bottom: 1px dashed #F0E4E8;
        }
        .product-card-row span:first-child { color: #A88F97; }
        .product-card-row span:last-child { font-weight: 600; color: #4A3B41; }

        .product-card-footer {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-top: 12px;
        }
        .price-block .sp {
            font-size: 18px;
            font-weight: 700;
            color: #9E4F68;
        }
        .price-block .cp {
            font-size: 11px;
            color: #B49BA3;
        }
        .stock-badge {
            font-size: 10.5px;
            font-weight: 700;
            padding: 4px 10px;
            border-radius: 999px;
            letter-spacing: 0.3px;
        }
        .stock-in { background: #E6F4EA; color: #2E7D32; }
        .stock-out { background: #FBE9E7; color: #C62828; }

        .page-indicator {
            text-align: center;
            font-weight: 600;
            color: #9E4F68;
            padding-top: 8px;
        }

        /* Login card */
        .st-key-login_card {
            max-width: 380px;
            margin: 48px auto 0 auto;
            background: #ffffff;
            border: 1.5px solid #E3AEBC;
            border-radius: 16px;
            padding: 32px 28px 24px 28px;
            box-shadow: 0 6px 24px rgba(158,79,104,0.12);
        }
        .st-key-login_card h3 {
            font-family: 'Playfair Display', serif;
            color: #9E4F68;
            text-align: center;
            margin-top: 0;
            margin-bottom: 18px;
        }

        /* Hide the default Streamlit top toolbar (Deploy button, menu) for a cleaner branded header */
        [data-testid="stHeader"] { display: none; }
        [data-testid="stToolbar"] { display: none; }
        .block-container { padding-top: 1.5rem; }

        /* Text inputs / number inputs - border on the actual <input> element
           itself, since Streamlit's wrapper div class names vary by
           version and weren't reliably matching here. */
        [data-testid="stTextInput"] input,
        [data-testid="stNumberInput"] input,
        [data-testid="stTextArea"] textarea {
            border: 1.5px solid #E3AEBC !important;
            border-radius: 8px !important;
            background-color: #ffffff !important;
            padding: 8px 12px !important;
        }
        [data-testid="stTextInput"] input:focus,
        [data-testid="stNumberInput"] input:focus,
        [data-testid="stTextArea"] textarea:focus {
            border-color: #B5637A !important;
            box-shadow: 0 0 0 1px #B5637A !important;
        }
        [data-testid="stTextInput"] input::placeholder,
        [data-testid="stTextArea"] textarea::placeholder {
            color: #C9A3AF !important;
            opacity: 1 !important;
        }
        /* Some Streamlit versions also render a bordered wrapper div around
           the input - keep it transparent so it doesn't double up or clash. */
        div[data-baseweb="base-input"],
        div[data-baseweb="input"] {
            border: none !important;
            background-color: transparent !important;
        }

        /* Select / multiselect boxes - same treatment */
        div[data-baseweb="select"] > div {
            border: 1.5px solid #E3AEBC !important;
            border-radius: 8px !important;
            background-color: #ffffff !important;
        }
        div[data-baseweb="select"]:focus-within > div {
            border-color: #B5637A !important;
        }

        /* Branded spinner */
        [data-testid="stSpinner"] > div > div {
            border-top-color: #B5637A !important;
        }
        [data-testid="stSpinner"] p {
            color: #9E4F68 !important;
            font-weight: 600;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_header():
    st.markdown(
        f"""
        <div class="brand-header">
            <div class="brand-title">{html_lib.escape(APP_TITLE)}</div>
            <div class="brand-subtitle">{html_lib.escape(APP_SUBTITLE)}</div>
        </div>
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


@st.cache_data(ttl=DATA_TTL_SECONDS, show_spinner="Loading product & inventory data...")
def build_inventory_df() -> pd.DataFrame:
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
    product_images = _read_delta_lazy(
        lakehouse_id_bronze, "Odoo", "product_images",
        columns=["product_id", "image_1920"],
    ).with_columns(
        pl.col("product_id").cast(pl.Utf8)
    ).filter(
        pl.col("image_1920").is_not_null()
        & (pl.col("image_1920") != "")
        & (pl.col("image_1920") != "False")
    ).unique(subset=["product_id"], keep="first")

    # Inner join: only keep products that actually have a matching row in
    # product_images (drops products with no image reference at all).
    # IMPORTANT: product_images is keyed by the internal product_id
    # (Odoo's product.product id), not by the display sku - so we join on
    # product_id here even though sku is now a separate, human-readable field.
    final = inventory.join(
        product_images, on="product_id",
        how="inner", suffix="_img",
    )

    final = final.select([
        "company", "category", "vendor", "sku", "lot_number", "location",
        "product_name", "cp", "sp", "available_inventory",
        "available_selling_price", "product_id", "overall_age", "image_1920",
    ]).filter(
        # Belt-and-braces: also drop rows where image_1920 itself is
        # null/empty, or the literal string "False" that Odoo sometimes
        # writes for an empty image field.
        pl.col("image_1920").is_not_null()
        & (pl.col("image_1920") != "")
        & (pl.col("image_1920") != "False")
    ).sort(
        ["company", "category", "vendor", "sku", "lot_number"],
        nulls_last=True,
    )

    # .collect(engine="streaming") runs the whole lazy join plan in
    # chunks rather than materializing everything in one block - this
    # matters on a machine with limited RAM once the underlying tables
    # get large, since it avoids single huge allocations.
    return final.collect(engine="streaming").to_pandas()


# ===========================================================
# Filtering
# ===========================================================
def apply_filters(df, f):
    out = df
    if f["company"]:
        out = out[out["company"].isin(f["company"])]
    if f["category"]:
        out = out[out["category"].isin(f["category"])]
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
    if f["price_range"]:
        lo, hi = f["price_range"]
        out = out[out["sp"].fillna(0).between(lo, hi)]
    return out


def render_filters(df) -> dict:
    with st.container(key="filter_panel"):
        st.markdown("<h3>Filters</h3>", unsafe_allow_html=True)

        company = st.multiselect("Company", sorted(df["company"].dropna().unique()))
        category = st.multiselect("Category", sorted(df["category"].dropna().unique()))
        vendor = st.multiselect("Vendor", sorted(df["vendor"].dropna().unique()))
        location = st.multiselect("Location", sorted(df["location"].dropna().unique()))

        search = st.text_input("Product/SKU)")
        lot_number = st.text_input("Lot number")

        in_stock_only = st.checkbox("In stock only", value=False)

        sp_series = df["sp"].dropna()
        price_range = None
        # if not sp_series.empty:
        #     lo, hi = float(sp_series.min()), float(sp_series.max())
        #     if lo < hi:
        #         price_range = st.slider("Selling price range", lo, hi, (lo, hi))

        cols_per_row = st.slider("Columns per row", min_value=2, max_value=6, value=4)

        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("🔄 Refresh data now", width="stretch"):
            build_inventory_df.clear()
            load_image_cached.clear()
            st.session_state.page = 1
            st.rerun()

        if st.button("🚪 Logout", width="stretch"):
            set_authenticated(False)
            st.rerun()

    return {
        "company": company,
        "category": category,
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
            full_df = build_inventory_df()
    except Exception as e:
        st.error(f"Failed to connect / fetch data: {e}")
        return

    # Desktop: left pane (filters) / right pane (results).
    # On narrow / mobile viewports Streamlit stacks these vertically,
    # so filters appear above the results instead of beside them.
    filter_col, results_col = st.columns([1, 3])

    with filter_col:
        filters = render_filters(full_df)

    with results_col:
        df = apply_filters(full_df, filters)  # empty filters => full_df, unchanged
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
            f"<div style='color:#9C8890; font-size:13px; margin-bottom:8px;'>"
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
