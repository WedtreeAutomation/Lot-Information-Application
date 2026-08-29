# Product Gallery

A Streamlit app that shows live inventory from Fabric/OneLake as a
branded, filterable product gallery — desktop two-pane layout
(filters left, results right), stacking to a single column on mobile.

## Files

```
app.py                          <- the Streamlit app
requirements.txt                <- Python dependencies (no OS driver needed)
.streamlit/config.toml          <- native theme colors (rose/mauve brand palette)
.streamlit/secrets.toml.example <- template for the credentials the app needs
README.md                       <- this file
```

## 1. Set up credentials

The app needs an Azure AD **service principal** (app registration) with
read access to your Fabric workspace's OneLake, plus the workspace and
lakehouse IDs.

Copy the template and fill in real values:

```bash
cp .streamlit/secrets.toml.example .streamlit/secrets.toml
```

Edit `.streamlit/secrets.toml`:

```toml
FABRIC_CLIENT_ID = "..."
FABRIC_CLIENT_SECRET = "..."
FABRIC_TENANT_ID = "..."
FABRIC_WORKSPACE_ID = "..."
FABRIC_LAKEHOUSE_ID_SILVER = "..."
FABRIC_LAKEHOUSE_ID_BRONZE = "..."
```

**Never commit a filled-in `secrets.toml` to git.** Add it to `.gitignore`:

```
.streamlit/secrets.toml
```

### Where to find these IDs
- **Workspace ID / Lakehouse ID**: open the Lakehouse in Fabric, look at
  the URL — `.../groups/<workspace_id>/lakehouses/<lakehouse_id>`.
- **Client ID / Secret / Tenant ID**: from your Azure AD App Registration
  (Entra ID) — the app registration must be granted read access to the
  Silver and Bronze lakehouses (via workspace role or item permissions).

## 2. Run locally

```bash
pip install -r requirements.txt
streamlit run app.py
```

## 3. Deploy to Streamlit Community Cloud

1. Push `app.py`, `requirements.txt`, and `.streamlit/config.toml` to a
   GitHub repo. **Do not push `secrets.toml`.**
2. On [share.streamlit.io](https://share.streamlit.io), create a new app
   pointing at the repo and `app.py`.
3. In the app's **Settings → Secrets**, paste the same key/value pairs
   from `secrets.toml.example` (filled in with real values).
4. Deploy. No extra system packages or ODBC drivers are required — every
   dependency here is a pure Python wheel.

## How data refreshes

- Inventory data is cached for **24 hours** (`DATA_TTL_SECONDS` in
  `app.py`) — it re-fetches from OneLake automatically once a day.
- The **"🔄 Refresh data now"** button in the filter panel clears the
  cache immediately, for anyone who needs the latest data sooner.
- Lower `DATA_TTL_SECONDS` (e.g. `60 * 60` for hourly) if daily isn't
  fresh enough for your workflow.

## Customizing the brand header

Set these in secrets (or leave unset to use the defaults shown):

```toml
APP_TITLE = "Man Mandir Silks and Saris"
APP_SUBTITLE = "PRODUCT GALLERY"
APP_ICON = "🧵"
```

Colors live in `.streamlit/config.toml` (native widgets) and in the
`<style>` block at the top of `inject_theme()` in `app.py` (cards,
header, buttons) — both use the same rose/mauve palette
(`#B5637A` / `#9E4F68`) so update both together if you change it.

## Scaling note

Joins (product_product → product_template → stock_quant_n1 →
product_images) run via Polars lazy frames rather than pandas, which
handles large tables far better. If tables grow into the tens of
millions of rows, the next scaling step would be pre-joining this data
into a single Fabric view/table so the app reads one flat table instead
of joining four at request time.
