# streamlit_app.py — v8
# - Clean UX (spinner+toast), busy-guarded buttons
# - Practice Mode
# - Archive & Reset + Reset (No Archive) + optional Purge Archives
# - Robust Secrets reader (GOOGLE_SERVICE_ACCOUNT_JSON OR [gcp_service_account] table)
# - Sleeper league header
# - NEW: Upload raw FFA CSV -> clean & map -> write to Google Sheet "Projections"

import json
from datetime import datetime
import io
import pandas as pd
import requests
import streamlit as st

# Optional write libs
try:
    import gspread
    from google.oauth2.service_account import Credentials
    from gspread.utils import rowcol_to_a1
except Exception:
    gspread = None
    Credentials = None

st.set_page_config(page_title="Auction GM", layout="wide")

# ------------- Secrets (robust) -------------
SHEET_ID = st.secrets.get("SHEET_ID", "")
SLEEPER_LEAGUE_ID = st.secrets.get("SLEEPER_LEAGUE_ID", "")

SA_JSON = st.secrets.get("GOOGLE_SERVICE_ACCOUNT_JSON", None)
# Fallback: accept a table like [gcp_service_account] or [google_service_account]
if not SA_JSON:
    for tbl_key in ("gcp_service_account", "google_service_account"):
        if tbl_key in st.secrets:
            SA_JSON = json.dumps(dict(st.secrets[tbl_key]))
            break

# ------------- Helpers -------------
@st.cache_data(ttl=300)
def sleeper_get(path):
    url = f"https://api.sleeper.app/v1/{path.lstrip('/')}"
    r = requests.get(url, timeout=15)
    r.raise_for_status()
    return r.json()

def service_account_client(json_str: str, sheet_id: str):
    """Return (spreadsheet, error_message) using service account JSON from Secrets."""
    if not json_str:
        return None, "No service account JSON found in Secrets."
    if not (gspread and Credentials):
        return None, "gspread/google-auth not available."
    try:
        info = json.loads(json_str)
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = Credentials.from_service_account_info(info, scopes=scopes)
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(sheet_id)
        return sh, None
    except Exception as e:
        return None, str(e)

def _batch_clear(ws, start_row, col_idx, end_row):
    if not col_idx or end_row < start_row:
        return
    a1 = f"{rowcol_to_a1(start_row, col_idx)}:{rowcol_to_a1(end_row, col_idx)}"
    ws.batch_clear([a1])

def reset_live_fields_only(sh):
    """
    Clears live draft fields without creating an archive:
      - Players: status, drafted_by, price_paid
      - League_Teams: rows (A2:Z)
      - Draft_Log: rows (A2:Z)
      - Phase_Budgets: only columns whose header starts with '(auto)'
    """
    try:
        # Players
        ws_p = sh.worksheet("Players")
        header = ws_p.row_values(1)
        nrows = len(ws_p.get_all_values())
        def idx(name): return header.index(name) + 1 if name in header else None
        c_status, c_by, c_price = idx("status"), idx("drafted_by"), idx("price_paid")
        if nrows > 1:
            _batch_clear(ws_p, 2, c_status, nrows)
            _batch_clear(ws_p, 2, c_by, nrows)
            _batch_clear(ws_p, 2, c_price, nrows)

        # League_Teams
        ws_lt = sh.worksheet("League_Teams")
        if len(ws_lt.get_all_values()) > 1:
            ws_lt.batch_clear(["A2:Z1000"])

        # Draft_Log
        ws_dl = sh.worksheet("Draft_Log")
        if len(ws_dl.get_all_values()) > 1:
            ws_dl.batch_clear(["A2:Z100000"])

        # Phase_Budgets (auto cols only)
        ws_pb = sh.worksheet("Phase_Budgets")
        header_pb = ws_pb.row_values(1)
        nrows_pb = len(ws_pb.get_all_values())
        for i, col_name in enumerate(header_pb, start=1):
            if col_name.startswith("(auto)") and nrows_pb > 1:
                a1 = f"{rowcol_to_a1(2, i)}:{rowcol_to_a1(nrows_pb, i)}"
                ws_pb.batch_clear([a1])

        return True, "Reset complete (live fields cleared, no archive)."
    except Exception as e:
        return False, f"Reset failed: {e}"

def archive_and_reset(sh):
    """
    1) Create Archive_{timestamp} worksheet and copy Draft_Log, League_Teams, Settings_League
    2) Clear live fields in Players / League_Teams / Draft_Log / Phase_Budgets (auto cols)
    """
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    archive_title = f"Archive_{ts}"
    try:
        sh.add_worksheet(title=archive_title, rows=2000, cols=60)
        ws_arc = sh.worksheet(archive_title)
    except Exception as e:
        return False, f"Could not create archive sheet: {e}"

    def copy_tab(name):
        try:
            ws = sh.worksheet(name)
            rows = ws.get_all_values() or [[]]
            ws_arc.append_row([f"== {name} =="])
            for row in rows:
                ws_arc.append_row(row if row else [""])
            ws_arc.append_row([""])
        except Exception as e:
            raise RuntimeError(f"Archive failed for {name}: {e}")

    try:
        for tab in ["Draft_Log", "League_Teams", "Settings_League"]:
            copy_tab(tab)
    except Exception as e:
        return False, str(e)

    ok, msg = reset_live_fields_only(sh)
    if not ok:
        return False, msg
    return True, f"Archived to {archive_title} and reset live tabs."

def purge_archives(sh, keep_latest=True):
    """Delete Archive_* worksheets. If keep_latest=True, keeps the newest one."""
    try:
        ws_list = sh.worksheets()
        archives = [ws for ws in ws_list if ws.title.startswith("Archive_")]
        if not archives:
            return True, "No archive tabs found."
        archives.sort(key=lambda ws: ws.title, reverse=True)
        to_delete = archives[1:] if keep_latest and len(archives) > 1 else archives
        for ws in to_delete:
            sh.del_worksheet(ws)
        kept = archives[0].title if keep_latest and archives else None
        return True, f"Purged {len(to_delete)} archive tab(s)." + (f" Kept {kept}." if kept else "")
    except Exception as e:
        return False, f"Purge failed: {e}"

def upsert_worksheet(sh, title, rows=5000, cols=30):
    """Get worksheet by title or create if missing."""
    try:
        return sh.worksheet(title)
    except Exception:
        sh.add_worksheet(title=title, rows=rows, cols=cols)
        return sh.worksheet(title)

def write_dataframe_to_sheet(ws, df: pd.DataFrame, header=True):
    """Overwrite entire worksheet area with df (headers + values)."""
    values = [df.columns.tolist()] + df.fillna("").astype(str).values.tolist() if header else df.fillna("").astype(str).values.tolist()
    # Clear a big range then write once
    ws.batch_clear(["A1:Z100000"])
    ws.update("A1", values, value_input_option="RAW")

# ------------- UI -------------
st.title("🏈 Auction GM — v8")

# Sidebar: connection + modes
with st.sidebar:
    st.header("Connect")
    st.write("These values live in Secrets (not in code).")
    st.write(f"**Sheet ID:** {'✅ set' if SHEET_ID else '❌ missing'}")
    st.write(f"**Sleeper League ID:** {'✅ set' if SLEEPER_LEAGUE_ID else '❌ missing'}")

    write_ready = False
    sa_email = None
    if SA_JSON and SHEET_ID:
        sh, err = service_account_client(SA_JSON, SHEET_ID)
        if err:
            st.warning(f"Write access not ready: {err}")
        else:
            write_ready = True
            try:
                sa_email = json.loads(SA_JSON).get("client_email")
            except Exception:
                sa_email = None
            st.success("Write access enabled (service account connected).")
            if sa_email:
                st.caption(f"Shared with: {sa_email}")
    else:
        st.info("Write features (Archive/Reset) are optional. Add GOOGLE_SERVICE_ACCOUNT_JSON in Secrets and share your Sheet with that service account to enable them.")

    st.divider()
    st.header("Practice Mode")
    practice = st.toggle("Enable Practice Mode (no writes; safe sandbox)", value=True)

    st.divider()
    st.header("Draft Controls")

    if "busy" not in st.session_state:
        st.session_state["busy"] = False

    # Archive & Reset
    clicked_archive = st.button("📦 Archive & Reset", use_container_width=True, type="primary", disabled=not (write_ready and not practice))
    if clicked_archive and write_ready and not practice and not st.session_state["busy"]:
        st.session_state["busy"] = True
        try:
            with st.spinner("Archiving and resetting…"):
                ok, msg = archive_and_reset(sh)
            st.toast(msg if ok else f"⚠️ {msg}")
        finally:
            st.session_state["busy"] = False
    elif clicked_archive and (practice or not write_ready):
        st.toast("ℹ️ Enable write access and turn off Practice Mode to use Archive & Reset.")

    # Reset (No Archive)
    clicked_reset = st.button("♻️ Reset (No Archive)", use_container_width=True, disabled=not (write_ready and not practice))
    if clicked_reset and write_ready and not practice and not st.session_state["busy"]:
        st.session_state["busy"] = True
        try:
            with st.spinner("Resetting…"):
                ok, msg = reset_live_fields_only(sh)
            st.toast(msg if ok else f"⚠️ {msg}")
        finally:
            st.session_state["busy"] = False

    # Purge older archives (keep latest)
    clicked_purge = st.button("🧹 Purge Archives (keep latest)", use_container_width=True, disabled=not (write_ready and not practice))
    if clicked_purge and write_ready and not practice and not st.session_state["busy"]:
        st.session_state["busy"] = True
        try:
            with st.spinner("Purging archive tabs…"):
                ok, msg = purge_archives(sh, keep_latest=True)
            st.toast(msg if ok else f"⚠️ {msg}")
        finally:
            st.session_state["busy"] = False

# League header (Sleeper)
col1, col2, col3 = st.columns(3)
if SLEEPER_LEAGUE_ID:
    try:
        league = sleeper_get(f"league/{SLEEPER_LEAGUE_ID}")
        with col1: st.metric("League", league.get("name","—"))
        with col2: st.metric("Teams", league.get("total_rosters","—"))
        with col3: st.metric("Season", league.get("season","—"))
    except Exception as e:
        st.warning(f"Could not fetch league info: {e}")
else:
    st.info("Add SLEEPER_LEAGUE_ID in Secrets to fetch league info.")

st.divider()

# ------------- NEW: FFA CSV Upload -> Clean -> Write to Sheet -------------
st.subheader("📥 Import Projections (Raw FFA CSV → Projections sheet)")

st.caption("Upload the unedited CSV from FFA Season Projections. The app will drop unneeded columns, rename to the expected schema, and write it to the 'Projections' tab in your backend.")

uploaded = st.file_uploader("Upload FFA CSV", type=["csv"], accept_multiple_files=False)

# Desired output schema and canonical order
target_cols = [
    "Position",
    "Player",
    "Team",
    "Points",
    "VOR",
    "ADP",
    "AAV",
    "Rank Overall",
    "Rank Position",
]

# Flexible column name mapping (lowercased keys)
name_map = {
    "position": "Position",
    "pos": "Position",
    "player": "Player",
    "name": "Player",
    "team": "Team",
    "tm": "Team",
    "points": "Points",
    "proj_points": "Points",
    "proj_fp": "Points",
    "fp": "Points",
    "vor": "VOR",
    "points_vor": "VOR",
    "value_over_replacement": "VOR",
    "adp": "ADP",
    "aav": "AAV",
    "auction_value": "AAV",
    "rank": "Rank Overall",
    "overall_rank": "Rank Overall",
    "rank_overall": "Rank Overall",
    "position_rank": "Rank Position",
    "pos_rank": "Rank Position",
    "rank_position": "Rank Position",
}

def clean_projection_csv(file_bytes: bytes) -> pd.DataFrame:
    df = pd.read_csv(io.BytesIO(file_bytes))
    # Build a mapping from existing columns -> target names
    col_map = {}
    for c in df.columns:
        key = str(c).strip().lower()
        if key in name_map:
            col_map[c] = name_map[key]
    df = df.rename(columns=col_map)

    # Keep only target columns; create missing ones empty
    for col in target_cols:
        if col not in df.columns:
            df[col] = None
    df = df[target_cols]

    # Coerce numeric fields
    for col in ["Points", "VOR", "ADP", "AAV", "Rank Overall", "Rank Position"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Drop rows with no player name
    df = df[df["Player"].notna() & (df["Player"].astype(str).str.strip() != "")]
    df = df.reset_index(drop=True)
    return df

if uploaded is not None:
    try:
        with st.spinner("Parsing and cleaning CSV…"):
            cleaned = clean_projection_csv(uploaded.read())
        st.success(f"Loaded {len(cleaned):,} players. Preview below:")
        st.dataframe(cleaned.head(20), use_container_width=True)
        st.caption("Columns ↓ match the backend schema exactly:")
        st.code(", ".join(cleaned.columns.tolist()))
        can_write = write_ready and not practice
        btn = st.button("✍️ Write to Google Sheet → 'Projections'", type="primary", disabled=not can_write)
        if btn:
            if not write_ready:
                st.toast("⚠️ Write access not enabled.")
            elif practice:
                st.toast("ℹ️ Turn off Practice Mode to write to the sheet.")
            else:
                try:
                    with st.spinner("Writing cleaned data to 'Projections'…"):
                        ws_proj = upsert_worksheet(sh, "Projections", rows=max(5000, len(cleaned)+10), cols=len(target_cols)+3)
                        write_dataframe_to_sheet(ws_proj, cleaned, header=True)
                    st.toast("✅ Projections updated in Google Sheet.")
                except Exception as e:
                    st.error(f"Write failed: {e}")
    except Exception as e:
        st.error(f"Could not read CSV: {e}")

st.divider()
st.subheader("Players — Draft Board (coming next)")
st.write("Next update wires the Players table (search/filters/drafted/price) and shows Soft/Hard Recommended $ computed from these projections plus your league context.")
st.caption("Tip: Keep Practice Mode ON for safe testing; toggle OFF only when you’re ready to write.")
