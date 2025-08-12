# streamlit_app.py — v7 (clean UX: spinner+toast, safe buttons)

import json
from datetime import datetime
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

# Read secrets in a robust way (supports both formats)
SHEET_ID = st.secrets.get("SHEET_ID", "")
SLEEPER_LEAGUE_ID = st.secrets.get("SLEEPER_LEAGUE_ID", "")

SA_JSON = st.secrets.get("GOOGLE_SERVICE_ACCOUNT_JSON", None)
# Fallback: accept a table like [gcp_service_account] or [google_service_account]
if not SA_JSON:
    for tbl_key in ("gcp_service_account", "google_service_account"):
        if tbl_key in st.secrets:
            # Reassemble a valid JSON blob from the table
            info = dict(st.secrets[tbl_key])  # copy
            SA_JSON = json.dumps(info)
            break


# ---------------- Helpers ----------------
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

# ---------------- UI ----------------
st.title("🏈 Auction GM — v7 Starter (Reset + Practice)")

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

    # Use a simple busy guard to prevent double-click spam
    if "busy" not in st.session_state:
        st.session_state["busy"] = False

    # Archive & Reset
    clicked_archive = st.button("📦 Archive & Reset", use_container_width=True, type="primary", disabled=not (write_ready and not practice or not write_ready))
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

# League header info (from Sleeper)
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
st.subheader("Players — coming next")
st.write("Next we’ll wire this to your `Players` tab for search/drafted/price input and refresh ADP/AAV.")
st.caption("Tip: Practice Mode leaves your sheet untouched; toggle it off to enable Archive/Reset.")
