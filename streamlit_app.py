
import json
from datetime import datetime
import requests
import streamlit as st

try:
    import gspread
    from google.oauth2.service_account import Credentials
except Exception:
    gspread = None
    Credentials = None

st.set_page_config(page_title="Auction GM", layout="wide")

SHEET_ID = st.secrets.get("SHEET_ID", "")
SLEEPER_LEAGUE_ID = st.secrets.get("SLEEPER_LEAGUE_ID", "")
SA_JSON = st.secrets.get("GOOGLE_SERVICE_ACCOUNT_JSON", None)

@st.cache_data(ttl=300)
def sleeper_get(path):
    url = f"https://api.sleeper.app/v1/{path.lstrip('/')}"
    r = requests.get(url, timeout=15)
    r.raise_for_status()
    return r.json()

def service_account_client(json_str: str, sheet_id: str):
    if not json_str or not (gspread and Credentials):
        return None, "Missing service account or libraries."
    try:
        info = json.loads(json_str)
        scopes = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_info(info, scopes=scopes)
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(sheet_id)
        return sh, None
    except Exception as e:
        return None, str(e)

def archive_and_reset(sh):
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

    try:
        ws_p = sh.worksheet("Players")
        header = ws_p.row_values(1)
        def idx(colname):
            return header.index(colname) + 1 if colname in header else None
        c_status = idx("status"); c_by = idx("drafted_by"); c_price = idx("price_paid")
        nrows = len(ws_p.get_all_values())
        if nrows > 1:
            ranges = []
            if c_status: ranges.append(f"A2:A{nrows}".replace("A", chr(64+c_status)))
            if c_by:     ranges.append(f"A2:A{nrows}".replace("A", chr(64+c_by)))
            if c_price:  ranges.append(f"A2:A{nrows}".replace("A", chr(64+c_price)))
            if ranges:
                ws_p.batch_clear(ranges)

        ws_lt = sh.worksheet("League_Teams")
        if len(ws_lt.get_all_values()) > 1:
            ws_lt.batch_clear(["A2:Z1000"])

        ws_dl = sh.worksheet("Draft_Log")
        if len(ws_dl.get_all_values()) > 1:
            ws_dl.batch_clear(["A2:Z100000"])

        ws_pb = sh.worksheet("Phase_Budgets")
        header_pb = ws_pb.row_values(1)
        nrows_pb = len(ws_pb.get_all_values())
        for i,c in enumerate(header_pb, start=1):
            if c.startswith("(auto)") and nrows_pb > 1:
                col_letter = chr(64+i)
                ws_pb.batch_clear([f"{col_letter}2:{col_letter}{nrows_pb}"])

    except Exception as e:
        return False, f"Clear failed: {e}"

    return True, f"Archived to {archive_title} and reset live tabs."

st.title("🏈 Auction GM — v7 Starter")

with st.sidebar:
    st.header("Connect")
    st.write(f"Sheet ID: {'✅ set' if SHEET_ID else '❌ missing'}")
    st.write(f"Sleeper League ID: {'✅ set' if SLEEPER_LEAGUE_ID else '❌ missing'}")

    write_ready = False; sa_email = None
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
        st.info("Write features are optional. Add GOOGLE_SERVICE_ACCOUNT_JSON in Secrets and share your Sheet with that service account to enable them.")

    st.divider()
    practice = st.toggle("Practice Mode (no writes)", value=True)

    st.divider()
    st.header("Draft Controls")
    if write_ready and not practice:
        if st.button("📦 Archive & Reset", use_container_width=True, type="primary"):
            ok, msg = archive_and_reset(sh)
            st.success(msg) if ok else st.error(msg)
        if st.button("♻️ Reset (No Archive)", use_container_width=True):
            ok, msg = archive_and_reset(sh)
            st.success("Reset done (archive created as safety).") if ok else st.error(msg)
    else:
        st.button("📦 Archive & Reset (requires write access)", disabled=True, use_container_width=True)
        st.button("♻️ Reset (No Archive) (requires write access)", disabled=True, use_container_width=True)

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
