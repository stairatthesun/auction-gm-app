# streamlit_app.py — v9 (all-in-one)
# - Secrets: robust creds load (string JSON or [gcp_service_account] table)
# - Modes: Practice + Admin
# - Admin tools: Upload FFA CSV -> clean -> write Projections; Sync Projections -> Players
# - Draft controls: Archive & Reset, Reset (No Archive), Purge Archives, Undo Last
# - Sleeper header
# - Draft Board UI: search/filters/hide drafted/price entry/drafted toggle
# - RAV engine: uses AAV > VOR > Points, live market inflation, soft/hard caps

import json
from datetime import datetime
import io
import math
import pandas as pd
import requests
import streamlit as st

# Optional write libs (available on Streamlit Cloud)
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

def ws_to_df(ws):
    rows = ws.get_all_values()
    if not rows:
        return pd.DataFrame()
    header, data = rows[0], rows[1:]
    return pd.DataFrame(data, columns=header)

def write_dataframe_to_sheet(ws, df: pd.DataFrame, header=True):
    values = [df.columns.tolist()] + df.fillna("").astype(str).values.tolist() if header else df.fillna("").astype(str).values.tolist()
    ws.batch_clear(["A1:Z100000"])
    ws.update("A1", values, value_input_option="RAW")

def upsert_worksheet(sh, title, rows=5000, cols=30):
    try:
        return sh.worksheet(title)
    except Exception:
        sh.add_worksheet(title=title, rows=rows, cols=cols)
        return sh.worksheet(title)

# Reset + Archive
def reset_live_fields_only(sh):
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
    if not ok: return False, msg
    return True, f"Archived to {archive_title} and reset live tabs."

def purge_archives(sh, keep_latest=True):
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

# -------- CSV Import (Admin) --------
TARGET_PROJ_COLS = [
    "Position","Player","Team","Points","VOR","ADP","AAV","Rank Overall","Rank Position"
]
NAME_MAP = {
    "position":"Position","pos":"Position",
    "player":"Player","name":"Player",
    "team":"Team","tm":"Team",
    "points":"Points","proj_points":"Points","proj_fp":"Points","fp":"Points","fantasy_points":"Points",
    "vor":"VOR","points_vor":"VOR","value_over_replacement":"VOR",
    "adp":"ADP",
    "aav":"AAV","auction_value":"AAV",
    "rank":"Rank Overall","overall_rank":"Rank Overall","rank_overall":"Rank Overall",
    "position_rank":"Rank Position","pos_rank":"Rank Position","rank_position":"Rank Position",
}

def clean_projection_csv(file_bytes: bytes) -> pd.DataFrame:
    df = pd.read_csv(io.BytesIO(file_bytes))
    # map known names
    col_map = {}
    for c in df.columns:
        key = str(c).strip().lower()
        if key in NAME_MAP:
            col_map[c] = NAME_MAP[key]
    df = df.rename(columns=col_map)
    # ensure required cols
    for col in TARGET_PROJ_COLS:
        if col not in df.columns:
            df[col] = None
    df = df[TARGET_PROJ_COLS]
    # types
    for col in ["Points","VOR","ADP","AAV","Rank Overall","Rank Position"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["Player"] = df["Player"].astype(str).str.strip()
    df["Team"] = df["Team"].astype(str).str.strip()
    df["Position"] = df["Position"].astype(str).str.strip()
    df = df[df["Player"]!=""].reset_index(drop=True)
    return df

def left_join_update(base_df, join_df, keys, cols_to_pull):
    merged = base_df.merge(join_df[keys + cols_to_pull], on=keys, how="left", suffixes=("", "_new"))
    for c in cols_to_pull:
        nc = f"{c}_new"
        if nc in merged.columns:
            merged[c] = merged[nc].where(merged[nc].notna(), merged.get(c))
            merged.drop(columns=[nc], inplace=True, errors="ignore")
    return merged

def sync_projections_to_players(sh):
    ws_proj = sh.worksheet("Projections")
    ws_players = sh.worksheet("Players")

    df_p = ws_to_df(ws_players)
    df_r = ws_to_df(ws_proj)

    for df in (df_p, df_r):
        for k in ("Player","Team","Position"):
            if k in df.columns:
                df[k] = df[k].astype(str).str.strip()

    keys = ["Player","Team","Position"]
    pull = ["Points","VOR","ADP","AAV","Rank Overall","Rank Position"]
    for c in pull:
        if c not in df_r.columns:
            df_r[c] = pd.NA
        if c not in df_p.columns:
            df_p[c] = pd.NA

    out = left_join_update(df_p, df_r, keys, pull)
    write_dataframe_to_sheet(ws_players, out, header=True)
    return True, f"Synced {len(df_r):,} projections into Players."

# -------- RAV (Recommended Auction Value) --------
def compute_recommended_values(df_players: pd.DataFrame, total_budget_per_team=200, teams=14):
    """
    Returns df with added columns: soft_rec_$, hard_cap_$, inflation_index
    Strategy:
      - base_value = AAV if present else VOR_scaled else Points_scaled
      - inflation_index = actual_spend_so_far / expected_spend_so_far (safe if 0)
      - soft_rec_$ = base_value * inflation_adj, clamped >= $1
      - hard_cap_$ = soft_rec_$ * 1.10 (10% buffer), but not exceeding user's per-roster need heuristic
    Notes:
      - This is a league-wide baseline. Your personal team budget constraints are enforced at draft time.
    """
    df = df_players.copy()

    # Build expected spend baseline from AAV if available
    aav = pd.to_numeric(df.get("AAV"), errors="coerce")
    vor = pd.to_numeric(df.get("VOR"), errors="coerce")
    pts = pd.to_numeric(df.get("Points"), errors="coerce")

    # Simple scaling for VOR/Points if AAV missing
    vor_scaled = None
    pts_scaled = None
    if vor.notna().sum() > 0:
        # Scale VOR to typical auction pool (cap at positive VOR)
        pos_vor = vor.clip(lower=0)
        total_pos_vor = pos_vor.sum()
        total_dollars = total_budget_per_team * teams
        vor_scaled = (pos_vor / total_pos_vor * total_dollars) if total_pos_vor > 0 else pos_vor
    if pts.notna().sum() > 0 and (vor_scaled is None or vor_scaled.isna().all()):
        pos_pts = pts.clip(lower=0)
        total_pos_pts = pos_pts.sum()
        total_dollars = total_budget_per_team * teams
        pts_scaled = (pos_pts / total_pos_pts * total_dollars) if total_pos_pts > 0 else pos_pts

    # Choose base
    base = aav
    if base.isna().all() and vor_scaled is not None:
        base = vor_scaled
    if base.isna().all() and pts_scaled is not None:
        base = pts_scaled

    # Inflation index requires actual spend so far:
    # Expect the sheet to have price_paid in Players; if not, infer 0.
    actual_spend = pd.to_numeric(df.get("price_paid"), errors="coerce").fillna(0).sum()
    # expected spend so far: sum of base for drafted players
    drafted_mask = df.get("status", "").astype(str).str.lower().eq("drafted")
    if drafted_mask.any():
        expected_spend_so_far = base.where(drafted_mask, 0).sum(skipna=True)
    else:
        expected_spend_so_far = 0.0
    inflation_index = 1.0
    if expected_spend_so_far and expected_spend_so_far > 0:
        inflation_index = max(0.75, min(1.5, actual_spend / expected_spend_so_far))

    # Compute soft/hard
    soft = (base * inflation_index).fillna(0)
    soft = soft.clip(lower=1).round(0)

    hard = (soft * 1.10).round(0)

    out = df.copy()
    out["(auto) inflation_index"] = inflation_index
    out["soft_rec_$"] = soft
    out["hard_cap_$"] = hard
    return out

def write_recommendations_to_players(sh):
    ws_players = sh.worksheet("Players")
    df_p = ws_to_df(ws_players)

    # Ensure required cols exist
    for c in ["AAV","VOR","Points","status","price_paid"]:
        if c not in df_p.columns:
            df_p[c] = ""

    df_out = compute_recommended_values(df_p)
    # Merge only rec cols back
    cols_keep = list(df_p.columns)
    df_merge = df_p.merge(
        df_out[["Player","Team","Position","(auto) inflation_index","soft_rec_$","hard_cap_$"]],
        on=["Player","Team","Position"], how="left", suffixes=("","_new")
    )
    for c in ["(auto) inflation_index","soft_rec_$","hard_cap_$"]:
        if f"{c}_new" in df_merge.columns:
            df_merge[c] = df_merge[f"{c}_new"]
            df_merge.drop(columns=[f"{c}_new"], inplace=True, errors="ignore")

    write_dataframe_to_sheet(ws_players, df_merge, header=True)
    return True, "Recommendations updated (soft/hard + inflation)."

# -------- Draft Actions --------
def append_draft_log(sh, row: dict):
    ws = sh.worksheet("Draft_Log")
    header = ws.row_values(1)
    # ensure header
    needed = ["timestamp","player","team","position","manager","price","note"]
    if not header:
        ws.update("A1", [needed])
        header = needed
    # align row
    out = []
    for c in header:
        out.append(row.get(c,""))
    ws.append_row(out, value_input_option="RAW")

def update_player_drafted(sh, player_key, manager, price):
    # player_key is (Player, Team, Position)
    ws = sh.worksheet("Players")
    df = ws_to_df(ws)
    for k in ("Player","Team","Position"):
        if k not in df.columns:
            raise RuntimeError(f"Players sheet missing column: {k}")
    mask = (df["Player"]==player_key[0]) & (df["Team"]==player_key[1]) & (df["Position"]==player_key[2])
    if not mask.any():
        raise RuntimeError("Player not found in Players sheet.")
    idx = df.index[mask][0]
    # create columns if missing
    for c in ["status","drafted_by","price_paid"]:
        if c not in df.columns:
            df[c] = ""
    df.loc[idx, "status"] = "drafted"
    df.loc[idx, "drafted_by"] = manager
    df.loc[idx, "price_paid"] = str(int(price)) if pd.notna(price) and price != "" else ""

    write_dataframe_to_sheet(ws, df, header=True)
    return True

def update_league_team_budget(sh, manager, price, delta_roster=1):
    ws = sh.worksheet("League_Teams")
    df = ws_to_df(ws)
    # expected cols: team_name, budget_remaining, roster_spots_open (at least)
    for c in ["team_name","budget_remaining","roster_spots_open"]:
        if c not in df.columns:
            return False, "League_Teams sheet missing columns team_name/budget_remaining/roster_spots_open."
    # normalize name
    df["team_name"] = df["team_name"].astype(str)
    m = df["team_name"].str.strip().str.lower() == str(manager).strip().lower()
    if not m.any():
        return False, f"Manager '{manager}' not found in League_Teams."
    i = df.index[m][0]
    # update budget & roster
    try:
        b = pd.to_numeric(df.at[i, "budget_remaining"], errors="coerce")
        b = 0 if pd.isna(b) else b
        price = float(price)
        df.at[i, "budget_remaining"] = max(0, b - price)
    except Exception:
        pass
    try:
        r = pd.to_numeric(df.at[i, "roster_spots_open"], errors="coerce")
        r = 0 if pd.isna(r) else r
        df.at[i, "roster_spots_open"] = max(0, r - delta_roster)
    except Exception:
        pass
    write_dataframe_to_sheet(ws, df, header=True)
    return True, "Budget/roster updated."

def undo_last_pick(sh):
    ws_log = sh.worksheet("Draft_Log")
    rows = ws_log.get_all_values()
    if len(rows) <= 1:
        return False, "No picks to undo."
    header, data = rows[0], rows[1:]
    last = data[-1]
    rec = dict(zip(header, last))
    # Rollback player
    ws_p = sh.worksheet("Players")
    df = ws_to_df(ws_p)
    mask = (df["Player"]==rec.get("player")) & (df["Team"]==rec.get("team")) & (df["Position"]==rec.get("position"))
    if mask.any():
        idx = df.index[mask][0]
        for c in ["status","drafted_by","price_paid"]:
            if c not in df.columns: df[c] = ""
        df.loc[idx, ["status","drafted_by","price_paid"]] = ["","",""]
        write_dataframe_to_sheet(ws_p, df, header=True)
    # Rollback team budget
    try:
        update_league_team_budget(sh, rec.get("manager",""), -float(rec.get("price","0")), delta_roster=-1)
    except Exception:
        pass
    # Delete last log row
    ws_log.delete_rows(len(data)+1)
    return True, f"Undid last pick: {rec.get('player')} for ${rec.get('price')}."

# ------------- UI -------------
st.title("🏈 Auction GM — v9")

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
        st.info("Write features are optional. Add GOOGLE_SERVICE_ACCOUNT_JSON in Secrets and share your Sheet with that service account to enable them.")

    st.divider()
    st.header("Modes")
    practice = st.toggle("Practice Mode (no writes)", value=True)
    admin_mode = st.toggle("Admin Mode (show admin tools)", value=False)

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

    # Data Actions
    st.divider()
    st.header("Data Actions")
    btn_sync = st.button("🔄 Sync Projections → Players", use_container_width=True, disabled=not (write_ready and not practice))
    if btn_sync and write_ready and not practice and not st.session_state["busy"]:
        st.session_state["busy"] = True
        try:
            with st.spinner("Syncing projections into Players…"):
                ok, msg = sync_projections_to_players(sh)
            st.toast(msg if ok else f"⚠️ {msg}")
        finally:
            st.session_state["busy"] = False

    btn_recs = st.button("💡 Recompute Recommended $", use_container_width=True, disabled=not (write_ready and not practice))
    if btn_recs and write_ready and not practice and not st.session_state["busy"]:
        st.session_state["busy"] = True
        try:
            with st.spinner("Computing RAV soft/hard and inflation…"):
                ok, msg = write_recommendations_to_players(sh)
            st.toast(msg if ok else f"⚠️ {msg}")
        finally:
            st.session_state["busy"] = False

    btn_undo = st.button("↩️ Undo Last Pick", use_container_width=True, disabled=not (write_ready and not practice))
    if btn_undo and write_ready and not practice and not st.session_state["busy"]:
        st.session_state["busy"] = True
        try:
            ok, msg = undo_last_pick(sh)
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

# -------- Admin: CSV Import (hidden unless Admin Mode) --------
if write_ready and admin_mode:
    st.subheader("📥 Admin • Import Projections")
    with st.expander("Upload raw FFA CSV (click to open)", expanded=False):
        st.caption("Upload the unedited CSV from FFA Season Projections. It will be cleaned and written to the 'Projections' tab.")
        uploaded = st.file_uploader("Upload FFA CSV", type=["csv"], accept_multiple_files=False)
        if uploaded is not None:
            try:
                with st.spinner("Parsing and cleaning CSV…"):
                    cleaned = clean_projection_csv(uploaded.read())
                st.success(f"Loaded {len(cleaned):,} players. Preview below:")
                st.dataframe(cleaned.head(20), use_container_width=True)
                st.caption("Columns ↓ match the backend schema exactly:")
                st.code(", ".join(cleaned.columns.tolist()))
                can_write = write_ready and not practice
                btn_write = st.button("✍️ Write to Google Sheet → 'Projections'", type="primary", disabled=not can_write)
                if btn_write:
                    try:
                        with st.spinner("Writing cleaned data to 'Projections'…"):
                            ws_proj = upsert_worksheet(sh, "Projections", rows=max(5000, len(cleaned)+10), cols=len(TARGET_PROJ_COLS)+3)
                            write_dataframe_to_sheet(ws_proj, cleaned, header=True)
                        st.toast("✅ Projections updated in Google Sheet.")
                    except Exception as e:
                        st.error(f"Write failed: {e}")
            except Exception as e:
                st.error(f"Could not read CSV: {e}")

st.divider()

# -------- Draft Board UI --------
st.subheader("📋 Draft Board")

players_df = pd.DataFrame()
if write_ready:
    try:
        ws_p = sh.worksheet("Players")
        players_df = ws_to_df(ws_p)
    except Exception as e:
        st.error(f"Could not load Players sheet: {e}")

if not players_df.empty:
    # Normalize types and missing columns
    for c in ["Points","VOR","ADP","AAV","soft_rec_$","hard_cap_$","price_paid"]:
        if c not in players_df.columns:
            players_df[c] = ""
        players_df[c] = pd.to_numeric(players_df[c], errors="coerce")
    for c in ["status","drafted_by"]:
        if c not in players_df.columns:
            players_df[c] = ""

    # Filters row
    f1, f2, f3, f4, f5 = st.columns([2,1,1,1,1])
    with f1:
        q = st.text_input("Search name", "")
    with f2:
        pos_opts = sorted(players_df["Position"].dropna().unique().tolist())
        pos_sel = st.multiselect("Positions", pos_opts, default=[])
    with f3:
        team_opts = sorted(players_df["Team"].dropna().unique().tolist())
        team_sel = st.multiselect("Teams", team_opts, default=[])
    with f4:
        hide_drafted = st.toggle("Hide drafted", value=True)
    with f5:
        sort_by = st.selectbox("Sort by", ["Rank Overall","soft_rec_$","AAV","VOR","Points","ADP"], index=0)

    view = players_df.copy()

    # Apply filters
    if q:
        view = view[view["Player"].str.contains(q, case=False, na=False)]
    if pos_sel:
        view = view[view["Position"].isin(pos_sel)]
    if team_sel:
        view = view[view["Team"].isin(team_sel)]
    if hide_drafted:
        view = view[~view["status"].astype(str).str.lower().eq("drafted")]

    # Sort
    if sort_by in view.columns:
        # For ranks, lower is better; others higher is better
        ascending = sort_by in ["Rank Overall","ADP","Rank Position"]
        view = view.sort_values(by=sort_by, ascending=ascending, na_position="last")
    else:
        view = view.sort_values(by="Rank Overall", ascending=True, na_position="last")

    # Display limited columns for clarity
    show_cols = ["Position","Player","Team","soft_rec_$","hard_cap_$","AAV","VOR","Points","ADP","Rank Overall","status","drafted_by","price_paid"]
    for c in show_cols:
        if c not in view.columns:
            view[c] = ""
    st.dataframe(view[show_cols], use_container_width=True, height=520)

    st.markdown("### 📝 Draft a Player")
    c1, c2, c3, c4, c5 = st.columns([2,1.5,1.5,1,1.5])
    with c1:
        sel_name = st.selectbox("Player", players_df["Player"].tolist())
    with c2:
        sel_team = st.selectbox("Team", sorted(players_df.loc[players_df["Player"]==sel_name, "Team"].unique().tolist()))
    with c3:
        sel_pos  = st.selectbox("Position", sorted(players_df.loc[players_df["Player"]==sel_name, "Position"].unique().tolist()))
    with c4:
        sel_price = st.number_input("Price", min_value=1, max_value=500, step=1, value=1)
    with c5:
        sel_mgr = st.text_input("Manager", "")

    draft_btn = st.button("✅ Mark Drafted & Log", type="primary", disabled=not (write_ready and not practice))
    if draft_btn and write_ready and not practice and not st.session_state.get("busy", False):
        st.session_state["busy"] = True
        try:
            # Update player row
            ok = update_player_drafted(sh, (sel_name, sel_team, sel_pos), sel_mgr, sel_price)
            # Append log
            append_draft_log(sh, {
                "timestamp": datetime.now().isoformat(timespec="seconds"),
                "player": sel_name,
                "team": sel_team,
                "position": sel_pos,
                "manager": sel_mgr,
                "price": str(int(sel_price)),
                "note": "",
            })
            # Update team budget
            update_league_team_budget(sh, sel_mgr, sel_price, delta_roster=1)
            # Recompute recommendations (market inflation)
            write_recommendations_to_players(sh)
            st.toast(f"Drafted {sel_name} ({sel_pos} {sel_team}) for ${sel_price}.")
        except Exception as e:
            st.error(f"Draft failed: {e}")
        finally:
            st.session_state["busy"] = False
else:
    st.info("Players sheet is empty or unavailable. Use Admin → Sync Projections → Players first.")
