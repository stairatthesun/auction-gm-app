# streamlit_app.py — Auction GM (Teams, Draft Log + Trends, Best Values, Heatmap, Trap Finder)
# Clean: Phase Budgets removed; richer trends; real heatmap; Avoid-weighted trap finder; spacing polish.

import io, json
from datetime import datetime

import pandas as pd
import requests
import streamlit as st

# Optional write libs (enabled when service account is configured)
try:
    import gspread
    from google.oauth2.service_account import Credentials
except Exception:
    gspread = None
    Credentials = None

st.set_page_config(page_title="Auction GM", layout="wide")

# --------------------------- Secrets / Config ---------------------------
SHEET_ID = st.secrets.get("SHEET_ID", "")
SLEEPER_LEAGUE_ID = st.secrets.get("SLEEPER_LEAGUE_ID", "")
SA_JSON = st.secrets.get("GOOGLE_SERVICE_ACCOUNT_JSON", None)
if not SA_JSON:
    for k in ("gcp_service_account", "google_service_account"):
        if k in st.secrets:
            SA_JSON = json.dumps(dict(st.secrets[k]))
            break

# League defaults used by Reset
DEFAULT_BUDGET = 200
DEFAULT_SLOTS = {
    "open_QB": 1,
    "open_RB": 2,
    "open_WR": 2,
    "open_TE": 1,
    "open_FLEX": 1,
    "open_DST": 1,
    "open_K": 1,
    "open_BENCH": 6,
}

# Positions to *exclude* (IDP)
IDP_POS = {"LB","DL","DE","DT","EDGE","OLB","MLB","ILB","DB","CB","S","FS","SS","IDP"}
POS_KEYS = ["QB","RB","WR","TE","FLEX","K","DST","BENCH"]

# Canonical header normalization (common CSV variations)
CANON = {
    "position":"Position","pos":"Position","player_position":"Position",
    "player":"Player","name":"Player","player_name":"Player",
    "team":"Team","tm":"Team","player team":"Team","player_team":"Team",
    "points":"Points","proj_points":"Points","proj_pts":"Points","fp":"Points","fantasy_points":"Points",
    "vor":"VOR","points_vor":"VOR",
    "adp":"ADP","adp_sleeper":"ADP",
    "aav":"AAV","aav_sleeper":"AAV","auction_value":"AAV",
    "rank":"Rank Overall","overall_rank":"Rank Overall","rank_overall":"Rank Overall",
    "position_rank":"Rank Position","pos_rank":"Rank Position","rank_position":"Rank Position",
    "status":"status","drafted_by":"drafted_by","owner":"drafted_by","price_paid":"price_paid",
    "(auto)_recommended_soft$":"soft_rec_$",
    "(auto)_recommended_cap$":"hard_cap_$",
    "(auto)_inflation_global":"(auto) inflation_index",
}
IDENTITY_COLS = ["Player","Team","Position"]
PROJ_COLS = ["Points","VOR","ADP","AAV","Rank Overall","Rank Position"]
TARGET_PROJ_COLS = ["Position","Player","Team","Points","VOR","ADP","AAV","Rank Overall","Rank Position"]

# --------------------------- Utilities ---------------------------
def normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    rename = {}
    for c in list(df.columns):
        k = str(c).strip().lower()
        if k in CANON:
            rename[c] = CANON[k]
    if rename:
        df = df.rename(columns=rename)
    for c in df.columns:
        if pd.api.types.is_object_dtype(df[c]):
            df[c] = df[c].astype(str).str.strip()
    return df

def choose_keys(df_left, df_right):
    for ks in (["player_id"], ["Player","Team","Position"], ["Player","Position"], ["Player","Team"], ["Player"]):
        if all(k in df_left.columns for k in ks) and all(k in df_right.columns for k in ks):
            return ks
    return None

def get_tag_columns(df: pd.DataFrame):
    if df is None or df.empty:
        return []
    cols = [c for c in df.columns if c.upper().startswith(("FFG_","FFB_"))]
    # Ensure the standard set always exists in-memory
    needed = ["FFG_MyGuy","FFG_Sleeper","FFG_Bust","FFG_Value","FFG_Breakout","FFG_Avoid"]
    for c in needed:
        if c not in df.columns:
            df[c] = ""
            cols.append(c)
    return cols

def is_truthy(v):
    s = str(v).strip().lower()
    return s in ("1","true","yes","y")

def safe_int_or(v, default=1):
    try:
        s = pd.to_numeric(v, errors="coerce")
        if isinstance(s, pd.Series):
            s = s.iloc[0] if len(s) else None
        return int(s) if pd.notna(s) and s >= 0 else default
    except Exception:
        return default

def set_pending_draft(row_dict: dict | None):
    if row_dict is None:
        st.session_state.pop("pending_draft", None)
    else:
        st.session_state["pending_draft"] = row_dict

def clamp(v, lo, hi):
    try:
        v = float(v)
    except Exception:
        return lo
    return max(lo, min(hi, v))

# --------------------------- Sleeper ---------------------------
@st.cache_data(ttl=300)
def sleeper_get(path):
    url = f"https://api.sleeper.app/v1/{path.lstrip('/')}"
    r = requests.get(url, timeout=15)
    r.raise_for_status()
    return r.json()

# --------------------------- Google Sheets helpers ---------------------------
def service_account_client(json_str: str, sheet_id: str):
    if not json_str:
        return None, "No service account JSON in Secrets."
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

@st.cache_data(ttl=120)
def ws_to_df_cached(sheet_id: str, ws_title: str, sa_json: str):
    if not (gspread and Credentials):
        raise RuntimeError("gspread/google-auth not available.")
    info = json.loads(sa_json)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(sheet_id)
    ws = sh.worksheet(ws_title)
    rows = ws.get_all_values()
    if not rows:
        return pd.DataFrame()
    header, data = rows[0], rows[1:]
    return pd.DataFrame(data, columns=header)

def ws_to_df(ws):
    rows = ws.get_all_values()
    if not rows:
        return pd.DataFrame()
    header, data = rows[0], rows[1:]
    return pd.DataFrame(data, columns=header)

def write_dataframe_to_sheet(ws, df: pd.DataFrame, header=True):
    values = [df.columns.tolist()] + df.fillna("").astype(str).values.tolist() if header else df.fillna("").astype(str).values.tolist()
    ws.clear()
    ws.update("A1", values, value_input_option="RAW")

def upsert_worksheet(sh, title, rows=5000, cols=60):
    try:
        return sh.worksheet(title)
    except Exception:
        sh.add_worksheet(title=title, rows=rows, cols=cols)
        return sh.worksheet(title)

# --------------------------- Projections Import ---------------------------
NAME_MAP = {
    "position":"Position","pos":"Position",
    "player":"Player","name":"Player","player_name":"Player",
    "team":"Team","tm":"Team",
    "points":"Points","proj_points":"Points","proj_pts":"Points","fp":"Points",
    "vor":"VOR","points_vor":"VOR",
    "adp":"ADP","adp_sleeper":"ADP",
    "aav":"AAV","aav_sleeper":"AAV","auction_value":"AAV",
    "rank":"Rank Overall","overall_rank":"Rank Overall",
    "position_rank":"Rank Position","pos_rank":"Rank Position",
}
def clean_projection_csv(file_bytes: bytes) -> pd.DataFrame:
    df = pd.read_csv(io.BytesIO(file_bytes))
    col_map = {}
    for c in df.columns:
        k = str(c).strip().lower()
        if k in NAME_MAP:
            col_map[c] = NAME_MAP[k]
    df = df.rename(columns=col_map)
    for col in TARGET_PROJ_COLS:
        if col not in df.columns:
            df[col] = None
    df = df[TARGET_PROJ_COLS]
    for col in ["Points","VOR","ADP","AAV","Rank Overall","Rank Position"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    for c in ["Player","Team","Position"]:
        df[c] = df[c].astype(str).str.strip()
    df = df[~df["Position"].str.upper().isin(IDP_POS)].reset_index(drop=True)
    df = df.drop_duplicates(subset=["Player","Team","Position"], keep="first")
    return df

# --------------------------- Smart Sync (Projections → Players) ---------------------------
def smart_sync_projections_to_players(sh, preserve_tags=True, update_identity=False):
    ws_proj = sh.worksheet("Projections")
    ws_players = sh.worksheet("Players")

    df_p = normalize_cols(ws_to_df(ws_players))
    df_r = normalize_cols(ws_to_df(ws_proj))

    if "Position" in df_r.columns:
        df_r = df_r[~df_r["Position"].str.upper().isin(IDP_POS)]
    if {"Player","Team","Position"}.issubset(df_r.columns):
        df_r = df_r.drop_duplicates(subset=["Player","Team","Position"], keep="first")

    for c in IDENTITY_COLS:
        if c not in df_p.columns:
            df_p[c] = ""
    for c in PROJ_COLS:
        if c not in df_p.columns:
            df_p[c] = ""
        if c not in df_r.columns:
            df_r[c] = ""

    tag_cols = get_tag_columns(df_p)
    keys = choose_keys(df_p, df_r)
    if not keys:
        return False, ("Couldn’t find matching join keys. Keep Players headers, clear rows 2+, "
                       "run with identity updates ON for first sync.")

    merged = df_p.merge(df_r[IDENTITY_COLS + PROJ_COLS], how="left", on=keys, suffixes=("","_new"))
    for c in PROJ_COLS:
        nc = f"{c}_new"
        if nc in merged.columns:
            merged[c] = merged[nc].where(merged[nc].notna(), merged[c])
            merged.drop(columns=[nc], inplace=True, errors="ignore")

    left_keys = merged[keys].astype(str).apply("|".join, axis=1)
    right_only = df_r[~df_r[keys].astype(str).apply("|".join, axis=1).isin(left_keys)]
    if not right_only.empty:
        new_rows = right_only.copy()
        for c in merged.columns:
            if c not in new_rows.columns:
                if preserve_tags and c in tag_cols:
                    new_rows[c] = "FALSE"
                else:
                    new_rows[c] = ""
        merged = pd.concat([merged, new_rows[merged.columns]], ignore_index=True)

    if not update_identity:
        for c in [col for col in IDENTITY_COLS if c in df_p.columns]:
            merged[c] = merged[c].where(df_p[c].notna() & (df_p[c]!=""), df_p[c])

    if "player_id" in merged.columns:
        merged = merged.drop_duplicates(subset=["player_id"], keep="first")
    else:
        merged = merged.drop_duplicates(subset=["Player","Team","Position"], keep="first")

    if merged.empty:
        return False, "Smart Sync aborted — result was empty (likely key mismatch)."

    write_dataframe_to_sheet(ws_players, merged, header=True)
    return True, f"Smart Sync done: updated {len(df_r):,} rows; added {len(right_only):,}."

# --------------------------- Bias & Recs ---------------------------
def load_bias_map(sh):
    try:
        ws = sh.worksheet("Bias_Teams"); df = normalize_cols(ws_to_df(ws))
        if df.empty:
            return {}
        team_col = next((c for c in df.columns if c.lower() in ("team","nfl_team","tm")), None)
        if not team_col:
            return {}
        bias_col=None
        for c in df.columns:
            if c==team_col:
                continue
            ser = pd.to_numeric(df[c], errors="coerce")
            if ser.notna().any():
                bias_col=c; break
        if not bias_col:
            return {}
        out={}
        for _,r in df.iterrows():
            t=str(r.get(team_col,"")).strip()
            v=pd.to_numeric(r.get(bias_col,""), errors="coerce")
            if t and pd.notna(v):
                out[t]=float(v)
        return out
    except Exception:
        return {}

def compute_recommended_values(df_players: pd.DataFrame, bias_map=None, budget=DEFAULT_BUDGET, teams=14):
    df = df_players.copy()
    if bias_map is None:
        bias_map={}
    aav = pd.to_numeric(df.get("AAV"), errors="coerce")
    vor = pd.to_numeric(df.get("VOR"), errors="coerce")
    pts = pd.to_numeric(df.get("Points"), errors="coerce")

    base = aav.copy() if "AAV" in df.columns else pd.Series([float("nan")]*len(df))
    if base.isna().all() and vor.notna().sum()>0:
        pos_v = vor.clip(lower=0); pool=budget*teams; tv=pos_v.sum()
        base = (pos_v/tv*pool) if tv>0 else pos_v
    if base.isna().all() and pts.notna().sum()>0:
        pos_p = pts.clip(lower=0); pool=budget*teams; tp=pos_p.sum()
        base = (pos_p/tp*pool) if tp>0 else pos_p
    if base.isna().all():
        base = pd.Series([0.0]*len(df))

    paid = pd.to_numeric(df.get("price_paid"), errors="coerce").fillna(0)
    drafted = df.get("status","").astype(str).str.lower().eq("drafted")
    exp_spend = base.where(drafted, 0).sum(skipna=True)
    act_spend = paid.sum()
    inflation = 1.0
    if exp_spend and exp_spend>0:
        inflation = max(0.75, min(1.5, act_spend/exp_spend))

    team_ser = df.get("Team","").astype(str)
    bias_factor = team_ser.map(lambda t: 1.0 + (float(bias_map.get(t,0))/100.0 if t in bias_map else 0.0))
    bias_adj = bias_factor.replace(0,1.0)

    soft = (base * inflation / bias_adj).clip(lower=1).round(0)
    hard = (soft * 1.10).round(0)

    out = df.copy()
    out["(auto) inflation_index"] = inflation
    out["soft_rec_$"] = soft
    out["hard_cap_$"] = hard
    return out

def write_recommendations_to_players(sh, teams=14, budget=DEFAULT_BUDGET):
    ws = sh.worksheet("Players")
    df = normalize_cols(ws_to_df(ws))
    for c in ["AAV","VOR","Points","status","price_paid","Team"]:
        if c not in df.columns:
            df[c] = ""
    bias = load_bias_map(sh)
    out = compute_recommended_values(df, bias_map=bias, budget=budget, teams=teams)
    merged = df.merge(
        out[["Player","Team","Position","(auto) inflation_index","soft_rec_$","hard_cap_$"]],
        on=["Player","Team","Position"], how="left", suffixes=("","_new")
    )
    for c in ["(auto) inflation_index","soft_rec_$","hard_cap_$"]:
        if f"{c}_new" in merged.columns:
            merged[c] = merged[f"{c}_new"]; merged.drop(columns=[f"{c}_new"], inplace=True, errors="ignore")
    write_dataframe_to_sheet(ws, merged, header=True)
    return True, "Recommendations updated."

# --------------------------- League_Teams helpers ---------------------------
def detect_open_cols(df_league: pd.DataFrame):
    mapping={}
    cols = {c.lower(): c for c in df_league.columns}
    for pos in POS_KEYS:
        want = f"open_{pos.lower()}"
        for lc, orig in cols.items():
            if lc == want:
                mapping[pos] = orig
                break
    return mapping

def total_open_slots(row, open_map):
    total = 0
    for _, col in open_map.items():
        total += int(pd.to_numeric(row.get(col,0), errors="coerce") or 0)
    return int(max(0, total))

def has_slot_for_position(row, position, open_map):
    p = str(position).upper()
    get = lambda c: int(pd.to_numeric(row.get(c,0), errors="coerce") or 0)
    if p in ("QB","K","DST"):
        col = open_map.get(p)
        return get(col) > 0 if col else False
    if p in ("RB","WR","TE"):
        if open_map.get(p) and get(open_map[p])>0: return True
        if open_map.get("FLEX") and get(open_map["FLEX"])>0: return True
        if open_map.get("BENCH") and get(open_map["BENCH"])>0: return True
        return False
    return open_map.get("BENCH") and get(open_map["BENCH"])>0

def decrement_slot_for_pick(row, position, open_map):
    p = str(position).upper()
    def dec(col):
        val = int(pd.to_numeric(row.get(col,0), errors="coerce") or 0)
        row[col] = max(0, val - 1)
    if p in ("QB","K","DST"):
        col = open_map.get(p)
        if col: dec(col); return
        if open_map.get("BENCH"): dec(open_map["BENCH"]); return
    if p in ("RB","WR","TE"):
        if open_map.get(p) and int(pd.to_numeric(row.get(open_map[p],0), errors="coerce") or 0)>0:
            dec(open_map[p]); return
        if open_map.get("FLEX") and int(pd.to_numeric(row.get("open_FLEX", row.get("open_flex",0)), errors="coerce") or 0)>0:
            dec(open_map["FLEX"]); return
        if open_map.get("BENCH"):
            dec(open_map["BENCH"]); return
    if open_map.get("BENCH"): dec(open_map["BENCH"])

def recompute_maxbid_and_pps(row, open_map):
    b = float(pd.to_numeric(row.get("budget_remaining",0), errors="coerce") or 0)
    total = total_open_slots(row, open_map)
    max_bid = int(max(0, round(b - max(0, total - 1))))   # ensure $1 for each remaining slot
    pps = int(round(b / total)) if total>0 else int(b)
    return max_bid, pps

def update_league_team_after_pick(sh, team_name, position, price):
    ws = sh.worksheet("League_Teams")
    df = normalize_cols(ws_to_df(ws))
    if df.empty or "team_name" not in df.columns or "budget_remaining" not in df.columns:
        return False, "League_Teams missing team_name/budget_remaining."

    m = df["team_name"].astype(str).str.strip().str.lower() == str(team_name).strip().lower()
    if not m.any():
        return False, f"Team '{team_name}' not found."
    i = df.index[m][0]

    open_map = detect_open_cols(df)
    try:
        b = float(pd.to_numeric(df.at[i,"budget_remaining"], errors="coerce") or 0)
        df.at[i,"budget_remaining"] = int(max(0, round(b - float(price))))
    except Exception:
        pass

    if open_map:
        row = df.loc[i, :].to_dict()
        decrement_slot_for_pick(row, position, open_map)
        for _, col in open_map.items():
            df.at[i, col] = int(pd.to_numeric(row[col], errors="coerce") or 0)
        max_bid, pps = recompute_maxbid_and_pps(df.loc[i, :], open_map)
        if "max_bid" in df.columns: df.at[i, "max_bid"] = int(max_bid)
        if "(auto)_$per_open_slot" in df.columns: df.at[i, "(auto)_$per_open_slot"] = int(pps)
    write_dataframe_to_sheet(ws, df, header=True)
    return True, "Team updated."

# --------------------------- Draft Updates ---------------------------
def append_draft_log(sh, row: dict):
    ws = upsert_worksheet(sh, "Draft_Log")
    header = ws.row_values(1)
    needed = ["pick","player","team","position","manager","price"]
    # Create header if needed or normalize if older version
    if not header:
        ws.update("A1",[needed]); header = needed
    else:
        # Backward-compat: if header misses 'pick' or others, rebuild
        want = set(needed)
        have = [h.strip() for h in header]
        if set(have) != want:
            ws.clear()
            ws.update("A1",[needed]); header = needed
    # Compute next pick number from current rows
    rows = ws.get_all_values()
    current_picks = max(0, len(rows)-1)
    row["pick"] = current_picks + 1
    out = [row.get(c,"") for c in header]
    ws.append_row(out, value_input_option="RAW")

def update_player_drafted(sh, player_key, manager, price):
    ws = sh.worksheet("Players")
    df = normalize_cols(ws_to_df(ws))
    mask = (df["Player"]==player_key[0]) & (df["Team"]==player_key[1]) & (df["Position"]==player_key[2])
    if not mask.any():
        raise RuntimeError("Player not found in Players sheet.")
    idx = df.index[mask][0]
    for c in ["status","drafted_by","price_paid"]:
        if c not in df.columns:
            df[c]=""
    df.loc[idx,"status"]="drafted"
    df.loc[idx,"drafted_by"]=manager
    df.loc[idx,"price_paid"]=str(int(price)) if pd.notna(price) and price!="" else ""
    write_dataframe_to_sheet(ws, df, header=True)
    return True

# --------------------------- Reset & Archive ---------------------------
def ensure_league_columns(df):
    need_cols = ["team_name","budget_remaining","players_owned","max_bid","(auto)_$per_open_slot"]
    for k in DEFAULT_SLOTS.keys():
        need_cols.append(k)
    for c in need_cols:
        if c not in df.columns:
            df[c] = 0 if c!="team_name" else ""
    return df

def ensure_player_columns(df):
    need_cols = ["status","drafted_by","price_paid","(auto) inflation_index","soft_rec_$","hard_cap_$",
                 "FFG_MyGuy","FFG_Sleeper","FFG_Bust","FFG_Value","FFG_Breakout","FFG_Avoid"]
    for c in need_cols:
        if c not in df.columns:
            df[c] = ""
    return df

def reset_players_and_league(sh):
    try:
        ws = sh.worksheet("Players")
        df = normalize_cols(ws_to_df(ws))
        df = ensure_player_columns(df)
        df["status"] = ""
        df["drafted_by"] = ""
        df["price_paid"] = ""
        df["(auto) inflation_index"] = ""
        df["soft_rec_$"] = ""
        df["hard_cap_$"] = ""
        write_dataframe_to_sheet(ws, df, header=True)
    except Exception as e:
        return False, f"Players reset failed: {e}"

    try:
        ws = sh.worksheet("League_Teams")
        df = normalize_cols(ws_to_df(ws))
        df = ensure_league_columns(df)
        if df.empty or "team_name" not in df.columns:
            return False, "League_Teams missing 'team_name' column."
        first_row_idx = df.index[0]
        if str(df.loc[first_row_idx,"team_name"]).strip().lower() != "my team":
            return False, "League_Teams row 2 must be 'My Team'. Fix and rerun."
        for i in df.index:
            df.at[i,"budget_remaining"] = int(DEFAULT_BUDGET)
            for k, default_val in DEFAULT_SLOTS.items():
                start_col = f"{k}_start"
                if start_col in df.columns and pd.notna(df.at[i,start_col]) and str(df.at[i,start_col]).strip()!="":
                    df.at[i, k] = int(pd.to_numeric(df.at[i,start_col], errors="coerce") or default_val)
                else:
                    df.at[i, k] = int(default_val)
            open_map = detect_open_cols(df)
            max_bid, pps = recompute_maxbid_and_pps(df.loc[i,:], open_map)
            df.at[i,"max_bid"] = int(max_bid)
            df.at[i,"(auto)_$per_open_slot"] = int(pps)
        write_dataframe_to_sheet(ws, df, header=True)
    except Exception as e:
        return False, f"League_Teams reset failed: {e}"

    try:
        ws_log = upsert_worksheet(sh, "Draft_Log")
        ws_log.clear()
        ws_log.update("A1", [["pick","player","team","position","manager","price"]])
    except Exception as e:
        return False, f"Draft_Log reset failed: {e}"

    return True, "Reset done."

def archive_current_state(sh):
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    try:
        ws_p = sh.worksheet("Players")
        dfp = normalize_cols(ws_to_df(ws_p))
        ws_a = upsert_worksheet(sh, f"Archive_Players_{ts}")
        write_dataframe_to_sheet(ws_a, dfp, header=True)
    except Exception as e:
        return False, f"Archive Players failed: {e}"
    try:
        ws_l = sh.worksheet("League_Teams")
        dfl = normalize_cols(ws_to_df(ws_l))
        ws_b = upsert_worksheet(sh, f"Archive_League_{ts}")
        write_dataframe_to_sheet(ws_b, dfl, header=True)
    except Exception as e:
        return False, f"Archive League_Teams failed: {e}"
    return True, f"Archived to Archive_Players_{ts} & Archive_League_{ts}."

# --------------------------- Nomination Recs ---------------------------
def build_nomination_list(players_df: pd.DataFrame, league_df: pd.DataFrame, top_n: int = 8):
    df = players_df.copy() if players_df is not None else pd.DataFrame()
    needed_cols = ["status","Position","Player","Team","soft_rec_$","AAV","ADP","VOR","Points","Rank Position"]
    for c in needed_cols:
        if c not in df.columns:
            df[c] = "" if c not in ("soft_rec_$","AAV","ADP","VOR","Points","Rank Position") else float("nan")
    df = df[~df["status"].astype(str).str.lower().eq("drafted")].copy()
    for c in ["soft_rec_$","AAV","ADP","VOR","Points","Rank Position"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    if df.empty:
        return pd.DataFrame(), pd.DataFrame()

    base_val = df["AAV"].copy()
    if base_val.isna().all():
        base_val = (df["soft_rec_$"] / 1.15)
    df["value_surplus"] = df["soft_rec_$"] - base_val

    pos_list = df["Position"].dropna().unique().tolist()
    pos_supply = {p: max(1, int((df["Position"] == p).sum())) for p in pos_list}

    scarcity = {}
    if league_df is not None and not league_df.empty:
        open_map = detect_open_cols(league_df)
        if open_map:
            flex_total = 0
            if "FLEX" in open_map:
                fcol = open_map["FLEX"]
                flex_total = pd.to_numeric(league_df.get(fcol), errors="coerce").fillna(0).sum()
            for p in pos_list:
                base_need = 0
                if p in open_map:
                    base_need = pd.to_numeric(league_df.get(open_map[p]), errors="coerce").fillna(0).sum()
                extra = round(flex_total / 3) if p in ("RB","WR","TE") else 0
                scarcity[p] = max(0, int(base_need) + int(extra))

    def sca(p):
        need = scarcity.get(p, 0)
        sup  = max(1, pos_supply.get(p, 1))
        return float(need) / float(sup)

    df["scarcity_factor"] = df["Position"].map(sca).fillna(0.0)

    outbid_counts = []
    if league_df is not None and not league_df.empty and "budget_remaining" in league_df.columns:
        open_map = detect_open_cols(league_df)
        for _, r in df.iterrows():
            price = r.get("soft_rec_$")
            pos   = r.get("Position","")
            cnt = 0
            for _, t in league_df.iterrows():
                br = pd.to_numeric(t.get("budget_remaining",""), errors="coerce")
                if pd.isna(price) or pd.isna(br) or br < price:
                    continue
                if open_map and not has_slot_for_position(t, pos, open_map):
                    continue
                cnt += 1
            outbid_counts.append(cnt)
    df["outbid_count"] = outbid_counts if outbid_counts else 0

    val_surplus  = (df["value_surplus"]  - df["value_surplus"].median(skipna=True)).fillna(0)
    scarcity_norm= (df["scarcity_factor"]- df["scarcity_factor"].median(skipna=True)).fillna(0)
    outbid_norm  = (df["outbid_count"]  - df["outbid_count"].median(skipna=True)).fillna(0)
    rp = pd.to_numeric(df.get("Rank Position"), errors="coerce")
    rp_inv = (-rp.fillna(rp.max() if rp.notna().any() else 999)).fillna(0)

    df["nom_score"] = 0.45*val_surplus + 0.30*scarcity_norm + 0.15*outbid_norm + 0.10*rp_inv

    value_targets = df.sort_values(["nom_score"], ascending=False).head(top_n).copy()
    enforcers    = df.sort_values(["outbid_count","scarcity_factor"], ascending=[False,False]).head(top_n).copy()

    def reason(row):
        parts=[]
        if pd.notna(row.get("value_surplus")) and row["value_surplus"]>0: parts.append(f"+${int(row['value_surplus'])} surplus")
        if pd.notna(row.get("scarcity_factor")) and row["scarcity_factor"]>0.5: parts.append("scarce pos")
        if pd.notna(row.get("outbid_count")) and row["outbid_count"]>=3: parts.append(f"{int(row['outbid_count'])} can outbid")
        return " • ".join(parts) if parts else "balanced"

    if not value_targets.empty:
        value_targets["why"] = value_targets.apply(reason, axis=1)
    if not enforcers.empty:
        enforcers["why"] = enforcers.apply(reason, axis=1)

    return value_targets, enforcers

# --------------------------- UI ---------------------------
st.title("🏈 Auction GM")

# Sidebar: connect + actions
with st.sidebar:
    st.header("Connect")
    st.write(f"**Sheet ID:** {'✅ set' if SHEET_ID else '❌ missing'}")
    st.write(f"**Sleeper League ID:** {'✅ set' if SLEEPER_LEAGUE_ID else '❌ missing'}")

    write_ready=False; sa_email=None; sh=None
    if SA_JSON and SHEET_ID:
        sh, err = service_account_client(SA_JSON, SHEET_ID)
        if err: st.warning(f"Write access not ready: {err}")
        else:
            write_ready=True
            try: sa_email=json.loads(SA_JSON).get("client_email")
            except Exception: sa_email=None
            st.success("Write access enabled.")
            if sa_email: st.caption(f"Shared: {sa_email}")
    else:
        st.info("Write features optional. Add GOOGLE_SERVICE_ACCOUNT_JSON & share your Sheet.")

    st.divider()
    st.header("Modes")
    practice = st.toggle("Practice Mode (no writes)", value=True)
    admin_mode = st.toggle("Admin Mode (show admin tools)", value=False)

    st.divider()
    st.header("Data Actions")
    preserve_tags = st.toggle("Preserve tags on sync", value=True)
    update_identity = st.toggle("Allow identity updates (first sync)", value=True)

    btn_sync = st.button("🔄 Smart Sync: Projections → Players", use_container_width=True, disabled=not (write_ready and not practice))
    if btn_sync and write_ready and not practice:
        with st.spinner("Smart syncing…"):
            ok,msg = smart_sync_projections_to_players(sh, preserve_tags=preserve_tags, update_identity=update_identity)
        st.toast(msg if ok else f"⚠️ {msg}")
        st.cache_data.clear(); st.rerun()

    btn_recs = st.button("💡 Recompute Recommended $", use_container_width=True, disabled=not (write_ready and not practice))
    if btn_recs and write_ready and not practice:
        teams=14; budget=DEFAULT_BUDGET
        try:
            ws = sh.worksheet("Settings_League")
            df = normalize_cols(ws_to_df(ws))
            for c in df.columns:
                if c.lower()=="teams":
                    v=pd.to_numeric(df[c], errors="coerce")
                    if v.notna().any(): teams=int(v.dropna().iloc[0])
                if c.lower()=="budget":
                    v=pd.to_numeric(df[c], errors="coerce")
                    if v.notna().any(): budget=int(v.dropna().iloc[0])
        except Exception: pass
        with st.spinner("Computing $…"):
            ok,msg = write_recommendations_to_players(sh, teams=teams, budget=budget)
        st.toast(msg if ok else f"⚠️ {msg}")
        st.cache_data.clear(); st.rerun()

    # --- Refresh Data (Clear Cache) ---
    st.divider()
    if st.button("🔁 Refresh Data (clear cache)", use_container_width=True):
        st.cache_data.clear()
        st.toast("Caches cleared. Reloading…")
        st.rerun()

    if admin_mode:
        st.divider()
        st.header("Admin")
        up = st.file_uploader("📥 Import Projections CSV → Projections sheet", type=["csv"], accept_multiple_files=False, key="proj_csv")
        mode = st.selectbox("Write mode", ["Replace Projections", "Append to Projections"], index=0)
        do_import = st.button("Import CSV", use_container_width=True, disabled=not (write_ready and up is not None and not practice))
        if do_import and write_ready and not practice:
            try:
                df_clean = clean_projection_csv(up.read())
                ws = upsert_worksheet(sh, "Projections")
                if mode.startswith("Replace"):
                    write_dataframe_to_sheet(ws, df_clean, header=True)
                    st.success(f"Replaced Projections with {len(df_clean):,} rows.")
                else:
                    existing = normalize_cols(ws_to_df(ws))
                    if existing.empty:
                        write_dataframe_to_sheet(ws, df_clean, header=True)
                        st.success(f"Wrote {len(df_clean):,} rows to empty Projections.")
                    else:
                        allc = pd.concat([existing, df_clean], ignore_index=True)
                        allc = allc.drop_duplicates(subset=["Player","Team","Position"], keep="first")
                        write_dataframe_to_sheet(ws, allc, header=True)
                        st.success(f"Appended; Projections now {len(allc):,} rows.")
                st.toast("Now run Smart Sync.")
                st.cache_data.clear(); st.rerun()
            except Exception as e:
                st.error(f"Import failed: {e}")

        c1, c2 = st.columns(2)
        with c1:
            if st.button("🗂️ Reset & Archive", use_container_width=True, disabled=not (write_ready and not practice)):
                okA, msgA = archive_current_state(sh)
                okR, msgR = reset_players_and_league(sh)
                st.toast(msgA if okA else f"⚠️ {msgA}")
                st.toast(msgR if okR else f"⚠️ {msgR}")
                st.cache_data.clear(); st.rerun()
        with c2:
            if st.button("♻️ Reset (no archive)", use_container_width=True, disabled=not (write_ready and not practice)):
                okR, msgR = reset_players_and_league(sh)
                st.toast(msgR if okR else f"⚠️ {msgR}")
                st.cache_data.clear(); st.rerun()

# Tiny league header
if SLEEPER_LEAGUE_ID:
    try:
        league = sleeper_get(f"league/{SLEEPER_LEAGUE_ID}")
        st.caption(f"**{league.get('name','—')}** • {league.get('total_rosters','—')} teams • Season {league.get('season','—')}")
    except Exception:
        pass

# ----------------- Load data (cached) -----------------
players_df = pd.DataFrame()
league_df = pd.DataFrame()
draft_log_df = pd.DataFrame()
if 'sh' in locals() and write_ready:
    try:
        players_df = normalize_cols(ws_to_df_cached(SHEET_ID, "Players", SA_JSON))
        if "player_id" in players_df.columns:
            players_df = players_df.drop_duplicates(subset=["player_id"], keep="first")
        elif {"Player","Team","Position"}.issubset(players_df.columns):
            players_df = players_df.drop_duplicates(subset=["Player","Team","Position"], keep="first")
        if "Position" in players_df.columns:
            players_df = players_df[~players_df["Position"].str.upper().isin(IDP_POS)]
    except Exception as e:
        st.error(f"Could not load Players sheet: {e}")
    try:
        league_df = normalize_cols(ws_to_df_cached(SHEET_ID, "League_Teams", SA_JSON))
        if not league_df.empty and "team_name" in league_df.columns:
            first_idx = league_df.index[0]
            val = str(league_df.loc[first_idx, "team_name"]).strip().lower()
            if val != "my team":
                st.error("League_Teams row 2 must be 'My Team'. Please fix and reload.")
                st.stop()
    except Exception:
        league_df = pd.DataFrame()
    try:
        draft_log_df = normalize_cols(ws_to_df_cached(SHEET_ID, "Draft_Log", SA_JSON))
    except Exception:
        draft_log_df = pd.DataFrame()

# Ensure required columns exist (avoid KeyErrors)
if players_df is None or players_df.empty:
    players_df = pd.DataFrame()
for c in ["status","Position","Player","Team","soft_rec_$","AAV","ADP","VOR","Points","Rank Overall","Rank Position","drafted_by","price_paid"]:
    if c not in players_df.columns:
        players_df[c] = (float("nan") if c in ("soft_rec_$","AAV","ADP","VOR","Points","Rank Overall","Rank Position") else "")

# --------------------------- Helper views ---------------------------
def build_team_roster_view(players: pd.DataFrame, team_name: str):
    drafted = players[(players.get("status","").astype(str).str.lower()=="drafted") &
                      (players.get("drafted_by","").astype(str)==team_name)].copy()

    rows = [{"Slot":"QB","Player":"","Pos":"","Price":""},
            {"Slot":"RB","Player":"","Pos":"","Price":""},
            {"Slot":"RB","Player":"","Pos":"","Price":""},
            {"Slot":"WR","Player":"","Pos":"","Price":""},
            {"Slot":"WR","Player":"","Pos":"","Price":""},
            {"Slot":"TE","Player":"","Pos":"","Price":""},
            {"Slot":"FLEX","Player":"","Pos":"","Price":""}]
    for i in range(DEFAULT_SLOTS.get("open_BENCH", 6)):
        rows.append({"Slot":f"BENCH{i+1}","Player":"","Pos":"","Price":""})

    def place(player_row):
        pos = str(player_row["Position"]).upper()
        price = safe_int_or(player_row.get("price_paid"), default=0)
        name = str(player_row["Player"])
        if pos in ("QB","RB","WR","TE"):
            for r in rows:
                if r["Slot"]==pos and r["Player"]=="":
                    r.update({"Player":name,"Pos":pos,"Price":f"${price}"}); return
            if pos in ("RB","WR","TE"):
                for r in rows:
                    if r["Slot"]=="FLEX" and r["Player"]=="":
                        r.update({"Player":name,"Pos":pos,"Price":f"${price}"}); return
        for r in rows:
            if r["Slot"].startswith("BENCH") and r["Player"]=="":
                r.update({"Player":name,"Pos":pos,"Price":f"${price}"}); return

    for _, prow in drafted.sort_values(by="price_paid", ascending=False).iterrows():
        place(prow)
    return pd.DataFrame(rows)

def biggest_needs_for_team(league_row: pd.Series, players_pool: pd.DataFrame):
    open_map = detect_open_cols(pd.DataFrame([league_row]))
    p = players_pool.copy()
    p = p[~p.get("status","").astype(str).str.lower().eq("drafted")]
    thr_ser = pd.to_numeric(p.get("soft_rec_$"), errors="coerce")
    thresh = thr_ser.median(skipna=True) if thr_ser.notna().any() else 0

    needs = []
    for pos_key, open_col in open_map.items():
        if pos_key not in POS_KEYS: 
            continue
        slots_left = safe_int_or(league_row.get(open_col), 0)
        if slots_left <= 0:
            continue
        if pos_key in ("FLEX","BENCH"):
            continue
        mask_pos = p.get("Position","").astype(str).str.upper().eq(pos_key)
        hp = pd.to_numeric(p.loc[mask_pos, "soft_rec_$"], errors="coerce")
        left_cnt = int((hp >= thresh).sum()) if hp.notna().any() else 0
        needs.append((pos_key, int(slots_left), int(left_cnt)))

    needs.sort(key=lambda x: (x[1], -x[2]), reverse=True)
    return needs

# --------------------------- CSS spacing (subtle gutters) ---------------------------
st.markdown("""
<style>
.block-container { padding-top: 1.2rem; }
.section-gap { margin-top: 0.6rem; margin-bottom: 0.6rem; }
.top-row-module { padding: 0.25rem 0.35rem; }
</style>
""", unsafe_allow_html=True)

# --------------------------- NEW TOP ROW: Draft Log + Trends | Best Values | Teams ---------------------------
st.divider()
c_log, c_best, c_team = st.columns([1.2,1.1,1.2], vertical_alignment="top")

with c_log:
    st.container().markdown("<div class='top-row-module'></div>", unsafe_allow_html=True)
    st.subheader("🧾 Draft Log + Live Trends")
    # Draft Log (show latest 12, with Pick #)
    if draft_log_df is not None and not draft_log_df.empty:
        # Normalize columns (support legacy)
        d = draft_log_df.copy()
        # If no 'pick', derive it
        if "pick" not in d.columns:
            d.insert(0, "pick", range(1, len(d)+1))
        for c in ["player","team","position","manager","price","pick"]:
            if c not in d.columns: d[c] = ""
        d = d[["pick","player","team","position","manager","price"]].copy()
        d["pick"] = pd.to_numeric(d["pick"], errors="coerce").fillna(0).astype(int)
        d = d.sort_values(by="pick", ascending=False).head(12).sort_values(by="pick")
        st.dataframe(d, hide_index=True, height=220, use_container_width=True)
    else:
        st.caption("No picks yet.")

    # Live Trends
    try:
        dfp = players_df.copy()
        dfp["soft_rec_$"] = pd.to_numeric(dfp.get("soft_rec_$"), errors="coerce")
        dfp["price_paid"] = pd.to_numeric(dfp.get("price_paid"), errors="coerce")
        drafted_mask = dfp.get("status","").astype(str).str.lower().eq("drafted")
        exp = dfp.loc[drafted_mask, "soft_rec_$"].sum(skipna=True)
        act = dfp.loc[drafted_mask, "price_paid"].sum(skipna=True)
        infl = clamp((act/exp) if exp and exp>0 else 1.0, 0.5, 1.8)

        # Spend velocity & liquidity
        picks = len(draft_log_df) if (draft_log_df is not None and not draft_log_df.empty) else 0
        total_bankroll = DEFAULT_BUDGET * (league_df["team_name"].nunique() if not league_df.empty else 14)
        avg_spend_per_pick = (act / picks) if picks>0 else 0
        projected_picks_to_zero = int((total_bankroll - act) / avg_spend_per_pick) if avg_spend_per_pick>0 else 0

        st.metric("Inflation (est.)", f"{infl:.2f}×")
        c1, c2 = st.columns(2)
        c1.caption(f"Total spent: **${int(act):,}** • Picks: **{picks}** • Avg/pick: **${int(avg_spend_per_pick):,}**")
        c2.caption(f"Remaining bankroll: **${int(total_bankroll - act):,}** • ~picks to zero: **{projected_picks_to_zero}**")

        # Positional heat (paid / expected for last 12 picks)
        if draft_log_df is not None and not draft_log_df.empty:
            recent = draft_log_df.copy()
            if "pick" not in recent.columns:
                recent.insert(0, "pick", range(1, len(recent)+1))
            recent = recent.sort_values(by="pick", ascending=False).head(12)
            merged = recent.merge(
                players_df[["Player","Team","Position","soft_rec_$"]],
                left_on=["player","team","position"],
                right_on=["Player","Team","Position"],
                how="left"
            )
            merged["price"] = pd.to_numeric(merged.get("price"), errors="coerce")
            merged["soft_rec_$"] = pd.to_numeric(merged.get("soft_rec_$"), errors="coerce")
            pos_ratio = merged.groupby("position").apply(
                lambda g: (g["price"].sum() / g["soft_rec_$"].sum()) if g["soft_rec_$"].sum()>0 else 1.0
            ).rename("heat").reset_index()
            if not pos_ratio.empty:
                pos_ratio["heat"] = pos_ratio["heat"].map(lambda x: f"{x:.2f}×")
                st.caption("Last 12 picks market heat (paid ÷ expected):")
                st.dataframe(pos_ratio, hide_index=True, use_container_width=True, height=100)
    except Exception:
        pass

with c_best:
    st.container().markdown("<div class='top-row-module'></div>", unsafe_allow_html=True)
    st.subheader("💎 Best Remaining Values")
    if players_df.empty:
        st.caption("Load players first.")
    else:
        v = players_df.copy()
        for c in ["soft_rec_$","AAV","VOR","Points","ADP"]:
            if c in v.columns: v[c]=pd.to_numeric(v[c], errors="coerce")
        v = v[~v.get("status","").astype(str).str.lower().eq("drafted")]
        # Local multi-position filter
        pos_opts = sorted([p for p in v.get("Position", pd.Series()).dropna().unique().tolist() if p])
        pos_sel = st.multiselect("Positions", pos_opts, default=pos_opts, key="best_values_positions")
        if pos_sel:
            v = v[v["Position"].isin(pos_sel)]
        base = v["AAV"].copy()
        if base.isna().all():
            base = (v["soft_rec_$"]/1.15)
        v["surplus_$"] = (v["soft_rec_$"] - base).round(0)
        cols = ["Position","Player","Team","soft_rec_$","AAV","surplus_$","VOR","ADP"]
        for c in cols:
            if c not in v.columns: v[c]=""
        v = v.sort_values(by=["surplus_$","soft_rec_$"], ascending=[False,False]).head(12)
        st.dataframe(v[cols], hide_index=True, height=300, use_container_width=True)

with c_team:
    st.container().markdown("<div class='top-row-module'></div>", unsafe_allow_html=True)
    st.subheader("👥 Teams")
    if league_df.empty or "team_name" not in league_df.columns:
        st.caption("League_Teams not available.")
    else:
        team_options = [t for t in league_df["team_name"].astype(str).tolist() if t.strip()]
        default_team = "My Team" if "My Team" in team_options else (team_options[0] if team_options else "")
        selected_team = st.selectbox("Team", team_options, index=team_options.index(default_team) if default_team in team_options else 0, key="teams_selector")
        roster_df = build_team_roster_view(players_df, selected_team)
        st.dataframe(roster_df, hide_index=True, height=290, use_container_width=True)
        lr = league_df.loc[league_df["team_name"].astype(str)==selected_team]
        if not lr.empty:
            row = lr.iloc[0]
            open_map = detect_open_cols(league_df)
            total_slots = 0
            if open_map:
                for _, col in open_map.items():
                    total_slots += safe_int_or(row.get(col), 0)
            budget_rem = safe_int_or(row.get("budget_remaining"), DEFAULT_BUDGET)
            st.caption(f"Spots remaining: **{total_slots}** • Budget remaining: **${budget_rem}** • $/slot ≈ **${(budget_rem//max(1,total_slots))}**")
            needs = biggest_needs_for_team(row, players_df)
            if needs:
                txt = " • ".join([f"{p} ({s} slots, {hv} HV left)" for p,s,hv in needs[:3]])
                st.caption(f"Top needs: {txt}")
            else:
                st.caption("Top needs: —")

# --------------------------- Filters ---------------------------
st.divider()
f1,f2,f3,f4,f5 = st.columns([2,1,1,1,1])
with f1: q = st.text_input("Search", "")
with f2:
    pos_opts = sorted([p for p in players_df.get("Position", pd.Series()).dropna().unique().tolist() if p])
    pos_sel = st.multiselect("Positions", pos_opts, default=[])
with f3:
    team_opts = sorted([t for t in players_df.get("Team", pd.Series()).dropna().unique().tolist() if t])
    team_sel = st.multiselect("Teams", team_opts, default=[])
with f4: hide_drafted = st.toggle("Hide drafted", value=True)
with f5: sort_by = st.selectbox("Sort by", ["Rank Overall","soft_rec_$","AAV","VOR","Points","ADP","Rank Position"], index=0)

# --------------------------- Draft Board ---------------------------
st.subheader("📋 Draft Board")

if players_df.empty:
    st.info("Players sheet is empty or unavailable. Import Projections and run Smart Sync.")
else:
    view = players_df.copy()
    for c in ["Points","VOR","ADP","AAV","soft_rec_$","hard_cap_$","price_paid","Rank Overall","Rank Position"]:
        if c in view.columns:
            view[c] = pd.to_numeric(view[c], errors="coerce")

    def tag_icons(row):
        out=[]
        if is_truthy(row.get("FFG_MyGuy", row.get("FFB_MyGuy",""))): out.append("⭐")
        if is_truthy(row.get("FFG_Sleeper", row.get("FFB_Sleeper",""))): out.append("💤")
        if is_truthy(row.get("FFG_Bust", row.get("FFB_Bust",""))): out.append("⚠️")
        if is_truthy(row.get("FFG_Value", row.get("FFB_Value",""))): out.append("💎")
        if is_truthy(row.get("FFG_Breakout", row.get("FFB_Breakout",""))): out.append("🚀")
        if is_truthy(row.get("FFG_Avoid","")): out.append("⛔")
        return " ".join(out)
    view["Tags"] = view.apply(tag_icons, axis=1) if not view.empty else ""

    if q: view = view[view["Player"].str.contains(q, case=False, na=False)]
    if pos_sel: view = view[view["Position"].isin(pos_sel)]
    if team_sel: view = view[view["Team"].isin(team_sel)]
    if hide_drafted and "status" in view.columns:
        view = view[~view["status"].astype(str).str.lower().eq("drafted")]

    ascending = sort_by in ["Rank Overall","ADP","Rank Position"]
    if sort_by in view.columns:
        view = view.sort_values(by=sort_by, ascending=ascending, na_position="last")
    else:
        view = view.sort_values(by="Rank Overall", ascending=True, na_position="last")

    show_cols = ["Tags","Position","Player","Team","soft_rec_$","hard_cap_$","AAV","VOR","Points","ADP","Rank Overall","status","drafted_by","price_paid"]
    for c in show_cols:
        if c not in view.columns: view[c]=""

    board = view.copy()
    board.insert(0, "Draft", False)

    edited = st.data_editor(
        board[["Draft"] + show_cols],
        use_container_width=True,
        height=520,
        hide_index=True,
        disabled=show_cols,
        column_config={
            "Draft": st.column_config.CheckboxColumn(width="small"),
            "soft_rec_$": st.column_config.NumberColumn(format="$%d"),
            "hard_cap_$": st.column_config.NumberColumn(format="$%d"),
            "AAV": st.column_config.NumberColumn(format="$%d"),
            "VOR": st.column_config.NumberColumn(format="%d"),
            "Points": st.column_config.NumberColumn(format="%.1f"),
            "ADP": st.column_config.NumberColumn(format="%.1f"),
            "price_paid": st.column_config.NumberColumn(format="$%d"),
        },
    )

    sel = edited[edited["Draft"] == True]
    c1, c2, _ = st.columns([1,1,6])
    with c1:
        do_draft = st.button(
            "✅ Draft Selected",
            type="primary",
            use_container_width=True,
            disabled=not (len(sel)==1 and write_ready and not practice),
            key="btn_draft_selected"
        )
    with c2:
        if st.button("✖️ Clear Picks", use_container_width=True, key="btn_clear_picks"):
            st.cache_data.clear(); st.rerun()

    if do_draft and len(sel) == 1 and write_ready and not practice:
        row = sel.iloc[0]
        set_pending_draft({
            "player": str(row["Player"]),
            "team": str(row["Team"]),
            "pos": str(row["Position"]),
            "soft": safe_int_or(row.get("soft_rec_$"), default=1)
        })
        st.rerun()

    if "pending_draft" in st.session_state:
        pdraft = st.session_state["pending_draft"]
        st.info(f"Drafting **{pdraft['player']}** ({pdraft['pos']} · {pdraft['team']})")
        with st.form(key="confirm_draft_form", clear_on_submit=False):
            mgr_opts = []
            if not league_df.empty and "team_name" in league_df.columns:
                mgr_opts = sorted([t for t in league_df["team_name"].dropna().astype(str).tolist() if t])
            price = st.number_input("Price", min_value=1, max_value=500, step=1, value=int(pdraft["soft"]), key="confirm_price")
            mgr = st.selectbox("Team (buyer)", mgr_opts if mgr_opts else [""], index=0 if mgr_opts else 0, key="confirm_mgr")
            colF1, colF2 = st.columns([1,1])
            confirm = colF1.form_submit_button("Confirm Draft", type="primary", disabled=not (write_ready and not practice))
            cancel  = colF2.form_submit_button("Cancel")
        if cancel:
            set_pending_draft(None); st.rerun()
        if confirm:
            try:
                update_player_drafted(sh, (pdraft["player"], pdraft["team"], pdraft["pos"]), mgr, safe_int_or(price,1))
                append_draft_log(sh, {
                    "player": pdraft["player"], "team": pdraft["team"], "position": pdraft["pos"],
                    "manager": mgr, "price": str(int(safe_int_or(price,1))),
                })
                ok,msg = update_league_team_after_pick(sh, mgr, pdraft["pos"], safe_int_or(price,1))
                if not ok: st.warning(msg)
                write_recommendations_to_players(sh)
                set_pending_draft(None)
                st.toast(f"Drafted {pdraft['player']} for ${int(safe_int_or(price,1))} to {mgr}.")
                st.cache_data.clear(); st.rerun()
            except Exception as e:
                st.error(f"Draft failed: {e}")

# --------------------------- League: #1 Need per Team (compact) ---------------------------
st.subheader("📊 League: #1 Need per Team")
if not league_df.empty and "team_name" in league_df.columns:
    rows=[]
    for _, t in league_df.iterrows():
        needs = biggest_needs_for_team(t, players_df)
        if needs:
            pos, slots, hv = needs[0]
            rows.append({"Team": t.get("team_name",""), "Top Need": pos, "Slots Left": slots, "Hi-Value Left": hv})
        else:
            rows.append({"Team": t.get("team_name",""), "Top Need": "—", "Slots Left": 0, "Hi-Value Left": 0})
    need_df = pd.DataFrame(rows)
    st.dataframe(need_df, hide_index=True, use_container_width=True, height=200)
else:
    st.caption("League_Teams not available.")

# --------------------------- Nomination Recs ---------------------------
with st.expander("🧠 Nomination Recommendations (position-aware)", expanded=False):
    val_list, enf_list = build_nomination_list(players_df if not players_df.empty else pd.DataFrame(),
                                               league_df if not league_df.empty else pd.DataFrame(),
                                               top_n=8)
    c_left, c_right = st.columns(2)
    with c_left:
        st.markdown("**Value Targets**")
        if val_list.empty:
            st.caption("No candidates found.")
        else:
            cols = ["Position","Player","Team","soft_rec_$","AAV","value_surplus","why"]
            for c in cols:
                if c not in val_list.columns: val_list[c]=""
            st.dataframe(val_list[cols], hide_index=True, use_container_width=True, height=240)
    with c_right:
        st.markdown("**Price Enforcers**")
        if enf_list.empty:
            st.caption("No candidates found.")
        else:
            cols = ["Position","Player","Team","soft_rec_$","AAV","outbid_count","why"]
            for c in cols:
                if c not in enf_list.columns: enf_list[c]=""
            st.dataframe(enf_list[cols], hide_index=True, use_container_width=True, height=240)

# --------------------------- NEW: Bidding Heatmap (intelligent, color) ---------------------------
with st.expander("🔥 Bidding Heatmap (position pressure)", expanded=False):
    if league_df.empty or "team_name" not in league_df.columns or players_df.empty:
        st.caption("League_Teams / Players not available.")
    else:
        # Compute league scarcity per position (demand/supply)
        open_map = detect_open_cols(league_df)
        remaining = players_df[~players_df.get("status","").astype(str).str.lower().eq("drafted")]
        pos_supply = remaining["Position"].str.upper().value_counts().to_dict()
        demand = {p: 0 for p in ["QB","RB","WR","TE","FLEX","K","DST"]}
        if open_map:
            # FLEX spread across RB/WR/TE evenly
            flex_sum = int(pd.to_numeric(league_df.get(open_map.get("FLEX","")), errors="coerce").fillna(0).sum()) if "FLEX" in open_map else 0
            for p in ["QB","RB","WR","TE","K","DST"]:
                demand[p] = int(pd.to_numeric(league_df.get(open_map.get(p,"")), errors="coerce").fillna(0).sum())
            demand["RB"] += round(flex_sum/3)
            demand["WR"] += round(flex_sum/3)
            demand["TE"] += round(flex_sum/3)
        scarcity = {}
        for p in ["QB","RB","WR","TE","K","DST"]:
            sup = max(1, pos_supply.get(p, 1))
            scarcity[p] = clamp(demand.get(p,0)/sup, 0, 3.0)

        # Team pressure score by position
        teams = league_df["team_name"].astype(str).tolist()
        heat = pd.DataFrame(index=teams, columns=["QB","RB","WR","TE","K","DST"])
        tip_parts = []

        max_mb = pd.to_numeric(league_df.get("max_bid"), errors="coerce").fillna(0).max() if "max_bid" in league_df.columns else 0

        for _, t in league_df.iterrows():
            team = t.get("team_name","")
            br = safe_int_or(t.get("budget_remaining"), 0)
            mb = safe_int_or(t.get("max_bid", br), br)
            total_slots_open = 0
            if open_map:
                for _, col in open_map.items():
                    total_slots_open += safe_int_or(t.get(col), 0)
                total_slots_open = max(1, total_slots_open)
            dollars_per_slot = br / total_slots_open if total_slots_open>0 else br

            for p in ["QB","RB","WR","TE","K","DST"]:
                need = 1.0 if (open_map and has_slot_for_position(t, p, open_map)) else 0.0
                pos_open = safe_int_or(t.get(open_map.get(p,""), 0), 0) if open_map and p in open_map else 0
                target_share = (pos_open / total_slots_open) if total_slots_open>0 else 0
                liquidity = (mb / max(1, max_mb)) if max_mb>0 else 0.0
                # Composite pressure: league scarcity counts most, then team need, then liquidity, then $/slot
                pressure = (
                    0.45 * scarcity.get(p,0) +
                    0.30 * need +
                    0.15 * liquidity +
                    0.10 * clamp(dollars_per_slot / 25.0, 0, 2.0)  # scale of $/slot
                )
                heat.at[team, p] = round(pressure, 3)

        # Show as a real heatmap with color-blind friendly gradient
        styled = heat.fillna(0).astype(float).style.background_gradient(cmap="viridis")
        st.dataframe(styled, use_container_width=True, height=280)
        st.caption("Pressure blends league scarcity, team need, liquidity, and $/slot. Darker = more likely to bid hard.")

# --------------------------- NEW: Nomination Trap Finder (Avoid-weighted) ---------------------------
with st.expander("🪤 Nomination Trap Finder", expanded=False):
    if players_df.empty:
        st.caption("Load players first.")
    else:
        dfT = players_df.copy()
        for c in ["soft_rec_$","AAV","ADP"]:
            if c in dfT.columns: dfT[c]=pd.to_numeric(dfT[c], errors="coerce")
        dfT = dfT[~dfT.get("status","").astype(str).str.lower().eq("drafted")]

        # Optional position filter
        pos_opts_tf = sorted([p for p in dfT.get("Position", pd.Series()).dropna().unique().tolist() if p])
        pos_sel_tf = st.multiselect("Positions", pos_opts_tf, default=pos_opts_tf, key="trap_positions")
        if pos_sel_tf:
            dfT = dfT[dfT["Position"].isin(pos_sel_tf)]

        base = dfT["AAV"].copy()
        if base.isna().all():
            base = (dfT["soft_rec_$"]/1.15)
        dfT["surplus_$"] = (dfT["soft_rec_$"] - base)

        # Outbid count as demand proxy
        outbid = []
        if not league_df.empty and "budget_remaining" in league_df.columns:
            open_map = detect_open_cols(league_df)
            for _, r in dfT.iterrows():
                price = r.get("AAV") if pd.notna(r.get("AAV")) else r.get("soft_rec_$")
                pos = r.get("Position","")
                cnt=0
                for _, t in league_df.iterrows():
                    br = pd.to_numeric(t.get("budget_remaining",""), errors="coerce")
                    if pd.isna(price) or pd.isna(br) or br < price: 
                        continue
                    if open_map and not has_slot_for_position(t, pos, open_map):
                        continue
                    cnt+=1
                outbid.append(cnt)
        dfT["outbid_count"] = outbid if outbid else 0

        # Safe Avoid mask
        if "FFG_Avoid" not in dfT.columns:
            dfT["FFG_Avoid"] = ""
        avoid_mask = dfT["FFG_Avoid"].astype(str).str.lower().isin(["true","1","yes","y"])

        # Trap scoring: heavier Avoid weight, plus demand and negative surplus
        # Normalize pieces
        demand = (-dfT["ADP"].fillna(999)).rank(pct=True) + (dfT["outbid_count"]).rank(pct=True)
        neg_surplus = (-dfT["surplus_$"].fillna(0)).rank(pct=True)
        avoid_boost = pd.Series(0.0, index=dfT.index); avoid_boost.loc[avoid_mask] = 1.0  # strong boost
        dfT["trap_score"] = 0.45*demand + 0.35*neg_surplus + 0.20*avoid_boost

        traps = dfT.sort_values(by=["trap_score"], ascending=False).head(12)
        cols = ["Position","Player","Team","AAV","soft_rec_$","surplus_$","ADP","outbid_count"]
        for c in cols:
            if c not in traps.columns: traps[c]=""
        st.dataframe(traps[cols], hide_index=True, use_container_width=True, height=280)
        st.caption("Avoid-tagged + high-demand or negative value rise to the top. Consider nominating to drain rivals’ budgets.")

# --------------------------- Quick Tag Editor (sticky open) ---------------------------
with st.expander("🏷️ Quick Tag Editor", expanded=True):
    if players_df.empty:
        st.caption("Load and sync players first.")
    else:
        tg_player = st.selectbox("Player", players_df["Player"].tolist(), key="tag_player")
        meta_row = players_df.loc[players_df["Player"]==tg_player].head(1)
        if not meta_row.empty:
            st.caption(f"{str(meta_row['Position'].iloc[0])} · {str(meta_row['Team'].iloc[0])}")
        tag_cols = get_tag_columns(players_df)
        pretty = []; map_pretty={}
        for c in tag_cols:
            base = c.replace("FFG_","").replace("FFB_","")
            label = {"MyGuy":"My Guy","Sleeper":"Sleeper","Bust":"Bust","Value":"Value","Breakout":"Breakout","Avoid":"Avoid"}.get(base, base)
            pretty.append(label); map_pretty[label]=c
        if not pretty:
            st.caption("No tag columns found (FFG_/FFB_).")
        else:
            choice = st.selectbox("Tag", pretty, key="tag_choice")
            tgt_col = map_pretty[choice]
            do_tag = st.button("💾 Toggle Tag", disabled=not (write_ready and not practice))
            if do_tag and write_ready and not practice:
                try:
                    ws = sh.worksheet("Players"); df = normalize_cols(ws_to_df(ws))
                    if tgt_col not in df.columns: df[tgt_col]=""
                    mask = df["Player"].astype(str).eq(tg_player)
                    if not mask.any():
                        st.error("Player not found in Players sheet.")
                    else:
                        idx = df.index[mask][0]
                        df.at[idx, tgt_col] = "FALSE" if is_truthy(df.at[idx, tgt_col]) else "TRUE"
                        write_dataframe_to_sheet(ws, df, header=True)
                        st.toast(f"{choice} toggled for {tg_player}")
                        st.cache_data.clear(); st.rerun()
                except Exception as e:
                    st.error(f"Tag update failed: {e}")

# Footer
st.caption("Auction GM • Fast draft flow • Teams & Needs • Draft Log & Trends • Best Values • Heatmap • Trap Finder")
