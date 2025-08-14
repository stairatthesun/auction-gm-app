# streamlit_app.py — Auction GM (stable build + Draft Log fix + Tag columns + Injury tag)
# Changes in this version (over your last stable):
#  1) Draft_Log: enforced header on write + normalized read (fixes missing player/team/position in log)
#  2) Adds compact Tag column (emoji+label) to:
#       - Best Remaining Values
#       - Nomination Recommendations (Value Targets & Price Enforcers)
#       - Nomination Trap Finder
#  3) NEW tag: FFG_Injury → shows as "🩹 Injured" and increases trap weight in Trap Finder
#
# Everything else remains as in the prior working build (no perf/mirror rewrites).

import io, json, re
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

# Optional matplotlib for heatmap styling
MPL_OK = False
try:
    import matplotlib  # noqa: F401
    MPL_OK = True
except Exception:
    MPL_OK = False

st.set_page_config(page_title="Auction GM", layout="wide")

# --------------------------- Secrets / Config ---------------------------
SHEET_ID = st.secrets.get("SHEET_ID", "")
SLEEPER_LEAGUE_ID = st.secrets.get("SLEEPER_LEAGUE_ID", "")
SA_JSON = st.secrets.get("GOOGLE_SERVICE_ACCOUNT_JSON", None)
if not SA_JSON:
    # backward compat key names
    for k in ("gcp_service_account", "google_service_account"):
        if k in st.secrets:
            SA_JSON = json.dumps(dict(st.secrets[k]))
            break

# Positions to *exclude* (IDP)
IDP_POS = {"LB","DL","DE","DT","EDGE","OLB","MLB","ILB","DB","CB","S","FS","SS","IDP"}

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
POS_KEYS = ["QB","RB","WR","TE","FLEX","K","DST","BENCH"]

# --------------------------- Utilities ---------------------------
def normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty: 
        return pd.DataFrame()
    rename = {}
    for c in list(df.columns):
        k = str(c).strip().lower()
        if k in CANON: rename[c] = CANON[k]
    if rename:
        df = df.rename(columns=rename)
    for c in df.columns:
        if df[c].dtype == object:
            df[c] = df[c].astype(str).str.strip()
    return df

def choose_keys(df_left, df_right):
    for ks in (["player_id"], ["Player","Team","Position"], ["Player","Position"], ["Player","Team"], ["Player"]):
        if all(k in df_left.columns for k in ks) and all(k in df_right.columns for k in ks):
            return ks
    return None

def get_tag_columns(df):
    return [c for c in df.columns if c.startswith("FFG_") or c.startswith("FFB_")]

def is_truthy(v):
    s = str(v).strip().lower()
    return s in ("1","true","yes","y")

# Tag label helper (emoji + short) — includes NEW 🩹 Injured
def short_tag_for_row(row) -> str:
    """Return compact emoji+label for first matching tag in priority order."""
    def has(col):
        return str(row.get(col, "")).strip().lower() in ("1","true","yes","y")

    checks = [
        ("⭐ MyGuy",     ("FFG_MyGuy","FFB_MyGuy")),
        ("💎 Value",     ("FFG_Value","FFB_Value")),
        ("💤 Sleeper",   ("FFG_Sleeper","FFB_Sleeper")),
        ("🚀 Breakout",  ("FFG_Breakout","FFB_Breakout")),
        ("⚠️ Bust",      ("FFG_Bust","FFB_Bust")),
        ("🩹 Injured",   ("FFG_Injury","FFB_Injury")),  # <-- NEW
        ("⛔ Avoid",      ("FFG_Avoid","FFB_Avoid")),
    ]
    for label, cols in checks:
        for c in cols:
            if has(c):
                return label
    return ""

def inject_tag_column(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    if "Tag" not in df.columns:
        df = df.copy()
        try:
            df["Tag"] = df.apply(short_tag_for_row, axis=1)
        except Exception:
            df["Tag"] = ""
    return df

# --------------------------- Sleeper (mini header) ---------------------------
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
    if not rows: return pd.DataFrame()
    header, data = rows[0], rows[1:]
    return pd.DataFrame(data, columns=header)

def write_dataframe_to_sheet(ws, df: pd.DataFrame, header=True):
    values = [df.columns.tolist()] + df.fillna("").astype(str).values.tolist() if header else df.fillna("").astype(str).values.tolist()
    ws.clear()
    ws.update("A1", values, value_input_option="RAW")

def upsert_worksheet(sh, title, rows=5000, cols=60):
    try: return sh.worksheet(title)
    except Exception:
        sh.add_worksheet(title=title, rows=rows, cols=cols)
        return sh.worksheet(title)

# --------------------------- Projections Import ---------------------------
TARGET_PROJ_COLS = ["Position","Player","Team","Points","VOR","ADP","AAV","Rank Overall","Rank Position"]
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
        if k in NAME_MAP: col_map[c] = NAME_MAP[k]
    df = df.rename(columns=col_map)
    for col in TARGET_PROJ_COLS:
        if col not in df.columns: df[col] = None
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
        if c not in df_p.columns: df_p[c] = ""
    for c in PROJ_COLS:
        if c not in df_p.columns: df_p[c] = ""
        if c not in df_r.columns: df_r[c] = ""

    tag_cols = get_tag_columns(df_p)
    keys = choose_keys(df_p, df_r)
    if not keys:
        return False, ("Couldn’t find matching join keys between Players and Projections. "
                       "Keep Players headers, clear rows 2+, and run with identity updates ON for first sync.")

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
        return False, ("Smart Sync aborted — result was empty (likely key mismatch). "
                       "Keep Players headers, clear rows 2+, run with identity updates ON for first sync.")

    write_dataframe_to_sheet(ws_players, merged, header=True)
    updated = len(df_r); added = len(right_only)
    return True, f"Smart Sync done: updated {updated:,} rows, added {added:,}. Keys: {', '.join(keys)}."

# --------------------------- Bias & Recs ---------------------------
def load_bias_map(sh):
    try:
        ws = sh.worksheet("Bias_Teams"); df = normalize_cols(ws_to_df(ws))
        if df.empty: return {}
        team_col = next((c for c in df.columns if c.lower() in ("team","nfl_team","tm")), None)
        if not team_col: return {}
        bias_col=None
        for c in df.columns:
            if c==team_col: continue
            ser = pd.to_numeric(df[c], errors="coerce")
            if ser.notna().any(): bias_col=c; break
        if not bias_col: return {}
        out={}
        for _,r in df.iterrows():
            t=str(r.get(team_col,"")).strip()
            v=pd.to_numeric(r.get(bias_col,""), errors="coerce")
            if t and pd.notna(v): out[t]=float(v)
        return out
    except Exception:
        return {}

def compute_recommended_values(df_players: pd.DataFrame, bias_map=None, budget=200, teams=14):
    df = df_players.copy()
    if bias_map is None: bias_map={}
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
    if base.isna().all(): base = pd.Series([0.0]*len(df))

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

def write_recommendations_to_players(sh, teams=14, budget=200):
    ws = sh.worksheet("Players")
    df = normalize_cols(ws_to_df(ws))
    for c in ["AAV","VOR","Points","status","price_paid","Team"]:
        if c not in df.columns: df[c] = ""
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

# --------------------------- League_Teams helpers (per-position) ---------------------------
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
    for pos, col in open_map.items():
        total += int(pd.to_numeric(row.get(col,0), errors="coerce") or 0)
    return int(max(0, total))

def has_slot_for_position(row, position, open_map):
    p = position.upper()
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
    p = position.upper()
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
        if open_map.get("FLEX") and int(pd.to_numeric(row.get(open_map["FLEX"],0), errors="coerce") or 0)>0:
            dec(open_map["FLEX"]); return
        if open_map.get("BENCH"):
            dec(open_map["BENCH"]); return
    if open_map.get("BENCH"): dec(open_map["BENCH"])

def recompute_maxbid_and_pps(row, open_map):
    b = float(pd.to_numeric(row.get("budget_remaining",0), errors="coerce") or 0)
    total = total_open_slots(row, open_map)
    max_bid = int(max(0, round(b - max(0, total - 1))))
    pps = int(round(b / total)) if total>0 else int(b)
    return max_bid, pps

def update_league_team_after_pick(sh, team_name, position, price):
    ws = sh.worksheet("League_Teams")
    df = normalize_cols(ws_to_df(ws))
    if df.empty or "team_name" not in df.columns or "budget_remaining" not in df.columns:
        return False, "League_Teams missing team_name/budget_remaining."
    m = df["team_name"].astype(str).str.strip().str.lower() == str(team_name).strip().lower()
    if not m.any(): return False, f"Team '{team_name}' not found."
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
    else:
        if "roster_spots_open" in df.columns:
            r = int(pd.to_numeric(df.at[i,"roster_spots_open"], errors="coerce") or 0)
            df.at[i,"roster_spots_open"] = int(max(0, r-1))
            if "max_bid" in df.columns:
                b = int(pd.to_numeric(df.at[i,"budget_remaining"], errors="coerce") or 0)
                mb = int(max(0, b - max(0, int(df.at[i,"roster_spots_open"]) - 1)))
                df.at[i,"max_bid"] = mb
            if "(auto)_$per_open_slot" in df.columns:
                total = int(pd.to_numeric(df.at[i,"roster_spots_open"], errors="coerce") or 0)
                b = int(pd.to_numeric(df.at[i,"budget_remaining"], errors="coerce") or 0)
                df.at[i,"(auto)_$per_open_slot"] = int(round(b/total)) if total>0 else b

    write_dataframe_to_sheet(ws, df, header=True)
    return True, "Team updated."

# --------------------------- Draft Log (WRITE: enforce header) ---------------------------
def append_draft_log(sh, row: dict):
    """
    Append one pick to Draft_Log with a strict header:
    ['pick','player','team','position','manager','price'].
    Automatically computes the next pick number.
    """
    ws = upsert_worksheet(sh, "Draft_Log")
    needed = ["pick","player","team","position","manager","price"]

    # Force exact header (order + case)
    header = ws.row_values(1)
    if header != needed:
        ws.clear()
        ws.update("A1", [needed])
        header = needed

    # Determine pick number from current rows
    rows = ws.get_all_values()
    current_picks = max(0, len(rows) - 1)
    row["pick"] = current_picks + 1

    # Append in correct order
    out = [row.get(c, "") for c in header]
    ws.append_row(out, value_input_option="RAW")

# --------------------------- Draft Log (READ: normalize) ---------------------------
def normalize_draft_log_cols(df: pd.DataFrame) -> pd.DataFrame:
    """
    Map any legacy/hand-edited Draft_Log headers to canonical lowercase names,
    and ensure required columns exist.
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=["pick","player","team","position","manager","price"])

    mapping = {}
    for c in df.columns:
        lc = str(c).strip().lower()
        if lc in {"pick","player","team","position","manager","price","timestamp"}:
            mapping[c] = lc

    df = df.rename(columns=mapping)

    for col in ["pick","player","team","position","manager","price"]:
        if col not in df.columns:
            df[col] = ""

    # Typing for convenience
    try:
        df["pick"] = pd.to_numeric(df["pick"], errors="coerce")
        df["price"] = pd.to_numeric(df["price"], errors="coerce")
    except Exception:
        pass

    return df

# --------------------------- Draft Updates ---------------------------
def update_player_drafted(sh, player_key, manager, price):
    ws = sh.worksheet("Players")
    df = normalize_cols(ws_to_df(ws))
    mask = (df["Player"]==player_key[0]) & (df["Team"]==player_key[1]) & (df["Position"]==player_key[2])
    if not mask.any(): raise RuntimeError("Player not found in Players sheet.")
    idx = df.index[mask][0]
    for c in ["status","drafted_by","price_paid"]:
        if c not in df.columns: df[c]=""
    df.loc[idx,"status"]="drafted"
    df.loc[idx,"drafted_by"]=manager
    df.loc[idx,"price_paid"]=str(int(price)) if pd.notna(price) and price!="" else ""
    write_dataframe_to_sheet(ws, df, header=True)
    return True

# --------------------------- Nomination Recs (position-aware) ---------------------------
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
                flex_total = pd.to_numeric(league_df.get(open_map["FLEX"]), errors="coerce").fillna(0).sum()
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
    outbid_norm  = (df["outbid_count"]  - df["outbid_count"].median()).fillna(0)
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

# --------------------------- Trap Finder (now weighs Injury too) ---------------------------
def build_trap_finder(players_df: pd.DataFrame, league_df: pd.DataFrame, top_n: int = 10):
    df = players_df.copy() if players_df is not None else pd.DataFrame()
    if df.empty:
        return pd.DataFrame()

    # Ensure columns
    for c in ["status","Position","Player","Team","AAV","soft_rec_$","VOR","Rank Position","FFG_Avoid","FFG_Injury"]:
        if c not in df.columns:
            df[c] = "" if c not in ("AAV","soft_rec_$","VOR","Rank Position") else float("nan")

    df = df[~df["status"].astype(str).str.lower().eq("drafted")].copy()
    for c in ["AAV","soft_rec_$","VOR","Rank Position"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # Avoid / Injury masks
    avoid_mask = df.get("FFG_Avoid","").astype(str).str.lower().isin(["true","1","yes","y"])
    injury_mask = df.get("FFG_Injury","").astype(str).str.lower().isin(["true","1","yes","y"])  # NEW

    # Market pressure proxy
    surplus = (df["AAV"] - df["soft_rec_$"]).fillna(0)

    # Position scarcity (simple: fewer remaining implies scarcer)
    pos_supply = df["Position"].value_counts().to_dict()
    sc = df["Position"].map(lambda p: 1.0 / max(1, pos_supply.get(p, 1))).fillna(0)

    # Outbid count (roughly re-using nomination idea)
    outbid_counts = []
    if league_df is not None and not league_df.empty and "budget_remaining" in league_df.columns:
        open_map = detect_open_cols(league_df)
        for _, r in df.iterrows():
            price = r.get("AAV")
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

    # Trap score: heavier weight on Avoid + Injury, then market heat, scarcity, and outbidability.
    df["trap_score"] = (
        (avoid_mask.astype(int) * 3.0) +
        (injury_mask.astype(int) * 2.0) +     # <-- NEW: weight injury
        (surplus.fillna(0) / (df["soft_rec_$"].abs().replace(0,1))).fillna(0) * 1.0 +
        sc * 0.8 +
        (df["outbid_count"] - df["outbid_count"].median()).fillna(0) * 0.6
    )

    traps = df.sort_values(["trap_score"], ascending=False).head(top_n).copy()

    def why_row(r):
        parts=[]
        if is_truthy(r.get("FFG_Avoid","")): parts.append("avoid tag")
        if is_truthy(r.get("FFG_Injury","")): parts.append("injury risk")  # <-- NEW
        if pd.notna(r.get("AAV")) and pd.notna(r.get("soft_rec_$")) and (r["AAV"] > r["soft_rec_$"]):
            parts.append(f"market +${int(r['AAV']-r['soft_rec_$'])}")
        if pd.notna(r.get("outbid_count")) and r["outbid_count"]>=3: parts.append(f"{int(r['outbid_count'])} bidders")
        return " • ".join(parts) if parts else "opportunity"
    if not traps.empty:
        traps["why"] = traps.apply(why_row, axis=1)

    return traps

# --------------------------- UI ---------------------------
st.title("🏈 Auction GM")

# Sidebar: connect + actions
with st.sidebar:
    st.header("Connect")
    st.write(f"**Sheet ID:** {'✅ set' if SHEET_ID else '❌ missing'}")
    st.write(f"**Sleeper League ID:** {'✅ set' if SLEEPER_LEAGUE_ID else '❌ missing'}")

    write_ready=False; sa_email=None
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

    btn_recs = st.button("💡 Recompute Recommended $", use_container_width=True, disabled=not (write_ready and not practice))
    if btn_recs and write_ready and not practice:
        teams=14; budget=200
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

    st.divider()
    if st.button("🔁 Refresh Data (clear cache)"):
        st.cache_data.clear()
        st.toast("Caches cleared. Reloading data…")

# Tiny league header
if SLEEPER_LEAGUE_ID:
    try:
        league = sleeper_get(f"league/{SLEEPER_LEAGUE_ID}")
        st.caption(f"**{league.get('name','—')}** • {league.get('total_rosters','—')} teams • Season {league.get('season','—')}")
    except Exception:
        pass

# Load core data (cached)
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
    except Exception:
        league_df = pd.DataFrame()
    try:
        draft_log_df = normalize_cols(ws_to_df_cached(SHEET_ID, "Draft_Log", SA_JSON))
        draft_log_df = normalize_draft_log_cols(draft_log_df)
    except Exception:
        draft_log_df = pd.DataFrame()

# Safety net
if players_df is None or players_df.empty:
    players_df = pd.DataFrame()
for c in ["status","Position","Player","Team","soft_rec_$","AAV","ADP","VOR","Points","Rank Overall","Rank Position"]:
    if c not in players_df.columns:
        players_df[c] = (
            float("nan") if c in ("soft_rec_$","AAV","ADP","VOR","Points","Rank Overall","Rank Position")
            else ""
        )

# --------------------------- Top controls: Draft + Price-check ---------------------------
st.divider()
c_draft, c_price = st.columns([1.2, 1])

with c_draft:
    st.subheader("📝 Draft a Player")
    if players_df.empty:
        st.info("Load projections and run Smart Sync first.")
    else:
        mgr_opts = []
        if not league_df.empty and "team_name" in league_df.columns:
            mgr_opts = sorted([t for t in league_df["team_name"].dropna().astype(str).tolist() if t])
        sel_name = st.selectbox("Player", players_df["Player"].tolist(), key="pick_player")
        meta_row = players_df.loc[players_df["Player"]==sel_name].head(1)
        t_show = str(meta_row["Team"].iloc[0]) if not meta_row.empty and "Team" in meta_row.columns else ""
        p_show = str(meta_row["Position"].iloc[0]) if not meta_row.empty and "Position" in meta_row.columns else ""
        st.caption(f"{p_show} · {t_show}")
        base_soft = int(meta_row["soft_rec_$"].iloc[0]) if not meta_row.empty and pd.notna(meta_row["soft_rec_$"].iloc[0]) else 1
        sel_price = st.number_input("Price", min_value=1, max_value=500, step=1, value=base_soft, key="pick_price")
        sel_mgr = st.selectbox("Team (buyer)", mgr_opts if mgr_opts else [""], index=0 if mgr_opts else 0, placeholder="Select team…", key="pick_mgr")
        draft_btn = st.button("✅ Mark Drafted & Log", type="primary", use_container_width=True, disabled=not (write_ready and not practice))
        if draft_btn and write_ready and not practice:
            try:
                update_player_drafted(sh, (sel_name, t_show, p_show), sel_mgr, sel_price)
                append_draft_log(sh, {
                    "player": sel_name, "team": t_show, "position": p_show,
                    "manager": sel_mgr, "price": str(int(sel_price))
                })
                ok,msg = update_league_team_after_pick(sh, sel_mgr, p_show, sel_price)
                if not ok: st.warning(msg)
                # quick recalc $
                write_recommendations_to_players(sh)
                st.toast(f"Drafted {sel_name} for ${sel_price}.")
            except Exception as e:
                st.error(f"Draft failed: {e}")

with c_price:
    st.subheader("💬 Price-check")
    if players_df.empty:
        st.info("Load projections and sync first.")
    else:
        ob_player = st.selectbox("Player", players_df["Player"].tolist(), key="ob_player")
        row = players_df.loc[players_df["Player"]==ob_player].head(1)
        pos = str(row["Position"].iloc[0]) if not row.empty and "Position" in row.columns else ""
        team = str(row["Team"].iloc[0]) if not row.empty and "Team" in row.columns else ""
        st.caption(f"{pos} · {team}")
        base_soft = int(row["soft_rec_$"].iloc[0]) if not row.empty and pd.notna(row["soft_rec_$"].iloc[0]) else 1
        ob_price  = st.number_input("Price to check", min_value=1, max_value=500, step=1, value=base_soft, key="ob_price")

        can_list=[]
        if not league_df.empty and "budget_remaining" in league_df.columns:
            open_map = detect_open_cols(league_df)
            for _,trow in league_df.iterrows():
                br = pd.to_numeric(trow.get("budget_remaining",""), errors="coerce")
                if pd.isna(br) or br < ob_price: 
                    continue
                if open_map and not has_slot_for_position(trow, pos, open_map):
                    continue
                can_list.append(str(trow.get("team_name","")))
        st.metric("Teams that can outbid", len(can_list))
        if can_list: st.caption(", ".join(can_list))

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
    st.info("Players sheet is empty or unavailable. Run Smart Sync after importing Projections.")
else:
    view = players_df.copy()
    for c in ["Points","VOR","ADP","AAV","soft_rec_$","hard_cap_$","price_paid","Rank Overall","Rank Position"]:
        if c in view.columns:
            view[c] = pd.to_numeric(view[c], errors="coerce")

    # tag icons column
    def tag_icons(row):
        out=[]
        if is_truthy(row.get("FFG_MyGuy", row.get("FFB_MyGuy",""))): out.append("⭐")
        if is_truthy(row.get("FFG_Sleeper", row.get("FFB_Sleeper",""))): out.append("💤")
        if is_truthy(row.get("FFG_Bust", row.get("FFB_Bust",""))): out.append("⚠️")
        if is_truthy(row.get("FFG_Value", row.get("FFB_Value",""))): out.append("💎")
        if is_truthy(row.get("FFG_Breakout", row.get("FFB_Breakout",""))): out.append("🚀")
        if is_truthy(row.get("FFG_Injury", row.get("FFB_Injury",""))): out.append("🩹")
        if is_truthy(row.get("FFG_Avoid", row.get("FFB_Avoid",""))): out.append("⛔")
        return " ".join(out)
    view["Tags"] = view.apply(tag_icons, axis=1) if not view.empty else ""

    # filters
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
    st.dataframe(view[show_cols], use_container_width=True, height=520)

# --------------------------- Top Row: Draft Log + Best Values + Teams ---------------------------
st.divider()
col_log, col_best, col_teams = st.columns([1.1, 1.1, 1.2])

with col_log:
    st.subheader("🧾 Draft Log + Live Trends")
    if draft_log_df.empty:
        st.caption("No picks yet.")
    else:
        show = draft_log_df.copy()
        keep = ["pick","player","team","position","manager","price"]
        for c in keep:
            if c not in show.columns: show[c] = ""
        show = show[keep].sort_values("pick", ascending=True, na_position="last")
        st.dataframe(show, hide_index=True, use_container_width=True, height=250)

        # Mini trend: paid vs AAV delta
        try:
            merged = players_df.merge(show, left_on=["Player","Team","Position"], right_on=["player","team","position"], how="inner")
            paid = pd.to_numeric(merged["price"], errors="coerce")
            aav  = pd.to_numeric(merged.get("AAV",""), errors="coerce")
            delta = (paid - aav).dropna()
            avg_delta = int(delta.mean()) if not delta.empty else 0
            st.caption(f"Room trend vs AAV: {'+' if avg_delta>=0 else ''}{avg_delta} $/player")
        except Exception:
            pass

with col_best:
    st.subheader("💎 Best Remaining Values")
    if players_df.empty:
        st.caption("Load and sync players first.")
    else:
        dfb = players_df.copy()
        dfb = dfb[~dfb["status"].astype(str).str.lower().eq("drafted")]
        for c in ["soft_rec_$","AAV","VOR","Points"]:
            if c in dfb.columns:
                dfb[c] = pd.to_numeric(dfb[c], errors="coerce")
        base = dfb["AAV"]
        if base.isna().all():
            dfb["_surplus"] = dfb["soft_rec_$"]
        else:
            dfb["_surplus"] = (dfb["soft_rec_$"] - dfb["AAV"])
        dfb = inject_tag_column(dfb)
        best_cols = ["Tag","Position","Player","Team","soft_rec_$","AAV","VOR","Points","_surplus"]
        for c in best_cols:
            if c not in dfb.columns: dfb[c] = ""
        dfb = dfb.sort_values("_surplus", ascending=False, na_position="last").head(20)
        st.dataframe(dfb[best_cols], hide_index=True, use_container_width=True, height=250)

with col_teams:
    st.subheader("👥 Teams (My Team default)")
    if league_df.empty or "team_name" not in league_df.columns:
        st.caption("League_Teams not available.")
    else:
        team_opts = league_df["team_name"].astype(str).tolist()
        default_idx = 1 if len(team_opts)>=2 and team_opts[1].strip().lower()=="my team" else 0
        choose_team = st.selectbox("Team", team_opts, index=default_idx)
        row = league_df.loc[league_df["team_name"]==choose_team].head(1)
        if not row.empty:
            br = int(pd.to_numeric(row["budget_remaining"], errors="coerce").fillna(0).iloc[0])
            mb = int(pd.to_numeric(row.get("max_bid",0), errors="coerce").fillna(0).iloc[0]) if "max_bid" in row.columns else 0
            pps = int(pd.to_numeric(row.get("(auto)_$per_open_slot",0), errors="coerce").fillna(0).iloc[0]) if "(auto)_$per_open_slot" in row.columns else 0
            st.metric("Budget Remaining", f"${br}")
            st.metric("Max Bid", f"${mb}")
            st.metric("$ / Open Slot", f"${pps}")

# --------------------------- Nomination Recommendations ---------------------------
with st.expander("🧠 Nomination Recommendations (position-aware)", expanded=False):
    val_list, enf_list = build_nomination_list(players_df if not players_df.empty else pd.DataFrame(),
                                               league_df if not league_df.empty else pd.DataFrame(),
                                               top_n=8)
    # Inject Tag for both lists
    val_list = inject_tag_column(val_list)
    enf_list = inject_tag_column(enf_list)

    c_left, c_right = st.columns(2)
    with c_left:
        st.markdown("**Value Targets**")
        if val_list.empty:
            st.caption("No candidates found.")
        else:
            cols = ["Tag","Position","Player","Team","soft_rec_$","AAV","value_surplus","why"]
            for c in cols:
                if c not in val_list.columns: val_list[c]=""
            st.dataframe(val_list[cols], hide_index=True, use_container_width=True, height=240)
    with c_right:
        st.markdown("**Price Enforcers**")
        if enf_list.empty:
            st.caption("No candidates found.")
        else:
            cols = ["Tag","Position","Player","Team","soft_rec_$","AAV","outbid_count","why"]
            for c in cols:
                if c not in enf_list.columns: enf_list[c]=""
            st.dataframe(enf_list[cols], hide_index=True, use_container_width=True, height=240)

# --------------------------- Bidding Heatmap ---------------------------
with st.expander("🔥 Bidding Heatmap", expanded=False):
    if league_df.empty or players_df.empty:
        st.caption("Need Players & League_Teams data.")
    else:
        open_map = detect_open_cols(league_df)
        if not open_map:
            st.caption("No open_* columns found in League_Teams.")
        else:
            heat = pd.DataFrame(index=league_df["team_name"], columns=["QB","RB","WR","TE","K","DST"]).fillna(0)
            flex = pd.to_numeric(league_df.get(open_map.get("FLEX",""), 0), errors="coerce").fillna(0) if "FLEX" in open_map else 0
            for pos in ["QB","RB","WR","TE","K","DST"]:
                base = pd.to_numeric(league_df.get(open_map.get(pos,""),0), errors="coerce").fillna(0)
                if pos in ("RB","WR","TE") and isinstance(flex, pd.Series):
                    base = base + (flex/3.0)
                heat[pos] = base
            budgets = pd.to_numeric(league_df.get("budget_remaining",0), errors="coerce").fillna(0)
            slots = heat.sum(axis=1).replace(0, 1)
            liquidity = (budgets / slots).clip(lower=0)
            heat = heat.mul(liquidity, axis=0)

            hm_df = heat.fillna(0).astype(float)
            if MPL_OK:
                try:
                    styled = hm_df.style.background_gradient(cmap="viridis")
                    st.dataframe(styled, use_container_width=True, height=280)
                except Exception:
                    st.dataframe(hm_df, use_container_width=True, height=280)
            else:
                st.dataframe(hm_df, use_container_width=True, height=280)
            st.caption("Higher values indicate teams with both need and liquidity at that position.")

# --------------------------- Nomination Trap Finder ---------------------------
with st.expander("🪤 Nomination Trap Finder", expanded=False):
    traps_df = build_trap_finder(players_df if not players_df.empty else pd.DataFrame(),
                                 league_df if not league_df.empty else pd.DataFrame(),
                                 top_n=12)
    traps_df = inject_tag_column(traps_df)
    if traps_df.empty:
        st.caption("No trap candidates found.")
    else:
        trap_cols = ["Tag","Position","Player","Team","AAV","soft_rec_$","trap_score","why"]
        for c in trap_cols:
            if c not in traps_df.columns: traps_df[c]=""
        st.dataframe(traps_df[trap_cols], hide_index=True, use_container_width=True, height=260)

# --------------------------- Quick Tag Editor ---------------------------
with st.expander("🏷️ Quick Tag Editor", expanded=False):
    if players_df.empty:
        st.caption("Load and sync players first.")
    else:
        tg_player = st.selectbox("Player", players_df["Player"].tolist(), key="tag_player")
        meta_row = players_df.loc[players_df["Player"]==tg_player].head(1)
        if not meta_row.empty:
            st.caption(f"{str(meta_row['Position'].iloc[0])} · {str(meta_row['Team'].iloc[0])}")
        tag_cols = get_tag_columns(players_df)
        pretty = []; map_pretty={}

        # Ensure the common tag columns exist; NEW includes FFG_Injury
        common = ["FFG_MyGuy","FFG_Sleeper","FFG_Bust","FFG_Value","FFG_Breakout","FFG_Avoid","FFG_Injury"]
        for c in common:
            if c not in tag_cols and c in players_df.columns:
                tag_cols.append(c)

        for c in tag_cols:
            base = c.replace("FFG_","").replace("FFB_","")
            label = {
                "MyGuy":"My Guy","Sleeper":"Sleeper","Bust":"Bust","Value":"Value",
                "Breakout":"Breakout","Avoid":"Avoid","Injury":"Injured"  # <-- NEW label
            }.get(base, base)
            pretty.append(label); map_pretty[label]=c
        if not pretty:
            st.caption("No tag columns found (FFG_/FFB_).")
        else:
            choice = st.selectbox("Tag", sorted(pretty), key="tag_choice")
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
                        st.toast(f"{choice}: {'OFF' if is_truthy(df.at[idx, tgt_col]) else 'ON'} for {tg_player}")
                except Exception as e:
                    st.error(f"Tag update failed: {e}")

# Footer
st.caption("Auction GM • draft log fixed • tag columns added • 🩹 injury tag supported • stable build")
