# streamlit_app.py — Auction GM (stable build + tiers & alerts + liquidity & blended inflation + tags filter + value col + cadence + undo + hard guards)
import io, json, re, time
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
    # NEW: ensure Tier maps correctly from any source
    "tier":"Tier"
}

IDENTITY_COLS = ["Player","Team","Position"]
# NEW: include Tier in projection columns so Smart Sync carries it to Players
PROJ_COLS = ["Points","VOR","ADP","AAV","Rank Overall","Rank Position","Tier"]

# Known tag columns — used to ensure persistence on every write
TAG_COLS = ["FFG_MyGuy","FFG_Sleeper","FFG_Bust","FFG_Value","FFG_Breakout","FFG_Avoid","FFG_Injury"]

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

def ensure_tag_columns(df: pd.DataFrame, tags=TAG_COLS) -> pd.DataFrame:
    """Guarantee all FFG_* columns exist before writing Players; prevents accidental drops."""
    if df is None or df.empty:
        return df
    for c in tags:
        if c not in df.columns:
            df[c] = ""
    return df

def choose_keys(df_left, df_right):
    for ks in (["player_id"], ["Player","Team","Position"], ["Player","Position"], ["Player","Team"], ["Player"]):
        if all(k in df_left.columns for k in ks) and all(k in df_right.columns for k in ks):
            return ks
    return None

def get_tag_columns(df):
    # supports either FFG_ or FFB_ prefixes; front-end will look for both
    return [c for c in df.columns if c.startswith("FFG_") or c.startswith("FFB_")]

def is_truthy(v):
    s = str(v).strip().lower()
    return s in ("1","true","yes","y")

def safe_int_val(v, default=0):
    try:
        x = pd.to_numeric(v, errors="coerce")
        if pd.isna(x): return default
        return int(float(x))
    except Exception:
        return default

# Small retry-once helper for transient write hiccups
def _retry_once(fn, *args, **kwargs):
    try:
        return fn(*args, **kwargs)
    except Exception:
        time.sleep(0.6)
        return fn(*args, **kwargs)

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

# --------------------------- Draft_Log helpers ---------------------------
def ensure_draft_log_header(ws):
    header = ws.row_values(1)
    needed = ["pick","player","team","position","manager","price"]
    if not header:
        ws.update("A1",[needed])
        return needed
    if [h.strip().lower() for h in header] != needed:
        existing = ws.get_all_values()
        body = existing[1:] if len(existing)>1 else []
        ws.clear()
        ws.update("A1",[needed] + body)
    return needed

def next_pick_number(ws):
    rows = ws.get_all_values()
    if not rows or len(rows)==1: return 1
    try:
        last = rows[-1]
        return safe_int_val(last[0], 0) + 1
    except Exception:
        return len(rows)

@st.cache_data(ttl=30)
def get_draft_log_cached(sheet_id: str, sa_json: str):
    if not (gspread and Credentials) or not sheet_id or not sa_json:
        return pd.DataFrame(columns=["pick","player","team","position","manager","price"])
    info = json.loads(sa_json)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(sheet_id)
    ws = upsert_worksheet(sh, "Draft_Log")
    ensure_draft_log_header(ws)
    rows = ws.get_all_values()
    if not rows:
        return pd.DataFrame(columns=["pick","player","team","position","manager","price"])
    df = pd.DataFrame(rows[1:], columns=rows[0])
    return normalize_cols(df)

def append_draft_log(sh, row: dict):
    ws = upsert_worksheet(sh, "Draft_Log")
    ensure_draft_log_header(ws)
    header = ws.row_values(1)
    out = [str(row.get(c,"")) for c in header]
    ws.append_row(out, value_input_option="RAW")

# --------------------------- Projections Import (CSV cleaner) ---------------------------
# NEW: include Tier
TARGET_PROJ_COLS = ["Position","Player","Team","Points","VOR","ADP","AAV","Rank Overall","Rank Position","Tier"]
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
    # NEW
    "tier":"Tier"
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
    # ensure Tier is numeric if present
    if "Tier" in df.columns:
        df["Tier"] = pd.to_numeric(df["Tier"], errors="coerce")
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
        return False, ("Couldn’t find matching join keys. Keep Players headers, clear rows 2+, "
                       "and run with identity updates ON for first sync.")

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
        return False, "Smart Sync aborted — result empty (likely key mismatch)."

    # --- ensure all tag columns survive the write ---
    merged = ensure_tag_columns(merged)

    write_dataframe_to_sheet(ws_players, merged, header=True)
    updated = len(df_r); added = len(right_only)
    return True, f"Smart Sync done: updated {updated:,}, added {added:,}. Keys: {', '.join(keys)}."

# --------------------------- Bias & Recs ---------------------------
def load_bias_map(sh):
    # Retained for compatibility; no longer applied to recommendations.
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

# ---- Liquidity & scarcity helpers (starters-focused) ----
STARTER_POS = ("QB","RB","WR","TE","K","DST")

def detect_open_cols(df_league: pd.DataFrame):
    mapping={}
    cols = {c.lower(): c for c in df_league.columns}
    for pos in ["QB","RB","WR","TE","FLEX","K","DST","BENCH"]:
        want = f"open_{pos.lower()}"
        for lc, orig in cols.items():
            if lc == want:
                mapping[pos] = orig
                break
    return mapping

def starter_open_count_row(row, open_map):
    """Count only *starter* opens; FLEX counts fully here (we'll split across RB/WR/TE for scarcity later)."""
    get = lambda c: int(pd.to_numeric(row.get(c,0), errors="coerce") or 0)
    total = 0
    for p in STARTER_POS:
        if p in open_map:
            total += get(open_map[p])
    if "FLEX" in open_map:
        total += get(open_map["FLEX"])
    return int(max(0, total))

def total_open_slots(row, open_map):
    total = 0
    for _, col in open_map.items():
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

def increment_slot_for_unpick(row, position, open_map):
    """Inverse of decrement_slot_for_pick — prioritizes BENCH inverse the same way."""
    p = position.upper()
    def inc(col):
        val = int(pd.to_numeric(row.get(col,0), errors="coerce") or 0)
        row[col] = max(0, val + 1)
    if p in ("QB","K","DST"):
        col = open_map.get(p)
        if col: inc(col); return
        if open_map.get("BENCH"): inc(open_map["BENCH"]); return
    if p in ("RB","WR","TE"):
        # prefer exact position if that column exists; else FLEX; else BENCH
        if open_map.get(p): inc(open_map[p]); return
        if open_map.get("FLEX"): inc(open_map["FLEX"]); return
        if open_map.get("BENCH"): inc(open_map["BENCH"]); return
    if open_map.get("BENCH"): inc(open_map["BENCH"])

def recompute_maxbid_and_pps(row, open_map):
    """pps is per *open starter* now for UI purposes (bench ignored)."""
    b = float(pd.to_numeric(row.get("budget_remaining",0), errors="coerce") or 0)
    starters = starter_open_count_row(row, open_map)
    max_bid = int(max(0, round(b - max(0, starters - 1))))
    pps = int(round(b / starters)) if starters>0 else int(b)
    return max_bid, pps

def league_starter_needs(df_league: pd.DataFrame):
    """Return dict of league-wide open starter spots per position (FLEX contributes 1/3 to RB/WR/TE)."""
    needs = {p:0 for p in STARTER_POS}
    if df_league is None or df_league.empty: return needs
    open_map = detect_open_cols(df_league)
    if not open_map: return needs
    get = lambda s: pd.to_numeric(df_league.get(s, 0), errors="coerce").fillna(0).sum()
    for p in STARTER_POS:
        if p in open_map:
            needs[p] += int(get(open_map[p]))
    flex_total = int(get(open_map["FLEX"])) if "FLEX" in open_map else 0
    if flex_total:
        flex_share = round(flex_total/3)
        for p in ("RB","WR","TE"):
            needs[p] += flex_share
    return needs

def position_supply(df_players: pd.DataFrame):
    """Count undrafted players by position."""
    if df_players is None or df_players.empty: return {p:1 for p in STARTER_POS}
    df = df_players.copy()
    if "status" in df.columns:
        df = df[df["status"].astype(str).str.lower()!="drafted"]
    return {p:int((df["Position"]==p).sum()) for p in df["Position"].dropna().unique()}

def compute_liquidity_metrics(df_league: pd.DataFrame):
    """Returns DataFrame with team, $/open_starter and liquidity_factor (relative to league median)."""
    if df_league is None or df_league.empty or "team_name" not in df_league.columns:
        return pd.DataFrame(columns=["team_name","per_open_starter","liquidity_factor"])
    open_map = detect_open_cols(df_league)
    rows=[]
    for _,t in df_league.iterrows():
        br = float(pd.to_numeric(t.get("budget_remaining",0), errors="coerce") or 0)
        starters = starter_open_count_row(t, open_map) if open_map else int(pd.to_numeric(t.get("roster_spots_open",0), errors="coerce") or 0)
        pos = br / starters if starters>0 else br
        rows.append({"team_name":str(t.get("team_name","")), "per_open_starter":pos})
    L = pd.DataFrame(rows)
    med = float(L["per_open_starter"].median()) if not L.empty else 1.0
    if med == 0: med = 1.0
    L["liquidity_factor"] = L["per_open_starter"] / med
    return L

# --------------------------- Recommendations (inflation+scarcity, no team bias) ---------------------------
def compute_recommended_values(df_players: pd.DataFrame, budget=200, teams=14, league_df: pd.DataFrame=None, draft_df: pd.DataFrame=None):
    """Compute soft_rec_$ using:
       base=AAV (fallback VOR/Points) → blended inflation (global+recent) → positional scarcity multiplier.
       Team bias removed. hard_cap_$ = soft_rec_$ * 1.10.
    """
    df = df_players.copy()
    aav = pd.to_numeric(df.get("AAV"), errors="coerce")
    vor = pd.to_numeric(df.get("VOR"), errors="coerce")
    pts = pd.to_numeric(df.get("Points"), errors="coerce")

    # --- Base values ---
    base = aav.copy() if "AAV" in df.columns else pd.Series([float("nan")]*len(df), index=df.index)
    if base.isna().all() and vor.notna().sum()>0:
        pos_v = vor.clip(lower=0); pool=budget*teams; tv=pos_v.sum()
        base = (pos_v/tv*pool) if tv>0 else pos_v
    if base.isna().all() and pts.notna().sum()>0:
        pos_p = pts.clip(lower=0); pool=budget*teams; tp=pos_p.sum()
        base = (pos_p/tp*pool) if tp>0 else pos_p
    if base.isna().all():
        base = pd.Series([0.0]*len(df), index=df.index)

    drafted = df.get("status","").astype(str).str.lower().eq("drafted")
    paid = pd.to_numeric(df.get("price_paid"), errors="coerce").fillna(0)

    # --- Global inflation (remaining budget ÷ remaining base) or fallback to actual/expected ---
    inflation_global = 1.0
    used_remaining_model = False
    try:
        if league_df is not None and not league_df.empty and "budget_remaining" in league_df.columns:
            remaining_budget = pd.to_numeric(league_df["budget_remaining"], errors="coerce").fillna(0).sum()
            remaining_base   = base.where(~drafted, 0).sum(skipna=True)
            if remaining_base and remaining_base > 0:
                inflation_global = float(remaining_budget) / float(remaining_base)
                used_remaining_model = True
    except Exception:
        pass
    if not used_remaining_model:
        exp_spend = base.where(drafted, 0).sum(skipna=True)
        act_spend = paid.sum()
        if exp_spend and exp_spend>0:
            inflation_global = act_spend/exp_spend

    # --- Recent inflation from last ~12 picks (avg price ÷ avg AAV) ---
    inflation_recent = inflation_global
    try:
        if draft_df is not None and not draft_df.empty:
            recent = draft_df.tail(12).copy()
            # join recent picks to AAV by Player/Team/Position to get their baseline
            keys = ["Player","Team","Position"]
            have_keys = all(k in df.columns for k in keys)
            if have_keys and all(k in recent.columns for k in ["Player","Team","Position","price"]):
                tmp = recent.merge(df[keys+["AAV"]], on=keys, how="left", suffixes=("","_p"))
                avg_paid = pd.to_numeric(tmp["price"], errors="coerce").mean()
                avg_aav  = pd.to_numeric(tmp["AAV"], errors="coerce").mean()
                if avg_aav and avg_aav>0:
                    inflation_recent = max(0.5, min(2.0, float(avg_paid/avg_aav)))
    except Exception:
        pass

    # --- Blend and clamp (keep conservative to avoid overreacting) ---
    blended_infl = 0.5*float(inflation_global) + 0.5*float(inflation_recent)
    blended_infl = max(0.85, min(1.20, blended_infl))

    # --- Positional scarcity (starters only; FLEX split across RB/WR/TE) ---
    scarcity_mult = pd.Series([1.0]*len(df), index=df.index)
    try:
        needs = league_starter_needs(league_df) if league_df is not None else {}
        supply = position_supply(df)
        ratios = {}
        for p in df["Position"].dropna().unique().tolist():
            need = float(needs.get(p, 0))
            sup  = float(max(1, supply.get(p, 1)))
            ratios[p] = (need/sup) if sup>0 else 1.0
        if ratios:
            median_ratio = pd.Series(ratios).median()
            if not median_ratio or median_ratio<=0: median_ratio = 1.0
            # sensitivity 0.35, bounded 0.90–1.15
            def mult_for_pos(p):
                r = ratios.get(p, 1.0) / median_ratio
                m = 1.0 + 0.35*(r - 1.0)
                return float(max(0.90, min(1.15, m)))
            scarcity_mult = df["Position"].map(mult_for_pos).fillna(1.0)
    except Exception:
        pass

    soft = (base * blended_infl * scarcity_mult).clip(lower=1).round(0)
    hard = (soft * 1.10).round(0)

    out = df.copy()
    out["(auto) inflation_index"] = blended_infl
    out["soft_rec_$"] = soft
    out["hard_cap_$"] = hard
    return out

def write_recommendations_to_players(sh, teams=14, budget=200):
    ws = sh.worksheet("Players")
    df = normalize_cols(ws_to_df(ws))
    for c in ["AAV","VOR","Points","status","price_paid","Team","Player","Position","Rank Position"]:
        if c not in df.columns: df[c] = ""

    # Load League_Teams for remaining-budget model and scarcity
    league_df_local = pd.DataFrame()
    try:
        ws_league = sh.worksheet("League_Teams")
        league_df_local = normalize_cols(ws_to_df(ws_league))
    except Exception:
        pass

    # Load Draft_Log for recent inflation blending
    draft_df_local = pd.DataFrame()
    try:
        ws_d = sh.worksheet("Draft_Log")
        draft_df_local = normalize_cols(ws_to_df(ws_d))
        for c in ["price"]:
            if c in draft_df_local.columns:
                draft_df_local[c] = pd.to_numeric(draft_df_local[c], errors="coerce")
    except Exception:
        pass

    out = compute_recommended_values(df, budget=budget, teams=teams, league_df=league_df_local, draft_df=draft_df_local)
    merged = df.merge(
        out[["Player","Team","Position","(auto) inflation_index","soft_rec_$","hard_cap_$"]],
        on=["Player","Team","Position"], how="left", suffixes=("","_new")
    )
    for c in ["(auto) inflation_index","soft_rec_$","hard_cap_$"]:
        if f"{c}_new" in merged.columns:
            merged[c] = merged[f"{c}_new"]; merged.drop(columns=[f"{c}_new"], inplace=True, errors="ignore")

    # --- preserve tag columns on write ---
    merged = ensure_tag_columns(merged)

    write_dataframe_to_sheet(ws, merged, header=True)
    return True, "Recommendations updated."

# --------------------------- League_Teams helpers (per-position) ---------------------------
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
        # keep underlying column unchanged; we display per-starter in UI
        if "(auto)_$per_open_slot" in df.columns: df.at[i, "(auto)_$per_open_slot"] = int(pps)
    else:
        if "roster_spots_open" in df.columns:
            r = int(pd.to_numeric(df.at[i],"roster_spots_open", errors="coerce") or 0)  # legacy guard
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

# --------------------------- Draft Updates ---------------------------
def update_player_drafted(sh, player_key, manager, price):
    ws = sh.worksheet("Players")
    df = normalize_cols(ws_to_df(ws))
    for c in ["status","drafted_by","price_paid"]:
        if c not in df.columns: df[c]=""
    # --- ensure tag columns are retained ---
    df = ensure_tag_columns(df)

    mask = (df["Player"]==player_key[0]) & (df["Team"]==player_key[1]) & (df["Position"]==player_key[2])
    if not mask.any(): raise RuntimeError("Player not found in Players sheet.")
    idx = df.index[mask][0]
    df.loc[idx,"status"]="drafted"
    df.loc[idx,"drafted_by"]=manager
    df.loc[idx,"price_paid"]=str(int(price)) if pd.notna(price) and price!="" else ""
    write_dataframe_to_sheet(ws, df, header=True)
    return True

def clear_player_drafted(sh, player_key):
    """Inverse of update_player_drafted used by Undo."""
    ws = sh.worksheet("Players")
    df = normalize_cols(ws_to_df(ws))
    df = ensure_tag_columns(df)
    mask = (df["Player"]==player_key[0]) & (df["Team"]==player_key[1]) & (df["Position"]==player_key[2])
    if not mask.any(): return False
    idx = df.index[mask][0]
    df.loc[idx,"status"]=""
    df.loc[idx,"drafted_by"]=""
    df.loc[idx,"price_paid"]=""
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
    if df.empty: return pd.DataFrame(), pd.DataFrame()

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
        value_targets["Tags"] = value_targets.apply(lambda r: " ".join([
            "⭐" if is_truthy(r.get("FFG_MyGuy","")) else "",
            "💤" if is_truthy(r.get("FFG_Sleeper","")) else "",
            "⚠️" if is_truthy(r.get("FFG_Bust","")) else "",
            "💎" if is_truthy(r.get("FFG_Value","")) else "",
            "🚀" if is_truthy(r.get("FFG_Breakout","")) else "",
            "⛔" if is_truthy(r.get("FFG_Avoid","")) else "",
            "🩹" if is_truthy(r.get("FFG_Injury","")) else "",
        ]).replace("  "," ").strip(), axis=1)

    if not enforcers.empty:
        enforcers["why"] = enforcers.apply(reason, axis=1)
        enforcers["Tags"] = enforcers.apply(lambda r: " ".join([
            "⭐" if is_truthy(r.get("FFG_MyGuy","")) else "",
            "💤" if is_truthy(r.get("FFG_Sleeper","")) else "",
            "⚠️" if is_truthy(r.get("FFG_Bust","")) else "",
            "💎" if is_truthy(r.get("FFG_Value","")) else "",
            "🚀" if is_truthy(r.get("FFG_Breakout","")) else "",
            "⛔" if is_truthy(r.get("FFG_Avoid","")) else "",
            "🩹" if is_truthy(r.get("FFG_Injury","")) else "",
        ]).replace("  "," ").strip(), axis=1)

    return value_targets, enforcers

# --------------------------- Bidding Heatmap ---------------------------
def build_bidding_heatmap(league_df: pd.DataFrame):
    if league_df is None or league_df.empty or "team_name" not in league_df.columns or "budget_remaining" not in league_df.columns:
        return pd.DataFrame()
    df = league_df.copy()
    open_map = detect_open_cols(df)
    if not open_map:
        return pd.DataFrame()

    # Need weights: true slot=1.0, FLEX-eligible=0.6 (applied to RB/WR/TE via FLEX availability), BENCH-only=0.2
    rows = []
    for _, t in df.iterrows():
        team = str(t.get("team_name",""))
        br   = pd.to_numeric(t.get("budget_remaining",""), errors="coerce")
        mb   = pd.to_numeric(t.get("max_bid",""), errors="coerce")
        # For heatmap, keep legacy pps column usage (no functional change)
        pps  = pd.to_numeric(t.get("(auto)_$per_open_slot",""), errors="coerce")
        if pd.isna(br): br = 0
        if pd.isna(mb): mb = 0
        if pd.isna(pps): pps = 0

        def get(col):
            return int(pd.to_numeric(t.get(col,0), errors="coerce") or 0)

        avail = {p: get(open_map[p]) if p in open_map else 0 for p in ["QB","RB","WR","TE","FLEX","K","DST","BENCH"]}
        # baseline heat: if slot available, potential = max(pps, min(mb, br)) scaled by need type
        base_potential = max(int(pps), int(min(br, mb)))
        if base_potential < 0: base_potential = 0

        row_vals = {}
        for p in ("QB","RB","WR","TE","K","DST"):
            heat = 0.0
            if avail.get(p,0) > 0:
                heat = base_potential * 1.0
            elif p in ("RB","WR","TE") and avail.get("FLEX",0) > 0:
                heat = base_potential * 0.6
            elif avail.get("BENCH",0) > 0:
                heat = base_potential * 0.2
            row_vals[p] = round(heat, 0)
        rows.append({"Team": team, **row_vals})

    H = pd.DataFrame(rows)
    H = H.set_index("Team")
    return H
    
    def build_nomination_traps(players_df: pd.DataFrame, league_df: pd.DataFrame, top_n: int = 10):
    if players_df is None or players_df.empty:
        return pd.DataFrame()
    df = players_df.copy()
    df = df[df.get("status","").astype(str).str.lower()!="drafted"].copy()

    for c in ["soft_rec_$","AAV","ADP","VOR","Points","Rank Position"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    base_val = df["AAV"].copy()
    if base_val.isna().all():
        base_val = (df["soft_rec_$"] / 1.15)
    df["surplus_$"] = df["soft_rec_$"] - base_val  # negative is bad (overpay risk)

    # Avoid weight (very strong) — safe when column missing
    avoid_series = df["FFG_Avoid"] if "FFG_Avoid" in df.columns else pd.Series([""]*len(df), index=df.index)
    df["avoid_flag"] = avoid_series.astype(str).str.lower().isin(["true","1","yes","y"]).astype(int)

    # Outbid count awareness
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

    # Scarcity factor reused
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

    # Trap score (heavily weight Avoid)
    neg_surplus = (-df["surplus_$"]).clip(lower=0)
    df["trap_score"] = (
        0.60*df["avoid_flag"]
        + 0.20*neg_surplus.rank(pct=True)
        + 0.15*df["outbid_count"].rank(pct=True)
        + 0.05*df["scarcity_factor"].rank(pct=True)
    )

    df["Tags"] = df.apply(lambda r: " ".join([
        "⭐" if is_truthy(r.get("FFG_MyGuy","")) else "",
        "💤" if is_truthy(r.get("FFG_Sleeper","")) else "",
        "⚠️" if is_truthy(r.get("FFG_Bust","")) else "",
        "💎" if is_truthy(r.get("FFG_Value","")) else "",
        "🚀" if is_truthy(r.get("FFG_Breakout","")) else "",
        "⛔" if is_truthy(r.get("FFG_Avoid","")) else "",
        "🩹" if is_truthy(r.get("FFG_Injury","")) else "",
    ]).replace("  "," ").strip(), axis=1)

    cols = ["Tags","Position","Player","Team","soft_rec_$","AAV","surplus_$","outbid_count"]
    for c in cols:
        if c not in df.columns: df[c]=""
    return df.sort_values(["trap_score"], ascending=False).head(top_n)[cols + ["trap_score"]]
    

# --------------------------- Nomination Trap Finder ---------------------------
with st.expander("🪤 Nomination Trap Finder", expanded=False):
    traps = build_nomination_traps(players_df if not players_df.empty else pd.DataFrame(),
                                   league_df if not league_df.empty else pd.DataFrame(),
                                   top_n=12)
    if traps.empty:
        st.caption("No traps detected.")
    else:
        st.dataframe(traps, hide_index=True, use_container_width=True, height=300)
        st.caption("Heavily weights your Avoid tag, plus overpay pressure, outbid count, and scarcity.")

# --------------------------- Quick Tag Editor (sticky open + immediate refresh) ---------------------------
with st.expander("🏷️ Quick Tag Editor", expanded=True):
    if players_df.empty:
        st.caption("Load and sync players first.")
    else:
        tg_player = st.selectbox("Player", players_df["Player"].tolist(), key="tag_player")
        meta_row = players_df.loc[players_df["Player"]==tg_player].head(1)
        if not meta_row.empty:
            st.caption(f"{str(meta_row['Position'].iloc[0])} · {str(meta_row['Team'].iloc[0])}")
        candidate_cols = ["FFG_MyGuy","FFG_Sleeper","FFG_Bust","FFG_Value","FFG_Breakout","FFG_Avoid","FFG_Injury"]
        pretty_map = {"FFG_MyGuy":"My Guy","FFG_Sleeper":"Sleeper","FFG_Bust":"Bust","FFG_Value":"Value","FFG_Breakout":"Breakout","FFG_Avoid":"Avoid","FFG_Injury":"Injured"}
        present = [c for c in candidate_cols if c in players_df.columns]
        if not present:
            st.caption("No tag columns found. Toggling will create them in Players.")
            present = candidate_cols
        nice = [pretty_map[c] for c in present]
        choice = st.selectbox("Tag", nice, key="tag_choice")
        tgt_col = [k for k,v in pretty_map.items() if v==choice][0]
        do_tag = st.button("💾 Toggle Tag", disabled=not (write_ready and not practice))
        if do_tag and write_ready and not practice:
            try:
                ws = sh.worksheet("Players"); df = normalize_cols(ws_to_df(ws))
                # ensure all tag columns persist on write
                df = ensure_tag_columns(df)
                if tgt_col not in df.columns: df[tgt_col]=""
                mask = df["Player"].astype(str).eq(tg_player)
                if not mask.any():
                    st.error("Player not found in Players sheet.")
                else:
                    idx = df.index[mask][0]
                    df.at[idx, tgt_col] = "FALSE" if is_truthy(df.at[idx, tgt_col]) else "TRUE"
                    write_dataframe_to_sheet(ws, df, header=True)
                    st.toast(f"{choice} toggled for {tg_player}")
                    st.cache_data.clear()
                    st.rerun()  # immediate refresh of tags everywhere
            except Exception as e:
                st.error(f"Tag update failed: {e}")

# Footer
st.caption("Auction GM • in-table draft • roster grid • heatmap • traps • tiers • liquidity • cached logs")
