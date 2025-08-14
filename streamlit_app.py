# streamlit_app.py — Auction GM (stable build: in-table drafting, teams grid, heatmap, trap finder)
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
}

IDENTITY_COLS = ["Player","Team","Position"]
PROJ_COLS = ["Points","VOR","ADP","AAV","Rank Overall","Rank Position"]

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

    # --- preserve tag columns on write ---
    merged = ensure_tag_columns(merged)

    write_dataframe_to_sheet(ws, merged, header=True)
    return True, "Recommendations updated."

# --------------------------- League_Teams helpers (per-position) ---------------------------
POS_KEYS = ["QB","RB","WR","TE","FLEX","K","DST","BENCH"]

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
        pps  = pd.to_numeric(t.get("(auto)_$per_open_slot",""), errors="coerce")
        if pd.isna(br): br = 0
        if pd.isna(mb): mb = 0
        if pd.isna(pps): pps = 0

        def get(col):
            return int(pd.to_numeric(t.get(col,0), errors="coerce") or 0)

        avail = {p: get(open_map[p]) if p in open_map else 0 for p in POS_KEYS}
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

# --------------------------- Nomination Trap Finder ---------------------------
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
    df["trap_score"] = 0.60*df["avoid_flag"] + 0.20*neg_surplus.rank(pct=True) + 0.15*df["outbid_count"].rank(pct=True) + 0.05*df["scarcity_factor"].rank(pct=True)

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

# --------------------------- UI — Sidebar ---------------------------
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

    # --- Admin Console lives in the sidebar ---
    if admin_mode:
        st.divider()
        st.subheader("🛠️ Admin Console")
        c1, c2 = st.columns(2)
        if c1.button("♻️ Reset Draft"):
            if write_ready and not practice:
                with st.spinner("Resetting…"):
                    ok,msg = admin_reset(sh)  # assumes these helpers exist in your environment
                st.success(msg) if ok else st.error(msg)
                st.cache_data.clear(); st.rerun()
            else:
                st.warning("Enable write access and turn off Practice Mode.")
        if c2.button("📦 Archive + Reset"):
            if write_ready and not practice:
                with st.spinner("Archiving & resetting…"):
                    ok,msg = admin_archive_and_reset(sh)  # assumes these helpers exist in your environment
                st.success(msg) if ok else st.error(msg)
                st.cache_data.clear(); st.rerun()
            else:
                st.warning("Enable write access and turn off Practice Mode.")

# Tiny league header
if SLEEPER_LEAGUE_ID:
    try:
        league = sleeper_get(f"league/{SLEEPER_LEAGUE_ID}")
        st.caption(f"**{league.get('name','—')}** • {league.get('total_rosters','—')} teams • Season {league.get('season','—')}")
    except Exception:
        pass

# --------------------------- Load core data ---------------------------
players_df = pd.DataFrame()
league_df = pd.DataFrame()
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

# Safety net
if players_df is None or players_df.empty:
    players_df = pd.DataFrame()
for c in ["status","Position","Player","Team","soft_rec_$","AAV","ADP","VOR","Points","Rank Overall","Rank Position","drafted_by","price_paid"]:
    if c not in players_df.columns:
        players_df[c] = (float("nan") if c in ("soft_rec_$","AAV","ADP","VOR","Points","Rank Overall","Rank Position") else "")

# --------------------------- Top Row: Draft Log + Best Values + Teams ---------------------------
st.divider()
col_log, col_best, col_teams = st.columns([1.2, 1.1, 1.3], gap="large")

with col_log:
    st.subheader("📜 Draft Log + Live Trends")
    if not write_ready:
        st.caption("Connect write access to enable Draft Log.")
    else:
        dfD = get_draft_log_cached(SHEET_ID, SA_JSON)
        show_cols = ["pick","player","team","position","manager","price"]
        for c in show_cols:
            if c not in dfD.columns: dfD[c]=""
        if dfD.empty:
            st.caption("No picks yet.")
        else:
            dfD_disp = dfD.copy()
            dfD_disp["price"] = pd.to_numeric(dfD_disp["price"], errors="coerce").fillna(0).astype(int)
            st.dataframe(dfD_disp[show_cols].tail(15), hide_index=True, use_container_width=True, height=240)
            st.caption(f"Total picks: {len(dfD_disp)} • Avg price: ${int(dfD_disp['price'].mean()) if len(dfD_disp)>0 else 0}")

with col_best:
    st.subheader("💎 Best Remaining Values")
    if players_df.empty:
        st.caption("Load players first.")
    else:
        dfv = players_df.copy()
        for c in ["soft_rec_$","AAV","Points","VOR","ADP","Rank Overall","Rank Position"]:
            if c in dfv.columns:
                dfv[c] = pd.to_numeric(dfv[c], errors="coerce")
        dfv = dfv[dfv["status"].astype(str).str.lower()!="drafted"]
        dfv["value_surplus"] = dfv["soft_rec_$"] - dfv["AAV"]

        def tag_icons(row):
            out=[]
            if is_truthy(row.get("FFG_MyGuy","")): out.append("⭐")
            if is_truthy(row.get("FFG_Sleeper","")): out.append("💤")
            if is_truthy(row.get("FFG_Bust","")): out.append("⚠️")
            if is_truthy(row.get("FFG_Value","")): out.append("💎")
            if is_truthy(row.get("FFG_Breakout","")): out.append("🚀")
            if is_truthy(row.get("FFG_Avoid","")): out.append("⛔")
            if is_truthy(row.get("FFG_Injury","")): out.append("🩹")
            return " ".join(out)
        dfv["Tags"] = dfv.apply(tag_icons, axis=1) if not dfv.empty else ""

        pos_opts = sorted([p for p in dfv["Position"].dropna().unique().tolist() if p])
        pos_filter = st.multiselect("Filter positions", pos_opts, default=[])
        if pos_filter:
            dfv = dfv[dfv["Position"].isin(pos_filter)]

        # Build compact display with accurate Player, no Team, and Pos Rk
        disp_cols = ["Tags","Position","Player","soft_rec_$","AAV","value_surplus","Rank Position"]
        for c in disp_cols:
            if c not in dfv.columns: dfv[c] = ""
        display_df = dfv.sort_values(["value_surplus","soft_rec_$"], ascending=[False,False]).head(15)[disp_cols].rename(
            columns={
                "Position":"Pos",
                "soft_rec_$":"Rec $",
                "value_surplus":"Δ$",
                "Rank Position":"Pos Rk",
            }
        )
        st.dataframe(display_df, hide_index=True, use_container_width=True, height=240)

with col_teams:
    st.subheader("👥 Teams")
    if league_df.empty or "team_name" not in league_df.columns:
        st.caption("League_Teams not available.")
    else:
        team_opts = league_df["team_name"].astype(str).tolist()
        default_idx = 1 if len(team_opts)>=2 and team_opts[1].strip().lower()=="my team" else 0
        choose_team = st.selectbox("Team", team_opts, index=default_idx)

        rowT = league_df.loc[league_df["team_name"]==choose_team].head(1)
        if not rowT.empty:
            br = safe_int_val(rowT.get("budget_remaining", 0), 0)
            mb = safe_int_val(rowT.get("max_bid", 0), 0)
            pps = safe_int_val(rowT.get("(auto)_$per_open_slot", 0), 0)
            m1, m2, m3 = st.columns(3)
            m1.metric("Budget Remaining", f"${br}")
            m2.metric("Max Bid", f"${mb}")
            m3.metric("$ / Open Slot", f"${pps}")

        # Build a blank roster template
        template = [
            ("QB", 1),
            ("RB", 2),
            ("WR", 2),
            ("TE", 1),
            ("FLEX", 1),
            ("K", 1),
            ("DST", 1),
            ("BENCH", 6),
        ]
        roster_rows = []
        for slot, count in template:
            for _ in range(count):
                roster_rows.append({"Slot":slot, "Player":"", "Pos":"", "Team":"", "Paid":0})
        roster_df = pd.DataFrame(roster_rows)

        if players_df.empty:
            st.dataframe(roster_df, hide_index=True, use_container_width=True, height=260)
        else:
            drafted = players_df.copy()
            drafted["price_paid"] = pd.to_numeric(drafted.get("price_paid",""), errors="coerce").fillna(0)
            drafted = drafted[
                drafted.get("drafted_by","").astype(str).str.strip().str.lower() == choose_team.strip().lower()
            ].copy()

            def place(player_row):
                pos = str(player_row.get("Position","")).upper()
                paid = safe_int_val(player_row.get("price_paid",0), 0)
                name = player_row.get("Player","")
                tm   = player_row.get("Team","")
                def try_fill(slot):
                    idx = roster_df.index[(roster_df["Slot"]==slot) & (roster_df["Player"]=="")]
                    if len(idx)>0:
                        i = idx[0]
                        roster_df.at[i,"Player"]=name
                        roster_df.at[i,"Pos"]=pos
                        roster_df.at[i,"Team"]=tm
                        roster_df.at[i,"Paid"]=paid
                        return True
                    return False
                if pos in ("QB","RB","WR","TE","K","DST"):
                    if try_fill(pos): return True
                    if pos in ("RB","WR","TE") and try_fill("FLEX"): return True
                    return try_fill("BENCH")
                return try_fill("BENCH")

            for _, prow in drafted.sort_values(["Position","Player"]).iterrows():
                place(prow)

            st.dataframe(roster_df, hide_index=True, use_container_width=True, height=260)

# --------------------------- Filters (under the top 3 modules) ---------------------------
st.divider()
f1,f2,f3,f4,f5 = st.columns([2,1,1,1,1], gap="large")
with f1: q = st.text_input("Search", "")
with f2:
    pos_opts = sorted([p for p in players_df.get("Position", pd.Series()).dropna().unique().tolist() if p])
    pos_sel = st.multiselect("Positions", pos_opts, default=[])
with f3:
    team_opts = sorted([t for t in players_df.get("Team", pd.Series()).dropna().unique().tolist() if t])
    team_sel = st.multiselect("Teams", team_opts, default=[])
with f4: hide_drafted = st.toggle("Hide drafted", value=True)
with f5: sort_by = st.selectbox("Sort by", ["Rank Overall","soft_rec_$","AAV","VOR","Points","ADP","Rank Position"], index=0)

# --------------------------- Draft Board (with checkbox selection & inline form) ---------------------------
st.subheader("📋 Draft Board")

if players_df.empty:
    st.info("Players sheet is empty or unavailable. Run Smart Sync after importing Projections.")
else:
    view = players_df.copy()
    for c in ["Points","VOR","ADP","AAV","soft_rec_$","hard_cap_$","price_paid","Rank Overall","Rank Position"]:
        if c in view.columns:
            view[c] = pd.to_numeric(view[c], errors="coerce")

    def tag_icons_row(row):
        out=[]
        if is_truthy(row.get("FFG_MyGuy","")): out.append("⭐")
        if is_truthy(row.get("FFG_Sleeper","")): out.append("💤")
        if is_truthy(row.get("FFG_Bust","")): out.append("⚠️")
        if is_truthy(row.get("FFG_Value","")): out.append("💎")
        if is_truthy(row.get("FFG_Breakout","")): out.append("🚀")
        if is_truthy(row.get("FFG_Avoid","")): out.append("⛔")
        if is_truthy(row.get("FFG_Injury","")): out.append("🩹")
        return " ".join(out)
    view["Tags"] = view.apply(tag_icons_row, axis=1) if not view.empty else ""

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

    # Add "Pos Rk" derived column for display convenience
    view["Pos Rk"] = pd.to_numeric(view.get("Rank Position"), errors="coerce")

    show_cols = ["Tags","Position","Player","Team","soft_rec_$","hard_cap_$","AAV","VOR","Points","ADP","Rank Overall","status","drafted_by","price_paid","Pos Rk"]
    for c in show_cols:
        if c not in view.columns: view[c]=""

    # Checkbox select column (local only). Keep _key internal; don't display it.
    view = view.copy()
    view["Select"] = False
    view["_key"]   = view["Player"].astype(str) + " | " + view["Team"].astype(str) + " | " + view["Position"].astype(str)

    edited = st.data_editor(
        view[["Select"] + show_cols],  # NOTE: no "_key" in displayed columns
        use_container_width=True,
        height=520,
        column_config={
            "Select": st.column_config.CheckboxColumn("Draft?", help="Check to draft this player"),
            "Position": st.column_config.TextColumn("Pos"),
            "soft_rec_$": st.column_config.NumberColumn("Rec $"),
            "hard_cap_$": st.column_config.NumberColumn("Hard Cap"),
            "status": st.column_config.TextColumn("Status"),
            "drafted_by": st.column_config.TextColumn("Drafted By"),
            "price_paid": st.column_config.NumberColumn("Price"),
            "Pos Rk": st.column_config.NumberColumn("Pos Rk"),
        },
        disabled=show_cols,  # only Select is editable
        hide_index=True,
        key="draft_table"
    )

    st.write("")
    draft_sel_btn = st.button("✅ Draft Selected", type="primary", use_container_width=False, disabled=not (write_ready and not practice))

    # --- Inline confirmation form (no modal) ---
    if draft_sel_btn and write_ready and not practice:
        sel_rows = edited[edited["Select"]==True]
        if sel_rows.empty:
            st.warning("Check a player in the Draft? column first.")
        elif len(sel_rows) > 1:
            st.warning("Please select exactly one player at a time.")
        else:
            row = sel_rows.iloc[0]
            name, team, pos = row["Player"], row["Team"], row["Position"]
            default_price = safe_int_val(row.get("soft_rec_$", 1), 1)

            mgr_opts = []
            if not league_df.empty and "team_name" in league_df.columns:
                mgr_opts = sorted([t for t in league_df["team_name"].dropna().astype(str).tolist() if t])

            # keep everything above intact…
            if draft_sel_btn and write_ready and not practice:
                sel_rows = edited[edited["Select"]==True]
                if sel_rows.empty:
                    st.warning("Check a player in the Draft? column first.")
                elif len(sel_rows) > 1:
                    st.warning("Please select exactly one player at a time.")
                else:
                    row = sel_rows.iloc[0]
                    name, team, pos = row["Player"], row["Team"], row["Position"]
                    default_price = safe_int_val(row.get("soft_rec_$", 1), 1)

                    mgr_opts = []
                    if not league_df.empty and "team_name" in league_df.columns:
                        mgr_opts = sorted([t for t in league_df["team_name"].dropna().astype(str).tolist() if t])

                    # NEW: persist selection for the next rerun/submit
                    st.session_state["pending_draft"] = {
                        "name": name, "team": team, "pos": pos,
                        "default_price": default_price, "mgr_opts": mgr_opts
                    }
                    
                    # NEW: render/handle the confirmation form on every rerun if we have a pending draft
                    if "pending_draft" in st.session_state:
                        pdraft = st.session_state["pending_draft"]
                        name, team, pos = pdraft["name"], pdraft["team"], pdraft["pos"]
                        mgr_opts = pdraft.get("mgr_opts", []) or [""]

                        with st.form("confirm_draft_form", clear_on_submit=False):
                            st.markdown(f"**Draft:** {name} · {pos} · {team}")
                            sel_mgr = st.selectbox("Team (buyer)", mgr_opts, index=0, key="confirm_mgr")
                            sel_price = st.number_input("Price", min_value=1, max_value=500, step=1,
                                                        value=int(pdraft.get("default_price", 1)), key="confirm_price")
                            c1, c2 = st.columns(2)
                            do_confirm = c1.form_submit_button("Confirm Draft")
                            do_cancel  = c2.form_submit_button("Cancel")

                            if do_confirm:
                                try:
                                    update_player_drafted(sh, (name, team, pos), sel_mgr, sel_price)
                                    wsD = upsert_worksheet(sh, "Draft_Log")
                                    ensure_draft_log_header(wsD)
                                    pick_no = next_pick_number(wsD)
                                    append_draft_log(sh, {
                                        "pick": pick_no,
                                        "player": name,
                                        "team": team,
                                        "position": pos,
                                        "manager": sel_mgr,
                                        "price": str(int(sel_price))
                                    })
                                    ok, msg = update_league_team_after_pick(sh, sel_mgr, pos, sel_price)
                                    if not ok:
                                        st.warning(msg)
                                    write_recommendations_to_players(sh)
                                    st.toast(f"Drafted {name} for ${int(sel_price)}.")
                                except Exception as e:
                                    st.error(f"Draft flow failed: {e}")
                                finally:
                                    # Always clean up and rerun for a fresh UI/state
                                    del st.session_state["pending_draft"]
                                    st.cache_data.clear()
                                    st.rerun()

                            if do_cancel:
                                del st.session_state["pending_draft"]
                                st.rerun()


# --------------------------- Nomination Recommendations ---------------------------
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
            cols = ["Tags","Position","Player","Team","soft_rec_$","AAV","value_surplus","why"]
            for c in cols:
                if c not in val_list.columns: val_list[c]=""
            st.dataframe(val_list[cols], hide_index=True, use_container_width=True, height=240)
    with c_right:
        st.markdown("**Price Enforcers**")
        if enf_list.empty:
            st.caption("No candidates found.")
        else:
            cols = ["Tags","Position","Player","Team","soft_rec_$","AAV","outbid_count","why"]
            for c in cols:
                if c not in enf_list.columns: enf_list[c]=""
            st.dataframe(enf_list[cols], hide_index=True, use_container_width=True, height=240)

# --------------------------- Bidding Heatmap ---------------------------
with st.expander("🔥 Bidding Heatmap", expanded=False):
    H = build_bidding_heatmap(league_df if not league_df.empty else pd.DataFrame())
    if H.empty:
        st.caption("Heatmap unavailable (need League_Teams with open_* cols, budget_remaining, max_bid).")
    else:
        try:
            # format as integers for display, keep gradient
            styled = H.style.background_gradient(axis=None).format("{:.0f}")
            # dynamic height: ~28px per row + header room
            table_height = max(120, 32 + 28 * len(H.index))
            st.dataframe(styled, use_container_width=True, height=table_height)
            st.caption("Interpretation: hotter = higher potential spend & stronger need at that position.")
        except Exception:
            st.dataframe(H.round(0).astype(int), use_container_width=True, height=max(120, 32 + 28 * len(H.index)))

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
st.caption("Auction GM • in-table draft • roster grid • heatmap • traps • cached logs")
