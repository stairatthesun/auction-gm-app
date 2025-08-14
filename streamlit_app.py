# streamlit_app.py — Auction GM (stable build with fixes & tag-safe sync)
# - Draft Log fixed (records player/team/position + pick #)
# - Teams module shows roster grid + budgets
# - Admin Console: Ensure Tag Columns, Reset, Archive & Reset
# - Refresh button clears cache and reruns
# - Tag columns auto-preserved; never dropped on writes
# - BRV / Nom Recs / Trap Finder include Tags column (icons)
# - Phase Budgets removed per request
# - Safer casting for soft_rec_$ to avoid crashes

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
    # backward-compat keys
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
RECOGNIZED_TAGS = ["FFG_MyGuy","FFG_Sleeper","FFG_Bust","FFG_Value","FFG_Breakout","FFG_Avoid","FFG_Injury"]

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
            df[c] = df[c].astype(str).strip()
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

def safe_int_val(x, default=1):
    try:
        v = pd.to_numeric(x, errors="coerce")
        if getattr(v, "size", 1) != 1:
            v = v.iloc[0]
        return int(v) if pd.notna(v) else default
    except Exception:
        return default

def safe_num(x, default=0.0):
    try:
        v = pd.to_numeric(x, errors="coerce")
        if getattr(v, "size", 1) != 1:
            v = v.iloc[0]
        return float(v) if pd.notna(v) else default
    except Exception:
        return default

# --------------------------- Sleeper mini header ---------------------------
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
    # Ensure tag columns never disappear on write
    for col in RECOGNIZED_TAGS:
        if col not in df.columns:
            df[col] = ""
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
    # rename columns to target names
    col_map = {}
    for c in df.columns:
        k = str(c).strip().lower()
        if k in NAME_MAP: col_map[c] = NAME_MAP[k]
    df = df.rename(columns=col_map)
    # ensure columns exist
    for col in TARGET_PROJ_COLS:
        if col not in df.columns: df[col] = None
    df = df[TARGET_PROJ_COLS]
    # numeric coercion
    for col in ["Points","VOR","ADP","AAV","Rank Overall","Rank Position"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    for c in ["Player","Team","Position"]:
        df[c] = df[c].astype(str).str.strip()
    # strip IDP
    df = df[~df["Position"].str.upper().isin(IDP_POS)].reset_index(drop=True)
    # de-dup
    df = df.drop_duplicates(subset=["Player","Team","Position"], keep="first")
    return df

# --------------------------- Smart Sync (Projections → Players) ---------------------------
def smart_sync_projections_to_players(sh, preserve_tags=True, update_identity=False):
    ws_proj = sh.worksheet("Projections")
    ws_players = sh.worksheet("Players")

    df_p = normalize_cols(ws_to_df(ws_players))
    df_r = normalize_cols(ws_to_df(ws_proj))

    # strip IDP + de-dup in Projections
    if "Position" in df_r.columns:
        df_r = df_r[~df_r["Position"].str.upper().isin(IDP_POS)]
    if {"Player","Team","Position"}.issubset(df_r.columns):
        df_r = df_r.drop_duplicates(subset=["Player","Team","Position"], keep="first")

    # ensure required columns exist
    for c in IDENTITY_COLS:
        if c not in df_p.columns: df_p[c] = ""
    for c in PROJ_COLS:
        if c not in df_p.columns: df_p[c] = ""
        if c not in df_r.columns: df_r[c] = ""

    # ensure tags exist in Players and are preserved
    tag_cols = get_tag_columns(df_p)
    for col in RECOGNIZED_TAGS:
        if col not in df_p.columns:
            df_p[col] = ""  # create if missing
            tag_cols = get_tag_columns(df_p)

    keys = choose_keys(df_p, df_r)
    if not keys:
        return False, ("Couldn’t find matching join keys between Players and Projections. "
                       "Keep Players headers, clear rows 2+, and run with identity updates ON for first sync.")

    # update existing rows (projection fields only)
    merged = df_p.merge(df_r[IDENTITY_COLS + PROJ_COLS], how="left", on=keys, suffixes=("","_new"))
    for c in PROJ_COLS:
        nc = f"{c}_new"
        if nc in merged.columns:
            merged[c] = merged[nc].where(merged[nc].notna(), merged[c])
            merged.drop(columns=[nc], inplace=True, errors="ignore")

    # append new rows found in Projections
    left_keys = merged[keys].astype(str).apply("|".join, axis=1)
    right_only = df_r[~df_r[keys].astype(str).apply("|".join, axis=1).isin(left_keys)]
    if not right_only.empty:
        new_rows = right_only.copy()
        # ensure all columns exist and preserve tag cols
        for c in merged.columns:
            if c not in new_rows.columns:
                if preserve_tags and c in tag_cols:
                    new_rows[c] = "FALSE"
                else:
                    new_rows[c] = ""
        merged = pd.concat([merged, new_rows[merged.columns]], ignore_index=True)

    # identity lock unless allowed
    if not update_identity:
        for c in [col for col in IDENTITY_COLS if c in df_p.columns]:
            merged[c] = merged[c].where(df_p[c].notna() & (df_p[c]!=""), df_p[c])

    # final de-dup
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
    if (base.isna().all() if hasattr(base,"isna") else True) and vor.notna().sum()>0:
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
        if open_map.get("FLEX") and int(pd.to_numeric(row.get("open_flex",0), errors="coerce") or 0)>0:
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
def ensure_draft_log_header(ws):
    header = ws.row_values(1)
    need = ["pick","player","team","position","manager","price"]
    if not header:
        ws.update("A1",[need]); return need
    # re-order / ensure all present
    if header != need:
        ws.clear(); ws.update("A1",[need]); return need
    return header

def next_pick_number(ws):
    vals = ws.get_all_values()
    if not vals or len(vals)<=1:
        return 1
    return max(1, len(vals))  # header + rows ⇒ next pick = len(vals)

def append_draft_log(sh, row: dict):
    ws = upsert_worksheet(sh, "Draft_Log")
    header = ensure_draft_log_header(ws)
    out = [row.get(c,"") for c in header]
    ws.append_row(out, value_input_option="RAW")

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
    outbid_norm  = (df["outbid_count"]  - df["outbid_count"].median(skipna=True)).fillna(0)
    rp = pd.to_numeric(df.get("Rank Position"), errors="coerce")
    rp_inv = (-rp.fillna(rp.max() if rp.notna().any() else 999)).fillna(0)
    df["nom_score"] = 0.45*val_surplus + 0.30*scarcity_norm + 0.15*outbid_norm + 0.10*rp_inv

    value_targets = df.sort_values(["nom_score"], ascending=False).head(top_n).copy()
    enforcers    = df.sort_values(["outbid_count","scarcity_factor"], ascending=[False,False]).head(top_n).copy()

    # Tags icon column
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

    for dfX in (value_targets, enforcers):
        if not dfX.empty:
            dfX["Tags"] = dfX.apply(tag_icons, axis=1)

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

    if st.button("🔁 Refresh Data (clear cache)"):
        st.cache_data.clear()
        st.toast("Caches cleared.")
        st.rerun()

    if admin_mode:
        st.divider()
        st.header("Admin Console")
        st.caption("Tools write to the backend. Turn off Practice Mode to enable.")

        btn_tags = st.button("🧩 Ensure Tag Columns (FFG_*)", use_container_width=True, disabled=not (write_ready and not practice))
        if btn_tags and write_ready and not practice:
            try:
                ws = sh.worksheet("Players")
                df = normalize_cols(ws_to_df(ws))
                changed = False
                for col in RECOGNIZED_TAGS:
                    if col not in df.columns:
                        df[col] = ""
                        changed = True
                if changed:
                    write_dataframe_to_sheet(ws, df, header=True)
                st.success("Tag columns ensured.")
            except Exception as e:
                st.error(f"Failed: {e}")

        do_archive = st.checkbox("Archive before reset", value=False)
        btn_reset = st.button(("📦 Archive & Reset" if do_archive else "♻️ Reset League"), use_container_width=True, disabled=not (write_ready and not practice))
        if btn_reset and write_ready and not practice:
            try:
                if do_archive:
                    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                    for tab in ("Players","League_Teams","Draft_Log","Projections"):
                        try:
                            ws_src = sh.worksheet(tab)
                            rows = ws_src.get_all_values()
                            ws_dst = upsert_worksheet(sh, f"{tab}_Archive_{ts}", rows=max(1000,len(rows)+10), cols=max(30, len(rows[0]) if rows else 30))
                            if rows:
                                ws_dst.clear(); ws_dst.update("A1", rows, value_input_option="RAW")
                        except Exception:
                            pass

                # Reset Players
                wsP = sh.worksheet("Players")
                dfP = normalize_cols(ws_to_df(wsP))
                for c in ["status","drafted_by","price_paid"]:
                    if c not in dfP.columns: dfP[c] = ""
                dfP["status"] = ""
                dfP["drafted_by"] = ""
                dfP["price_paid"] = ""
                write_dataframe_to_sheet(wsP, dfP, header=True)

                # Reset Draft_Log
                wsD = upsert_worksheet(sh, "Draft_Log")
                wsD.clear(); wsD.update("A1", [["pick","player","team","position","manager","price"]])

                # Reset League_Teams
                wsL = sh.worksheet("League_Teams")
                dfL = normalize_cols(ws_to_df(wsL))
                budget_default = 200
                try:
                    wsS = sh.worksheet("Settings_League")
                    dfS = normalize_cols(ws_to_df(wsS))
                    bcol = next((c for c in dfS.columns if c.strip().lower()=="budget"), None)
                    if bcol and not dfS.empty:
                        val = pd.to_numeric(dfS[bcol], errors="coerce").dropna()
                        if not val.empty:
                            budget_default = int(val.iloc[0])
                except Exception:
                    pass
                if "budget_remaining" in dfL.columns:
                    dfL["budget_remaining"] = budget_default

                open_map = detect_open_cols(dfL)
                if open_map:
                    for i in dfL.index:
                        max_bid, pps = recompute_maxbid_and_pps(dfL.loc[i, :], open_map)
                        if "max_bid" in dfL.columns: dfL.at[i,"max_bid"] = int(max_bid)
                        if "(auto)_$per_open_slot" in dfL.columns: dfL.at[i,"(auto)_$per_open_slot"] = int(pps)

                write_dataframe_to_sheet(wsL, dfL, header=True)
                st.success("League reset complete.")
                st.rerun()
            except Exception as e:
                st.error(f"Reset failed: {e}")

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
if 'sh' in locals() and write_ready:
    try:
        players_df = normalize_cols(ws_to_df_cached(SHEET_ID, "Players", SA_JSON))
        # ensure tag columns exist in-memory
        for col in RECOGNIZED_TAGS:
            if col not in players_df.columns:
                players_df[col] = ""
        # de-dup
        if "player_id" in players_df.columns:
            players_df = players_df.drop_duplicates(subset=["player_id"], keep="first")
        elif {"Player","Team","Position"}.issubset(players_df.columns):
            players_df = players_df.drop_duplicates(subset=["Player","Team","Position"], keep="first")
        # strip IDP from view
        if "Position" in players_df.columns:
            players_df = players_df[~players_df["Position"].str.upper().isin(IDP_POS)]
    except Exception as e:
        st.error(f"Could not load Players sheet: {e}")
    try:
        league_df = normalize_cols(ws_to_df_cached(SHEET_ID, "League_Teams", SA_JSON))
    except Exception:
        league_df = pd.DataFrame()

# Safety net: ensure required columns exist
if players_df is None or players_df.empty:
    players_df = pd.DataFrame(columns=IDENTITY_COLS + PROJ_COLS + RECOGNIZED_TAGS)

for c in ["status","Position","Player","Team","soft_rec_$","AAV","ADP","VOR","Points","Rank Overall","Rank Position","price_paid","drafted_by"]:
    if c not in players_df.columns:
        players_df[c] = (float("nan") if c in ("soft_rec_$","AAV","ADP","VOR","Points","Rank Overall","Rank Position") else "")

# --------------------------- Top controls: Draft + Price-check ---------------------------
st.divider()
c_draft, c_price = st.columns([1.2, 1])

with c_draft:
    st.subheader("📝 Draft a Player")
    if players_df.empty:
        st.info("Load projections and run Smart Sync first.")
    else:
        # Options
        mgr_opts = []
        if not league_df.empty and "team_name" in league_df.columns:
            mgr_opts = sorted([t for t in league_df["team_name"].dropna().astype(str).tolist() if t])
        sel_name = st.selectbox("Player", players_df["Player"].tolist(), key="pick_player")
        meta_row = players_df.loc[players_df["Player"]==sel_name].head(1)
        t_show = str(meta_row["Team"].iloc[0]) if not meta_row.empty and "Team" in meta_row.columns else ""
        p_show = str(meta_row["Position"].iloc[0]) if not meta_row.empty and "Position" in meta_row.columns else ""
        st.caption(f"{p_show} · {t_show}")
        raw_soft = meta_row["soft_rec_$"].iloc[0] if (not meta_row.empty and "soft_rec_$" in meta_row.columns) else None
        base_soft = safe_int_val(raw_soft, default=1)
        sel_price = st.number_input("Price", min_value=1, max_value=500, step=1, value=base_soft, key="pick_price")
        sel_mgr = st.selectbox("Team (buyer)", mgr_opts if mgr_opts else [""], index=0 if mgr_opts else 0, placeholder="Select team…", key="pick_mgr")
        draft_btn = st.button("✅ Mark Drafted & Log", type="primary", use_container_width=True, disabled=not (write_ready and not practice))
        if draft_btn and write_ready and not practice:
            try:
                update_player_drafted(sh, (sel_name, t_show, p_show), sel_mgr, sel_price)
                # draft log with enforced header & pick number
                wsD = upsert_worksheet(sh, "Draft_Log")
                ensure_draft_log_header(wsD)
                pick_no = next_pick_number(wsD)
                append_draft_log(sh, {
                    "pick": pick_no,
                    "player": sel_name,
                    "team": t_show,
                    "position": p_show,
                    "manager": sel_mgr,
                    "price": str(int(sel_price))
                })
                # position-aware team updates
                ok,msg = update_league_team_after_pick(sh, sel_mgr, p_show, sel_price)
                if not ok: st.warning(msg)
                # quick recalc $ (not blocking)
                write_recommendations_to_players(sh)
                st.toast(f"Drafted {sel_name} for ${sel_price}.")
                st.rerun()
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
        raw_soft = row["soft_rec_$"].iloc[0] if (not row.empty and "soft_rec_$" in row.columns) else None
        base_soft  = safe_int_val(raw_soft, default=1)
        ob_price   = st.number_input("Price to check", min_value=1, max_value=500, step=1, value=base_soft, key="ob_price")

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

# --------------------------- Filters row ---------------------------
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

# --------------------------- Three-up row: Draft Log + BRV + Teams ---------------------------
col_log, col_brv, col_teams = st.columns([1.1, 1.1, 1.3])

with col_log:
    st.subheader("🧾 Draft Log + Live Trends")
    if 'sh' in locals() and write_ready:
        try:
            wsD = upsert_worksheet(sh, "Draft_Log")
            ensure_draft_log_header(wsD)
            dfD = normalize_cols(ws_to_df(wsD))
            if not dfD.empty:
                # basic live deltas
                dfD["price"] = pd.to_numeric(dfD.get("price", ""), errors="coerce")
                spent = dfD["price"].sum(skipna=True)
                st.caption(f"Total spent so far: ${int(spent)}")
                show_cols = ["pick","player","team","position","manager","price"]
                for c in show_cols:
                    if c not in dfD.columns: dfD[c]=""
                st.dataframe(dfD[show_cols].tail(15), use_container_width=True, height=260)
            else:
                st.caption("No picks yet.")
        except Exception as e:
            st.error(f"Log error: {e}")
    else:
        st.caption("Connect your Sheet to view logs.")

with col_brv:
    st.subheader("💎 Best Remaining Values")
    view_brv = players_df.copy()
    for c in ["soft_rec_$","AAV","VOR","Points","ADP","Rank Overall","Rank Position"]:
        if c in view_brv.columns:
            view_brv[c] = pd.to_numeric(view_brv[c], errors="coerce")
    view_brv = view_brv[~view_brv["status"].astype(str).str.lower().eq("drafted")]
    # multi-select positions
    brv_pos = st.multiselect("Filter positions", pos_opts, default=[])
    if brv_pos:
        view_brv = view_brv[view_brv["Position"].isin(brv_pos)]
    # Tags icon column
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
    if not view_brv.empty:
        base_val = view_brv["AAV"].copy()
        if base_val.isna().all():
            base_val = (view_brv["soft_rec_$"]/1.15)
        view_brv["surplus_$"] = (view_brv["soft_rec_$"] - base_val).round(1)
        view_brv["Tags"] = view_brv.apply(tag_icons, axis=1)
        cols = ["Tags","Position","Player","Team","soft_rec_$","AAV","surplus_$","VOR","Points","ADP","Rank Overall"]
        for c in cols:
            if c not in view_brv.columns: view_brv[c]=""
        view_brv = view_brv.sort_values(["surplus_$","soft_rec_$"], ascending=[False,False]).head(25)
        st.dataframe(view_brv[cols], use_container_width=True, height=260)
    else:
        st.caption("No undrafted players.")

with col_teams:
    st.subheader("👥 Teams (My Team default)")
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

        if players_df.empty:
            st.caption("Players not loaded.")
        else:
            drafted = players_df.copy()
            for c in ["price_paid"]:
                if c in drafted.columns:
                    drafted[c] = pd.to_numeric(drafted[c], errors="coerce")
            drafted = drafted[
                drafted.get("drafted_by","").astype(str).str.strip().str.lower() == choose_team.strip().lower()
            ].copy()

            def take(df, pos, n):
                mask = df["Position"].str.upper().eq(pos)
                got = df[mask].head(n)
                remaining = df.drop(got.index)
                return got, remaining

            remaining = drafted.sort_values(["Position","Player"]).copy()
            roster_rows = []

            qb, remaining = take(remaining, "QB", 1)
            rb, remaining = take(remaining, "RB", 2)
            wr, remaining = take(remaining, "WR", 2)
            te, remaining = take(remaining, "TE", 1)
            k,  remaining = take(remaining, "K", 1)
            dst,remaining = take(remaining, "DST", 1)

            flex_pool = remaining[remaining["Position"].str.upper().isin(["RB","WR","TE"])]
            flex = flex_pool.head(1)
            remaining = remaining.drop(flex.index)

            bench = remaining.head(6)

            def add_rows(tag, df_part):
                for _, r in df_part.iterrows():
                    roster_rows.append({
                        "Slot": tag,
                        "Player": r.get("Player",""),
                        "Pos": r.get("Position",""),
                        "Team": r.get("Team",""),
                        "Paid": safe_int_val(r.get("price_paid",""), 0)
                    })

            add_rows("QB", qb)
            add_rows("RB", rb)
            add_rows("WR", wr)
            add_rows("TE", te)
            add_rows("FLEX", flex)
            add_rows("K", k)
            add_rows("DST", dst)
            add_rows("BENCH", bench)

            roster_df = pd.DataFrame(roster_rows, columns=["Slot","Player","Pos","Team","Paid"])
            st.dataframe(roster_df, hide_index=True, use_container_width=True, height=260)

# --------------------------- Draft Board ---------------------------
st.subheader("📋 Draft Board")

if players_df.empty:
    st.info("Players sheet is empty or unavailable. Run Smart Sync after importing Projections.")
else:
    view = players_df.copy()
    for c in ["Points","VOR","ADP","AAV","soft_rec_$","hard_cap_$","price_paid","Rank Overall","Rank Position"]:
        if c in view.columns:
            view[c] = pd.to_numeric(view[c], errors="coerce")

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
    st.dataframe(view[show_cols], use_container_width=True, height=520)

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

# --------------------------- Quick Tag Editor ---------------------------
with st.expander("🏷️ Quick Tag Editor", expanded=False):
    if players_df.empty:
        st.caption("Load and sync players first.")
    else:
        tg_player = st.selectbox("Player", players_df["Player"].tolist(), key="tag_player")
        meta_row = players_df.loc[players_df["Player"]==tg_player].head(1)
        if not meta_row.empty:
            st.caption(f"{str(meta_row['Position'].iloc[0])} · {str(meta_row['Team'].iloc[0])}")
        # Present all recognized tags
        label_map = {
            "FFG_MyGuy":"My Guy (⭐)","FFG_Sleeper":"Sleeper (💤)","FFG_Bust":"Bust (⚠️)",
            "FFG_Value":"Value (💎)","FFG_Breakout":"Breakout (🚀)","FFG_Avoid":"Avoid (⛔)",
            "FFG_Injury":"Injured (🩹)"
        }
        tgt = st.selectbox("Tag", [label_map[k] for k in RECOGNIZED_TAGS], index=0)
        tgt_col = next(k for k,v in label_map.items() if v==tgt)
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
                    st.toast(f"{tgt}: {'OFF' if is_truthy(df.at[idx, tgt_col]) else 'ON'} for {tg_player}")
                    st.rerun()
            except Exception as e:
                st.error(f"Tag update failed: {e}")

# Footer
st.caption("Auction GM • stable build")
