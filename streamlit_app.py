# streamlit_app.py — Auction GM (FINAL compact build)
# Features:
# - Draft from board (checkbox + confirm form). No top draft panel.
# - Immediate UI refresh after any write (clear cache + rerun).
# - Phase Budgets above filters, collapsible.
# - Tag editor always works (auto-creates FFG_/FFB_ columns if missing).
# - Admin: Import Projections CSV (replace/append), Smart Sync, Recompute $, Reset, Reset & Archive.
# - Position-aware price-check & nomination recs using League_Teams open_* per position.
# - Safe numeric coercion, IDP strip, de-dup, integer writes for budgets/slots.

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
    """Find or create tag columns so the Tag editor always works."""
    if df is None or df.empty:
        return []
    cols = [c for c in df.columns if c.upper().startswith(("FFG_","FFB_"))]
    if not cols:
        for c in ["FFG_MyGuy","FFG_Sleeper","FFG_Bust","FFG_Value","FFG_Breakout"]:
            if c not in df.columns:
                df[c] = ""
        return ["FFG_MyGuy","FFG_Sleeper","FFG_Bust","FFG_Value","FFG_Breakout"]
    return cols

def is_truthy(v):
    s = str(v).strip().lower()
    return s in ("1","true","yes","y")

def safe_int_or(v, default=1):
    """Coerce a scalar or 1-row Series to int safely; blank/NaN -> default."""
    try:
        s = pd.to_numeric(v, errors="coerce")
        if isinstance(s, pd.Series):
            s = s.iloc[0] if len(s) else None
        return int(s) if pd.notna(s) and s >= 1 else default
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
        if k in NAME_MAP:
            col_map[c] = NAME_MAP[k]
    df = df.rename(columns=col_map)
    # ensure columns exist
    for col in TARGET_PROJ_COLS:
        if col not in df.columns:
            df[col] = None
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
    return True, f"Smart Sync done: updated {len(df_r):,} rows; added {len(right_only):,}."

# --------------------------- Recs & Bias ---------------------------
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

def compute_recommended_values(df_players: pd.DataFrame, bias_map=None, budget=200, teams=14):
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

    # Inflation from observed market (paid vs base for drafted)
    paid = pd.to_numeric(df.get("price_paid"), errors="coerce").fillna(0)
    drafted = df.get("status","").astype(str).str.lower().eq("drafted")
    exp_spend = base.where(drafted, 0).sum(skipna=True)
    act_spend = paid.sum()
    inflation = 1.0
    if exp_spend and exp_spend>0:
        inflation = max(0.75, min(1.5, act_spend/exp_spend))

    # Team bias adjust
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


# --------------------------- League_Teams helpers (per-position) ---------------------------
def detect_open_cols(df_league: pd.DataFrame):
    # returns dict like {"QB":"open_QB", ...} if found
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
    if not m.any():
        return False, f"Team '{team_name}' not found."
    i = df.index[m][0]

    open_map = detect_open_cols(df)
    # decrement budget
    try:
        b = float(pd.to_numeric(df.at[i,"budget_remaining"], errors="coerce") or 0)
        df.at[i,"budget_remaining"] = int(max(0, round(b - float(price))))
    except Exception:
        pass
    # decrement slot counts (integers)
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
def append_draft_log(sh, row: dict):
    ws = upsert_worksheet(sh, "Draft_Log")
    header = ws.row_values(1)
    needed = ["timestamp","player","team","position","manager","price","note"]
    if not header:
        ws.update("A1",[needed]); header = needed
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
def reset_players_and_league(sh):
    """Soft reset. Players: clear drafted fields. League_Teams: try to reset budgets/slots if hints exist."""
    # Players
    try:
        ws = sh.worksheet("Players")
        df = normalize_cols(ws_to_df(ws))
        for c in ["status","drafted_by","price_paid"]:
            if c not in df.columns: df[c] = ""
        df["status"] = ""
        df["drafted_by"] = ""
        df["price_paid"] = ""
        write_dataframe_to_sheet(ws, df, header=True)
    except Exception as e:
        return False, f"Players reset failed: {e}"

    # League_Teams
    try:
        ws = sh.worksheet("League_Teams")
        df = normalize_cols(ws_to_df(ws))
        if not df.empty:
            # Budget reset: prefer 'budget_start' if present; else Settings_League 'budget'
            budget_val = None
            if "budget_start" in df.columns:
                ser = pd.to_numeric(df["budget_start"], errors="coerce"); 
                if ser.notna().any(): budget_val = int(ser.dropna().iloc[0])
            if budget_val is None:
                try:
                    ws_s = sh.worksheet("Settings_League")
                    dsl = normalize_cols(ws_to_df(ws_s))
                    bcol = next((c for c in dsl.columns if c.lower()=="budget"), None)
                    if bcol:
                        ser = pd.to_numeric(dsl[bcol], errors="coerce")
                        if ser.notna().any(): budget_val = int(ser.dropna().iloc[0])
                except Exception:
                    pass
            if budget_val is not None and "budget_remaining" in df.columns:
                df["budget_remaining"] = int(budget_val)

            # open_* reset: if *_start columns exist, copy them back; else leave as-is
            open_map = detect_open_cols(df)
            for pos, col in open_map.items():
                start_col = f"{col}_start"
                if start_col in df.columns:
                    df[col] = pd.to_numeric(df[start_col], errors="coerce").fillna(0).astype(int)

            # recompute max_bid & $/slot
            if open_map:
                for i in df.index:
                    max_bid, pps = recompute_maxbid_and_pps(df.loc[i, :], open_map)
                    if "max_bid" in df.columns: df.at[i, "max_bid"] = int(max_bid)
                    if "(auto)_$per_open_slot" in df.columns: df.at[i, "(auto)_$per_open_slot"] = int(pps)

            write_dataframe_to_sheet(ws, df, header=True)
    except Exception as e:
        return False, f"League_Teams reset partial: {e}"

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


# --------------------------- Nomination Recs (position-aware) ---------------------------
def build_nomination_list(players_df: pd.DataFrame, league_df: pd.DataFrame, top_n: int = 8):
    df = players_df.copy() if players_df is not None else pd.DataFrame()

    # Ensure columns exist (avoid KeyError)
    needed_cols = ["status","Position","Player","Team","soft_rec_$","AAV","ADP","VOR","Points","Rank Position"]
    for c in needed_cols:
        if c not in df.columns:
            df[c] = "" if c not in ("soft_rec_$","AAV","ADP","VOR","Points","Rank Position") else float("nan")

    # Filter out drafted
    df = df[~df["status"].astype(str).str.lower().eq("drafted")].copy()

    # Coerce numerics
    for c in ["soft_rec_$","AAV","ADP","VOR","Points","Rank Position"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    if df.empty:
        return pd.DataFrame(), pd.DataFrame()

    # Baseline market value (fallback if AAV missing)
    base_val = df["AAV"].copy()
    if base_val.isna().all():
        base_val = (df["soft_rec_$"] / 1.15)

    df["value_surplus"] = df["soft_rec_$"] - base_val

    # Position supply (remaining)
    pos_list = df["Position"].dropna().unique().tolist()
    pos_supply = {p: max(1, int((df["Position"] == p).sum())) for p in pos_list}

    # Position demand from League_Teams (sum of open_* cols)
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

    # Outbid count: teams that both have budget and a relevant opening
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

    # Score
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
        st.cache_data.clear(); st.rerun()

    st.button("🔁 Refresh Data (clear cache)", on_click=lambda: (st.cache_data.clear(), st.toast("Caches cleared; reloading…"), st.rerun()))

    if admin_mode:
        st.divider()
        st.header("Admin")
        # Import projections
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

        # Reset & Archive
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
if 'sh' in locals() and write_ready:
    try:
        players_df = normalize_cols(ws_to_df_cached(SHEET_ID, "Players", SA_JSON))
        # de-dup (id or identity)
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

# --- Safety net: ensure required columns exist to avoid KeyError downstream
if players_df is None or players_df.empty:
    players_df = pd.DataFrame()
for c in ["status","Position","Player","Team","soft_rec_$","AAV","ADP","VOR","Points","Rank Overall","Rank Position","drafted_by","price_paid"]:
    if c not in players_df.columns:
        players_df[c] = (float("nan") if c in ("soft_rec_$","AAV","ADP","VOR","Points","Rank Overall","Rank Position") else "")

# --------------------------- Phase Budgets (above filters) ---------------------------
if 'sh' in locals() and write_ready:
    rows=[]
    try:
        ws = sh.worksheet("Phase_Budgets")
        pdf = normalize_cols(ws_to_df(ws))
        if not pdf.empty:
            for c in ("phase","target","spent","remain"):
                if c not in pdf.columns: pdf[c]=""
            rows = pdf.to_dict("records")
    except Exception:
        rows=[]
    with st.expander("🎯 Phase Budgets", expanded=False):
        if rows:
            cols = st.columns(min(len(rows), 6))
            for i, r in enumerate(rows[:6]):
                try:
                    tgt = "—" if not r.get("target") else f"${int(pd.to_numeric(r['target'], errors='coerce')):,}"
                    sp  = "—" if not r.get("spent")  else f"${int(pd.to_numeric(r['spent'],  errors='coerce')):,}"
                    rm  = "—" if not r.get("remain") else f"${int(pd.to_numeric(r['remain'], errors='coerce')):,}"
                    cols[i].metric(r.get("phase","Phase"), f"{rm} left", f"{sp} spent / {tgt} target")
                except Exception:
                    pass
        else:
            st.caption("No phase budget rows defined.")

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

# --------------------------- Draft Board (with "Draft" selection) ---------------------------
st.subheader("📋 Draft Board")

if players_df.empty:
    st.info("Players sheet is empty or unavailable. Import Projections and run Smart Sync.")
else:
    view = players_df.copy()

    # numeric types
    for c in ["Points","VOR","ADP","AAV","soft_rec_$","hard_cap_$","price_paid","Rank Overall","Rank Position"]:
        if c in view.columns:
            view[c] = pd.to_numeric(view[c], errors="coerce")

    # tag icons column (⭐ 💤 ⚠️ 💎 🚀)
    def tag_icons(row):
        out=[]
        if is_truthy(row.get("FFG_MyGuy", row.get("FFB_MyGuy",""))): out.append("⭐")
        if is_truthy(row.get("FFG_Sleeper", row.get("FFB_Sleeper",""))): out.append("💤")
        if is_truthy(row.get("FFG_Bust", row.get("FFB_Bust",""))): out.append("⚠️")
        if is_truthy(row.get("FFG_Value", row.get("FFB_Value",""))): out.append("💎")
        if is_truthy(row.get("FFG_Breakout", row.get("FFB_Breakout",""))): out.append("🚀")
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

    # Add selection column
    board = view.copy()
    board.insert(0, "Draft", False)

    edited = st.data_editor(
        board[["Draft"] + show_cols],
        use_container_width=True,
        height=520,
        hide_index=True,
        disabled=show_cols,  # read-only for non-pick columns
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
    c1, c2, c3 = st.columns([1,1,6])
    with c1:
        do_draft = st.button("✅ Draft Selected", type="primary", use_container_width=True, disabled=not (len(sel)==1 and write_ready and not practice))
    with c2:
        if st.button("✖️ Clear Picks", use_container_width=True):
            st.cache_data.clear(); st.rerun()

    # Compact confirm form when exactly one is selected
    if do_draft and len(sel)==1 and write_ready and not practice:
        row = sel.iloc[0]
        st.info(f"Drafting **{row['Player']}** ({row['Position']} · {row['Team']})")
        with st.form(key=f"confirm_draft_form_{row.name}", clear_on_submit=True):
            # Team options
            mgr_opts = []
            if not league_df.empty and "team_name" in league_df.columns:
                mgr_opts = sorted([t for t in league_df["team_name"].dropna().astype(str).tolist() if t])
            price_val = safe_int_or(row.get("soft_rec_$"), default=1)
            price = st.number_input("Price", min_value=1, max_value=500, step=1, value=price_val, key=f"board_price_{row.name}")
            mgr = st.selectbox("Team (buyer)", mgr_opts if mgr_opts else [""], key=f"board_mgr_{row.name}")
            colF1, colF2 = st.columns([1,1])
            confirm = colF1.form_submit_button("Confirm Draft", type="primary")
            cancel  = colF2.form_submit_button("Cancel")
        if cancel:
            st.experimental_rerun()
        if confirm:
            try:
                update_player_drafted(sh, (row["Player"], row["Team"], row["Position"]), mgr, price)
                append_draft_log(sh, {
                    "timestamp": datetime.now().isoformat(timespec="seconds"),
                    "player": row["Player"], "team": row["Team"], "position": row["Position"],
                    "manager": mgr, "price": str(int(price)), "note": ""
                })
                ok,msg = update_league_team_after_pick(sh, mgr, row["Position"], price)
                if not ok: st.warning(msg)
                # refresh recs + reload UI
                write_recommendations_to_players(sh)
                st.toast(f"Drafted {row['Player']} for ${int(price)} to {mgr}.")
                st.cache_data.clear(); st.rerun()
            except Exception as e:
                st.error(f"Draft failed: {e}")


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
        for c in tag_cols:
            base = c.replace("FFG_","").replace("FFB_","")
            label = {"MyGuy":"My Guy","Sleeper":"Sleeper","Bust":"Bust","Value":"Value","Breakout":"Breakout"}.get(base, base)
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
st.caption("Auction GM • position-aware compact build • optimized for 1920×1080")
