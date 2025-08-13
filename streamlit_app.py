# streamlit_app.py — Auction GM (final + on-demand player news)
# Features: Smart Sync, Admin CSV import, Bias-aware RAV, Phase chips,
# Nomination Recommendations, Draft/Undo/Outbid, Tag editor, Archive/Reset,
# on-demand RSS player news (lazy + cached), schema debug, manual cache refresh.
# Clean header; version in footer.

import json, io, re
from datetime import datetime
import pandas as pd
import requests
import streamlit as st

# Optional write libs (Streamlit Cloud usually has these)
try:
    import gspread
    from google.oauth2.service_account import Credentials
    from gspread.utils import rowcol_to_a1
except Exception:
    gspread = None
    Credentials = None

# Optional RSS parser for news
try:
    import feedparser
except Exception:
    feedparser = None

st.set_page_config(page_title="Auction GM", layout="wide")

# ----------------- Secrets -----------------
SHEET_ID = st.secrets.get("SHEET_ID", "")
SLEEPER_LEAGUE_ID = st.secrets.get("SLEEPER_LEAGUE_ID", "")
SA_JSON = st.secrets.get("GOOGLE_SERVICE_ACCOUNT_JSON", None)
if not SA_JSON:
    for k in ("gcp_service_account", "google_service_account"):
        if k in st.secrets:
            SA_JSON = json.dumps(dict(st.secrets[k]))
            break

# ----------------- Helpers -----------------
@st.cache_data(ttl=300)
def sleeper_get(path):
    url = f"https://api.sleeper.app/v1/{path.lstrip('/')}"
    r = requests.get(url, timeout=15)
    r.raise_for_status()
    return r.json()

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

@st.cache_data(ttl=60)
def ws_to_df_cached(sheet_id: str, ws_title: str, sa_json: str):
    """Cached pull of a worksheet -> DataFrame. Use manual 'Refresh Data' to clear."""
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
    ws.batch_clear(["A1:Z100000"])
    ws.update("A1", values, value_input_option="RAW")

def upsert_worksheet(sh, title, rows=5000, cols=30):
    try: return sh.worksheet(title)
    except Exception:
        sh.add_worksheet(title=title, rows=rows, cols=cols)
        return sh.worksheet(title)

# ---- Column canonicalization ----
CANON = {
    "position":"Position","pos":"Position","player_position":"Position",
    "player":"Player","name":"Player","player_name":"Player",
    "team":"Team","tm":"Team","player team":"Team","player_team":"Team",
    "points":"Points","proj_points":"Points","proj_fp":"Points","fp":"Points","fantasy_points":"Points",
    "vor":"VOR","points_vor":"VOR","value_over_replacement":"VOR",
    "adp":"ADP",
    "aav":"AAV","auction_value":"AAV",
    "rank":"Rank Overall","overall_rank":"Rank Overall","rank_overall":"Rank Overall",
    "position_rank":"Rank Position","pos_rank":"Rank Position","rank_position":"Rank Position",
    "status":"status","drafted_by":"drafted_by","price_paid":"price_paid",
    # Footballers tags
    "ffg_myguy":"FFG_MyGuy","ffg_my_guy":"FFG_MyGuy","ffg_myguys":"FFG_MyGuy",
    "ffg_sleeper":"FFG_Sleeper","ffg_sleepers":"FFG_Sleeper",
    "ffg_bust":"FFG_Bust","ffg_busts":"FFG_Bust",
    "ffg_value":"FFG_Value","ffg_values":"FFG_Value",
    "ffg_breakout":"FFG_Breakout","ffg_breakouts":"FFG_Breakout",
    "my_guys":"FFG_MyGuy","myguy":"FFG_MyGuy","my_guy":"FFG_MyGuy",
    "sleepers":"FFG_Sleeper","sleeper":"FFG_Sleeper",
    "busts":"FFG_Bust","bust":"FFG_Bust",
    "values":"FFG_Value","value":"FFG_Value",
    "breakouts":"FFG_Breakout","breakout":"FFG_Breakout",
}
PROJ_COLS = ["Points","VOR","ADP","AAV","Rank Overall","Rank Position"]
IDENTITY_COLS = ["Player","Team","Position"]
TAG_COLS_CANON = ["FFG_MyGuy","FFG_Sleeper","FFG_Bust","FFG_Value","FFG_Breakout"]

def normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return df
    rename = {}
    for c in list(df.columns):
        k = str(c).strip().lower()
        if k in CANON: rename[c] = CANON[k]
    if rename: df.rename(columns=rename, inplace=True)
    for c in df.columns:
        if df[c].dtype == object:
            df[c] = df[c].astype(str).str.strip()
    return df

def choose_keys(df_left, df_right):
    for ks in (["Player","Team","Position"], ["Player","Position"], ["Player","Team"], ["Player"]):
        if all(k in df_left.columns for k in ks) and all(k in df_right.columns for k in ks):
            return ks
    return None

def get_tag_columns(df):
    ffg = [c for c in TAG_COLS_CANON if c in df.columns]
    if ffg: return ffg
    return [c for c in ["my_guys","sleepers","busts","values","breakouts"] if c in df.columns]

# ----------------- Reset / Archive -----------------
def _batch_clear(ws, start_row, col_idx, end_row):
    if not col_idx or end_row < start_row: return
    a1 = f"{rowcol_to_a1(start_row, col_idx)}:{rowcol_to_a1(end_row, col_idx)}"
    ws.batch_clear([a1])

def reset_live_fields_only(sh):
    try:
        ws_p = sh.worksheet("Players")
        header = ws_p.row_values(1)
        nrows = len(ws_p.get_all_values())
        def idx(name): return header.index(name)+1 if name in header else None
        for nm in ["status","drafted_by","price_paid"]:
            ci = idx(nm)
            if nrows>1 and ci: _batch_clear(ws_p, 2, ci, nrows)
        ws_lt = sh.worksheet("League_Teams")
        if len(ws_lt.get_all_values())>1: ws_lt.batch_clear(["A2:Z1000"])
        ws_dl = sh.worksheet("Draft_Log")
        if len(ws_dl.get_all_values())>1: ws_dl.batch_clear(["A2:Z100000"])
        ws_pb = sh.worksheet("Phase_Budgets")
        header_pb = ws_pb.row_values(1); nrows_pb = len(ws_pb.get_all_values())
        for i, col_name in enumerate(header_pb, start=1):
            if col_name.startswith("(auto)") and nrows_pb>1:
                a1 = f"{rowcol_to_a1(2,i)}:{rowcol_to_a1(nrows_pb,i)}"
                ws_pb.batch_clear([a1])
        return True, "Reset complete (live fields cleared, no archive)."
    except Exception as e:
        return False, f"Reset failed: {e}"

def archive_and_reset(sh):
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    title = f"Archive_{ts}"
    try:
        sh.add_worksheet(title=title, rows=2000, cols=60)
        ws_arc = sh.worksheet(title)
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
        for tab in ["Draft_Log","League_Teams","Settings_League"]:
            copy_tab(tab)
    except Exception as e:
        return False, str(e)

    ok, msg = reset_live_fields_only(sh)
    return (True, f"Archived to {title} and reset live tabs.") if ok else (False, msg)

def purge_archives(sh, keep_latest=True):
    try:
        archives = [ws for ws in sh.worksheets() if ws.title.startswith("Archive_")]
        if not archives: return True, "No archive tabs found."
        archives.sort(key=lambda ws: ws.title, reverse=True)
        to_del = archives[1:] if keep_latest and len(archives)>1 else archives
        for ws in to_del: sh.del_worksheet(ws)
        kept = archives[0].title if keep_latest and archives else None
        return True, f"Purged {len(to_del)} archive tab(s)."+(f" Kept {kept}." if kept else "")
    except Exception as e:
        return False, f"Purge failed: {e}"

# ----------------- Admin: Projections Import -----------------
TARGET_PROJ_COLS = ["Position","Player","Team","Points","VOR","ADP","AAV","Rank Overall","Rank Position"]
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
    # rename flexibly
    col_map = {}
    for c in df.columns:
        k = str(c).strip().lower()
        if k in NAME_MAP: col_map[c] = NAME_MAP[k]
    df = df.rename(columns=col_map)
    # ensure required
    for col in TARGET_PROJ_COLS:
        if col not in df.columns: df[col] = None
    df = df[TARGET_PROJ_COLS]
    # types
    for col in PROJ_COLS: df[col] = pd.to_numeric(df[col], errors="coerce")
    for c in ["Player","Team","Position"]:
        df[c] = df[c].astype(str).str.strip()
    df = df[df["Player"]!=""].reset_index(drop=True)
    return df

# ----------------- Smart Sync (Projections → Players) -----------------
def smart_sync_projections_to_players(sh, preserve_tags=True, update_identity=False):
    ws_proj = sh.worksheet("Projections")
    ws_players = sh.worksheet("Players")
    df_p = normalize_cols(ws_to_df(ws_players))
    df_r = normalize_cols(ws_to_df(ws_proj))

    # basic columns
    for c in IDENTITY_COLS:
        if c not in df_p.columns: df_p[c] = ""
    for c in PROJ_COLS:
        if c not in df_p.columns: df_p[c] = ""
        if c not in df_r.columns: df_r[c] = ""
    tag_cols = get_tag_columns(df_p)

    keys = choose_keys(df_p, df_r)
    if not keys:
        return False, ("Couldn’t find matching join keys between Players and Projections. "
                       "Check headers or run Admin → Debug schema.")

    # Update existing rows: only projection fields
    merged = df_p.merge(df_r[IDENTITY_COLS + PROJ_COLS], how="left", on=keys, suffixes=("","_new"))
    for c in PROJ_COLS:
        nc = f"{c}_new"
        if nc in merged.columns:
            merged[c] = merged[nc].where(merged[nc].notna(), merged[c])
            merged.drop(columns=[nc], inplace=True, errors="ignore")

    # Append new rows present only in Projections
    left_keys = merged[keys].astype(str).apply("|".join, axis=1)
    right_only = df_r[~df_r[keys].astype(str).apply("|".join, axis=1).isin(left_keys)]
    if not right_only.empty:
        new_rows = right_only.copy()
        for c in merged.columns:
            if c not in new_rows.columns:
                # default tags to FALSE ONLY for brand new rows
                if preserve_tags and c in tag_cols:
                    new_rows[c] = "FALSE"
                else:
                    new_rows[c] = ""
        merged = pd.concat([merged, new_rows[merged.columns]], ignore_index=True)

    # Keep identity from Players unless explicitly allowed
    if not update_identity:
        for c in [col for col in IDENTITY_COLS if c in df_p.columns]:
            merged[c] = merged[c].where(df_p[c].notna() & (df_p[c]!=""), df_p[c])

    write_dataframe_to_sheet(ws_players, merged, header=True)
    updated = len(df_r); added = len(right_only)
    return True, f"Smart Sync done: updated {updated:,} rows, added {added:,}. Keys used: {', '.join(keys)}."

# ----------------- Bias map + Phase Budgets + RAV -----------------
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

def load_phase_budgets(sh):
    rows=[]
    try:
        ws = sh.worksheet("Phase_Budgets"); df = normalize_cols(ws_to_df(ws))
        if df.empty: return rows
        phase_col = next((c for c in df.columns if "phase" in c.lower()), df.columns[0])
        target_col = next((c for c in df.columns if "target" in c.lower()), None)
        spent_col  = next((c for c in df.columns if "spent" in c.lower()), None)
        remain_col = next((c for c in df.columns if "remain" in c.lower()), None)
        for _, r in df.iterrows():
            phase = str(r.get(phase_col,"")).strip()
            if not phase: continue
            target = pd.to_numeric(r.get(target_col,""), errors="coerce") if target_col else None
            spent  = pd.to_numeric(r.get(spent_col,""),  errors="coerce") if spent_col  else None
            remain = pd.to_numeric(r.get(remain_col,""), errors="coerce") if remain_col else (None if target is None or spent is None else target-spent)
            rows.append({"phase":phase,"target":target,"spent":spent,"remain":remain})
    except Exception:
        pass
    return rows

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

    # Market inflation
    paid = pd.to_numeric(df.get("price_paid"), errors="coerce").fillna(0)
    drafted = df.get("status","").astype(str).str.lower().eq("drafted")
    exp_spend = base.where(drafted, 0).sum(skipna=True)
    act_spend = paid.sum()
    inflation = 1.0
    if exp_spend and exp_spend>0:
        inflation = max(0.75, min(1.5, act_spend/exp_spend))

    # Bias adjust (positive = league overpays → reduce our rec a bit)
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
    return True, "Recommendations updated (soft/hard + inflation + bias)."

# ----------------- Draft actions -----------------
def append_draft_log(sh, row: dict):
    ws = sh.worksheet("Draft_Log")
    header = ws.row_values(1)
    needed = ["timestamp","player","team","position","manager","price","note"]
    if not header:
        ws.update("A1",[needed]); header = needed
    out = [row.get(c,"") for c in header]
    ws.append_row(out, value_input_option="RAW")

def update_player_drafted(sh, player_key, manager, price):
    ws = sh.worksheet("Players")
    df = normalize_cols(ws_to_df(ws))
    for k in IDENTITY_COLS:
        if k not in df.columns: raise RuntimeError(f"Players sheet missing column: {k}")
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

def update_league_team_budget(sh, manager, price, delta_roster=1):
    ws = sh.worksheet("League_Teams"); df = normalize_cols(ws_to_df(ws))
    for c in ["team_name","budget_remaining","roster_spots_open"]:
        if c not in df.columns:
            return False, "League_Teams missing: team_name/budget_remaining/roster_spots_open."
    df["team_name"]=df["team_name"].astype(str)
    m = df["team_name"].str.strip().str.lower() == str(manager).strip().lower()
    if not m.any(): return False, f"Manager '{manager}' not found."
    i = df.index[m][0]
    try:
        b = pd.to_numeric(df.at[i,"budget_remaining"], errors="coerce"); b = 0 if pd.isna(b) else b
        df.at[i,"budget_remaining"]=max(0, b - float(price))
    except Exception: pass
    try:
        r = pd.to_numeric(df.at[i,"roster_spots_open"], errors="coerce"); r = 0 if pd.isna(r) else r
        df.at[i,"roster_spots_open"]=max(0, r - delta_roster)
    except Exception: pass
    write_dataframe_to_sheet(ws, df, header=True)
    return True, "Budget/roster updated."

def undo_last_pick(sh):
    ws_log = sh.worksheet("Draft_Log"); rows = ws_log.get_all_values()
    if len(rows)<=1: return False, "No picks to undo."
    header, data = rows[0], rows[1:]; last = data[-1]; rec = dict(zip(header,last))
    ws_p = sh.worksheet("Players"); df = normalize_cols(ws_to_df(ws_p))
    mask = (df["Player"]==rec.get("player")) & (df["Team"]==rec.get("team")) & (df["Position"]==rec.get("position"))
    if mask.any():
        idx = df.index[mask][0]
        for c in ["status","drafted_by","price_paid"]:
            if c not in df.columns: df[c]=""
        df.loc[idx,["status","drafted_by","price_paid"]] = ["","",""]
        write_dataframe_to_sheet(ws_p, df, header=True)
    try:
        update_league_team_budget(sh, rec.get("manager",""), -float(rec.get("price","0")), delta_roster=-1)
    except Exception: pass
    ws_log.delete_rows(len(data)+1)
    return True, f"Undid last pick: {rec.get('player')} for ${rec.get('price')}."

# ----------------- Nomination Recommendations -----------------
def build_nomination_list(players_df: pd.DataFrame, league_df: pd.DataFrame, you_team_name: str, top_n: int = 8):
    df = players_df.copy()
    df = df[~df["status"].astype(str).str.lower().eq("drafted")].copy()

    for c in ["soft_rec_$","AAV","ADP","VOR","Points","Rank Position"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    base_val = df["AAV"].copy() if "AAV" in df.columns else pd.Series([float("nan")]*len(df))
    if base_val.isna().all():
        base_val = (pd.to_numeric(df.get("soft_rec_$", pd.Series([0]*len(df))), errors="coerce") / 1.15)
    df["value_surplus"] = pd.to_numeric(df.get("soft_rec_$",0), errors="coerce") - pd.to_numeric(base_val, errors="coerce")

    pos_list = df["Position"].dropna().unique().tolist()
    pos_supply = {p: max(1, df[df["Position"]==p].shape[0]) for p in pos_list}

    pos_demand = {p: 0 for p in pos_list}
    if not league_df.empty and {"team_name","roster_spots_open"}.issubset(set(league_df.columns)):
        weight_map = {"QB":0.6,"RB":1.0,"WR":1.0,"TE":0.8,"DST":0.4,"D/ST":0.4,"K":0.4,"FLEX":0.9}
        for _, row in league_df.iterrows():
            open_spots = pd.to_numeric(row.get("roster_spots_open",""), errors="coerce")
            if pd.isna(open_spots): continue
            for p in pos_list:
                w = weight_map.get(p.upper(), 0.8)
                pos_demand[p] += w * float(open_spots)/max(1,len(pos_list))

    scarcity = {p: max(0.1, pos_demand.get(p,0) / pos_supply.get(p,1)) for p in pos_list}
    df["scarcity_factor"] = df["Position"].map(lambda p: scarcity.get(p,0.3))
    rival_need = 0
    if not league_df.empty and "roster_spots_open" in league_df.columns:
        rival_need = (pd.to_numeric(league_df["roster_spots_open"], errors="coerce") > 0).sum()
    df["rival_need"] = rival_need

    if not league_df.empty and "budget_remaining" in league_df.columns:
        br = pd.to_numeric(league_df["budget_remaining"], errors="coerce").fillna(0).tolist()
        def count_outbid(x):
            if pd.isna(x): return 0
            thresh = 1.2*float(x)
            return sum(1 for b in br if b >= thresh)
        df["outbid_count"] = df["soft_rec_$"].apply(count_outbid)
    else:
        df["outbid_count"] = 0

    val_surplus = (df["value_surplus"] - df["value_surplus"].median(skipna=True)).fillna(0)
    scarcity_norm = (df["scarcity_factor"] - pd.Series(list(scarcity.values())).median()).fillna(0)
    outbid_norm = (df["outbid_count"] - df["outbid_count"].median(skipna=True)).fillna(0)
    rp = pd.to_numeric(df.get("Rank Position"), errors="coerce")
    rp_inv = (-rp.fillna(rp.max() or 999)).fillna(0)

    df["nom_score"] = 0.45*val_surplus + 0.25*scarcity_norm + 0.20*outbid_norm + 0.10*rp_inv

    value_targets = df.sort_values(["nom_score"], ascending=False).head(top_n).copy()
    enforcers = df.sort_values(["outbid_count","scarcity_factor"], ascending=[False,False]).head(top_n).copy()

    def reason(row):
        parts=[]
        if pd.notna(row.get("value_surplus")) and row["value_surplus"]>0: parts.append(f"+${int(row['value_surplus'])} surplus")
        if pd.notna(row.get("scarcity_factor")) and row["scarcity_factor"]>0.5: parts.append("scarce pos")
        if pd.notna(row.get("outbid_count")) and row["outbid_count"]>=3: parts.append(f"{int(row['outbid_count'])} can outbid")
        return " • ".join(parts) if parts else "balanced"
    value_targets["why"] = value_targets.apply(reason, axis=1)
    enforcers["why"] = enforcers.apply(reason, axis=1)

    return value_targets, enforcers

# ----------------- Player News (on-demand, cached) -----------------
DEFAULT_FEEDS = [
    "https://www.fantasypros.com/rss/nfl/news.xml",
    "https://www.nbcsportsedge.com/football/nfl/player-news/rss",
    "https://www.sleeper.com/feed/nfl_news",
    "https://www.espn.com/espn/rss/nfl/news",
]

@st.cache_data(ttl=900)  # 15 minutes
def fetch_player_news(player_name: str, team: str = "", feeds=None, max_items: int = 10):
    if feedparser is None:
        return {"error": "feedparser not available (install feedparser in requirements.txt)."}
    feeds = feeds or DEFAULT_FEEDS

    def normalize(n: str) -> str:
        return re.sub(r"[^A-Za-z ]+", "", n or "").strip()
    name = normalize(player_name)
    tokens = [t for t in name.split() if t]
    if not tokens:
        return {"items": []}

    name_re = re.compile(r"\b" + r"\s+".join(tokens) + r"\b", re.IGNORECASE)
    last, first = tokens[-1], tokens[0]
    alt_re = re.compile(rf"\b{last}\b.*\b{first}\b|\b{first}\b.*\b{last}\b", re.IGNORECASE)

    results = []
    for url in feeds:
        try:
            d = feedparser.parse(url)
            for e in d.entries[:60]:
                title = getattr(e, "title", "")
                summary = getattr(e, "summary", "")
                text = f"{title} {summary}"
                if name_re.search(text) or alt_re.search(text):
                    results.append({
                        "title": title.strip(),
                        "link": getattr(e, "link", ""),
                        "published": getattr(e, "published", "") or getattr(e, "updated", ""),
                        "source": getattr(d.feed, "title", url),
                    })
        except Exception:
            continue

    seen = set()
    deduped = []
    for it in results:
        k = (it["title"], it["source"])
        if k not in seen:
            seen.add(k); deduped.append(it)
    deduped.sort(key=lambda x: x.get("published",""), reverse=True)
    return {"items": deduped[:max_items]}

# ----------------- UI -----------------
st.title("🏈 Auction GM")  # version info in footer only

# Sidebar: connection + modes
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
            if sa_email: st.caption(f"Shared with: {sa_email}")
    else:
        st.info("Write features optional. Add GOOGLE_SERVICE_ACCOUNT_JSON in Secrets and share your Sheet with it.")

    st.divider()
    st.header("Modes")
    practice = st.toggle("Practice Mode (no writes)", value=True)
    admin_mode = st.toggle("Admin Mode (show admin tools)", value=False)

    st.divider()
    st.header("Draft Controls")
    if "busy" not in st.session_state: st.session_state["busy"]=False

    btn_archive = st.button("📦 Archive & Reset", use_container_width=True, type="primary", disabled=not (write_ready and not practice))
    if btn_archive and write_ready and not practice and not st.session_state["busy"]:
        st.session_state["busy"]=True
        try:
            with st.spinner("Archiving and resetting…"):
                ok,msg = archive_and_reset(sh)
            st.toast(msg if ok else f"⚠️ {msg}")
        finally: st.session_state["busy"]=False

    btn_reset = st.button("♻️ Reset (No Archive)", use_container_width=True, disabled=not (write_ready and not practice))
    if btn_reset and write_ready and not practice and not st.session_state["busy"]:
        st.session_state["busy"]=True
        try:
            with st.spinner("Resetting…"):
                ok,msg = reset_live_fields_only(sh)
            st.toast(msg if ok else f"⚠️ {msg}")
        finally: st.session_state["busy"]=False

    btn_purge = st.button("🧹 Purge Archives (keep latest)", use_container_width=True, disabled=not (write_ready and not practice))
    if btn_purge and write_ready and not practice and not st.session_state["busy"]:
        st.session_state["busy"]=True
        try:
            with st.spinner("Purging archives…"):
                ok,msg = purge_archives(sh, keep_latest=True)
            st.toast(msg if ok else f"⚠️ {msg}")
        finally: st.session_state["busy"]=False

    st.divider()
    st.header("Data Actions")
    preserve_tags = st.toggle("Preserve FFG tags on sync", value=True, help="Keeps your FFG_* tag columns unchanged for existing players.")
    update_identity = st.toggle("Allow identity updates (Player/Team/Position)", value=False)

    btn_sync = st.button("🔄 Smart Sync: Projections → Players", use_container_width=True, disabled=not (write_ready and not practice))
    if btn_sync and write_ready and not practice and not st.session_state["busy"]:
        st.session_state["busy"]=True
        try:
            with st.spinner("Smart syncing projections into Players…"):
                ok,msg = smart_sync_projections_to_players(sh, preserve_tags=preserve_tags, update_identity=update_identity)
            st.toast(msg if ok else f"⚠️ {msg}")
        finally:
            st.session_state["busy"]=False

    btn_recs = st.button("💡 Recompute Recommended $", use_container_width=True, disabled=not (write_ready and not practice))
    if btn_recs and write_ready and not practice and not st.session_state["busy"]:
        st.session_state["busy"]=True
        try:
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
            with st.spinner("Computing RAV…"):
                ok,msg = write_recommendations_to_players(sh, teams=teams, budget=budget)
            st.toast(msg if ok else f"⚠️ {msg}")
        finally:
            st.session_state["busy"]=False

    btn_undo = st.button("↩️ Undo Last Pick", use_container_width=True, disabled=not (write_ready and not practice))
    if btn_undo and write_ready and not practice and not st.session_state["busy"]:
        st.session_state["busy"]=True
        try:
            ok,msg = undo_last_pick(sh)
            st.toast(msg if ok else f"⚠️ {msg}")
        finally:
            st.session_state["busy"]=False

    # Manual cache clear (useful if you edited the Sheet directly)
    st.divider()
    if st.button("🔁 Refresh Data (clear cache)"):
        st.cache_data.clear()
        st.toast("Caches cleared. Reloading data…")

    if write_ready and admin_mode:
        with st.expander("🧪 Debug • Sheet schema", expanded=False):
            for ws in sh.worksheets():
                try: row1 = ws.row_values(1)
                except Exception: row1=[]
                st.caption(f"**{ws.title}**")
                st.code(", ".join(row1) if row1 else "(no header row)")

# Header (Sleeper)
c1,c2,c3 = st.columns([2,1,1])
if SLEEPER_LEAGUE_ID:
    try:
        league = sleeper_get(f"league/{SLEEPER_LEAGUE_ID}")
        with c1: st.metric("League", league.get("name","—"))
        with c2: st.metric("Teams", league.get("total_rosters","—"))
        with c3: st.metric("Season", league.get("season","—"))
    except Exception as e:
        st.warning(f"Could not fetch league info: {e}")
else:
    st.info("Add SLEEPER_LEAGUE_ID in Secrets for league header.")

# Phase budget chips
if 'sh' in locals() and write_ready:
    rows=[]
    try: rows = load_phase_budgets(sh)
    except Exception: pass
    if rows:
        st.divider()
        st.subheader("🎯 Phase Budgets")
        cols = st.columns(len(rows))
        for i, r in enumerate(rows):
            tgt = "—" if r["target"] is None else f"${int(r['target']):,}"
            sp  = "—" if r["spent"]  is None else f"${int(r['spent']):,}"
            rm  = "—" if r["remain"] is None else f"${int(r['remain']):,}"
            cols[i].metric(r["phase"], f"{rm} left", f"{sp} spent / {tgt} target")

st.divider()

# Admin importer (hidden unless Admin Mode)
if 'sh' in locals() and write_ready and admin_mode:
    st.subheader("📥 Admin • Import Projections (overwrite 'Projections')")
    with st.expander("Upload raw FFA CSV (click to open)", expanded=False):
        st.caption("Upload the unedited CSV from FFA Season Projections. It will be cleaned and the Projections tab will be fully replaced.")
        uploaded = st.file_uploader("Upload FFA CSV", type=["csv"], accept_multiple_files=False)
        if uploaded is not None:
            try:
                with st.spinner("Parsing and cleaning CSV…"):
                    cleaned = clean_projection_csv(uploaded.read())
                st.success(f"Loaded {len(cleaned):,} players. Preview below:")
                st.dataframe(cleaned.head(20), use_container_width=True)
                st.caption("Columns ↓ match backend schema:")
                st.code(", ".join(cleaned.columns.tolist()))
                if st.button("✍️ Write to Google Sheet → 'Projections'", type="primary", disabled=practice):
                    try:
                        ws_proj = upsert_worksheet(sh, "Projections", rows=max(5000, len(cleaned)+10), cols=len(TARGET_PROJ_COLS)+3)
                        write_dataframe_to_sheet(ws_proj, cleaned, header=True)
                        st.toast("✅ Projections updated.")
                    except Exception as e:
                        st.error(f"Write failed: {e}")
            except Exception as e:
                st.error(f"Could not read CSV: {e}")

# -------- Draft Board --------
st.subheader("📋 Draft Board")
players_df = pd.DataFrame()
league_df = pd.DataFrame()
if 'sh' in locals() and write_ready:
    try:
        ws_p = sh.worksheet("Players")
        players_df = normalize_cols(ws_to_df(ws_p))
    except Exception as e:
        st.error(f"Could not load Players sheet: {e}")
    try:
        ws_lt = sh.worksheet("League_Teams")
        league_df = normalize_cols(ws_to_df(ws_lt))
    except Exception:
        league_df = pd.DataFrame()

if players_df.empty:
    st.info("Players sheet is empty or unavailable. Run Smart Sync after importing Projections.")
else:
    for c in ["Points","VOR","ADP","AAV","soft_rec_$","hard_cap_$","price_paid","Rank Overall","Rank Position"]:
        if c not in players_df.columns: players_df[c]=""
        players_df[c] = pd.to_numeric(players_df[c], errors="coerce")
    for c in ["status","drafted_by","Team","Player","Position"]:
        if c not in players_df.columns: players_df[c]=""

    tag_cols = get_tag_columns(players_df)
    tag_labels = {
        "FFG_MyGuy":"My Guy", "FFG_Sleeper":"Sleeper", "FFG_Bust":"Bust", "FFG_Value":"Value", "FFG_Breakout":"Breakout",
        "my_guys":"My Guy","sleepers":"Sleeper","busts":"Bust","values":"Value","breakouts":"Breakout",
    }

    # Filters
    f1,f2,f3,f4,f5,f6 = st.columns([2,1,1,1,1,1])
    with f1: q = st.text_input("Search", "")
    with f2:
        pos_opts = sorted([p for p in players_df["Position"].dropna().unique().tolist() if p])
        pos_sel = st.multiselect("Positions", pos_opts, default=[])
    with f3:
        team_opts = sorted([t for t in players_df["Team"].dropna().unique().tolist() if t])
        team_sel = st.multiselect("Teams", team_opts, default=[])
    with f4: hide_drafted = st.toggle("Hide drafted", value=True)
    with f5: sort_by = st.selectbox("Sort by", ["Rank Overall","soft_rec_$","AAV","VOR","Points","ADP","Rank Position"], index=0)
    with f6:
        tag_sel=[]
        if tag_cols:
            tag_sel = st.multiselect("Tags", [tag_labels.get(tc, tc) for tc in tag_cols], default=[])

    view = players_df.copy()
    if q: view = view[view["Player"].str.contains(q, case=False, na=False)]
    if pos_sel: view = view[view["Position"].isin(pos_sel)]
    if team_sel: view = view[view["Team"].isin(team_sel)]
    if hide_drafted: view = view[~view["status"].astype(str).str.lower().eq("drafted")]
    if tag_sel:
        chosen_cols = []
        for pretty in tag_sel:
            key = next((k for k,v in tag_labels.items() if v==pretty), pretty)
            chosen_cols.append(key)
        mask=None
        for c in chosen_cols:
            if c in view.columns:
                m = view[c].astype(str).str.lower().isin(["1","true","yes","y"])
                mask = m if mask is None else (mask | m)
        if mask is not None:
            view = view[mask]

    ascending = sort_by in ["Rank Overall","ADP","Rank Position"]
    if sort_by in view.columns:
        view = view.sort_values(by=sort_by, ascending=ascending, na_position="last")
    else:
        view = view.sort_values(by="Rank Overall", ascending=True, na_position="last")

    show_cols = ["Position","Player","Team","soft_rec_$","hard_cap_$","AAV","VOR","Points","ADP","Rank Overall","status","drafted_by","price_paid"]
    for c in show_cols:
        if c not in view.columns: view[c]=""
    st.dataframe(view[show_cols], use_container_width=True, height=520)

    # ---------- Player News (on demand, lazy + cached) ----------
    st.markdown("### 📰 Player News (on demand)")
    if feedparser is None:
        st.caption("To enable news: add `feedparser==6.0.10` to requirements.txt and redeploy.")
    news_name = st.selectbox("Choose a player for news", players_df["Player"].tolist(), key="news_player")
    news_team = ""
    if news_name:
        news_team = st.selectbox(
            "Team",
            sorted(players_df.loc[players_df["Player"]==news_name, "Team"].dropna().unique().tolist()),
            key="news_team"
        )
    col_news_btn, col_news_note = st.columns([1,3])
    with col_news_btn:
        show_news = st.button("Show News", use_container_width=True, disabled=(feedparser is None))
    with col_news_note:
        st.caption("News is fetched only when you click. Cached ~15 minutes to keep things fast.")
    if show_news:
        with st.spinner("Fetching headlines…"):
            data = fetch_player_news(news_name, news_team)
        if "error" in data:
            st.error(data["error"])
        else:
            items = data.get("items", [])
            if not items:
                st.info("No recent headlines matched this player.")
            else:
                for it in items:
                    st.write(f"• [{it['title']}]({it['link']})  \n"
                             f"<span style='opacity:0.7'>{it['source']} — {it['published']}</span>", unsafe_allow_html=True)

    # ---------- Nomination Recommendations ----------
    st.subheader("🧠 Nomination Recommendations")
    you_name = ""
    if not league_df.empty and "team_name" in league_df.columns:
        you_name = st.selectbox("Your team (for context)", sorted(league_df["team_name"].dropna().unique().tolist()))
    val_list, enf_list = build_nomination_list(players_df, league_df, you_name or "", top_n=8)

    c_left, c_right = st.columns(2)
    with c_left:
        st.markdown("**Value Targets** (best surplus + helpful scarcity)")
        if val_list.empty:
            st.caption("No candidates found.")
        else:
            cols = ["Position","Player","Team","soft_rec_$","AAV","value_surplus","why"]
            for c in cols:
                if c not in val_list.columns: val_list[c]=""
            st.dataframe(val_list[cols], hide_index=True, use_container_width=True, height=260)
    with c_right:
        st.markdown("**Price Enforcers** (drain rival budgets / scarce spots)")
        if enf_list.empty:
            st.caption("No candidates found.")
        else:
            cols = ["Position","Player","Team","soft_rec_$","AAV","outbid_count","why"]
            for c in cols:
                if c not in enf_list.columns: enf_list[c]=""
            st.dataframe(enf_list[cols], hide_index=True, use_container_width=True, height=260)

    # ---------- Outbid helper ----------
    st.caption("💡 Outbid helper shows how many managers can beat a price based on remaining budgets.")
    ob1,ob2 = st.columns([2,1])
    with ob1:
        ob_player = st.selectbox("Player to price-check", players_df["Player"].tolist(), key="ob_player")
        ob_team   = st.selectbox("Team", sorted(players_df.loc[players_df["Player"]==ob_player, "Team"].unique().tolist()), key="ob_team")
        ob_pos    = st.selectbox("Position", sorted(players_df.loc[players_df["Player"]==ob_player, "Position"].unique().tolist()), key="ob_pos")
        base_row  = players_df[(players_df["Player"]==ob_player)&(players_df["Team"]==ob_team)&(players_df["Position"]==ob_pos)].head(1)
        base_soft = int(base_row["soft_rec_$"].iloc[0]) if not base_row.empty and pd.notna(base_row["soft_rec_$"].iloc[0]) else 1
        ob_price  = st.number_input("Check price", min_value=1, max_value=500, step=1, value=base_soft, key="ob_price")
    with ob2:
        can_list=[]
        try:
            ws_lt = sh.worksheet("League_Teams"); df_lt = normalize_cols(ws_to_df(ws_lt))
            if {"team_name","budget_remaining"}.issubset(set(df_lt.columns)):
                for _,r in df_lt.iterrows():
                    br = pd.to_numeric(r.get("budget_remaining",""), errors="coerce")
                    if pd.notna(br) and br >= ob_price: can_list.append(str(r.get("team_name","")))
        except Exception: pass
        st.metric("Can outbid you", len(can_list))
        if can_list: st.caption(", ".join(can_list))

    # ---------- Draft action ----------
    st.markdown("### 📝 Draft a Player")
    c1_,c2_,c3_,c4_,c5_ = st.columns([2,1.5,1.5,1,1.5])
    with c1_:
        sel_name = st.selectbox("Player", players_df["Player"].tolist(), key="pick_player")
    with c2_:
        sel_team = st.selectbox("Team", sorted(players_df.loc[players_df["Player"]==sel_name, "Team"].unique().tolist()), key="pick_team")
    with c3_:
        sel_pos  = st.selectbox("Position", sorted(players_df.loc[players_df["Player"]==sel_name, "Position"].unique().tolist()), key="pick_pos")
    with c4_:
        sel_price = st.number_input("Price", min_value=1, max_value=500, step=1, value=base_soft, key="pick_price")
    with c5_:
        sel_mgr = st.text_input("Manager", "", key="pick_mgr")

    draft_btn = st.button("✅ Mark Drafted & Log", type="primary", disabled=not (write_ready and not practice))
    if draft_btn and write_ready and not practice and not st.session_state.get("busy", False):
        st.session_state["busy"]=True
        try:
            update_player_drafted(sh, (sel_name, sel_team, sel_pos), sel_mgr, sel_price)
            append_draft_log(sh, {
                "timestamp": datetime.now().isoformat(timespec="seconds"),
                "player": sel_name, "team": sel_team, "position": sel_pos,
                "manager": sel_mgr, "price": str(int(sel_price)), "note": ""
            })
            update_league_team_budget(sh, sel_mgr, sel_price, delta_roster=1)
            # recompute RAV using Settings_League if available
            teams=14; budget=200
            try:
                ws = sh.worksheet("Settings_League")
                df = normalize_cols(ws_to_df(ws))
                for c in df.columns:
                    if c.lower()=="teams":
                        v=pd.to_numeric(df[c], errors="coerce"); 
                        if v.notna().any(): teams=int(v.dropna().iloc[0])
                    if c.lower()=="budget":
                        v=pd.to_numeric(df[c], errors="coerce"); 
                        if v.notna().any(): budget=int(v.dropna().iloc[0])
            except Exception: pass
            write_recommendations_to_players(sh, teams=teams, budget=budget)
            st.toast(f"Drafted {sel_name} ({sel_pos} {sel_team}) for ${sel_price}.")
        except Exception as e:
            st.error(f"Draft failed: {e}")
        finally:
            st.session_state["busy"]=False

    # ---------- Quick Tag Editor ----------
    if tag_cols:
        st.markdown("### 🏷️ Quick Tag Editor")
        te1,te2,te3 = st.columns([2,1.5,2.5])
        with te1:
            tg_player = st.selectbox("Player", players_df["Player"].tolist(), key="tag_player")
        with te2:
            tg_team = st.selectbox("Team", sorted(players_df.loc[players_df["Player"]==tg_player, "Team"].unique().tolist()), key="tag_team")
            tg_pos  = st.selectbox("Position", sorted(players_df.loc[players_df["Player"]==tg_player, "Position"].unique().tolist()), key="tag_pos")
        with te3:
            pretty_options = [tag_labels.get(c,c) for c in tag_cols]
            tg_choice = st.selectbox("Tag", pretty_options, key="tag_choice")
            tgt_col = next((k for k,v in tag_labels.items() if v==tg_choice), tg_choice)

        do_tag = st.button("💾 Toggle Tag (On/Off)", disabled=not (write_ready and not practice))
        if do_tag and write_ready and not practice:
            try:
                ws = sh.worksheet("Players"); df = normalize_cols(ws_to_df(ws))
                mask = (df["Player"]==tg_player)&(df["Team"]==tg_team)&(df["Position"]==tg_pos)
                if not mask.any():
                    st.error("Player not found in Players sheet.")
                else:
                    idx = df.index[mask][0]
                    if tgt_col not in df.columns:
                        df[tgt_col] = ""
                    current = str(df.at[idx, tgt_col]).strip().lower() in ("1","true","yes","y")
                    df.at[idx, tgt_col] = "FALSE" if current else "TRUE"
                    write_dataframe_to_sheet(ws, df, header=True)
                    st.toast(f"{tg_choice}: {'OFF' if current else 'ON'} for {tg_player}")
            except Exception as e:
                st.error(f"Tag update failed: {e}")

# Footer (small)
st.caption("Auction GM • final build + news")
