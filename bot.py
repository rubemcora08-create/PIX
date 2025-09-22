# -*- coding: utf-8 -*-
"""
Coleta Pix/BCB por município, filtra RS e agrega (sem municípios).
Mantém:
- wide_t: séries (métricas x meses) + snapshot em COLUNA (run_YYYYMMDD_HHMMSS)
- snapshots_run: snapshot em LINHA (uma por execução)
- snapshots_diff: SALDO DIÁRIO = (dia atual) - (dia anterior)
  * se não houver dia anterior, escreve linha com métricas em branco (NaN)

SNAPSHOT_MODE:
- 'column' -> só coluna em wide_t
- 'row'    -> só linha em snapshots_run (+ snapshots_diff)
- 'both'   -> coluna + linha (+ snapshots_diff) [DEFAULT]
"""

from __future__ import annotations

try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
except Exception:
    webdriver = None  # type: ignore
    Options = None    # type: ignore

import pandas as pd
import numpy as np
import os
import sys
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
from urllib.parse import quote
import re

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# =======================
# CONFIGURAÇÕES
# =======================
GHA = os.getenv("GITHUB_ACTIONS", "").lower() in ("1", "true", "yes")

DEFAULT_OUT_DIR = r"D:\User\Desktop\pix"
OUT_DIR = os.getenv("OUT_DIR", "." if GHA else DEFAULT_OUT_DIR)
OUT_XLSX = os.path.join(OUT_DIR, "pix_pordia.xlsx")

SHEET_WIDE_T   = "wide_t"
SHEET_SNAP_ROW = "snapshots_run"
SHEET_SNAP_DIFF= "snapshots_diff"   # <<< NOVA ABA
SNAPSHOT_MODE  = os.getenv("SNAPSHOT_MODE", "both").strip().lower()  # 'column'|'row'|'both'

BASE_URL = (
    "https://olinda.bcb.gov.br/olinda/servico/"
    "Pix_DadosAbertos/versao/v1/odata/TransacoesPixPorMunicipio"
)

# =======================
# FUNÇÕES DE APOIO
# =======================
def month_to_yyyymm(d: date) -> int:
    return d.year * 100 + d.month

def yyyymm_ok(s: str) -> bool:
    return len(s) == 6 and s.isdigit() and "01" <= s[4:] <= "12"

def parse_cli_range(args: list[str]) -> tuple[int, int]:
    current_yyyymm = month_to_yyyymm(date.today())
    if len(args) == 0:
        return current_yyyymm, current_yyyymm
    if len(args) == 1:
        s = args[0].strip()
        if not yyyymm_ok(s):
            raise ValueError("Parâmetro inválido (AAAAMM). Ex.: 202508")
        v = int(s)
        return v, v
    s1, s2 = args[0].strip(), args[1].strip()
    if not yyyymm_ok(s1) or not yyyymm_ok(s2):
        raise ValueError("Parâmetros inválidos (AAAAMM AAAAMM). Ex.: 202501 202508")
    a, b = int(s1), int(s2)
    if a > b:
        raise ValueError("Intervalo inválido: início maior que fim.")
    return a, b

def add_months_yyyymm(yyyymm: int, n: int) -> int:
    y, m = divmod(yyyymm, 100)
    m += n
    y += (m - 1) // 12
    m = (m - 1) % 12 + 1
    return y * 100 + m

def iter_yyyymm(start_yyyymm: int, end_yyyymm: int):
    cur = start_yyyymm
    while cur <= end_yyyymm:
        yield cur
        cur = add_months_yyyymm(cur, 1)

def build_url_parametrized(base_url: str, yyyymm: int) -> str:
    param = quote(f"'{yyyymm}'", safe="'")
    return (
        f"{base_url}(DataBase=@DataBase)"
        f"?@DataBase={param}"
        f"&$format=json"
        f"&$top=10000"
    )

def fetch_json_via_http(url: str) -> dict:
    sess = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=0.8,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    sess.mount("https://", HTTPAdapter(max_retries=retries))
    r = sess.get(url, timeout=60)
    r.raise_for_status()
    return r.json()

def fetch_json_via_browser(driver: "webdriver.Chrome", url: str) -> dict:  # type: ignore[name-defined]
    script = """
        const url = arguments[0];
        const done = arguments[1];
        (async () => {
            try {
                const r = await fetch(url, { method: 'GET' });
                const text = await r.text();
                if (!r.ok) { done({ ok: false, status: r.status, body: text, url }); return; }
                let json;
                try { json = JSON.parse(text); } catch (e) { done({ ok: false, status: r.status, body: text, parseError: String(e), url }); return; }
                done({ ok: true, data: json, url });
            } catch (err) { done({ ok: false, error: String(err), url }); }
        })();
    """
    result = driver.execute_async_script(script, url)
    if not result or not result.get("ok"):
        msg = ["Falha no fetch via navegador."]
        if result:
            if "url" in result: msg.append(f"URL: {result['url']}")
            if "status" in result: msg.append(f"HTTP: {result['status']}")
            if "body" in result: msg.append(f"Corpo: {result['body'][:500]}")
            if "error" in result: msg.append(f"Erro: {result['error']}")
            if "parseError" in result: msg.append(f"Parse: {result['parseError']}")
        raise RuntimeError(" | ".join(msg))
    return result["data"]

def ensure_out_dir(path: str):
    os.makedirs(path, exist_ok=True)

def fetch_pix_por_municipio(driver: "webdriver.Chrome | None", yyyymm: int) -> pd.DataFrame:  # type: ignore[name-defined]
    use_http = GHA or (os.getenv("USE_HTTP", "").lower() in ("1", "true", "yes"))
    url = build_url_parametrized(BASE_URL, yyyymm)
    payload = fetch_json_via_http(url) if use_http else fetch_json_via_browser(driver, url)  # type: ignore[arg-type]
    if "value" not in payload:
        raise RuntimeError(f"Resposta inesperada do Olinda (sem 'value'). URL: {url}")

    rows = payload.get("value", [])
    next_link = payload.get("@odata.nextLink")
    while next_link:
        payload = fetch_json_via_http(next_link) if use_http else fetch_json_via_browser(driver, next_link)  # type: ignore[arg-type]
        if "value" not in payload:
            raise RuntimeError(f"Resposta inesperada em paginação. URL: {next_link}")
        rows.extend(payload.get("value", []))
        next_link = payload.get("@odata.nextLink")

    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    sort_cols = [c for c in ["DataBase", "UF", "Municipio", "MunicipioCodigoIBGE", "TipoPessoa", "Perspectiva"] if c in df.columns]
    if sort_cols:
        df.sort_values(by=sort_cols, inplace=True, ignore_index=True)
    return df

# ---------- filtro por RS ----------
def _find_mun_code_column(df: pd.DataFrame) -> str | None:
    candidates_by_name = [c for c in df.columns if any(k in c.lower() for k in ["ibge", "cod", "codigo"]) and "mun" in c.lower()]
    preferred = ["MunicipioCodigoIBGE", "MunicipioIBGE", "CodigoIBGE", "CodIBGE", "MunicipioCodigo", "CodMunicipioIBGE"]
    for p in preferred:
        if p in df.columns:
            return p
    for c in candidates_by_name:
        return c
    for c in df.columns:
        s = df[c]
        if pd.api.types.is_integer_dtype(s) or pd.api.types.is_float_dtype(s):
            valid_frac = (s.dropna().astype(int).between(1100000, 5399999)).mean() if len(s.dropna()) else 0
            if valid_frac > 0.8:
                return c
        elif pd.api.types.is_object_dtype(s):
            sample = s.dropna().astype(str).str.replace(r"\D", "", regex=True)
            valid_frac = ((sample.str.len() == 7) & sample.str.isnumeric()).mean() if len(sample) else 0
            if valid_frac > 0.8:
                return c
    return None

def filter_only_rs(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    if "UF" in df.columns:
        return df[df["UF"].astype(str).str.upper() == "RS"].copy()
    code_col = _find_mun_code_column(df)
    if not code_col:
        raise RuntimeError("Não encontrei coluna de código de município IBGE para filtrar RS.")
    work = df.copy()
    code_str = work[code_col].astype(str).str.replace(r"\D", "", regex=True).str.zfill(7)
    work = work[code_str.str.startswith("43")].copy()
    return work

def identify_id_and_value_columns(df: pd.DataFrame):
    df_local = df.copy()
    if "DataBase" in df_local.columns:
        try:
            df_local["DataBase"] = pd.to_numeric(df_local["DataBase"], errors="ignore")
        except Exception:
            pass
    ignore_ids = {"DataBase", "Municipio"}
    code_col = _find_mun_code_column(df_local)
    if code_col:
        ignore_ids.add(code_col)
    id_cols, val_cols = [], []
    for c in df_local.columns:
        if c in ignore_ids:
            continue
        if pd.api.types.is_numeric_dtype(df_local[c]):
            val_cols.append(c)
        else:
            id_cols.append(c)
    return id_cols, val_cols

def to_wide_for_month(df_raw: pd.DataFrame, yyyymm: int) -> pd.DataFrame:
    if df_raw.empty:
        return pd.DataFrame()
    df_work = filter_only_rs(df_raw)
    drop_cols = []
    if "Municipio" in df_work.columns:
        drop_cols.append("Municipio")
    code_col = _find_mun_code_column(df_work)
    if code_col:
        drop_cols.append(code_col)
    if drop_cols:
        df_work = df_work.drop(columns=drop_cols)

    id_cols, val_cols = identify_id_and_value_columns(df_work)
    if not id_cols:
        id_cols = [c for c in df_work.columns if c != "DataBase"]

    df_num = df_work.copy()
    for c in val_cols:
        df_num[c] = pd.to_numeric(df_num[c], errors="coerce")

    if val_cols:
        df_agg = df_num.groupby(id_cols, dropna=False, as_index=False)[val_cols].sum()
    else:
        df_agg = df_num.drop_duplicates(subset=id_cols)

    rename_map = {}
    for c in df_agg.columns:
        if c in id_cols:
            continue
        new_name = f"{c}_{yyyymm}"
        if new_name in df_agg.columns:
            k = 2
            while f"{new_name}_v{k}" in df_agg.columns:
                k += 1
            new_name = f"{new_name}_v{k}"
        rename_map[c] = new_name
    df_agg = df_agg.rename(columns=rename_map)
    return df_agg

_COL_RE = re.compile(r"^(?P<base>.+?)_(?P<yyyymm>\d{6})(?:_v\d+)?$")

def _extract_col_meta(col: str):
    m = _COL_RE.match(col)
    if not m:
        return None
    return m.group("base"), m.group("yyyymm")

def transpose_wide(df_wide: pd.DataFrame) -> pd.DataFrame:
    if df_wide is None or df_wide.empty:
        return pd.DataFrame()
    id_cols = [c for c in df_wide.columns if not pd.api.types.is_numeric_dtype(df_wide[c])]
    metrics_cols = []
    meta = []
    for c in df_wide.columns:
        if c in id_cols:
            continue
        info = _extract_col_meta(c)
        if info is None:
            continue
        base, yyyymm = info
        metrics_cols.append(c)
        meta.append((c, base, yyyymm))
    if not metrics_cols:
        return pd.DataFrame()
    long_rows = []
    for c, base, yyyymm in meta:
        chunk = df_wide[id_cols + [c]].copy()
        chunk["__base"] = base
        chunk["__yyyymm"] = yyyymm
        chunk = chunk.rename(columns={c: "__valor"})
        id_vals = chunk[id_cols].astype(str).apply(lambda r: "|".join(r.values), axis=1)
        chunk["ID_Metrica"] = id_vals + "|" + chunk["__base"].astype(str)
        long_rows.append(chunk[["ID_Metrica", "__yyyymm", "__valor"]])
    long_df = pd.concat(long_rows, ignore_index=True)
    long_df = (long_df
               .dropna(subset=["__valor"], how="all")
               .groupby(["ID_Metrica", "__yyyymm"], as_index=False)["__valor"]
               .last())
    wide_t = long_df.pivot(index="ID_Metrica", columns="__yyyymm", values="__valor")
    wide_t = wide_t.reindex(sorted(wide_t.columns), axis=1)
    wide_t = wide_t.reset_index()
    wide_t.columns.name = None
    return wide_t

def merge_wide_t(existing_wide_t: pd.DataFrame | None, new_wide_t: pd.DataFrame) -> pd.DataFrame:
    if existing_wide_t is None or existing_wide_t.empty:
        return new_wide_t.copy()
    if "ID_Metrica" not in existing_wide_t.columns:
        return new_wide_t.copy()
    merged = pd.merge(existing_wide_t, new_wide_t, on="ID_Metrica", how="outer", suffixes=("", "__new"))
    for col in list(new_wide_t.columns):
        if col == "ID_Metrica":
            continue
        new_col = col + "__new"
        if new_col in merged.columns:
            merged[col] = merged[new_col].combine_first(merged[col])
            merged.drop(columns=[new_col], inplace=True)
    month_cols = sorted([c for c in merged.columns if c != "ID_Metrica" and re.fullmatch(r"\d{6}", str(c) or "")])
    other_cols = [c for c in merged.columns if c not in ["ID_Metrica"] + month_cols]
    merged = merged[["ID_Metrica"] + month_cols + other_cols]
    return merged

def load_existing_sheet(xlsx_path: str, sheet_name: str) -> pd.DataFrame | None:
    if not os.path.exists(xlsx_path):
        return None
    try:
        xl = pd.ExcelFile(xlsx_path, engine="openpyxl")
    except PermissionError as e:
        raise PermissionError(
            f"Não consegui abrir '{xlsx_path}'. Feche o arquivo no Excel e rode novamente."
        ) from e
    except Exception:
        return None
    if sheet_name not in xl.sheet_names:
        return None
    try:
        return xl.parse(sheet_name)
    except Exception:
        return None

def write_sheets(xlsx_path: str,
                 df_wide_t: pd.DataFrame | None,
                 df_snap_row: pd.DataFrame | None,
                 df_snap_diff: pd.DataFrame | None):
    if ((df_wide_t is None or df_wide_t.empty)
        and (df_snap_row is None or df_snap_row.empty)
        and (df_snap_diff is None or df_snap_diff.empty)):
        raise RuntimeError("Nada para escrever.")
    ensure_out_dir(os.path.dirname(xlsx_path) or ".")
    with pd.ExcelWriter(xlsx_path, engine="openpyxl", mode="w") as writer:
        if df_wide_t is not None and not df_wide_t.empty:
            df_wide_t.to_excel(writer, sheet_name=SHEET_WIDE_T, index=False)
        if df_snap_row is not None and not df_snap_row.empty:
            df_snap_row.to_excel(writer, sheet_name=SHEET_SNAP_ROW, index=False)
        if df_snap_diff is not None and not df_snap_diff.empty:
            df_snap_diff.to_excel(writer, sheet_name=SHEET_SNAP_DIFF, index=False)

# ------------------- utilidades para SNAPSHOTS -------------------
_RUN_DATE_RE = re.compile(r"^run_(\d{8})_\d{6}")

def extract_run_date_from_stamp(run_stamp: str) -> str | None:
    """Extrai YYYYMMDD de run_YYYYMMDD_HHMMSS."""
    m = _RUN_DATE_RE.match(str(run_stamp))
    return m.group(1) if m else None

def ensure_run_date_col(df: pd.DataFrame | None) -> pd.DataFrame | None:
    """Garante coluna run_date (YYYYMMDD) em snapshots_run existente."""
    if df is None or df.empty:
        return df
    if "run_date" not in df.columns:
        df = df.copy()
        df["run_date"] = df["run_stamp"].apply(lambda s: extract_run_date_from_stamp(str(s)))
    return df

def build_daily_diff_row(existing_snap_row: pd.DataFrame | None,
                         new_row_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula (dia atual) - (dia anterior).
    Se não existir linha do dia anterior, retorna linha com métricas NaN.
    """
    # new_row_df tem 1 linha com run_stamp, run_date, target_month + métricas
    cur = new_row_df.iloc[0]
    cur_date = str(cur.get("run_date", "")) or extract_run_date_from_stamp(str(cur["run_stamp"]))
    cur_stamp = str(cur["run_stamp"])
    prev_date_dt = datetime.strptime(cur_date, "%Y%m%d").date() - timedelta(days=1)
    prev_date = prev_date_dt.strftime("%Y%m%d")

    fixed_cols = ["run_stamp", "run_date", "prev_run_stamp", "prev_run_date", "target_month"]

    # sem base anterior -> linha vazia (NaN nas métricas)
    if existing_snap_row is None or existing_snap_row.empty:
        out = {"run_stamp": cur_stamp, "run_date": cur_date, "prev_run_stamp": None,
               "prev_run_date": prev_date, "target_month": cur.get("target_month")}
        # métricas = todas as chaves métrica da própria new_row_df (exceto fixos)
        metric_cols = [c for c in new_row_df.columns if c not in ("run_stamp","run_date","target_month")]
        for c in metric_cols:
            out[c] = np.nan
        return pd.DataFrame([out])

    # garantir run_date na base
    base = ensure_run_date_col(existing_snap_row).copy()

    # localizar linha exatamente do dia anterior
    prev_rows = base.loc[base["run_date"] == prev_date]
    if prev_rows.empty:
        out = {"run_stamp": cur_stamp, "run_date": cur_date, "prev_run_stamp": None,
               "prev_run_date": prev_date, "target_month": cur.get("target_month")}
        metric_cols = [c for c in new_row_df.columns if c not in ("run_stamp","run_date","target_month")]
        for c in metric_cols:
            out[c] = np.nan
        return pd.DataFrame([out])

    # se houver várias no dia anterior, usa a última (ordem da planilha)
    prev = prev_rows.iloc[-1]

    # união de colunas de métricas
    metric_cols = sorted(list(set([c for c in base.columns if c not in ("run_stamp","run_date","target_month")]
                                  + [c for c in new_row_df.columns if c not in ("run_stamp","run_date","target_month")])))
    out = {
        "run_stamp": cur_stamp,
        "run_date": cur_date,
        "prev_run_stamp": prev["run_stamp"],
        "prev_run_date": prev_date,
        "target_month": cur.get("target_month")
    }
    for c in metric_cols:
        cur_val  = pd.to_numeric(cur.get(c, np.nan), errors="coerce")
        prev_val = pd.to_numeric(prev.get(c, np.nan), errors="coerce")
        if pd.notna(cur_val) and pd.notna(prev_val):
            out[c] = float(cur_val) - float(prev_val)
        else:
            out[c] = np.nan
    return pd.DataFrame([out])

# =======================
# EXECUÇÃO
# =======================
def main_batch(start_yyyymm: int, end_yyyymm: int):
    run_dt = datetime.now(ZoneInfo("America/Sao_Paulo"))
    run_stamp_col = run_dt.strftime("run_%Y%m%d_%H%M%S")
    run_date = run_dt.strftime("%Y%m%d")

    ensure_out_dir(OUT_DIR)
    print(f"[INFO] Execução em {run_dt.isoformat()} | Intervalo: {start_yyyymm}..{end_yyyymm} (inclusive)")
    print(f"[INFO] OUT_XLSX = {OUT_XLSX}")
    print(f"[INFO] SNAPSHOT_MODE = {SNAPSHOT_MODE}")

    use_http = GHA or (os.getenv("USE_HTTP", "").lower() in ("1", "true", "yes"))

    driver = None
    if not use_http:
        if webdriver is None or Options is None:
            raise RuntimeError("Selenium indisponível: instale 'selenium' ou defina USE_HTTP=1 para usar HTTP.")
        chrome_opts = Options()
        chrome_opts.add_argument("--headless=new")
        chrome_opts.add_argument("--disable-gpu")
        chrome_opts.add_argument("--no-sandbox")
        chrome_opts.add_argument("--window-size=1200,900")
        driver = webdriver.Chrome(options=chrome_opts)

    existing_wide_t = load_existing_sheet(OUT_XLSX, SHEET_WIDE_T)
    existing_snap_row = load_existing_sheet(OUT_XLSX, SHEET_SNAP_ROW)
    existing_snap_diff= load_existing_sheet(OUT_XLSX, SHEET_SNAP_DIFF)

    months_processed = 0
    months_skipped_empty = 0
    new_wide_list: list[pd.DataFrame] = []

    try:
        for yyyymm in iter_yyyymm(start_yyyymm, end_yyyymm):
            print("\n========================")
            print(f"[INFO] Coletando {yyyymm}...")
            try:
                df_raw = fetch_pix_por_municipio(driver, yyyymm)
            except Exception as e:
                print(f"[ERRO] Falha na coleta de {yyyymm}: {repr(e)}")
                continue

            if df_raw.empty:
                print(f"[AVISO] Nenhum dado retornado em {yyyymm}. Pulando.")
                months_skipped_empty += 1
                continue

            df_new_wide = to_wide_for_month(df_raw, yyyymm)
            if df_new_wide.empty:
                print(f"[AVISO] {yyyymm}: sem métricas após agregação. Pulando.")
                months_skipped_empty += 1
                continue

            new_wide_list.append(df_new_wide)
            months_processed += 1

    finally:
        if driver is not None:
            try:
                driver.quit()
            except Exception:
                pass

    if not new_wide_list:
        print("[AVISO] Nada novo para transpor (todas as coletas vazias?).")
        return

    df_wide_this_run = pd.concat(new_wide_list, ignore_index=True).drop_duplicates()
    df_transposed_new = transpose_wide(df_wide_this_run)
    df_transposed_final = merge_wide_t(existing_wide_t, df_transposed_new)

    # -------------- Determina mês-alvo para snapshot --------------
    target_month = str(end_yyyymm)
    month_cols_final = sorted([c for c in df_transposed_final.columns if re.fullmatch(r"\d{6}", str(c) or "")])
    if target_month not in month_cols_final and month_cols_final:
        target_month = month_cols_final[-1]

    # -------------- SNAPSHOT EM COLUNA (wide_t) --------------
    df_to_write_wide = df_transposed_final.copy()
    if SNAPSHOT_MODE in ("column", "both"):
        col_name = run_stamp_col
        while col_name in df_to_write_wide.columns:
            col_name = col_name + "_v2"
        if target_month in df_to_write_wide.columns:
            df_to_write_wide[col_name] = pd.to_numeric(df_to_write_wide[target_month], errors="coerce")
        else:
            df_to_write_wide[col_name] = np.nan

    # -------------- SNAPSHOT EM LINHA (snapshots_run) --------------
    df_to_write_snap = None
    snap_df_new = None
    if SNAPSHOT_MODE in ("row", "both"):
        if "ID_Metrica" not in df_transposed_final.columns or target_month not in df_transposed_final.columns:
            snap_series = pd.Series(dtype=float)
        else:
            snap_series = pd.to_numeric(
                df_transposed_final.set_index("ID_Metrica")[target_month],
                errors="coerce"
            )
        # linha da execução
        new_row = {"run_stamp": run_stamp_col, "run_date": run_date, "target_month": target_month}
        new_row.update({k: float(v) if pd.notna(v) else np.nan for k, v in snap_series.to_dict().items()})
        snap_df_new = pd.DataFrame([new_row])

        # concilia com existente
        if existing_snap_row is None or existing_snap_row.empty:
            df_to_write_snap = snap_df_new
        else:
            # união ordenada
            all_cols = list(existing_snap_row.columns)
            for c in snap_df_new.columns:
                if c not in all_cols:
                    all_cols.append(c)
            fixed = ["run_stamp", "run_date", "target_month"]
            metric_cols = [c for c in all_cols if c not in fixed]
            ordered = fixed + sorted(metric_cols)
            existing_aligned = existing_snap_row.reindex(columns=ordered)
            new_aligned = snap_df_new.reindex(columns=ordered)
            df_to_write_snap = pd.concat([existing_aligned, new_aligned], ignore_index=True)

    # -------------- SALDO DIÁRIO (snapshots_diff) --------------
    df_to_write_diff = None
    if SNAPSHOT_MODE in ("row", "both"):
        # Garantir base com run_date
        existing_snap_row = ensure_run_date_col(existing_snap_row)
        # Construir a linha de diff desta execução
        diff_row = build_daily_diff_row(existing_snap_row, snap_df_new if snap_df_new is not None else pd.DataFrame([{"run_stamp": run_stamp_col, "run_date": run_date, "target_month": target_month}]))
        if existing_snap_diff is None or existing_snap_diff.empty:
            df_to_write_diff = diff_row
        else:
            # união de colunas (fixos + métricas)
            all_cols = list(existing_snap_diff.columns)
            for c in diff_row.columns:
                if c not in all_cols:
                    all_cols.append(c)
            fixed = ["run_stamp", "run_date", "prev_run_stamp", "prev_run_date", "target_month"]
            metric_cols = [c for c in all_cols if c not in fixed]
            ordered = fixed + sorted(metric_cols)
            existing_aligned = existing_snap_diff.reindex(columns=ordered)
            new_aligned = diff_row.reindex(columns=ordered)
            df_to_write_diff = pd.concat([existing_aligned, new_aligned], ignore_index=True)

    # -------------- Escreve Excel --------------
    try:
        write_sheets(
            OUT_XLSX,
            df_wide_t = (df_to_write_wide if SNAPSHOT_MODE in ("column","both") else df_transposed_final),
            df_snap_row = df_to_write_snap,
            df_snap_diff = df_to_write_diff
        )
    except Exception as e:
        print("[ERRO] Falha ao escrever o Excel:", repr(e))
        sys.exit(1)

    print("\n========================")
    abas = [SHEET_WIDE_T]
    if SNAPSHOT_MODE in ('row','both'):
        abas.append(SHEET_SNAP_ROW)
        abas.append(SHEET_SNAP_DIFF)
    print(f"[OK] Atualizado: '{OUT_XLSX}' | Abas: {', '.join(abas)}")
    print(f"[RESUMO] Meses processados: {months_processed} | Vazios/pulados: {months_skipped_empty}")
    print(f"[INFO] Mês alvo no snapshot: {target_month}")
    print(f"[INFO] Modo de snapshot: {SNAPSHOT_MODE}")
    if SNAPSHOT_MODE in ("column", "both"):
        print(f"[INFO] Nova coluna em '{SHEET_WIDE_T}': {run_stamp_col} (valores numéricos de {target_month})")
    if SNAPSHOT_MODE in ("row", "both"):
        print(f"[INFO] Nova linha em '{SHEET_SNAP_ROW}': run_stamp={run_stamp_col} (run_date={run_date})")
        print(f"[INFO] Nova linha em '{SHEET_SNAP_DIFF}': saldo diário vs dia anterior (ou vazio se ausente)")
# =======================
# ENTRYPOINT
# =======================
if __name__ == "__main__":
    try:
        start_yyyymm, end_yyyymm = parse_cli_range(sys.argv[1:])
    except Exception as e:
        print("[ERRO] Parâmetros inválidos:", e)
        sys.exit(1)
    main_batch(start_yyyymm, end_yyyymm)
