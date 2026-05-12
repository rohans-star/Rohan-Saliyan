#!/usr/bin/env python3
"""
regenerate_dashboard.py — NL Bucketing Dashboard Data Regenerator

Patches these JS variables in the template HTML:
  RAW              daily NL grain (last 90 days)
  DAILY_TREND      last 35 days aggregated by date
  KL_DATA          KL-issue SKUs (SL−KL > 21 days)
  AM_DATA          monthly AM roll-up object keyed by "YYYY-MM"
  ORDERS_BY_PERIOD order count object (D1, WTD, MTD_YYYY_MM, YTD)

Usage:
  python3 regenerate_dashboard.py \
    --template NL_Bucketing_Enhanced_11.template.html \
    --output   Dashboard/NL_Bucketing_Enhanced_11.html \
    --service-account /path/to/sa.json   # omit to use ADC

Exit codes: 0 success | 1 BQ error | 2 file error | 3 config error
"""

import argparse
import json
import logging
import os
import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

try:
    from google.cloud import bigquery
    from google.oauth2 import service_account
    import pandas as pd
except ImportError as e:
    print(f"[FATAL] Missing package: {e}\n"
          f"        pip install google-cloud-bigquery pandas db-dtypes", file=sys.stderr)
    sys.exit(3)

# ── Fully-qualified BQ table references ──────────────────────────────────────

DAM      = "`noonbinimksa.darkstore.DAM_line_date_new_final`"
COGS     = "`noondwh.instant_instant_finance.txlog_cogs_output`"
MANAGERS = "`noondwh.mxfulfillment_user_management.ops_logistic_fulfillment_managers`"
ORDERS   = "`noonbinimksa.darkstore.odr_fulfillment_60_ae`"
KL_TABLE = "`noondwh.instant_instant_catalog.keep_life_new_table`"

# ── Logging ───────────────────────────────────────────────────────────────────

def setup_logging(log_file: Optional[str] = None) -> logging.Logger:
    logger = logging.getLogger("nl_regen")
    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s",
                            datefmt="%Y-%m-%d %H:%M:%S")
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    if log_file:
        try:
            fh = logging.FileHandler(log_file, mode="a")
            fh.setLevel(logging.DEBUG)
            fh.setFormatter(fmt)
            logger.addHandler(fh)
        except IOError as exc:
            print(f"[WARN] Cannot open log file {log_file}: {exc}", file=sys.stderr)
    return logger

# ── BigQuery client ───────────────────────────────────────────────────────────

def create_bq_client(sa_json: Optional[str], logger: logging.Logger) -> bigquery.Client:
    if sa_json and Path(sa_json).exists() and Path(sa_json).stat().st_size > 10:
        logger.info(f"Authenticating with service account: {sa_json}")
        creds = service_account.Credentials.from_service_account_file(sa_json)
        client = bigquery.Client(credentials=creds, project=creds.project_id)
    else:
        logger.info("Authenticating with Application Default Credentials (ADC)")
        client = bigquery.Client()
    try:
        next(iter(client.list_datasets(max_results=1)), None)
        logger.info("✓ BigQuery connection verified")
    except Exception as exc:
        logger.error(f"BigQuery connection check failed: {exc}")
        raise
    return client

# ── Query runner ──────────────────────────────────────────────────────────────

def run_query(client: bigquery.Client, sql: str,
              label: str, logger: logging.Logger) -> list[dict]:
    logger.info(f"  Running query: {label} …")
    try:
        df = client.query(sql).to_dataframe()
        logger.info(f"  ✓ {label}: {len(df)} rows")
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].dt.strftime("%Y-%m-%d")
            elif pd.api.types.is_integer_dtype(df[col]):
                df[col] = df[col].astype(int)
            elif pd.api.types.is_float_dtype(df[col]):
                df[col] = df[col].round(2)
        df = df.where(pd.notnull(df), None)
        return df.to_dict(orient="records")
    except Exception as exc:
        logger.error(f"  ✗ Query '{label}' failed: {exc}")
        raise

# ── BigQuery SQL ──────────────────────────────────────────────────────────────

SQL_RAW = f"""
SELECT
  FORMAT_DATE('%Y-%m-%d', DATE(d.created_at, 'Asia/Dubai')) AS date_,
  EXTRACT(ISOWEEK FROM DATE(d.created_at, 'Asia/Dubai'))    AS wk,
  COALESCE(d.l2_category, '-')                              AS cat,
  IF(d.fefo_tracked, 'FEFO', 'NonFEFO')                    AS ff,
  IF(d.partner_id = '9411', '9411', 'Non9411')             AS pt,
  COALESCE(d.overview_type, 'Other')                       AS ot,
  SUM(d.qty)                                               AS qty,
  ROUND(SUM(d.qty * COALESCE(c.closing_cost, d.cost_price)), 2) AS val,
  COUNT(DISTINCT d.store_code)                             AS stores,
  COUNT(DISTINCT d.zsku)                                   AS skus
FROM {DAM} d
LEFT JOIN {COGS} c ON d.zsku = c.zsku AND d.store_code = c.store_code
WHERE DATE(d.created_at, 'Asia/Dubai')
      BETWEEN DATE_SUB(CURRENT_DATE('Asia/Dubai'), INTERVAL 90 DAY)
          AND DATE_SUB(CURRENT_DATE('Asia/Dubai'), INTERVAL 1 DAY)
GROUP BY 1,2,3,4,5,6
ORDER BY 1 DESC, 7 DESC
"""

SQL_DAILY_TREND = f"""
SELECT
  FORMAT_DATE('%Y-%m-%d', DATE(d.created_at, 'Asia/Dubai')) AS d,
  CAST(ROUND(SUM(d.qty * COALESCE(c.closing_cost, d.cost_price)), 0) AS INT64) AS total,
  CAST(ROUND(SUM(IF(d.overview_type='Damage',
    d.qty*COALESCE(c.closing_cost,d.cost_price),0)),0) AS INT64) AS dam,
  CAST(ROUND(SUM(IF(d.overview_type='Expiry',
    d.qty*COALESCE(c.closing_cost,d.cost_price),0)),0) AS INT64) AS exp,
  CAST(ROUND(SUM(IF(d.overview_type='Quality Rejection',
    d.qty*COALESCE(c.closing_cost,d.cost_price),0)),0) AS INT64) AS qal,
  CAST(ROUND(SUM(IF(d.overview_type='Liquidation',
    d.qty*COALESCE(c.closing_cost,d.cost_price),0)),0) AS INT64) AS lqd
FROM {DAM} d
LEFT JOIN {COGS} c ON d.zsku = c.zsku AND d.store_code = c.store_code
WHERE DATE(d.created_at, 'Asia/Dubai')
      BETWEEN DATE_SUB(CURRENT_DATE('Asia/Dubai'), INTERVAL 35 DAY)
          AND DATE_SUB(CURRENT_DATE('Asia/Dubai'), INTERVAL 1 DAY)
GROUP BY 1
ORDER BY 1 ASC
"""

SQL_KL_DATA = f"""
SELECT
  COALESCE(d.l2_category, '-')                              AS cat,
  COALESCE(d.overview_type, 'Other')                       AS ot,
  COUNT(DISTINCT d.zsku)                                   AS flagged_skus,
  SUM(d.qty)                                               AS qty,
  CAST(ROUND(SUM(d.qty * COALESCE(c.closing_cost, d.cost_price)), 0) AS INT64) AS val,
  ROUND(AVG(ABS(DATE_DIFF(k.sl_date, k.kl_date, DAY))), 1) AS avg_diff,
  MAX(ABS(DATE_DIFF(k.sl_date, k.kl_date, DAY)))           AS max_diff
FROM {DAM} d
LEFT JOIN {COGS} c ON d.zsku = c.zsku AND d.store_code = c.store_code
JOIN {KL_TABLE} k ON d.zsku = k.zsku
WHERE DATE(d.created_at, 'Asia/Dubai')
      BETWEEN DATE_SUB(CURRENT_DATE('Asia/Dubai'), INTERVAL 60 DAY)
          AND DATE_SUB(CURRENT_DATE('Asia/Dubai'), INTERVAL 1 DAY)
  AND k.sl_date IS NOT NULL
  AND k.kl_date IS NOT NULL
  AND DATE_DIFF(k.sl_date, k.kl_date, DAY) > 21
GROUP BY 1, 2
ORDER BY 5 DESC
"""

SQL_AM_DATA = f"""
SELECT
  FORMAT_DATE('%Y-%m', DATE(d.created_at, 'Asia/Dubai'))   AS m,
  COALESCE(mgr.am_name, 'Unassigned')                      AS AM,
  COUNT(DISTINCT d.store_code)                             AS stores,
  SUM(d.qty)                                               AS qty,
  CAST(ROUND(SUM(d.qty * COALESCE(c.closing_cost, d.cost_price)), 0) AS INT64) AS val,
  CAST(ROUND(SUM(IF(d.overview_type='Damage',
    d.qty*COALESCE(c.closing_cost,d.cost_price),0)),0) AS INT64) AS dam,
  CAST(ROUND(SUM(IF(d.overview_type='Expiry',
    d.qty*COALESCE(c.closing_cost,d.cost_price),0)),0) AS INT64) AS exp,
  CAST(ROUND(SUM(IF(d.overview_type='Quality Rejection',
    d.qty*COALESCE(c.closing_cost,d.cost_price),0)),0) AS INT64) AS qal,
  CAST(ROUND(SUM(IF(d.overview_type='Liquidation',
    d.qty*COALESCE(c.closing_cost,d.cost_price),0)),0) AS INT64) AS lqd,
  CAST(ROUND(SUM(IF(d.bucket='DS',
    d.qty*COALESCE(c.closing_cost,d.cost_price),0)),0) AS INT64) AS ds,
  CAST(ROUND(SUM(IF(d.bucket='Instock',
    d.qty*COALESCE(c.closing_cost,d.cost_price),0)),0) AS INT64) AS ins,
  COUNT(DISTINCT d.zsku)                                   AS skus
FROM {DAM} d
LEFT JOIN {COGS} c ON d.zsku = c.zsku AND d.store_code = c.store_code
LEFT JOIN {MANAGERS} mgr ON d.store_code = mgr.store_code
WHERE DATE(d.created_at, 'Asia/Dubai')
      BETWEEN DATE_TRUNC(DATE_SUB(CURRENT_DATE('Asia/Dubai'), INTERVAL 3 MONTH), MONTH)
          AND DATE_SUB(CURRENT_DATE('Asia/Dubai'), INTERVAL 1 DAY)
GROUP BY 1, 2
ORDER BY 1 DESC, 5 DESC
"""

SQL_ORDERS = f"""
SELECT
  FORMAT_DATE('%Y-%m-%d', CAST(date AS DATE)) AS d,
  SUM(total_orders)                           AS orders
FROM {ORDERS}
WHERE country_code = 'ae'
  AND CAST(date AS DATE)
      BETWEEN DATE_SUB(CURRENT_DATE('Asia/Dubai'), INTERVAL 60 DAY)
          AND DATE_SUB(CURRENT_DATE('Asia/Dubai'), INTERVAL 1 DAY)
GROUP BY 1
ORDER BY 1 ASC
"""

# ── JS value builders ──────────────────────────────────────────────────────────

def rows_to_js_array(rows: list[dict]) -> str:
    if not rows:
        return "[]"
    parts = []
    for r in rows:
        parts.append("{" + ",".join(f'"{k}":{json.dumps(v)}' for k, v in r.items()) + "}")
    return "[\n" + ",\n".join(parts) + "\n]"


def build_am_data_js(rows: list[dict]) -> str:
    """Group AM rows by month key and build a nested JS object."""
    by_month: dict[str, list] = defaultdict(list)
    for r in rows:
        m = r["m"]
        entry = {k: v for k, v in r.items() if k != "m"}
        by_month[m].append(entry)

    parts = []
    for month in sorted(by_month.keys(), reverse=True):
        am_rows = by_month[month]
        items = []
        for r in am_rows:
            items.append("    {" + ",".join(f'"{k}":{json.dumps(v)}' for k, v in r.items()) + "}")
        arr = "[\n" + ",\n".join(items) + "\n  ]"
        parts.append(f'  "{month}":{arr}')

    return "{\n" + ",\n".join(parts) + "\n}"


def build_orders_js(rows: list[dict]) -> str:
    """Aggregate daily order rows into D1, WTD, MTD per month, YTD."""
    dubai_tz = timezone(timedelta(hours=4))
    today = datetime.now(dubai_tz).date()
    yesterday = today - timedelta(days=1)
    days_since_monday = yesterday.weekday()
    monday = yesterday - timedelta(days=days_since_monday)

    by_date: dict[str, int] = {r["d"]: int(r["orders"]) for r in rows}

    d1 = by_date.get(yesterday.strftime("%Y-%m-%d"), 0)
    wtd = sum(v for d, v in by_date.items() if d >= monday.strftime("%Y-%m-%d"))

    by_month: dict[str, int] = defaultdict(int)
    for d_str, v in by_date.items():
        by_month[d_str[:7]] += v  # "YYYY-MM"

    ytd = sum(by_date.values())

    lines = [f"  D1: {d1}", f"  WTD: {wtd}"]
    for m in sorted(by_month.keys()):
        key = "MTD_" + m.replace("-", "_")
        lines.append(f"  {key}: {by_month[m]}")
    lines.append(f"  YTD: {ytd}")

    return "{\n" + ",\n".join(lines) + "\n}"

# ── Template patching ─────────────────────────────────────────────────────────

def patch_js_var(html: str, var_name: str, new_value: str,
                 logger: logging.Logger) -> str:
    """
    Replace  const VAR_NAME = <array_or_object>;  with new_value.
    Uses bracket-depth tracking so nested [...] and {...} are handled correctly.
    """
    m = re.search(rf"const {re.escape(var_name)}\s*=\s*", html)
    if not m:
        logger.warning(f"⚠ Variable 'const {var_name}' not found in template")
        return html

    start = m.end()
    if start >= len(html):
        logger.warning(f"⚠ Unexpected end of file after 'const {var_name}'")
        return html

    open_char = html[start]
    if open_char == '[':
        open_b, close_b = '[', ']'
    elif open_char == '{':
        open_b, close_b = '{', '}'
    else:
        logger.warning(f"⚠ Unexpected initializer for 'const {var_name}': {html[start:start+20]!r}")
        return html

    # Walk forward tracking bracket depth; skip string contents
    depth = 0
    i = start
    in_str = False
    str_ch = None
    while i < len(html):
        ch = html[i]
        if in_str:
            if ch == '\\':
                i += 2
                continue
            if ch == str_ch:
                in_str = False
        else:
            if ch in ('"', "'", '`'):
                in_str = True
                str_ch = ch
            elif ch == open_b:
                depth += 1
            elif ch == close_b:
                depth -= 1
                if depth == 0:
                    break
        i += 1

    close_pos = i
    semi = html.find(';', close_pos)
    if semi == -1:
        logger.warning(f"⚠ No semicolon found after 'const {var_name}' value")
        return html

    result = html[:m.start()] + f"const {var_name} = {new_value}" + html[semi + 1:]
    logger.info(f"  ✓ Patched const {var_name}")
    return result


def patch_timestamps(html: str, logger: logging.Logger) -> str:
    dubai_tz = timezone(timedelta(hours=4))
    dubai_today = datetime.now(dubai_tz).strftime("%Y-%m-%d")
    html = re.sub(r'Generated: \d{4}-\d{2}-\d{2}',
                  f'Generated: {dubai_today}', html)
    html = re.sub(r'Data last refreshed: \d{4}-\d{2}-\d{2}',
                  f'Data last refreshed: {dubai_today}', html)
    html = re.sub(r'(const MAX_DATA_DATE\s*=\s*")[^"]*(")',
                  rf'\g<1>{dubai_today}\g<2>', html)
    logger.info(f"  ✓ Updated timestamps to {dubai_today}")
    return html

# ── Main ──────────────────────────────────────────────────────────────────────

def main(template_path: str, output_path: str,
         sa_json: Optional[str], log_file: Optional[str]) -> int:

    logger = setup_logging(log_file)
    logger.info("═" * 65)
    logger.info("NL Bucketing Dashboard — regeneration start")
    logger.info("═" * 65)

    try:
        logger.info("[1/6] Connecting to BigQuery …")
        client = create_bq_client(sa_json, logger)

        logger.info("[2/6] Querying RAW (90-day daily grain) …")
        raw_js = rows_to_js_array(run_query(client, SQL_RAW, "RAW", logger))

        logger.info("[3/6] Querying DAILY_TREND (35-day daily totals) …")
        trend_js = rows_to_js_array(run_query(client, SQL_DAILY_TREND, "DAILY_TREND", logger))

        logger.info("[4/6] Querying KL_DATA (wrong-KL SKUs) …")
        kl_js = rows_to_js_array(run_query(client, SQL_KL_DATA, "KL_DATA", logger))

        logger.info("[5/6] Querying AM_DATA and ORDERS_BY_PERIOD …")
        am_js = build_am_data_js(run_query(client, SQL_AM_DATA, "AM_DATA", logger))
        orders_js = build_orders_js(run_query(client, SQL_ORDERS, "ORDERS_BY_PERIOD", logger))

        logger.info("[6/6] Patching template …")
        tmpl = Path(template_path)
        if not tmpl.exists():
            logger.error(f"Template not found: {template_path}")
            return 2
        html = tmpl.read_text(encoding="utf-8")

        html = patch_js_var(html, "RAW",              raw_js,    logger)
        html = patch_js_var(html, "DAILY_TREND",      trend_js,  logger)
        html = patch_js_var(html, "KL_DATA",          kl_js,     logger)
        html = patch_js_var(html, "AM_DATA",          am_js,     logger)
        html = patch_js_var(html, "ORDERS_BY_PERIOD", orders_js, logger)
        html = patch_timestamps(html, logger)

        out = Path(output_path)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(html, encoding="utf-8")
        logger.info(f"✓ Saved {out} ({out.stat().st_size:,} bytes)")
        logger.info("═" * 65)
        logger.info("Regeneration complete")
        logger.info("═" * 65)
        return 0

    except FileNotFoundError as exc:
        logger.error(f"File error: {exc}")
        return 2
    except Exception as exc:
        logger.error(f"Fatal: {exc}")
        logger.debug("", exc_info=True)
        return 1

# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Regenerate NL Bucketing Dashboard from BigQuery")
    p.add_argument("--template", required=True,
                   help="Path to NL_Bucketing_Enhanced_11.template.html")
    p.add_argument("--output", required=True,
                   help="Output path for NL_Bucketing_Enhanced_11.html")
    p.add_argument("--service-account", default=None,
                   help="Path to GCP service-account JSON (omit to use ADC)")
    p.add_argument("--log-file", default=None,
                   help="Optional log file path")
    return p.parse_args()

if __name__ == "__main__":
    args = parse_args()
    sys.exit(main(
        template_path=args.template,
        output_path=args.output,
        sa_json=args.service_account,
        log_file=args.log_file or os.environ.get("REGEN_LOG"),
    ))
