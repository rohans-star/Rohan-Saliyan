#!/usr/bin/env python3
"""
regenerate_dashboard.py — NL Bucketing Dashboard Data Regenerator

Queries BigQuery for the five JS arrays the dashboard template expects:
  RAW            daily NL grain (90-day rolling window)
  AM_RAW         monthly Area Manager roll-up
  AM_STORES      store-level detail per AM
  BREAKING_CAT   category × NL-type drill-down with wrong-KL flags
  BREAKING_STORES store × category drill-down with wrong-KL flags

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
from datetime import datetime, timezone
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
    # Lightweight check — list datasets (first page only)
    try:
        next(iter(client.list_datasets(max_results=1)), None)
        logger.info("✓ BigQuery connection verified")
    except Exception as exc:
        logger.error(f"BigQuery connection check failed: {exc}")
        raise
    return client

# ── Query helpers ─────────────────────────────────────────────────────────────

def run_query(client: bigquery.Client, sql: str,
              label: str, logger: logging.Logger) -> list[dict]:
    logger.info(f"  Running query: {label} …")
    try:
        df = client.query(sql).to_dataframe()
        logger.info(f"  ✓ {label}: {len(df)} rows")
        # Normalise types so json.dumps won't choke
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

def rows_to_js_array(rows: list[dict]) -> str:
    """Serialise a list of dicts to a compact JS array literal."""
    if not rows:
        return "[]"
    lines = []
    for r in rows:
        lines.append("{" + ",".join(
            f'"{k}":{json.dumps(v)}' for k, v in r.items()
        ) + "}")
    return "[\n" + ",\n".join(lines) + "\n]"

# ── BigQuery queries ──────────────────────────────────────────────────────────
# All five match the variable names and field shapes the template JS expects.
# Adjust table/column names to match your exact schema if needed.

def build_queries(project: str, dataset: str) -> dict[str, str]:
    p = f"`{project}.{dataset}"       # shorthand prefix

    return {

        # ── RAW ──────────────────────────────────────────────────────────────
        # Shape: {date_, wk, cat, ff, pt, ot, qty, val, stores, skus}
        "RAW": f"""
        SELECT
          FORMAT_DATE('%Y-%m-%d', DATE(created_at, 'Asia/Dubai'))   AS date_,
          EXTRACT(ISOWEEK FROM DATE(created_at, 'Asia/Dubai'))       AS wk,
          COALESCE(l2_category, '-')                                 AS cat,
          IF(fefo_tracked, 'FEFO', 'NonFEFO')                       AS ff,
          IF(partner_id = '9411', '9411', 'Non9411')                AS pt,
          COALESCE(overview_type, 'Other')                          AS ot,
          SUM(qty)                                                   AS qty,
          ROUND(SUM(qty * COALESCE(c.closing_cost, d.cost_price)), 2) AS val,
          COUNT(DISTINCT d.store_code)                               AS stores,
          COUNT(DISTINCT d.zsku)                                     AS skus
        FROM {p}.DAM_line_date_new_final` d
        LEFT JOIN {p}.txlog_cogs_output`  c
          ON d.zsku = c.zsku AND d.store_code = c.store_code
        WHERE DATE(d.created_at, 'Asia/Dubai')
              BETWEEN DATE_SUB(CURRENT_DATE('Asia/Dubai'), INTERVAL 90 DAY)
                  AND DATE_SUB(CURRENT_DATE('Asia/Dubai'), INTERVAL 1 DAY)
        GROUP BY 1, 2, 3, 4, 5, 6
        ORDER BY 1 DESC, 7 DESC
        """,

        # ── AM_RAW ───────────────────────────────────────────────────────────
        # Shape: {m, AM, asst, stores, qty, val, dam, exp, qal, lqd, rtv, tnl,
        #         fefo, nfefo, ds, ins}
        "AM_RAW": f"""
        SELECT
          FORMAT_DATE('%Y-%m', DATE(d.created_at, 'Asia/Dubai'))    AS m,
          COALESCE(mgr.am_name,   'Unassigned')                     AS AM,
          COALESCE(mgr.asst_name, '—')                             AS asst,
          COUNT(DISTINCT d.store_code)                              AS stores,
          SUM(d.qty)                                                AS qty,
          ROUND(SUM(d.qty * COALESCE(c.closing_cost, d.cost_price)), 2) AS val,
          ROUND(SUM(IF(d.overview_type='Damage',
            d.qty*COALESCE(c.closing_cost,d.cost_price), 0)), 2)   AS dam,
          ROUND(SUM(IF(d.overview_type='Expiry',
            d.qty*COALESCE(c.closing_cost,d.cost_price), 0)), 2)   AS exp,
          ROUND(SUM(IF(d.overview_type='Quality Rejection',
            d.qty*COALESCE(c.closing_cost,d.cost_price), 0)), 2)   AS qal,
          ROUND(SUM(IF(d.overview_type='Liquidation',
            d.qty*COALESCE(c.closing_cost,d.cost_price), 0)), 2)   AS lqd,
          ROUND(SUM(IF(d.overview_type='RTV',
            d.qty*COALESCE(c.closing_cost,d.cost_price), 0)), 2)   AS rtv,
          ROUND(SUM(IF(d.overview_type='Temp NL',
            d.qty*COALESCE(c.closing_cost,d.cost_price), 0)), 2)   AS tnl,
          ROUND(SUM(IF(d.fefo_tracked,
            d.qty*COALESCE(c.closing_cost,d.cost_price), 0)), 2)   AS fefo,
          ROUND(SUM(IF(NOT d.fefo_tracked,
            d.qty*COALESCE(c.closing_cost,d.cost_price), 0)), 2)   AS nfefo,
          ROUND(SUM(IF(d.bucket='DS',
            d.qty*COALESCE(c.closing_cost,d.cost_price), 0)), 2)   AS ds,
          ROUND(SUM(IF(d.bucket='Instock',
            d.qty*COALESCE(c.closing_cost,d.cost_price), 0)), 2)   AS ins
        FROM {p}.DAM_line_date_new_final` d
        LEFT JOIN {p}.txlog_cogs_output` c
          ON d.zsku = c.zsku AND d.store_code = c.store_code
        LEFT JOIN {p}.ops_logistic_fulfillment_managers` mgr
          ON d.store_code = mgr.store_code
        WHERE DATE(d.created_at, 'Asia/Dubai')
              BETWEEN DATE_SUB(CURRENT_DATE('Asia/Dubai'), INTERVAL 120 DAY)
                  AND DATE_SUB(CURRENT_DATE('Asia/Dubai'), INTERVAL 1 DAY)
        GROUP BY 1, 2, 3
        ORDER BY 1 DESC, 6 DESC
        """,

        # ── AM_STORES ─────────────────────────────────────────────────────────
        # Shape: {m, AM, sc, sn, qty, val, dam, exp, qal, lqd}
        "AM_STORES": f"""
        SELECT
          FORMAT_DATE('%Y-%m', DATE(d.created_at, 'Asia/Dubai'))    AS m,
          COALESCE(mgr.am_name, 'Unassigned')                       AS AM,
          d.store_code                                              AS sc,
          COALESCE(mgr.store_name, d.store_code)                    AS sn,
          SUM(d.qty)                                                AS qty,
          ROUND(SUM(d.qty * COALESCE(c.closing_cost, d.cost_price)), 2) AS val,
          ROUND(SUM(IF(d.overview_type='Damage',
            d.qty*COALESCE(c.closing_cost,d.cost_price), 0)), 2)   AS dam,
          ROUND(SUM(IF(d.overview_type='Expiry',
            d.qty*COALESCE(c.closing_cost,d.cost_price), 0)), 2)   AS exp,
          ROUND(SUM(IF(d.overview_type='Quality Rejection',
            d.qty*COALESCE(c.closing_cost,d.cost_price), 0)), 2)   AS qal,
          ROUND(SUM(IF(d.overview_type='Liquidation',
            d.qty*COALESCE(c.closing_cost,d.cost_price), 0)), 2)   AS lqd
        FROM {p}.DAM_line_date_new_final` d
        LEFT JOIN {p}.txlog_cogs_output` c
          ON d.zsku = c.zsku AND d.store_code = c.store_code
        LEFT JOIN {p}.ops_logistic_fulfillment_managers` mgr
          ON d.store_code = mgr.store_code
        WHERE DATE(d.created_at, 'Asia/Dubai')
              BETWEEN DATE_SUB(CURRENT_DATE('Asia/Dubai'), INTERVAL 60 DAY)
                  AND DATE_SUB(CURRENT_DATE('Asia/Dubai'), INTERVAL 1 DAY)
        GROUP BY 1, 2, 3, 4
        ORDER BY 1 DESC, 6 DESC
        """,

        # ── BREAKING_CAT ─────────────────────────────────────────────────────
        # Shape: {m, cat, ot, qty, val, stores, skus, wrongKL, wrongKLVal,
        #         avgDiff, maxDiff}
        "BREAKING_CAT": f"""
        SELECT
          FORMAT_DATE('%Y-%m', DATE(d.created_at, 'Asia/Dubai'))    AS m,
          COALESCE(d.l2_category, '-')                              AS cat,
          COALESCE(d.overview_type, 'Other')                       AS ot,
          SUM(d.qty)                                               AS qty,
          ROUND(SUM(d.qty * COALESCE(c.closing_cost, d.cost_price)), 2) AS val,
          COUNT(DISTINCT d.store_code)                             AS stores,
          COUNT(DISTINCT d.zsku)                                   AS skus,
          COUNTIF(k.sl_date IS NOT NULL
                  AND k.kl_date IS NOT NULL
                  AND DATE_DIFF(k.sl_date, k.kl_date, DAY) > 21)  AS wrongKL,
          ROUND(SUM(
            IF(k.sl_date IS NOT NULL AND k.kl_date IS NOT NULL
               AND DATE_DIFF(k.sl_date, k.kl_date, DAY) > 21,
               d.qty * COALESCE(c.closing_cost, d.cost_price), 0)
          ), 2)                                                    AS wrongKLVal,
          ROUND(AVG(
            IF(k.sl_date IS NOT NULL AND k.kl_date IS NOT NULL,
               ABS(DATE_DIFF(k.sl_date, k.kl_date, DAY)), NULL)
          ), 1)                                                    AS avgDiff,
          MAX(
            IF(k.sl_date IS NOT NULL AND k.kl_date IS NOT NULL,
               ABS(DATE_DIFF(k.sl_date, k.kl_date, DAY)), NULL)
          )                                                        AS maxDiff
        FROM {p}.DAM_line_date_new_final` d
        LEFT JOIN {p}.txlog_cogs_output` c
          ON d.zsku = c.zsku AND d.store_code = c.store_code
        LEFT JOIN {p}.keep_life_new_table` k
          ON d.zsku = k.zsku
        WHERE DATE(d.created_at, 'Asia/Dubai')
              BETWEEN DATE_SUB(CURRENT_DATE('Asia/Dubai'), INTERVAL 60 DAY)
                  AND DATE_SUB(CURRENT_DATE('Asia/Dubai'), INTERVAL 1 DAY)
        GROUP BY 1, 2, 3
        ORDER BY 1 DESC, 5 DESC
        """,

        # ── BREAKING_STORES ───────────────────────────────────────────────────
        # Shape: {m, cat, sc, sn, ot, qty, val, skus, wrongKL, wrongKLVal,
        #         maxDiff}
        "BREAKING_STORES": f"""
        SELECT
          FORMAT_DATE('%Y-%m', DATE(d.created_at, 'Asia/Dubai'))    AS m,
          COALESCE(d.l2_category, '-')                              AS cat,
          d.store_code                                             AS sc,
          COALESCE(mgr.store_name, d.store_code)                   AS sn,
          COALESCE(d.overview_type, 'Other')                       AS ot,
          SUM(d.qty)                                               AS qty,
          ROUND(SUM(d.qty * COALESCE(c.closing_cost, d.cost_price)), 2) AS val,
          COUNT(DISTINCT d.zsku)                                   AS skus,
          COUNTIF(k.sl_date IS NOT NULL
                  AND k.kl_date IS NOT NULL
                  AND DATE_DIFF(k.sl_date, k.kl_date, DAY) > 21)  AS wrongKL,
          ROUND(SUM(
            IF(k.sl_date IS NOT NULL AND k.kl_date IS NOT NULL
               AND DATE_DIFF(k.sl_date, k.kl_date, DAY) > 21,
               d.qty * COALESCE(c.closing_cost, d.cost_price), 0)
          ), 2)                                                    AS wrongKLVal,
          MAX(
            IF(k.sl_date IS NOT NULL AND k.kl_date IS NOT NULL,
               ABS(DATE_DIFF(k.sl_date, k.kl_date, DAY)), NULL)
          )                                                        AS maxDiff
        FROM {p}.DAM_line_date_new_final` d
        LEFT JOIN {p}.txlog_cogs_output` c
          ON d.zsku = c.zsku AND d.store_code = c.store_code
        LEFT JOIN {p}.keep_life_new_table` k
          ON d.zsku = k.zsku
        LEFT JOIN {p}.ops_logistic_fulfillment_managers` mgr
          ON d.store_code = mgr.store_code
        WHERE DATE(d.created_at, 'Asia/Dubai')
              BETWEEN DATE_SUB(CURRENT_DATE('Asia/Dubai'), INTERVAL 60 DAY)
                  AND DATE_SUB(CURRENT_DATE('Asia/Dubai'), INTERVAL 1 DAY)
        GROUP BY 1, 2, 3, 4, 5
        ORDER BY 1 DESC, 7 DESC
        """,
    }

# ── Template patching ─────────────────────────────────────────────────────────

def patch_array(html: str, var_name: str, new_array: str,
                logger: logging.Logger) -> str:
    """Replace  const VAR_NAME = [...];  with refreshed data."""
    pattern = rf"(const {re.escape(var_name)}\s*=\s*)\[[\s\S]*?\](\s*;)"
    result, n = re.subn(pattern, rf"\g<1>{new_array}\2", html, count=1)
    if n == 0:
        logger.warning(f"⚠ Placeholder 'const {var_name}' not found in template")
    else:
        logger.info(f"  ✓ Patched const {var_name}")
    return result

def patch_timestamps(html: str, logger: logging.Logger) -> str:
    """Update the hard-coded generation date visible in the dashboard header."""
    from datetime import date, timezone, timedelta
    dubai_today = (datetime.now(timezone(timedelta(hours=4)))).strftime("%Y-%m-%d")
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
        # 1. Auth
        logger.info("[1/4] Connecting to BigQuery …")
        client = create_bq_client(sa_json, logger)

        # 2. Queries — project/dataset from env or service-account project
        project = os.environ.get("BQ_PROJECT") or client.project
        dataset = os.environ.get("BQ_DATASET", "fulfillment")
        logger.info(f"[2/4] Running queries on {project}.{dataset} …")
        queries = build_queries(project, dataset)
        arrays: dict[str, list[dict]] = {}
        for name, sql in queries.items():
            arrays[name] = run_query(client, sql, name, logger)

        # 3. Load template and inject all five arrays
        logger.info("[3/4] Patching template …")
        tmpl = Path(template_path)
        if not tmpl.exists():
            logger.error(f"Template not found: {template_path}")
            return 2
        html = tmpl.read_text(encoding="utf-8")

        for var_name, rows in arrays.items():
            html = patch_array(html, var_name, rows_to_js_array(rows), logger)
        html = patch_timestamps(html, logger)

        # 4. Write output
        logger.info("[4/4] Writing output …")
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
