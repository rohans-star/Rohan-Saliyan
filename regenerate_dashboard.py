#!/usr/bin/env python3
"""
regenerate_dashboard.py — NL Bucketing Dashboard Data Regenerator

Generates fresh RAW JavaScript data arrays from BigQuery and injects them into
the NL_Bucketing_Enhanced_11.template.html template, producing the final
NL_Bucketing_Enhanced_11.html dashboard.

Supports:
  - BigQuery authentication via service account JSON or Application Default Credentials (ADC)
  - Configurable template and output paths
  - Comprehensive logging to stdout and optional log file
  - Error handling and recovery
  - Compatible with regen.sh wrapper script

Usage:
  python3 regenerate_dashboard.py \\
    --template /opt/nl_dashboard/NL_Bucketing_Enhanced_11.template.html \\
    --output /var/www/html/NL_Bucketing_Enhanced_11.html \\
    --service-account /opt/nl_dashboard/sa.json

Exit codes:
  0  Success
  1  BigQuery error
  2  Template/file error
  3  Configuration error
"""

import argparse
import logging
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict, List, Any

try:
    from google.cloud import bigquery
    from google.oauth2 import service_account
    import pandas as pd
except ImportError as e:
    print(f"[FATAL] Missing required package: {e}", file=sys.stderr)
    print("[FATAL] Install with: pip install google-cloud-bigquery pandas", file=sys.stderr)
    sys.exit(3)


# ══════════════════════════════════════════════════════════════════════════════
# LOGGING CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════

def setup_logging(log_file: Optional[str] = None) -> logging.Logger:
    """Configure dual logging to stdout and optional file."""
    logger = logging.getLogger("nl_dashboard_regen")
    logger.setLevel(logging.DEBUG)

    # Console handler (INFO level)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    # File handler (DEBUG level) if specified
    if log_file:
        try:
            file_handler = logging.FileHandler(log_file, mode="a")
            file_handler.setLevel(logging.DEBUG)
            file_formatter = logging.Formatter(
                "%(asctime)s [%(levelname)s] %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S"
            )
            file_handler.setFormatter(file_formatter)
            logger.addHandler(file_handler)
        except IOError as e:
            print(f"[WARN] Could not open log file {log_file}: {e}", file=sys.stderr)

    return logger


# ══════════════════════════════════════════════════════════════════════════════
# BIGQUERY OPERATIONS
# ══════════════════════════════════════════════════════════════════════════════

def create_bq_client(
    service_account_json: Optional[str] = None,
    logger: Optional[logging.Logger] = None
) -> bigquery.Client:
    """
    Create a BigQuery client.
    
    Args:
        service_account_json: Path to service account JSON. If None, uses ADC.
        logger: Logger instance.
    
    Returns:
        Authenticated BigQuery client.
    
    Raises:
        Exception: If authentication fails.
    """
    if logger is None:
        logger = logging.getLogger("nl_dashboard_regen")

    try:
        if service_account_json and Path(service_account_json).exists():
            logger.info(f"Authenticating with service account: {service_account_json}")
            credentials = service_account.Credentials.from_service_account_file(
                service_account_json
            )
            client = bigquery.Client(credentials=credentials)
        else:
            logger.info("Authenticating with Application Default Credentials (ADC)")
            client = bigquery.Client()

        # Test connection
        client.get_dataset("_")
        logger.info("✓ BigQuery authentication successful")
        return client

    except Exception as e:
        logger.error(f"BigQuery authentication failed: {e}")
        raise


def fetch_dashboard_data(client: bigquery.Client, logger: logging.Logger) -> Dict[str, Any]:
    """
    Fetch all NL dashboard datasets from BigQuery.
    
    This is a template implementation. Customize queries based on your actual schema.
    
    Returns:
        Dictionary with keys like 'nl_data', 'summary', 'details', etc.
    """
    data = {}
    
    try:
        logger.info("Fetching NL dashboard datasets from BigQuery...")

        # Example Query 1: Main NL data
        query_nl = """
        SELECT
          bucket,
          count,
          percentage,
          label
        FROM `your_project.your_dataset.nl_bucketing_data`
        WHERE date = CURRENT_DATE()
        ORDER BY bucket
        """
        
        logger.debug(f"Executing query: {query_nl[:80]}...")
        df_nl = client.query(query_nl).to_dataframe()
        data['nl_data'] = df_nl.to_dict(orient='records')
        logger.info(f"✓ Fetched {len(df_nl)} rows of NL bucketing data")

        # Example Query 2: Summary statistics
        query_summary = """
        SELECT
          metric,
          value,
          updated_at
        FROM `your_project.your_dataset.nl_summary_stats`
        WHERE date = CURRENT_DATE()
        """
        
        logger.debug(f"Executing query: {query_summary[:80]}...")
        df_summary = client.query(query_summary).to_dataframe()
        data['summary'] = df_summary.to_dict(orient='records')
        logger.info(f"✓ Fetched {len(df_summary)} summary statistics")

        # Example Query 3: Details
        query_details = """
        SELECT
          id,
          detail_key,
          detail_value
        FROM `your_project.your_dataset.nl_details`
        WHERE date = CURRENT_DATE()
        LIMIT 10000
        """
        
        logger.debug(f"Executing query: {query_details[:80]}...")
        df_details = client.query(query_details).to_dataframe()
        data['details'] = df_details.to_dict(orient='records')
        logger.info(f"✓ Fetched {len(df_details)} detail records")

    except Exception as e:
        logger.error(f"Failed to fetch BigQuery data: {e}")
        raise

    return data


# ══════════════════════════════════════════════════════════════════════════════
# DATA TRANSFORMATION
# ══════════════════════════════════════════════════════════════════════════════

def generate_raw_arrays(data: Dict[str, Any], logger: logging.Logger) -> Dict[str, str]:
    """
    Convert fetched data into JavaScript RAW array strings.
    
    Args:
        data: Dictionary from fetch_dashboard_data.
        logger: Logger instance.
    
    Returns:
        Dictionary with keys like 'RAW_NL_DATA', 'RAW_SUMMARY', etc.,
        each containing a complete JavaScript array literal.
    """
    raw_arrays = {}

    try:
        # Generate RAW_NL_DATA
        if 'nl_data' in data:
            nl_records = data['nl_data']
            raw_arrays['RAW_NL_DATA'] = _dict_to_js_array(nl_records)
            logger.debug(f"✓ Generated RAW_NL_DATA with {len(nl_records)} records")

        # Generate RAW_SUMMARY
        if 'summary' in data:
            summary_records = data['summary']
            raw_arrays['RAW_SUMMARY'] = _dict_to_js_array(summary_records)
            logger.debug(f"✓ Generated RAW_SUMMARY with {len(summary_records)} records")

        # Generate RAW_DETAILS
        if 'details' in data:
            details_records = data['details']
            raw_arrays['RAW_DETAILS'] = _dict_to_js_array(details_records)
            logger.debug(f"✓ Generated RAW_DETAILS with {len(details_records)} records")

        logger.info(f"✓ Generated {len(raw_arrays)} RAW arrays")

    except Exception as e:
        logger.error(f"Failed to generate RAW arrays: {e}")
        raise

    return raw_arrays


def _dict_to_js_array(records: List[Dict[str, Any]]) -> str:
    """
    Convert a list of dictionaries to a JavaScript array literal.
    
    Example:
        [{'name': 'Alice', 'age': 30}, {'name': 'Bob', 'age': 25}]
        becomes:
        [
          {"name":"Alice","age":30},
          {"name":"Bob","age":25}
        ]
    """
    if not records:
        return "[]"

    # Convert each record to JSON, handling special types
    json_records = []
    for record in records:
        try:
            # Handle pandas/numpy types
            clean_record = {}
            for key, value in record.items():
                if pd.isna(value):
                    clean_record[key] = None
                elif hasattr(value, 'isoformat'):  # datetime objects
                    clean_record[key] = value.isoformat()
                else:
                    clean_record[key] = value

            json_records.append(json.dumps(clean_record, separators=(',', ':')))
        except Exception:
            # Fallback: convert to string
            json_records.append(json.dumps(str(record), separators=(',', ':')))

    js_array = "[\n  " + ",\n  ".join(json_records) + "\n]"
    return js_array


# ══════════════════════════════════════════════════════════════════════════════
# TEMPLATE OPERATIONS
# ══════════════════════════════════════════════════════════════════════════════

def load_template(template_path: str, logger: logging.Logger) -> str:
    """Load and validate template file."""
    try:
        path = Path(template_path)
        if not path.exists():
            raise FileNotFoundError(f"Template not found: {template_path}")

        content = path.read_text(encoding='utf-8')
        logger.info(f"✓ Loaded template: {template_path}")
        return content

    except Exception as e:
        logger.error(f"Failed to load template: {e}")
        raise


def inject_raw_arrays(
    template: str,
    raw_arrays: Dict[str, str],
    logger: logging.Logger
) -> str:
    """
    Replace all RAW_* array placeholders in the template.
    
    Looks for patterns like:
      const RAW_NL_DATA = [...];
    
    And replaces with generated content.
    """
    output = template
    injected_count = 0

    try:
        for array_name, array_content in raw_arrays.items():
            # Pattern: const RAW_XXX = [...];
            pattern = rf'(const\s+{array_name}\s*=\s*)\[.*?\](;)'
            
            # Use DOTALL flag to match across newlines
            replacement = rf'\1{array_content}\2'
            
            new_output = re.sub(pattern, replacement, output, flags=re.DOTALL)
            
            if new_output != output:
                injected_count += 1
                output = new_output
                logger.debug(f"✓ Injected {array_name}")
            else:
                logger.warning(f"⚠ No placeholder found for {array_name}")

        logger.info(f"✓ Injected {injected_count} RAW arrays into template")

    except Exception as e:
        logger.error(f"Failed to inject RAW arrays: {e}")
        raise

    return output


def inject_timestamp(html_content: str, logger: logging.Logger) -> str:
    """
    Update the generated timestamp in the HTML.
    
    Looks for patterns like:
      <!-- Generated: 2026-05-11 12:34:56 UTC -->
      id="generated_timestamp" value="2026-05-11T12:34:56Z"
    """
    now_utc = datetime.now(timezone.utc)
    timestamp_str = now_utc.strftime("%Y-%m-%d %H:%M:%S UTC")
    timestamp_iso = now_utc.isoformat()

    try:
        # Update HTML comment
        output = re.sub(
            r'<!-- Generated:.*?-->',
            f'<!-- Generated: {timestamp_str} -->',
            html_content
        )

        # Update timestamp field (if it exists)
        output = re.sub(
            r'(id="generated_timestamp"\s+value=)"[^"]*"',
            rf'\1"{timestamp_iso}"',
            output
        )

        logger.info(f"✓ Updated timestamp: {timestamp_str}")
        return output

    except Exception as e:
        logger.error(f"Failed to update timestamp: {e}")
        raise


def save_html(content: str, output_path: str, logger: logging.Logger) -> None:
    """Save final HTML to disk."""
    try:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding='utf-8')
        file_size = path.stat().st_size
        logger.info(f"✓ Saved HTML output: {output_path} ({file_size} bytes)")

    except Exception as e:
        logger.error(f"Failed to save HTML: {e}")
        raise


# ══════════════════════════════════════════════════════════════════════════════
# MAIN ORCHESTRATION
# ══════════════════════════════════════════════════════════════════════════════

def main(
    template_path: str,
    output_path: str,
    service_account_json: Optional[str] = None,
    log_file: Optional[str] = None
) -> int:
    """
    Main regeneration workflow.
    
    Returns:
        Exit code (0 for success, 1+ for failure).
    """
    logger = setup_logging(log_file)

    try:
        logger.info("═" * 70)
        logger.info("NL Dashboard Regeneration Starting")
        logger.info("═" * 70)

        # Step 1: Create BigQuery client
        logger.info("[1/5] Authenticating with BigQuery...")
        client = create_bq_client(service_account_json, logger)

        # Step 2: Fetch data
        logger.info("[2/5] Fetching data from BigQuery...")
        data = fetch_dashboard_data(client, logger)

        # Step 3: Generate RAW arrays
        logger.info("[3/5] Generating JavaScript RAW arrays...")
        raw_arrays = generate_raw_arrays(data, logger)

        # Step 4: Load template and inject
        logger.info("[4/5] Loading template and injecting data...")
        template = load_template(template_path, logger)
        html = inject_raw_arrays(template, raw_arrays, logger)
        html = inject_timestamp(html, logger)

        # Step 5: Save output
        logger.info("[5/5] Saving final HTML...")
        save_html(html, output_path, logger)

        logger.info("═" * 70)
        logger.info("✓ Regeneration completed successfully")
        logger.info("═" * 70)
        return 0

    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
        return 2
    except PermissionError as e:
        logger.error(f"Permission denied: {e}")
        return 2
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        logger.debug("", exc_info=True)
        return 1


# ══════════════════════════════════════════════════════════════════════════════
# CLI ARGUMENT PARSING
# ══════════════════════════════════════════════════════════════════════════════

def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Regenerate NL Bucketing Dashboard from BigQuery data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # With service account
  python3 regenerate_dashboard.py \\
    --template ./NL_Bucketing_Enhanced_11.template.html \\
    --output ./NL_Bucketing_Enhanced_11.html \\
    --service-account ./sa.json

  # With ADC (Application Default Credentials)
  python3 regenerate_dashboard.py \\
    --template ./NL_Bucketing_Enhanced_11.template.html \\
    --output ./NL_Bucketing_Enhanced_11.html

  # With logging to file
  python3 regenerate_dashboard.py \\
    --template ./NL_Bucketing_Enhanced_11.template.html \\
    --output ./NL_Bucketing_Enhanced_11.html \\
    --log-file ./regen.log
        """
    )

    parser.add_argument(
        '--template',
        required=True,
        help='Path to NL_Bucketing_Enhanced_11.template.html'
    )
    parser.add_argument(
        '--output',
        required=True,
        help='Output path for NL_Bucketing_Enhanced_11.html'
    )
    parser.add_argument(
        '--service-account',
        default=None,
        help='Path to BigQuery service account JSON (optional; uses ADC if not provided)'
    )
    parser.add_argument(
        '--log-file',
        default=None,
        help='Optional log file path (defaults to stderr only)'
    )

    return parser.parse_args()


# ══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    args = parse_arguments()

    # Check if log file is specified in environment (for regen.sh integration)
    log_file = args.log_file or os.environ.get('REGEN_LOG')

    exit_code = main(
        template_path=args.template,
        output_path=args.output,
        service_account_json=args.service_account,
        log_file=log_file
    )

    sys.exit(exit_code)
