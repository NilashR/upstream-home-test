"""This module provides functionality to detect potential SQL injection patterns
in the bronze layer data using DuckDB and regex pattern matching.
"""
import json
import sys
import argparse
from pathlib import Path
from typing import Optional

import duckdb
import polars as pl
from pydantic import BaseModel
from upstream_home_test.constant import BRONZE_PATH


class SQLInjectionResult(BaseModel):
    """Result of SQL injection detection for a single record.

    Attributes:
        file_path: Path to the parquet file containing the violation
        row_index: Index of the row with the violation
        column_name: Name of the column with the violation
        column_value: The actual value that triggered the detection
        matched_pattern: The regex pattern that matched
        message_json: JSON string of the original violating message
    """

    file_path: str
    row_index: int
    column_name: str
    column_value: str
    matched_pattern: str
    message_json: Optional[str] = None


class SQLInjectionReport(BaseModel):
    """Complete SQL injection detection report.

    Attributes:
        total_files_scanned: Number of parquet files scanned
        total_rows_scanned: Total number of rows examined
        violations_found: Number of violations detected
        violations: List of individual violation details
        scan_duration_ms: Time taken to complete the scan in milliseconds
    """

    total_files_scanned: int
    total_rows_scanned: int
    violations_found: int
    violations: list[SQLInjectionResult]
    scan_duration_ms: float


def sql_injection_report(
    columns_to_check: list[str],
    injection_patterns: list[str],
    data_path: str = BRONZE_PATH,
) -> SQLInjectionReport:
    """Detect potential SQL injection patterns.

    This function scans all parquet files in a given directory
    for potential SQL injection patterns in specified columns using
    DuckDB for efficient querying and regex pattern matching.

    Args:
        columns_to_check: List of column names to check for SQL injection patterns
        injection_patterns: List of regex patterns to detect SQL injection attempts
        data_path: Path to the data directory containing parquet files

    Returns:
        SQLInjectionReport containing all detected violations and scan statistics

    Raises:
        FileNotFoundError: If data directory doesn't exist
        ValueError: If columns_to_check or injection_patterns are empty
        Exception: If there's an error during the scan process
    """
    import time

    start_time = time.time()

    # Validate inputs
    if not columns_to_check:
        raise ValueError("columns_to_check cannot be empty")

    if not injection_patterns:
        raise ValueError("injection_patterns cannot be empty")

    data_path_obj = Path(data_path)
    if not data_path_obj.exists():
        raise FileNotFoundError(f"Data directory not found: {data_path}")

    # Find all parquet files in data directory
    parquet_files = list(data_path_obj.rglob("*.parquet"))
    if not parquet_files:
        return SQLInjectionReport(
            total_files_scanned=0,
            total_rows_scanned=0,
            violations_found=0,
            violations=[],
            scan_duration_ms=0.0,
        )

    # Initialize DuckDB connection
    conn = duckdb.connect()
    violations: list[SQLInjectionResult] = []
    total_rows_scanned = 0

    try:
        for file_path in parquet_files:
            file_path_str = str(file_path)

            # Get file info first
            file_info_query = f"""
            SELECT COUNT(*) as row_count
            FROM read_parquet('{file_path_str}')
            """
            file_info = conn.execute(file_info_query).fetchone()
            file_row_count = file_info[0] if file_info else 0
            total_rows_scanned += file_row_count

            # Check each column for each pattern
            for column in columns_to_check:
                for i, pattern in enumerate(injection_patterns):
                    # Create a query to find violations
                    # We need to escape the pattern for SQL and handle the regex properly
                    escaped_pattern = pattern.replace(
                        "'", "''"
                    )  # Escape single quotes for SQL

                    violation_query = f"""
                    SELECT
                        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 as row_index,
                        "{column}" as column_value,
                        *
                    FROM read_parquet('{file_path_str}')
                    WHERE "{column}" IS NOT NULL
                    AND regexp_matches("{column}"::VARCHAR, '{escaped_pattern}')
                    """

                    try:
                        cur = conn.execute(violation_query)
                        results = cur.fetchall()
                        col_names = [d[0] for d in cur.description]

                        for row in results:
                            # Map row to dict using column names
                            row_dict = {
                                name: row[idx] for idx, name in enumerate(col_names)
                            }
                            row_index = int(row_dict.get("row_index", 0))
                            column_value = row_dict.get("column_value")
                            # Remove helper fields from message payload
                            row_dict.pop("row_index", None)
                            row_dict.pop("column_value", None)
                            # Serialize full message to JSON (best-effort via str())
                            safe_row = {
                                k: (
                                    v.isoformat() if hasattr(v, "isoformat") else str(v)
                                )
                                for k, v in row_dict.items()
                            }
                            message_json = json.dumps(safe_row, ensure_ascii=False)
                            violation = SQLInjectionResult(
                                file_path=file_path_str,
                                row_index=int(row_index),
                                column_name=column,
                                column_value=str(column_value),
                                matched_pattern=pattern,
                                message_json=message_json,
                            )
                            violations.append(violation)
                    except Exception as e:
                        # Log the error but continue with other patterns
                        print(
                            f"Warning: Error checking pattern {i+1} in column '{column}' of file '{file_path_str}': {e}"
                        )
                        continue

    finally:
        conn.close()

    end_time = time.time()
    scan_duration_ms = (end_time - start_time) * 1000

    return SQLInjectionReport(
        total_files_scanned=len(parquet_files),
        total_rows_scanned=total_rows_scanned,
        violations_found=len(violations),
        violations=violations,
        scan_duration_ms=scan_duration_ms,
    )


def _create_violations_dataframe(violations: list[SQLInjectionResult]) -> pl.DataFrame:
    """Create a Polars DataFrame from SQL injection violations.

    Args:
        violations: List of SQL injection violations

    Returns:
        Polars DataFrame with violation data
    """
    if not violations:
        return pl.DataFrame(
            {
                "file_path": [],
                "row_index": [],
                "column_name": [],
                "column_value": [],
                "matched_pattern": [],
                "message_json": [],
            }
        )

    # Convert violations to dictionary format for DataFrame creation
    violation_data = []
    for violation in violations:
        violation_data.append(
            {
                "file_path": violation.file_path,
                "row_index": violation.row_index,
                "column_name": violation.column_name,
                "column_value": violation.column_value,
                "matched_pattern": violation.matched_pattern,
                "message_json": violation.message_json or "",
            }
        )

    return pl.DataFrame(violation_data)


def _save_injection_report_to_parquet(
    report: SQLInjectionReport, output_dir: str
) -> str:
    """Save SQL injection report to parquet file.

    Args:
        report: SQL injection report to save
        output_dir: Directory to save the parquet file

    Returns:
        Path to the saved parquet file
    """
    from datetime import datetime


    # Create output directory if it doesn't exist
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    parquet_path = output_path / "sql_injection_report.parquet"

    # Create violations DataFrame
    violations_df = _create_violations_dataframe(report.violations)

    try:
        if parquet_path.exists():
            parquet_path.unlink()

        violations_df.write_parquet(str(parquet_path), compression="zstd")
        print(
            f"📁 SQL injection report saved: {parquet_path} ({len(violations_df)} violations)"
        )

    except Exception as e:
        print(f"⚠️  Warning: Could not save parquet file: {e}")
        # Fallback: create a simple text file with the report
        text_file = parquet_path.with_suffix(".txt")
        with open(text_file, "w") as f:
            f.write(f"SQL Injection Report - {timestamp}\n")
            f.write(f"Files scanned: {report.total_files_scanned}\n")
            f.write(f"Rows scanned: {report.total_rows_scanned:,}\n")
            f.write(f"Violations found: {report.violations_found}\n")
            f.write(f"Scan duration: {report.scan_duration_ms:.2f} ms\n\n")
        print(f"📁 SQL injection report saved as text: {text_file}")
        return str(text_file)

    return str(parquet_path)


def print_injection_report(report: SQLInjectionReport) -> None:
    """Print a formatted SQL injection detection report.

    Args:
        report: SQLInjectionReport to print
    """
    print("=" * 80)
    print("SQL INJECTION DETECTION REPORT")
    print("=" * 80)
    print(f"Files scanned: {report.total_files_scanned}")
    print(f"Rows scanned: {report.total_rows_scanned:,}")
    print(f"Violations found: {report.violations_found}")
    print(f"Scan duration: {report.scan_duration_ms:.2f} ms")
    print()

    if report.violations:
        print("VIOLATIONS DETECTED:")
        print("-" * 80)
    else:
        print("✅ No SQL injection patterns detected!")


def main():
    try:
        parser = argparse.ArgumentParser(description="Run SQL injection detection")
        parser.add_argument(
            "--columns",
            nargs="*",
            default=["vin", "manufacturer", "model"],
            help="Columns to scan for SQL injection patterns",
        )
        parser.add_argument(
            "--patterns",
            nargs="*",
            default=[
                r"('(''|[^'])*')|(;)|(\b(ALTER|CREATE|DELETE|DROP|EXEC(UTE){0,1}|INSERT( +INTO){0,1}|MERGE|SELECT|UPDATE|UNION( +ALL){0,1})\b)"
            ],
            help="Regex patterns to detect SQL injection",
        )
        parser.add_argument(
            "--data-path",
            type=str,
            default=BRONZE_PATH,
            help="Path to directory with parquet files to scan",
        )
        parser.add_argument(
            "--output-dir",
            type=str,
            default=str(
                Path(__file__).parent.parent.parent.parent
                / "data"
                / "sql_injection_report"
            ),
            help="Directory to write parquet report to",
        )
        args = parser.parse_args()

        # Resolve absolute paths
        current_dir = Path(__file__).parent.parent.parent.parent
        bronze_path = (
            current_dir / args.data_path
            if not Path(args.data_path).is_absolute()
            else Path(args.data_path)
        )
        output_dir = Path(args.output_dir)

        print("🔍 Running SQL injection detection...")
        print(f"📁 Scanning directory: {bronze_path}")
        report = sql_injection_report(args.columns, args.patterns, str(bronze_path))
        print_injection_report(report)

        # Save report to parquet file
        parquet_path = _save_injection_report_to_parquet(report, str(output_dir))
        if report.violations_found > 0:
            print(
                f"⚠️  Found {report.violations_found} potential SQL injection patterns!"
            )
            print(f"📊 Report saved to: {parquet_path}")
            sys.exit(1)
        else:
            print("✅ No SQL injection patterns detected!")
            print(f"📊 Report saved to: {parquet_path}")
            sys.exit(0)

    except Exception as e:
        print(f"❌ SQL injection detection failed: {e!s}")
        sys.exit(1)


if __name__ == "__main__":
    main()
