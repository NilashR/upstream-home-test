"""Generic SQL report runner using DuckDB for Gold layer reports."""

import time
from pathlib import Path
from typing import Any, Dict, List

import polars as pl
import duckdb

from upstream_home_test.utils.logging_config import log_pipeline_step, setup_logging
from upstream_home_test.utils.timing import elapsed_ms_since
from upstream_home_test.constant import GOLD_LAYER


class SQLReportRunner:
    """Generic report runner that executes SQL files using DuckDB."""
    
    def __init__(self, queries_dir: str = None):
        """Initialize the SQL report runner.
        
        Args:
            queries_dir: Directory containing SQL query files. If None, uses the queries subdirectory.
        """
        self.logger = setup_logging(clear_log_file=False)
        self.queries_dir = Path(queries_dir) if queries_dir else Path(__file__).parent / "queries"
        self.available_reports = self._discover_sql_reports()
    
    def _discover_sql_reports(self) -> List[str]:
        """Discover available SQL report files.
        
        Returns:
            List of report names (SQL file names without .sql extension)
        """
        sql_files = list(self.queries_dir.glob("*.sql"))
        return [f.stem for f in sql_files]
    
    def list_available_reports(self) -> List[str]:
        """List all available SQL report files.
        
        Returns:
            List of available report names
        """
        return self.available_reports.copy()
    
    def _load_sql_query(self, report_name: str) -> tuple[str, str]:
        """Load SQL query from file.
        
        Args:
            report_name: Name of the report
            
        Returns:
            Tuple of (sql_file_path, sql_query_string)
            
        Raises:
            FileNotFoundError: If SQL file not found
        """
        sql_file = self.queries_dir / f"{report_name}.sql"
        if not sql_file.exists():
            raise FileNotFoundError(f"SQL file not found: {sql_file}")
        
        with open(sql_file, 'r') as f:
            sql_query = f.read().strip()
        
        return str(sql_file), sql_query

    def _load_silver_data(self, silver_dir: str, report_name: str) -> pl.DataFrame:
        """Load Silver layer data.
        
        Args:
            silver_dir: Directory containing Silver parquet files
            report_name: Name of the report for logging
            
        Returns:
            Polars DataFrame with Silver data
            
        Raises:
            FileNotFoundError: If no Silver data found
        """
        log_pipeline_step(
            logger=self.logger,
            step="sql_report_runner",
            event=f"Reading Silver data for {report_name} report",
            metrics={"silver_dir": silver_dir}
        )
        
        # Scan all Silver Parquet files
        df = pl.scan_parquet(silver_dir).collect()
        
        if df.is_empty():
            error_msg = f"No Silver data found in {silver_dir}"
            log_pipeline_step(
                logger=self.logger,
                step="sql_report_runner",
                event=error_msg,
                metrics={"silver_dir": silver_dir},
                level="ERROR"
            )
            raise FileNotFoundError(error_msg)
        
        return df

    def _execute_report_query(self, df: pl.DataFrame, sql_query: str, report_name: str, sql_file: str) -> pl.DataFrame:
        """Execute SQL query on the DataFrame.
        
        Args:
            df: Input DataFrame
            sql_query: SQL query to execute
            report_name: Name of the report
            sql_file: Path to SQL file for logging
            
        Returns:
            Result DataFrame
        """
        log_pipeline_step(
            logger=self.logger,
            step=GOLD_LAYER,
            event=f"Executing SQL query for {report_name} report",
            metrics={"sql_file": sql_file, "input_rows": len(df)}
        )
        
        result = self._execute_sql_with_duckdb(df, sql_query)
        
        # Convert timestamp columns back to proper datetime if they exist
        for col in result.columns:
            if 'timestamp' in col.lower() and result[col].dtype == pl.Utf8:
                result = result.with_columns(
                    pl.col(col).str.to_datetime().dt.replace_time_zone("UTC")
                )
        
        # Log completion
        log_pipeline_step(
            logger=self.logger,
            step=GOLD_LAYER,
            event=f"Completed {report_name} report execution",
            metrics={"output_rows": len(result), "sql_file": sql_file}
        )
        
        return result

    def run_sql_report(self, report_name: str, silver_dir: str, **kwargs) -> Dict[str, Any]:
        """Run a single SQL report.
        
        Args:
            report_name: Name of the SQL report to run (without .sql extension)
            silver_dir: Directory containing Silver layer Parquet files
            **kwargs: Additional parameters (unused for SQL reports)
            
        Returns:
            Dictionary with report results
            
        Raises:
            ValueError: If report name is not found
            FileNotFoundError: If SQL file or Silver data not found
        """
        # Load SQL query from file
        sql_file, sql_query = self._load_sql_query(report_name)
        
        # Load Silver data
        df = self._load_silver_data(silver_dir, report_name)
        
        # Execute SQL query
        result = self._execute_report_query(df, sql_query, report_name, sql_file)
        
        return {
            "status": "completed",
            "report_name": report_name,
            "sql_file": sql_file,
            "input_rows": len(df),
            "output_rows": len(result),
            "result_data": result
        }
    
    def run_multiple_reports(self, report_names: List[str], silver_dir: str, **kwargs) -> Dict[str, Any]:
        """Run multiple SQL reports.
        
        Args:
            report_names: List of report names to run
            silver_dir: Directory containing Silver layer Parquet files
            **kwargs: Additional parameters (unused for SQL reports)
            
        Returns:
            Dictionary with results from all reports
            
        Raises:
            ValueError: If any report name is not found
        """
        # Validate all report names
        invalid_reports = [name for name in report_names if name not in self.available_reports]
        if invalid_reports:
            available = ", ".join(self.available_reports)
            raise ValueError(f"Unknown reports: {invalid_reports}. Available reports: {available}")
        
        runner_start = time.time()
        
        log_pipeline_step(
            logger=self.logger,
            step="sql_report_runner",
            event=f"Running multiple SQL reports: {', '.join(report_names)}",
            metrics={"report_names": report_names, "silver_dir": silver_dir}
        )
        
        results = {}
        successful_reports = []
        failed_reports = []
        
        for report_name in report_names:
            try:
                log_pipeline_step(
                    logger=self.logger,
                    step="sql_report_runner",
                    event=f"Starting SQL report: {report_name}",
                    metrics={"report_name": report_name}
                )
                
                result = self.run_sql_report(report_name, silver_dir, **kwargs)
                results[report_name] = result
                successful_reports.append(report_name)
                
                log_pipeline_step(
                    logger=self.logger,
                    step="sql_report_runner",
                    event=f"Completed SQL report: {report_name}",
                    metrics={"report_name": report_name, "status": "success"}
                )
                
            except Exception as e:
                error_msg = f"Failed to run SQL report '{report_name}': {str(e)}"
                log_pipeline_step(
                    logger=self.logger,
                    step="sql_report_runner",
                    event=error_msg,
                    metrics={"report_name": report_name, "error": str(e)},
                    level="ERROR"
                )
                
                results[report_name] = {
                    "status": "failed",
                    "error": str(e),
                    "report_name": report_name
                }
                failed_reports.append(report_name)
        
        # Calculate total duration
        total_duration_ms = elapsed_ms_since(runner_start)
        
        # Log completion
        log_pipeline_step(
            logger=self.logger,
            step="sql_report_runner",
            event="Multiple SQL reports execution completed",
            metrics={
                "total_reports": len(report_names),
                "successful_reports": len(successful_reports),
                "failed_reports": len(failed_reports),
                "total_duration_ms": round(total_duration_ms, 2)
            }
        )
        
        return {
            "status": "completed",
            "total_reports": len(report_names),
            "successful_reports": successful_reports,
            "failed_reports": failed_reports,
            "total_duration_ms": round(total_duration_ms, 2),
            "results": results
        }
    
    def run_all_reports(self, silver_dir: str, **kwargs) -> Dict[str, Any]:
        """Run all available SQL reports.
        
        Args:
            silver_dir: Directory containing Silver layer Parquet files
            **kwargs: Additional parameters (unused for SQL reports)
            
        Returns:
            Dictionary with results from all reports
        """
        all_reports = self.list_available_reports()
        return self.run_multiple_reports(all_reports, silver_dir, **kwargs)
    
    @staticmethod
    def _execute_sql_with_duckdb(df: pl.DataFrame, sql_query: str) -> pl.DataFrame:
        """Execute SQL query using DuckDB with polars DataFrame.
        
        Args:
            df: Input polars DataFrame to be used as 'report_query' table
            sql_query: SQL query string from .sql file
            
        Returns:
            Result DataFrame from SQL query execution
        """
        # Create DuckDB connection (in-memory by default)
        conn = duckdb.connect()
        
        try:
            # Register DataFrame as 'report_query' table in DuckDB
            conn.register('report_table', df)
            
            # Execute SQL query using DuckDB's native query execution
            result = conn.execute(sql_query).pl()
            
            return result
            
        finally:
            # Clean up
            conn.close()

