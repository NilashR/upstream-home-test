import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from upstream_home_test.pipelines.reports.sql_report_runner import SQLReportRunner
from upstream_home_test.io.parquet_writer import GenericParquetWriter
from upstream_home_test.constant import SILVER_PATH, GOLD_PATH


def _resolve_silver_directory(silver_dir: str = None) -> str:
    """Resolve absolute path for silver directory.
    
    Args:
        silver_dir: Silver directory path (None for default)
        
    Returns:
        Absolute path to silver directory
    """
    if silver_dir is None:
        from upstream_home_test.utils.logging_config import get_project_root
        project_root = get_project_root()
        return str(project_root / SILVER_PATH)
    
    return silver_dir


def _get_report_names_to_run(report_names: List[str] = None) -> List[str]:
    """Get list of report names to run.
    
    Args:
        report_names: List of specific report names (None for all)
        
    Returns:
        List of report names to run
    """
    if report_names is None:
        runner = SQLReportRunner()
        return runner.list_available_reports()
    
    return report_names


def _run_sql_reports(report_names: List[str], silver_dir: str, **kwargs) -> Dict[str, Any]:
    """Run SQL reports using the report runner.
    
    Args:
        report_names: List of report names to run
        silver_dir: Directory containing Silver layer Parquet files
        **kwargs: Additional report-specific parameters
        
    Returns:
        Dictionary with report results and statistics
    """
    runner = SQLReportRunner()
    return runner.run_multiple_reports(report_names, silver_dir, **kwargs)


def _perform_gold_side_effects(result: Dict[str, Any]) -> None:
    """Perform side effects for Gold layer (cleanup and write parquet).
    
    Args:
        result: Report results dictionary
    """
    cleanup_old_parquet_files()
    write_reports_to_parquet(result)


def run_gold_reports(
    report_names: List[str] = None,
    silver_dir: str = None,
    **kwargs
) -> Dict[str, Any]:
    """Run Gold layer reports using SQL files and DuckDB.
    
    This function provides a unified interface to run one or more SQL-based reports
    from the available Gold layer report collection.
    
    Args:
        report_names: List of report names to run. If None, runs all reports.
        silver_dir: Directory containing Silver layer Parquet files
        **kwargs: Additional report-specific parameters (unused for SQL reports)
        
    Returns:
        Dictionary with report results and statistics
        
    Raises:
        ValueError: If any report name is invalid
        FileNotFoundError: If no Silver data found
    """
    # Resolve silver directory path
    silver_dir = _resolve_silver_directory(silver_dir)
    
    # Get report names to run
    report_names = _get_report_names_to_run(report_names)
    
    # Run SQL reports
    result = _run_sql_reports(report_names, silver_dir, **kwargs)

    # Perform side-effects like Silver: cleanup and write parquet inside the run function
    _perform_gold_side_effects(result)

    return result


def cleanup_old_parquet_files(gold_dir: str = None) -> None:
    """Delete existing parquet files in the gold directory.
    
    Args:
        gold_dir: Directory containing parquet files to clean up
    """
    if gold_dir is None:
        from upstream_home_test.utils.logging_config import get_project_root
        project_root = get_project_root()
        gold_dir = str(project_root / GOLD_PATH)
    
    gold_path = Path(gold_dir)
    if gold_path.exists():
        parquet_files = list(gold_path.glob("*.parquet"))
        if parquet_files:
            for parquet_file in parquet_files:
                parquet_file.unlink()
        else:
            print("🧹 No existing parquet files to clean up")
    else:
        print("🧹 Gold directory doesn't exist yet, no cleanup needed")


def write_reports_to_parquet(result: Dict[str, Any], gold_dir: str = None) -> None:
    """Write report results to parquet files in the gold directory using GenericParquetWriter.
    
    Args:
        result: Dictionary containing report results from run_gold_reports
        gold_dir: Directory to write parquet files to
    """
    if gold_dir is None:
        from upstream_home_test.utils.logging_config import get_project_root
        project_root = get_project_root()
        gold_dir = str(project_root / GOLD_PATH)
    
    # Generate timestamp for file naming
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    for report_name, report_result in result['results'].items():
        if report_result['status'] == 'completed' and 'result_data' in report_result:
            # Create parquet filename with timestamp
            parquet_filename = f"{report_name}_{timestamp}.parquet"
            parquet_path = Path(gold_dir) / parquet_filename
            
            # Use GenericParquetWriter with partitioning disabled for gold layer
            writer = GenericParquetWriter(
                output_dir=str(parquet_path.parent),
                partitioning_enabled=False,
                max_file_size_mb=1000.0,  # Large files OK for gold layer
                compression="zstd"
            )
            
            # Write the DataFrame
            write_result = writer.write(report_result['result_data'])
            
            # Rename the output file to the desired name
            default_file = parquet_path.parent / "data.parquet"
            if default_file.exists():
                default_file.rename(parquet_path)
            
            print(f"   📁 Parquet saved: {parquet_path} ({write_result['rows']} rows)")
        else:
            print(f"   ⚠️  No parquet written for {report_name} (status: {report_result['status']})")


def main():
    try:
        silver_dir = SILVER_PATH
        report_names = ['fastest_vehicles_per_hour', 'vin_last_state']  # Default reports
        
        run_gold_reports(report_names, silver_dir)
        
    except Exception as e:
        print(f"Gold reports generation failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
