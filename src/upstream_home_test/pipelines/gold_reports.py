"""Gold layer reports pipeline for generating aggregated reports from Silver data using SQL files."""

import sys
from typing import Any, Dict, List

from upstream_home_test.pipelines.reports.sql_report_runner import SQLReportRunner


def run_gold_reports(
    report_names: List[str] = None,
    silver_dir: str = "data/silver",
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
    runner = SQLReportRunner()
    
    # If no specific reports requested, run all
    if report_names is None:
        report_names = runner.list_available_reports()
    
    # Always run multiple reports (simplified approach)
    return runner.run_multiple_reports(report_names, silver_dir, **kwargs)


def main():
    """CLI entry point for Gold reports generation."""
    try:
        # Parse command line arguments
        silver_dir = "data/silver"
        report_names = ['fastest_vehicles_per_hour', 'vin_last_state']  # Default reports
        
        if len(sys.argv) > 1:
            first_arg = sys.argv[1]
            
            # Handle special cases
            if first_arg == "list":
                runner = SQLReportRunner()
                runner.print_available_reports()
                return
            elif first_arg == "all":
                report_names = None  # Will run all reports
            else:
                # Parse report names
                report_names = [arg for arg in sys.argv[1:] if not arg.startswith("--")]
                
                # Parse additional arguments
                for i, arg in enumerate(sys.argv[1:], 1):
                    if arg == "--silver-dir" and i + 1 < len(sys.argv):
                        silver_dir = sys.argv[i + 1]
                    elif arg.startswith("--silver-dir="):
                        silver_dir = arg.split("=", 1)[1]
        
        # Run reports
        result = run_gold_reports(report_names, silver_dir)
        
        # Print results summary
        print(f"\nGold reports generation completed!")
        print(f"Total reports: {result['total_reports']}")
        print(f"Successful: {len(result['successful_reports'])}")
        print(f"Failed: {len(result['failed_reports'])}")
        print(f"Total duration: {result['total_duration_ms']:.2f}ms")
        
        # Print detailed results for each report
        for report_name, report_result in result['results'].items():
            if report_result['status'] == 'completed':
                print(f"\n📊 {report_name.upper().replace('_', ' ')}")
                print(f"   Input rows: {report_result['input_rows']}")
                print(f"   Output rows: {report_result['output_rows']}")
                print(f"   SQL file: {report_result['sql_file']}")
                
                # Print first few rows of result data
                if 'result_data' in report_result and not report_result['result_data'].empty:
                    print(f"   Sample data:")
                    print(report_result['result_data'].head().to_string(index=False))
            else:
                print(f"\n❌ {report_name.upper().replace('_', ' ')} - FAILED")
                print(f"   Error: {report_result['error']}")
        
        if result['failed_reports']:
            print(f"\nFailed reports: {', '.join(result['failed_reports'])}")
        
    except Exception as e:
        print(f"Gold reports generation failed: {str(e)}")
        print("\nUsage:")
        print("  python -m upstream_home_test.pipelines.gold_reports list")
        print("  python -m upstream_home_test.pipelines.gold_reports vin_last_state")
        print("  python -m upstream_home_test.pipelines.gold_reports vin_last_state data_quality")
        print("  python -m upstream_home_test.pipelines.gold_reports all")
        print("  python -m upstream_home_test.pipelines.gold_reports vin_last_state --silver-dir data/silver")
        sys.exit(1)


if __name__ == "__main__":
    main()
