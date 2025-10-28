"""Tests for Gold layer reports pipeline."""

import builtins
from unittest.mock import patch, MagicMock

import pytest

from upstream_home_test.pipelines.gold_reports import run_gold_reports, main


class TestGoldReports:
    """Test Gold reports orchestration."""

    @patch("upstream_home_test.pipelines.gold_reports.SQLReportRunner")
    @patch("upstream_home_test.pipelines.gold_reports.write_reports_to_parquet")
    @patch("upstream_home_test.pipelines.gold_reports.cleanup_old_parquet_files")
    def test_run_gold_reports_runs_all_when_none(self, mock_cleanup, mock_write, mock_runner_cls):
        runner = MagicMock()
        runner.list_available_reports.return_value = ["a", "b"]
        runner.run_multiple_reports.return_value = {"results": {}}
        mock_runner_cls.return_value = runner

        result = run_gold_reports(report_names=None, silver_dir="data/silver")

        runner.list_available_reports.assert_called_once()
        runner.run_multiple_reports.assert_called_once_with(["a", "b"], "data/silver")
        mock_cleanup.assert_called_once()
        mock_write.assert_called_once_with(result)

    @patch("upstream_home_test.pipelines.gold_reports.SQLReportRunner")
    @patch("upstream_home_test.pipelines.gold_reports.write_reports_to_parquet")
    @patch("upstream_home_test.pipelines.gold_reports.cleanup_old_parquet_files")
    def test_run_gold_reports_with_specific_reports(self, mock_cleanup, mock_write, mock_runner_cls):
        runner = MagicMock()
        runner.run_multiple_reports.return_value = {"results": {}}
        mock_runner_cls.return_value = runner

        reports = ["fastest_vehicles_per_hour"]
        result = run_gold_reports(report_names=reports, silver_dir="data/silver")

        runner.run_multiple_reports.assert_called_once_with(reports, "data/silver")
        mock_cleanup.assert_called_once()
        mock_write.assert_called_once_with(result)

    @patch("upstream_home_test.pipelines.gold_reports.run_gold_reports")
    def test_main_success(self, mock_run):
        mock_run.return_value = {"status": "completed"}
        # Ensure main does not raise
        main()
        mock_run.assert_called_once()

    @patch("upstream_home_test.pipelines.gold_reports.run_gold_reports", side_effect=RuntimeError("boom"))
    def test_main_failure_exits(self, mock_run):
        with pytest.raises(SystemExit) as exc:
            main()
        assert exc.value.code == 1
        mock_run.assert_called_once()


