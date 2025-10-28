"""Unit tests for SQL injection detection functionality."""

import tempfile
from pathlib import Path
from typing import List

import pandas as pd
import pytest
from pydantic import ValidationError

from src.upstream_home_test.utils.sql_injection_detector import (
    SQLInjectionResult,
    SQLInjectionReport,
    sql_injection_report,
    print_injection_report,
)

# Common SQL injection patterns for testing
COMMON_SQL_INJECTION_PATTERNS = {
    "quotes_and_semicolons": r"('(''|[^'])*')|(;)",
    "sql_keywords": r"\b(ALTER|CREATE|DELETE|DROP|EXEC(UTE){0,1}|INSERT( +INTO){0,1}|MERGE|SELECT|UPDATE|UNION( +ALL){0,1})\b",
    "comment_patterns": r"(/\*.*?\*/)|(--.*$)|(;.*$)",
    "union_attacks": r"UNION\s+(ALL\s+)?SELECT",
    "or_attacks": r"\bOR\b.*=.*\bOR\b",
    "and_attacks": r"\bAND\b.*=.*\bAND\b",
    "script_injection": r"<script.*?>.*?</script>",
    "javascript_injection": r"javascript:",
    "vbscript_injection": r"vbscript:",
    "data_injection": r"data:",
}


class TestSQLInjectionResult:
    """Test SQLInjectionResult model."""
    
    def test_valid_result(self) -> None:
        """Test creating a valid SQLInjectionResult."""
        result = SQLInjectionResult(
            file_path="test.parquet",
            row_index=0,
            column_name="vin",
            column_value="test'; DROP TABLE users; --",
            matched_pattern="test_pattern"
        )
        
        assert result.file_path == "test.parquet"
        assert result.row_index == 0
        assert result.column_name == "vin"
        assert result.column_value == "test'; DROP TABLE users; --"
        assert result.matched_pattern == "test_pattern"
    
    def test_invalid_result_missing_fields(self) -> None:
        """Test that missing required fields raise ValidationError."""
        with pytest.raises(ValidationError):
            SQLInjectionResult(
                file_path="test.parquet",
                # Missing required fields
            )


class TestSQLInjectionReport:
    """Test SQLInjectionReport model."""
    
    def test_valid_report(self) -> None:
        """Test creating a valid SQLInjectionReport."""
        violations = [
            SQLInjectionResult(
                file_path="test1.parquet",
                row_index=0,
                column_name="vin",
                column_value="test'; DROP TABLE users; --",
                matched_pattern="test_pattern"
            )
        ]
        
        report = SQLInjectionReport(
            total_files_scanned=1,
            total_rows_scanned=100,
            violations_found=1,
            violations=violations,
            scan_duration_ms=50.0
        )
        
        assert report.total_files_scanned == 1
        assert report.total_rows_scanned == 100
        assert report.violations_found == 1
        assert len(report.violations) == 1
        assert report.scan_duration_ms == 50.0
    
    def test_empty_report(self) -> None:
        """Test creating an empty report."""
        report = SQLInjectionReport(
            total_files_scanned=0,
            total_rows_scanned=0,
            violations_found=0,
            violations=[],
            scan_duration_ms=0.0
        )
        
        assert report.total_files_scanned == 0
        assert report.total_rows_scanned == 0
        assert report.violations_found == 0
        assert len(report.violations) == 0


class TestSQLInjectionDetection:
    """Test SQL injection detection functionality."""
    
    @pytest.fixture
    def temp_data_dir(self) -> Path:
        """Create a temporary data directory with test data."""
        temp_dir = Path(tempfile.mkdtemp())
        data_dir = temp_dir / "data"
        data_dir.mkdir(parents=True)
        return data_dir
    
    def create_test_parquet(
        self, 
        data_dir: Path, 
        filename: str, 
        data: List[dict]
    ) -> Path:
        """Create a test parquet file with given data."""
        df = pd.DataFrame(data)
        file_path = data_dir / filename
        df.to_parquet(file_path, index=False)
        return file_path
    
    def test_detect_sql_injection_patterns(self, temp_data_dir: Path) -> None:
        """Test detection of various SQL injection patterns."""
        # Test data with various SQL injection patterns
        test_data = [
            # Normal data
            {"vin": "1G4AP6949BX114240", "manufacturer": "Buick", "model": "LeSabre"},
            # SQL injection in VIN
            {"vin": "1G4AP6949BX114241'; DROP TABLE users; --", "manufacturer": "Honda", "model": "Civic"},
            # SQL injection in manufacturer
            {"vin": "1G4AP6949BX114242", "manufacturer": "'; SELECT * FROM passwords; --", "model": "Accord"},
            # SQL injection in model
            {"vin": "1G4AP6949BX114243", "manufacturer": "Toyota", "model": "Camry OR 1=1"},
            # Multiple patterns
            {"vin": "1G4AP6949BX114244", "manufacturer": "Ford; DROP TABLE cars; --", "model": "F-150"},
            # Comment patterns
            {"vin": "1G4AP6949BX114245", "manufacturer": "Chevrolet", "model": "Silverado /* comment */"},
        ]
        
        self.create_test_parquet(temp_data_dir, "test_data.parquet", test_data)
        
        # Test with the provided pattern from the user
        patterns = [r"('(''|[^'])*')|(;)|(\b(ALTER|CREATE|DELETE|DROP|EXEC(UTE){0,1}|INSERT( +INTO){0,1}|MERGE|SELECT|UPDATE|UNION( +ALL){0,1})\b)"]
        
        result = sql_injection_report(
            columns_to_check=["vin", "manufacturer", "model"],
            injection_patterns=patterns,
            data_path=str(temp_data_dir)
        )
        
        # Should detect multiple violations
        assert result.total_files_scanned == 1
        assert result.total_rows_scanned == 6
        assert result.violations_found >= 3  # At least 3 violations should be detected
        assert result.scan_duration_ms > 0
        
        # Check that violations contain expected patterns
        violation_values = [v.column_value for v in result.violations]
        assert any("DROP TABLE" in value for value in violation_values)
        assert any("SELECT * FROM" in value for value in violation_values)
        assert any(";" in value for value in violation_values)  # Semicolon pattern
    
    def test_detect_quotes_and_semicolons(self, temp_data_dir: Path) -> None:
        """Test detection of quote and semicolon patterns."""
        test_data = [
            {"vin": "1G4AP6949BX114240", "manufacturer": "Buick"},
            {"vin": "1G4AP6949BX114241'; --", "manufacturer": "Honda"},
            {"vin": "1G4AP6949BX114242", "manufacturer": "'; DROP TABLE users; --"},
        ]
        
        self.create_test_parquet(temp_data_dir, "quotes_test.parquet", test_data)
        
        patterns = [r"('(''|[^'])*')|(;)"]
        result = sql_injection_report(
            columns_to_check=["vin", "manufacturer"],
            injection_patterns=patterns,
            data_path=str(temp_data_dir)
        )
        
        assert result.violations_found >= 2
        violation_values = [v.column_value for v in result.violations]
        assert any("';" in value for value in violation_values)
        assert any(";" in value for value in violation_values)
    
    def test_detect_sql_keywords(self, temp_data_dir: Path) -> None:
        """Test detection of SQL keywords."""
        test_data = [
            {"vin": "1G4AP6949BX114240", "manufacturer": "Buick"},
            {"vin": "1G4AP6949BX114241", "manufacturer": "Honda", "model": "SELECT * FROM cars"},
            {"vin": "1G4AP6949BX114242", "manufacturer": "Toyota", "model": "UPDATE users SET password='hacked'"},
            {"vin": "1G4AP6949BX114243", "manufacturer": "Ford", "model": "DELETE FROM logs"},
        ]
        
        self.create_test_parquet(temp_data_dir, "keywords_test.parquet", test_data)
        
        patterns = [r"\b(ALTER|CREATE|DELETE|DROP|EXEC(UTE){0,1}|INSERT( +INTO){0,1}|MERGE|SELECT|UPDATE|UNION( +ALL){0,1})\b"]
        result = sql_injection_report(
            columns_to_check=["vin", "manufacturer", "model"],
            injection_patterns=patterns,
            data_path=str(temp_data_dir)
        )
        
        assert result.violations_found >= 3
        violation_values = [v.column_value for v in result.violations]
        assert any("SELECT" in value for value in violation_values)
        assert any("UPDATE" in value for value in violation_values)
        assert any("DELETE" in value for value in violation_values)
    
    def test_no_violations_clean_data(self, temp_data_dir: Path) -> None:
        """Test that clean data produces no violations."""
        clean_data = [
            {"vin": "1G4AP6949BX114240", "manufacturer": "Buick", "model": "LeSabre"},
            {"vin": "1G4AP6949BX114241", "manufacturer": "Honda", "model": "Civic"},
            {"vin": "1G4AP6949BX114242", "manufacturer": "Toyota", "model": "Camry"},
        ]
        
        self.create_test_parquet(temp_data_dir, "clean_data.parquet", clean_data)
        
        patterns = [r"('(''|[^'])*')|(;)|(\b(ALTER|CREATE|DELETE|DROP|EXEC(UTE){0,1}|INSERT( +INTO){0,1}|MERGE|SELECT|UPDATE|UNION( +ALL){0,1})\b)"]
        result = sql_injection_report(
            columns_to_check=["vin", "manufacturer", "model"],
            injection_patterns=patterns,
            data_path=str(temp_data_dir)
        )
        
        assert result.total_files_scanned == 1
        assert result.total_rows_scanned == 3
        assert result.violations_found == 0
        assert len(result.violations) == 0
    
    def test_multiple_files(self, temp_data_dir: Path) -> None:
        """Test detection across multiple parquet files."""
        # Create multiple files with different data
        file1_data = [
            {"vin": "1G4AP6949BX114240", "manufacturer": "Buick"},
            {"vin": "1G4AP6949BX114241'; DROP TABLE users; --", "manufacturer": "Honda"},
        ]
        
        file2_data = [
            {"vin": "1G4AP6949BX114242", "manufacturer": "Toyota"},
            {"vin": "1G4AP6949BX114243", "manufacturer": "'; SELECT * FROM passwords; --"},
        ]
        
        self.create_test_parquet(temp_data_dir, "file1.parquet", file1_data)
        self.create_test_parquet(temp_data_dir, "file2.parquet", file2_data)
        
        patterns = [r"('(''|[^'])*')|(;)|(\b(ALTER|CREATE|DELETE|DROP|EXEC(UTE){0,1}|INSERT( +INTO){0,1}|MERGE|SELECT|UPDATE|UNION( +ALL){0,1})\b)"]
        result = sql_injection_report(
            columns_to_check=["vin", "manufacturer"],
            injection_patterns=patterns,
            data_path=str(temp_data_dir)
        )
        
        assert result.total_files_scanned == 2
        assert result.total_rows_scanned == 4
        assert result.violations_found >= 2
        
        # Check that violations come from both files
        file_paths = set(v.file_path for v in result.violations)
        assert len(file_paths) == 2
    
    def test_empty_directory(self, temp_data_dir: Path) -> None:
        """Test behavior with empty directory."""
        result = sql_injection_report(
            columns_to_check=["vin"],
            injection_patterns=[r"('(''|[^'])*')|(;)"],
            data_path=str(temp_data_dir)
        )
        
        assert result.total_files_scanned == 0
        assert result.total_rows_scanned == 0
        assert result.violations_found == 0
        assert len(result.violations) == 0
    
    def test_nonexistent_directory(self) -> None:
        """Test behavior with nonexistent directory."""
        with pytest.raises(FileNotFoundError):
            sql_injection_report(
                columns_to_check=["vin"],
                injection_patterns=[r"('(''|[^'])*')|(;)"],
                data_path="nonexistent_directory"
            )
    
    def test_empty_columns_list(self, temp_data_dir: Path) -> None:
        """Test behavior with empty columns list."""
        with pytest.raises(ValueError, match="columns_to_check cannot be empty"):
            sql_injection_report(
                columns_to_check=[],
                injection_patterns=[r"('(''|[^'])*')|(;)"],
                data_path=str(temp_data_dir)
            )
    
    def test_empty_patterns_list(self, temp_data_dir: Path) -> None:
        """Test behavior with empty patterns list."""
        with pytest.raises(ValueError, match="injection_patterns cannot be empty"):
            sql_injection_report(
                columns_to_check=["vin"],
                injection_patterns=[],
                data_path=str(temp_data_dir)
            )
    
    def test_multiple_patterns_auto_naming(self, temp_data_dir: Path) -> None:
        """Test behavior with multiple patterns and automatic naming."""
        test_data = [
            {"vin": "1G4AP6949BX114240", "manufacturer": "Buick"},
            {"vin": "1G4AP6949BX114241'; DROP TABLE users; --", "manufacturer": "Honda"},
            {"vin": "1G4AP6949BX114242", "manufacturer": "'; SELECT * FROM passwords; --"},
        ]
        self.create_test_parquet(temp_data_dir, "test.parquet", test_data)
        
        # Test with multiple patterns - should auto-generate names
        result = sql_injection_report(
            columns_to_check=["vin", "manufacturer"],
            injection_patterns=[r"('(''|[^'])*')|(;)", r"\bSELECT\b"],
            data_path=str(temp_data_dir)
        )
        
        # Check that violations are detected correctly
        assert result.total_files_scanned == 1
        assert result.total_rows_scanned == 3
        assert result.violations_found >= 2
    
    def test_invalid_regex_pattern(self, temp_data_dir: Path) -> None:
        """Test behavior with invalid regex pattern."""
        test_data = [{"vin": "1G4AP6949BX114240", "manufacturer": "Buick"}]
        self.create_test_parquet(temp_data_dir, "test.parquet", test_data)
        
        # This should not raise an exception, but should log warnings
        result = sql_injection_report(
            columns_to_check=["vin"],
            injection_patterns=["[invalid_regex_pattern"],  # Invalid regex
            data_path=str(temp_data_dir)
        )
        
        # Should still return a valid report, just with warnings
        assert isinstance(result, SQLInjectionReport)
        assert result.total_files_scanned == 1


class TestCommonPatterns:
    """Test common SQL injection patterns functionality."""
    
    @pytest.fixture
    def temp_data_dir(self) -> Path:
        """Create a temporary data directory with test data."""
        temp_dir = Path(tempfile.mkdtemp())
        data_dir = temp_dir / "data"
        data_dir.mkdir(parents=True)
        return data_dir
    
    def create_test_parquet(
        self, 
        data_dir: Path, 
        filename: str, 
        data: List[dict]
    ) -> Path:
        """Create a test parquet file with given data."""
        df = pd.DataFrame(data)
        file_path = data_dir / filename
        df.to_parquet(file_path, index=False)
        return file_path

    def test_common_patterns_available(self) -> None:
        """Test that common patterns are available."""
        assert len(COMMON_SQL_INJECTION_PATTERNS) > 0
        assert "quotes_and_semicolons" in COMMON_SQL_INJECTION_PATTERNS
        assert "sql_keywords" in COMMON_SQL_INJECTION_PATTERNS
    
    def test_common_patterns_detection(self, temp_data_dir: Path) -> None:
        """Test that common patterns actually detect SQL injection attempts."""
        test_data = [
            {"vin": "1G4AP6949BX114240", "manufacturer": "Buick"},
            {"vin": "1G4AP6949BX114241'; DROP TABLE users; --", "manufacturer": "Honda"},
            {"vin": "1G4AP6949BX114242", "manufacturer": "'; SELECT * FROM passwords; --"},
            {"vin": "1G4AP6949BX114243", "manufacturer": "Toyota", "model": "UNION SELECT * FROM users"},
        ]
        
        self.create_test_parquet(temp_data_dir, "common_patterns_test.parquet", test_data)
        
        # Use patterns directly from COMMON_SQL_INJECTION_PATTERNS
        patterns = [
            COMMON_SQL_INJECTION_PATTERNS["quotes_and_semicolons"],
            COMMON_SQL_INJECTION_PATTERNS["sql_keywords"]
        ]
        result = sql_injection_report(
            columns_to_check=["vin", "manufacturer", "model"],
            injection_patterns=patterns,
            data_path=str(temp_data_dir)
        )
        
        assert result.violations_found >= 2
        violation_values = [v.column_value for v in result.violations]
        assert any("';" in value for value in violation_values)
        assert any("SELECT" in value for value in violation_values)


class TestPrintInjectionReport:
    """Test the print_injection_report function."""
    
    def test_print_empty_report(self, capsys) -> None:
        """Test printing an empty report."""
        report = SQLInjectionReport(
            total_files_scanned=0,
            total_rows_scanned=0,
            violations_found=0,
            violations=[],
            scan_duration_ms=0.0
        )
        
        print_injection_report(report)
        captured = capsys.readouterr()
        
        assert "SQL INJECTION DETECTION REPORT" in captured.out
        assert "Files scanned: 0" in captured.out
        assert "Rows scanned: 0" in captured.out
        assert "Violations found: 0" in captured.out
        assert "✅ No SQL injection patterns detected!" in captured.out
    
    def test_print_report_with_violations(self, capsys) -> None:
        """Test printing a report with violations."""
        violations = [
            SQLInjectionResult(
                file_path="test.parquet",
                row_index=0,
                column_name="vin",
                column_value="test'; DROP TABLE users; --",
                matched_pattern="test_pattern"
            )
        ]
        
        report = SQLInjectionReport(
            total_files_scanned=1,
            total_rows_scanned=100,
            violations_found=1,
            violations=violations,
            scan_duration_ms=50.0
        )
        
        print_injection_report(report)
        captured = capsys.readouterr()
        
        assert "SQL INJECTION DETECTION REPORT" in captured.out
        assert "Files scanned: 1" in captured.out
        assert "Rows scanned: 100" in captured.out
        assert "Violations found: 1" in captured.out
        assert "VIOLATIONS DETECTED:" in captured.out
        assert "test.parquet" in captured.out
        assert "vin" in captured.out
        assert "test'; DROP TABLE users; --" in captured.out
        assert "test_pattern" in captured.out
