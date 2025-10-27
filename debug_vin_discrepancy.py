#!/usr/bin/env python3
"""
Diagnostic script to investigate VIN row count discrepancy.
Run this with your actual data to find the root cause.
"""

import pandas as pd
import polars as pl
import duckdb
from pathlib import Path

def diagnose_vin_discrepancy(df_pandas, target_vin='1C4NJDBB0GD610265'):
    """Diagnose why a VIN has different row counts between input and result DataFrames."""
    
    print('=' * 80)
    print('VIN ROW COUNT DISCREPANCY DIAGNOSTIC')
    print('=' * 80)
    print(f'Target VIN: {target_vin}')
    print()
    
    # 1. Check original DataFrame
    print('1. ORIGINAL DATAFRAME ANALYSIS:')
    print(f'   Total rows: {len(df_pandas)}')
    print(f'   Total columns: {len(df_pandas.columns)}')
    print(f'   Columns: {list(df_pandas.columns)}')
    
    original_filtered = df_pandas[df_pandas['vin'] == target_vin]
    print(f'   Rows with VIN {target_vin}: {len(original_filtered)}')
    
    if len(original_filtered) > 0:
        print('   Sample original data:')
        print(original_filtered.head())
        print(f'   Original dtypes:')
        for col, dtype in original_filtered.dtypes.items():
            print(f'     {col}: {dtype}')
    print()
    
    # 2. Check for VIN variations (hidden characters, case differences)
    print('2. VIN VARIATION ANALYSIS:')
    all_vins = df_pandas['vin'].unique()
    print(f'   Total unique VINs: {len(all_vins)}')
    
    # Find VINs that contain the target VIN
    similar_vins = [vin for vin in all_vins if target_vin in str(vin)]
    print(f'   VINs containing target: {len(similar_vins)}')
    
    if len(similar_vins) > 1:
        print('   WARNING: Multiple VINs found containing target string!')
        for i, vin in enumerate(similar_vins[:10]):  # Show first 10
            print(f'     {i+1}. {repr(vin)} (length: {len(vin)})')
    print()
    
    # 3. Test DuckDB registration and queries
    print('3. DUCKDB PROCESSING TEST:')
    conn = duckdb.connect()
    
    try:
        # Register DataFrame
        conn.register('vin_d', df_pandas)
        print('   ✓ DataFrame registered successfully')
        
        # Test 1: Simple WHERE query
        where_result = conn.execute(f"SELECT * FROM vin_d WHERE vin = '{target_vin}'").df()
        print(f'   WHERE query result rows: {len(where_result)}')
        
        # Test 2: LIKE query (in case of hidden characters)
        like_result = conn.execute(f"SELECT * FROM vin_d WHERE vin LIKE '%{target_vin}%'").df()
        print(f'   LIKE query result rows: {len(like_result)}')
        
        # Test 3: Full query from your SQL file
        query_file = Path(__file__).parent / "src/upstream_home_test/pipelines/reports/query.sql"
        if query_file.exists():
            with open(query_file, 'r') as f:
                sql_query = f.read().strip()
            print(f'   SQL query from file: {sql_query[:100]}...')
            
            full_result = conn.execute(sql_query).df()
            print(f'   Full SQL query result rows: {len(full_result)}')
            
            # Filter the result for the target VIN
            filtered_result = full_result[full_result['vin'] == target_vin]
            print(f'   Filtered result rows for VIN {target_vin}: {len(filtered_result)}')
            
            if len(filtered_result) > 0:
                print('   Sample result data:')
                print(filtered_result.head())
                print(f'   Result dtypes:')
                for col, dtype in filtered_result.dtypes.items():
                    print(f'     {col}: {dtype}')
        else:
            print('   ⚠️  SQL query file not found')
        
        # Test 4: Check for data duplication
        all_duckdb_data = conn.execute('SELECT * FROM vin_d').df()
        print(f'   Total rows in DuckDB: {len(all_duckdb_data)}')
        print(f'   Original total rows: {len(df_pandas)}')
        
        if len(all_duckdb_data) != len(df_pandas):
            print('   ⚠️  WARNING: Row count mismatch between original and DuckDB!')
        
    except Exception as e:
        print(f'   ❌ Error during DuckDB processing: {e}')
    finally:
        conn.close()
    print()
    
    # 4. Summary and recommendations
    print('4. SUMMARY:')
    if len(original_filtered) == 0:
        print('   ❌ Target VIN not found in original DataFrame')
    elif len(original_filtered) == 2 and len(filtered_result) == 33:
        print('   ❌ CONFIRMED: 2 rows → 33 rows (16.5x multiplication)')
        print('   🔍 This suggests data duplication during SQL processing')
        print('   💡 Possible causes:')
        print('      - SQL query is creating Cartesian products')
        print('      - JOIN operations are multiplying rows')
        print('      - Window functions are creating unexpected results')
        print('      - Data type issues causing unexpected behavior')
    else:
        print(f'   Original: {len(original_filtered)} rows, Result: {len(filtered_result)} rows')
    
    print()
    print('5. NEXT STEPS:')
    print('   - Check if the SQL query contains JOINs or subqueries that could multiply rows')
    print('   - Verify that all VIN values are exactly identical (no hidden characters)')
    print('   - Test with a simpler SQL query to isolate the issue')
    print('   - Check if there are any data type conversions affecting the results')

if __name__ == "__main__":
    print("To use this diagnostic script:")
    print("1. Load your DataFrame: df = your_dataframe")
    print("2. Run: diagnose_vin_discrepancy(df, '1C4NJDBB0GD610265')")
    print()
    print("Or run with your actual data pipeline:")
    print("python debug_vin_discrepancy.py")

