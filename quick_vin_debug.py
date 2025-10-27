#!/usr/bin/env python3
"""
Quick diagnostic for VIN row count issue.
This will help identify why you're getting 33 rows instead of 2.
"""

import pandas as pd
import duckdb

def quick_debug(df, target_vin='1C4NJDBB0GD610265'):
    """Quick debug for VIN row count discrepancy."""
    
    print(f"🔍 Debugging VIN: {target_vin}")
    print("=" * 50)
    
    # 1. Check original data
    original_rows = df[df['vin'] == target_vin]
    print(f"📊 Original DataFrame: {len(original_rows)} rows")
    
    if len(original_rows) > 0:
        print("   Sample data:")
        print(original_rows[['vin', 'timestamp', 'frontLeftDoorState']].head())
        print(f"   Unique timestamps: {original_rows['timestamp'].nunique()}")
        print(f"   Unique door states: {original_rows['frontLeftDoorState'].nunique()}")
    
    # 2. Test DuckDB processing
    conn = duckdb.connect()
    conn.register('vin_d', df)
    
    # Test simple query
    simple_result = conn.execute(f"SELECT * FROM vin_d WHERE vin = '{target_vin}'").df()
    print(f"\n🔧 Simple DuckDB query: {len(simple_result)} rows")
    
    # Test your actual query
    query = '''
    WITH last_reported AS
    (SELECT vin, max(timestamp) as max_timestamp
     from vin_d group by vin),
        ll as
        (
    SELECT vin, frontLeftDoorState, timestamp,row_num
    FROM (SELECT vin, frontLeftDoorState, timestamp, ROW_NUMBER() OVER(PARTITION BY vin ORDER BY timestamp DESC) AS row_num
        from vin_d where frontLeftDoorState is not null and vin = '1C4NJDBB0GD610265')
        )
    select * from  ll
    '''
    
    try:
        result = conn.execute(query).df()
        print(f"🎯 Your SQL query result: {len(result)} rows")
        
        if len(result) > len(original_rows):
            print(f"⚠️  MULTIPLICATION DETECTED: {len(original_rows)} → {len(result)} rows")
            print(f"   Multiplication factor: {len(result) / len(original_rows):.1f}x")
            
            # Check for patterns in the result
            print("\n🔍 Result analysis:")
            print(f"   Unique timestamps in result: {result['timestamp'].nunique()}")
            print(f"   Unique door states in result: {result['frontLeftDoorState'].nunique()}")
            print(f"   Row number range: {result['row_num'].min()} - {result['row_num'].max()}")
            
            # Show sample of result
            print("\n📋 Sample result data:")
            print(result.head(10))
            
        else:
            print("✅ Row counts match - no multiplication detected")
            
    except Exception as e:
        print(f"❌ Error in SQL query: {e}")
    
    conn.close()

# Usage instructions
print("To debug your data:")
print("1. Load your DataFrame: df = your_dataframe")
print("2. Run: quick_debug(df, '1C4NJDBB0GD610265')")
print("\nOr if you have the data loaded, uncomment the lines below:")
print("# quick_debug(df, '1C4NJDBB0GD610265')")


