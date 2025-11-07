"""
Database Debugging Script

Run this to investigate why the query returns 0 rows.
Copy these cells into your Jupyter notebook to diagnose the issue.
"""

# %% [markdown]
# # Database Debugging - Find Out Why No Results
#
# Run these cells to understand the data structure

# %% Debug 1: Check date range being used
print("="*60)
print("DEBUG 1: Date Range Check")
print("="*60)
print(f"Start Date: {start_date}")
print(f"End Date: {end_date}")
print(f"This is checking for data from {start_date} to {end_date}")

# %% Debug 2: Check if DRUG_EXPOSURE table exists and has data
print("\n" + "="*60)
print("DEBUG 2: Check DRUG_EXPOSURE Table")
print("="*60)

query = f"""
SELECT COUNT(*) as total_rows
FROM {analyzer.schema}.DRUG_EXPOSURE
"""

try:
    result = airms.conn.sql(query).collect()
    print(f"✓ Total rows in DRUG_EXPOSURE table: {result[0]['total_rows']:,}")
except Exception as e:
    print(f"✗ Error accessing DRUG_EXPOSURE table: {e}")
    print("  → Check if schema name or table name is correct")

# %% Debug 3: Check date range of data in DRUG_EXPOSURE
print("\n" + "="*60)
print("DEBUG 3: Date Range in DRUG_EXPOSURE Table")
print("="*60)

query = f"""
SELECT
    MIN(drug_exposure_start_date) as earliest_date,
    MAX(drug_exposure_start_date) as latest_date,
    COUNT(*) as total_records
FROM {analyzer.schema}.DRUG_EXPOSURE
"""

try:
    result = airms.conn.sql(query).collect()
    df = pd.DataFrame(result)
    print(df)
    print(f"\nYour query is looking for: {start_date} to {end_date}")
    print(f"Data available from: {df['earliest_date'][0]} to {df['latest_date'][0]}")
except Exception as e:
    print(f"✗ Error: {e}")

# %% Debug 4: Check what drug_type_concept_id values exist
print("\n" + "="*60)
print("DEBUG 4: Available drug_type_concept_id Values")
print("="*60)

query = f"""
SELECT
    drug_type_concept_id,
    COUNT(*) as record_count
FROM {analyzer.schema}.DRUG_EXPOSURE
GROUP BY drug_type_concept_id
ORDER BY record_count DESC
"""

try:
    result = airms.conn.sql(query).collect()
    df = pd.DataFrame(result)
    print(df.head(20))
    print(f"\nOur query filters for: 38000175, 38000176, 581373")
    print("If these values don't appear above, that's the problem!")
except Exception as e:
    print(f"✗ Error: {e}")

# %% Debug 5: Get sample data WITHOUT filters
print("\n" + "="*60)
print("DEBUG 5: Sample Data (No Filters)")
print("="*60)

query = f"""
SELECT TOP 10
    person_id,
    drug_concept_id,
    drug_exposure_id,
    drug_exposure_start_date,
    drug_exposure_end_date,
    days_supply,
    refills,
    quantity,
    drug_type_concept_id
FROM {analyzer.schema}.DRUG_EXPOSURE
ORDER BY drug_exposure_start_date DESC
"""

try:
    result = airms.conn.sql(query).collect()
    df = pd.DataFrame(result)
    print(df)
    print("\nColumn names and sample values shown above")
except Exception as e:
    print(f"✗ Error: {e}")

# %% Debug 6: Check for NULL values in key fields
print("\n" + "="*60)
print("DEBUG 6: NULL Value Analysis")
print("="*60)

query = f"""
SELECT
    COUNT(*) as total_records,
    SUM(CASE WHEN drug_exposure_start_date IS NULL THEN 1 ELSE 0 END) as null_start_dates,
    SUM(CASE WHEN drug_exposure_end_date IS NULL THEN 1 ELSE 0 END) as null_end_dates,
    SUM(CASE WHEN days_supply IS NULL THEN 1 ELSE 0 END) as null_days_supply,
    SUM(CASE WHEN refills IS NULL THEN 1 ELSE 0 END) as null_refills,
    SUM(CASE WHEN days_supply IS NULL AND refills IS NULL AND drug_exposure_end_date IS NULL THEN 1 ELSE 0 END) as all_duration_fields_null
FROM {analyzer.schema}.DRUG_EXPOSURE
"""

try:
    result = airms.conn.sql(query).collect()
    df = pd.DataFrame(result)
    print(df.T)  # Transpose for better readability
except Exception as e:
    print(f"✗ Error: {e}")

# %% Debug 7: Test query WITH your date range but WITHOUT drug_type filter
print("\n" + "="*60)
print("DEBUG 7: Query With Date Range Only (No Type Filter)")
print("="*60)

query = f"""
SELECT COUNT(*) as matching_records
FROM {analyzer.schema}.DRUG_EXPOSURE
WHERE drug_exposure_start_date >= '{start_date}'
  AND drug_exposure_start_date <= '{end_date}'
"""

try:
    result = airms.conn.sql(query).collect()
    count = result[0]['matching_records']
    print(f"Records in your date range: {count:,}")

    if count == 0:
        print("\n⚠ Problem identified: No records in the specified date range!")
        print("   → Try adjusting the date range")
    else:
        print(f"\n✓ Found {count:,} records in date range")
        print("   → Problem is likely the drug_type_concept_id filter")
except Exception as e:
    print(f"✗ Error: {e}")

# %% Debug 8: Test query WITH date range AND drug_type filter
print("\n" + "="*60)
print("DEBUG 8: Query With Date Range AND Type Filter")
print("="*60)

query = f"""
SELECT COUNT(*) as matching_records
FROM {analyzer.schema}.DRUG_EXPOSURE
WHERE drug_exposure_start_date >= '{start_date}'
  AND drug_exposure_start_date <= '{end_date}'
  AND drug_type_concept_id IN (38000175, 38000176, 581373)
"""

try:
    result = airms.conn.sql(query).collect()
    count = result[0]['matching_records']
    print(f"Records with date range AND type filter: {count:,}")

    if count == 0:
        print("\n⚠ Problem identified: drug_type_concept_id filter is excluding all records!")
        print("   → Need to use different drug_type_concept_id values")
        print("   → See DEBUG 4 above for valid values")
    else:
        print(f"\n✓ Found {count:,} records - query should work!")
except Exception as e:
    print(f"✗ Error: {e}")

# %% [markdown]
# # Summary and Recommendations
#
# Based on the debug results above:
#
# **If date range is the issue:**
# - Adjust `MEASUREMENT_PERIOD_MONTHS` or manually set start_date/end_date
#
# **If drug_type_concept_id is the issue:**
# - Remove the filter entirely (analyze all drug exposures)
# - OR use the actual drug_type_concept_id values from DEBUG 4
#
# **Next step:** Run the cell below to get recommendations

# %% Generate Recommendations
print("\n" + "="*60)
print("RECOMMENDATIONS")
print("="*60)

# Check what the actual issue is and provide specific fix
query_date_count = f"""
SELECT COUNT(*) as cnt
FROM {analyzer.schema}.DRUG_EXPOSURE
WHERE drug_exposure_start_date >= '{start_date}'
  AND drug_exposure_start_date <= '{end_date}'
"""

query_type_count = f"""
SELECT COUNT(*) as cnt
FROM {analyzer.schema}.DRUG_EXPOSURE
WHERE drug_exposure_start_date >= '{start_date}'
  AND drug_exposure_start_date <= '{end_date}'
  AND drug_type_concept_id IN (38000175, 38000176, 581373)
"""

try:
    date_result = airms.conn.sql(query_date_count).collect()
    type_result = airms.conn.sql(query_type_count).collect()

    date_count = date_result[0]['cnt']
    type_count = type_result[0]['cnt']

    if date_count == 0:
        print("\n🔧 FIX: Date range issue")
        print("\nOption 1: Use a different date range")
        print("```python")
        print("# Example: Use last 24 months instead")
        print("start_date, end_date = get_date_range(months_back=24)")
        print("```")
        print("\nOption 2: Use full date range available in data")
        print("```python")
        print("# Get the actual date range from the data")
        query = f'''
        SELECT
            MIN(drug_exposure_start_date) as min_date,
            MAX(drug_exposure_start_date) as max_date
        FROM {analyzer.schema}.DRUG_EXPOSURE
        '''
        result = airms.conn.sql(query).collect()
        start_date = str(result[0]['min_date'])
        end_date = str(result[0]['max_date'])
        print(f"start_date = '{start_date}'")
        print(f"end_date = '{end_date}'")
        print("```")

    elif type_count == 0:
        print("\n🔧 FIX: drug_type_concept_id filter issue")
        print("\nThe drug_type_concept_id values in the query don't match your data.")
        print("\nOption 1: Remove the filter (recommended for hackathon)")
        print("This will analyze ALL drug exposures regardless of type.")
        print("\nI'll create a modified version of airms_helper.py in the next cell.")

    else:
        print("\n✓ Query should be working!")
        print(f"  Found {type_count:,} matching records")
        print("\nIf you're still getting 0 rows, there might be an issue with:")
        print("  - The limit parameter")
        print("  - The way results are being collected")
        print("  - Try running the query directly without the helper function")

except Exception as e:
    print(f"Could not generate recommendations: {e}")

print("\n" + "="*60)
