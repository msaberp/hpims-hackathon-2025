# Troubleshooting: "Test query returned 0 rows"

## Quick Diagnosis

Copy and run these cells in your Jupyter notebook to identify the problem:

### Step 1: Check what dates you're searching for

```python
print(f"Searching for data between:")
print(f"  Start: {start_date}")
print(f"  End: {end_date}")
```

### Step 2: Check if data exists in that date range

```python
query = f"""
SELECT
    MIN(drug_exposure_start_date) as earliest,
    MAX(drug_exposure_start_date) as latest,
    COUNT(*) as total_rows
FROM CDMDEID.DRUG_EXPOSURE
"""

result = airms.conn.sql(query).collect()
print(pd.DataFrame(result))
```

**If the earliest/latest dates don't overlap with your start_date/end_date, that's the problem!**

### Step 3: Check if drug_type filter is the issue

```python
# Count without filter
query1 = f"""
SELECT COUNT(*) as cnt
FROM CDMDEID.DRUG_EXPOSURE
WHERE drug_exposure_start_date >= '{start_date}'
  AND drug_exposure_start_date <= '{end_date}'
"""

# Count with filter
query2 = f"""
SELECT COUNT(*) as cnt
FROM CDMDEID.DRUG_EXPOSURE
WHERE drug_exposure_start_date >= '{start_date}'
  AND drug_exposure_start_date <= '{end_date}'
  AND drug_type_concept_id IN (38000175, 38000176, 581373)
"""

result1 = airms.conn.sql(query1).collect()
result2 = airms.conn.sql(query2).collect()

print(f"Records in date range (no filter): {result1[0]['cnt']:,}")
print(f"Records with drug_type filter: {result2[0]['cnt']:,}")
```

---

## Solution 1: Adjust Date Range

If the data doesn't overlap with your date range:

```python
# Option A: Get the actual date range from your data
query = """
SELECT
    MIN(drug_exposure_start_date) as min_date,
    MAX(drug_exposure_start_date) as max_date
FROM CDMDEID.DRUG_EXPOSURE
"""
result = airms.conn.sql(query).collect()
start_date = str(result[0]['min_date'])
end_date = str(result[0]['max_date'])

print(f"Using actual data range: {start_date} to {end_date}")

# Now retry the test
test_df = analyzer.get_drug_exposures(
    start_date=start_date,
    end_date=end_date,
    limit=100
)
```

**OR**

```python
# Option B: Try a longer time period
start_date, end_date = get_date_range(months_back=24)  # Try 24 months
print(f"Trying: {start_date} to {end_date}")
```

---

## Solution 2: Remove drug_type Filter (Recommended)

If the drug_type_concept_id filter is excluding all records:

### Quick Fix - Use the No-Filter Version

```python
# Import the version without drug_type filter
from airms_helper_no_filter import AdherenceAnalyzer, get_date_range

# Re-initialize analyzer
analyzer = AdherenceAnalyzer(airms, schema="CDMDEID")

# Get date range
start_date, end_date = get_date_range(12)

# This will analyze ALL drug exposures (no filter)
test_df = analyzer.get_drug_exposures(
    start_date=start_date,
    end_date=end_date,
    limit=100,
    filter_drug_type=False  # This is the key parameter
)

print(f"Test query returned {len(test_df)} rows")
```

### Then run the full PDC analysis:

```python
pdc_results = analyzer.calculate_pdc_server_side(
    start_date=start_date,
    end_date=end_date,
    pdc_threshold=0.80,
    filter_drug_type=False  # No filter
)

print(f"✓ Found {len(pdc_results):,} patient-drug combinations")
```

---

## Solution 3: Full Debugging Script

If you still have issues, run the complete debugging script:

```python
# Copy and run all cells from notebooks/debug_database.py
# This will give you a detailed diagnosis
```

---

## Most Likely Fixes

### 1. Date Range Issue

**Problem**: Your data is from 2020-2022, but you're searching for the last 12 months (2024).

**Fix**:
```python
# Use the full available date range
query = "SELECT MIN(drug_exposure_start_date) as min_date, MAX(drug_exposure_start_date) as max_date FROM CDMDEID.DRUG_EXPOSURE"
result = airms.conn.sql(query).collect()
start_date = str(result[0]['min_date'])
end_date = str(result[0]['max_date'])
```

### 2. Drug Type Filter Issue

**Problem**: The standard OHDSI drug_type_concept_id values (38000175, 38000176, 581373) don't exist in your database.

**Fix**:
```python
# Use airms_helper_no_filter.py instead
from airms_helper_no_filter import AdherenceAnalyzer
analyzer = AdherenceAnalyzer(airms, schema="CDMDEID")

# All functions now have filter_drug_type=False by default
test_df = analyzer.get_drug_exposures(start_date, end_date, limit=100)
```

---

## After Fixing

Once you get results, verify the data looks correct:

```python
print(f"✓ Retrieved {len(test_df)} rows")
print(f"  Patients: {test_df['person_id'].nunique()}")
print(f"  Drugs: {test_df['drug_concept_id'].nunique()}")
print(f"\nSample data:")
print(test_df.head())

# Check calculated fields
print(f"\nDays covered statistics:")
print(test_df['days_covered'].describe())
```

Then proceed with the full analysis!

---

## Still Not Working?

Share the output from these queries:

```python
# 1. Check table exists
query = "SELECT COUNT(*) as cnt FROM CDMDEID.DRUG_EXPOSURE"
print("Total rows:", airms.conn.sql(query).collect())

# 2. Check date range
query = "SELECT MIN(drug_exposure_start_date) as min_d, MAX(drug_exposure_start_date) as max_d FROM CDMDEID.DRUG_EXPOSURE"
print("Date range:", airms.conn.sql(query).collect())

# 3. Check sample
query = "SELECT TOP 5 * FROM CDMDEID.DRUG_EXPOSURE"
print("Sample:", pd.DataFrame(airms.conn.sql(query).collect()))
```
