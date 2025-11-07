"""
Test Connection and Data Availability

Run this to verify everything works before the full analysis.
Copy these cells into your Jupyter notebook.
"""

# %% Cell 1: Connect to Database
from airms_connect.connection import airms_connection

airms = airms_connection()
airms.on_minerva(login_host_name='li04e04')
airms.connect()

print("✓ Connected to database")

# %% Cell 2: Import Fixed Helper
import pandas as pd
from airms_helper_fixed import AdherenceAnalyzer, get_actual_date_range

print("✓ Helper imported")

# %% Cell 3: Initialize Analyzer
analyzer = AdherenceAnalyzer(airms, schema="CDMDEID")
print("✓ Analyzer initialized")

# %% Cell 4: Get Database Information
print("\n" + "="*60)
print("DATABASE INFORMATION")
print("="*60)

info = analyzer.get_database_info()

if 'error' in info:
    print(f"✗ Error: {info['error']}")
else:
    print(f"\nTotal drug exposures: {info['total_drug_exposures']:,}")
    print(f"Unique patients: {info['unique_patients']:,}")
    print(f"Unique drugs: {info['unique_drugs']:,}")
    print(f"\nDate range in database:")
    print(f"  From: {info['date_range']['min']}")
    print(f"  To:   {info['date_range']['max']}")

# %% Cell 5: Get Actual Date Range
print("\n" + "="*60)
print("USING ACTUAL DATE RANGE FROM DATA")
print("="*60)

start_date, end_date = get_actual_date_range(airms, schema="CDMDEID")
print(f"\nWill analyze data from:")
print(f"  Start: {start_date}")
print(f"  End:   {end_date}")

# %% Cell 6: Test Query (Small Sample)
print("\n" + "="*60)
print("TEST QUERY - 100 ROWS")
print("="*60)

test_df = analyzer.get_drug_exposures(
    start_date=start_date,
    end_date=end_date,
    limit=100,
    filter_drug_type=False  # No filter
)

print(f"\n✓ SUCCESS! Retrieved {len(test_df)} rows")
print(f"  Unique patients: {test_df['PERSON_ID'].nunique()}")
print(f"  Unique drugs: {test_df['DRUG_CONCEPT_ID'].nunique()}")

print("\nColumn names:")
print(test_df.columns.tolist())

print("\nFirst few rows:")
print(test_df.head())

# %% Cell 7: Check Calculated Fields
print("\n" + "="*60)
print("DATA QUALITY CHECK")
print("="*60)

print("\nDays covered statistics:")
print(test_df['DAYS_COVERED'].describe())

print("\nDate range in sample:")
print(f"  Earliest: {test_df['START_DATE'].min()}")
print(f"  Latest: {test_df['START_DATE'].max()}")

print("\nMissing values:")
print(test_df.isnull().sum())

# %% Cell 8: Test Query Without Limit (Get More Data)
print("\n" + "="*60)
print("LARGER TEST - 1000 ROWS")
print("="*60)

test_larger = analyzer.get_drug_exposures(
    start_date=start_date,
    end_date=end_date,
    limit=1000,
    filter_drug_type=False
)

print(f"\n✓ Retrieved {len(test_larger)} rows")
print(f"  Unique patients: {test_larger['PERSON_ID'].nunique()}")
print(f"  Unique drugs: {test_larger['DRUG_CONCEPT_ID'].nunique()}")

# %% [markdown]
# # All Tests Passed! ✓
#
# Your database connection is working correctly.
# You can now proceed with the full PDC analysis.

# %% Cell 9: Ready for Full Analysis
print("\n" + "="*60)
print("READY FOR FULL ANALYSIS")
print("="*60)
print("\nNext steps:")
print("1. Run the full PDC calculation:")
print("   pdc_results = analyzer.calculate_pdc_server_side(")
print("       start_date=start_date,")
print("       end_date=end_date,")
print("       pdc_threshold=0.80,")
print("       filter_drug_type=False")
print("   )")
print("\n2. This will analyze ALL drug exposures in your database")
print(f"   ({info['total_drug_exposures']:,} total records)")
print("\n3. The calculation happens server-side, so it's efficient")
print("   even with millions of rows!")
