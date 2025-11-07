"""
Medication Adherence Analysis - Main Notebook Script (UPDATED FOR AIRMS)

This script can be copied into the hackathon Jupyter notebook or run as a Python file.
It uses the airms_connect library to analyze medication adherence using PDC metric.

UPDATED: Uses airms_helper_fixed.py which properly handles DataFrame results
         and UPPERCASE column names from airms.conn.sql().collect()

Usage in Jupyter Notebook:
1. Connect to database using airms_connect
2. Import this module or copy the code
3. Run the analysis
"""

# %% [markdown]
# # Medication Adherence Analysis using PDC
#
# This notebook analyzes medication adherence using the Proportion of Days Covered (PDC) metric.
# We'll identify adherence gaps and classify patients as adherent (PDC >= 80%) or non-adherent.

# %% Setup and Imports
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# Set visualization style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 6)

print("Imports successful!")

# %% [markdown]
# ## Step 1: Database Connection
#
# Connect to the database using airms_connect (provided by hackathon team)

# %% Database Connection
# AIR-MS connect
from airms_connect.connection import airms_connection

# Establish a connection to AIR·MS
airms = airms_connection()
airms.on_minerva(login_host_name='li04e04')  # Adjust login_host_name as needed
airms.connect()

print("Connected to database successfully!")

# %% [markdown]
# ## Step 2: Import Helper Functions
#
# Import our custom helper class for adherence analysis (FIXED VERSION)

# %% Import Helper
# Import the FIXED version that handles DataFrames and UPPERCASE columns
try:
    from airms_helper_fixed import AdherenceAnalyzer, get_actual_date_range
    print("Helper module imported successfully!")
except ImportError:
    print("Could not import airms_helper_fixed. Make sure it's in the same directory or Python path.")
    print("Upload airms_helper_fixed.py to your notebook directory.")

# %% [markdown]
# ## Step 3: Configuration
#
# Set analysis parameters

# %% Configuration
# Database schema
SCHEMA = "CDMDEID"

# PDC threshold for adherence classification
PDC_THRESHOLD = 0.80

# Minimum treatment duration to include in analysis
MIN_TREATMENT_DAYS = 30

# Minimum gap duration to report
MIN_GAP_DAYS = 7

# Get actual date range from the database
start_date, end_date = get_actual_date_range(airms, schema=SCHEMA)

print(f"Analysis Configuration:")
print(f"  Schema: {SCHEMA}")
print(f"  Date Range: {start_date} to {end_date} (from actual data)")
print(f"  PDC Threshold: {PDC_THRESHOLD * 100}%")
print(f"  Minimum Treatment Days: {MIN_TREATMENT_DAYS}")

# %% [markdown]
# ## Step 4: Initialize Analyzer

# %% Initialize
analyzer = AdherenceAnalyzer(airms, schema=SCHEMA)
print("AdherenceAnalyzer initialized!")

# %% Get Database Info
print("\nDatabase Information:")
db_info = analyzer.get_database_info()
if 'error' not in db_info:
    print(f"  Total drug exposures: {db_info['total_drug_exposures']:,}")
    print(f"  Unique patients: {db_info['unique_patients']:,}")
    print(f"  Unique drugs: {db_info['unique_drugs']:,}")

# %% [markdown]
# ## Step 5: Test Query
#
# First, let's test with a small sample to make sure everything works

# %% Test Query
print("Running test query (limited to 100 rows)...")
test_df = analyzer.get_drug_exposures(
    start_date=start_date,
    end_date=end_date,
    limit=100,
    filter_drug_type=False  # No filter - analyze all drug types
)

print(f"\nTest query returned {len(test_df)} rows")
print(f"Columns: {list(test_df.columns)}")
print("\nFirst few rows:")
print(test_df.head())

# %% Data Quality Check
print("\nData Quality Check:")
print(f"  Unique patients: {test_df['PERSON_ID'].nunique()}")
print(f"  Unique drugs: {test_df['DRUG_CONCEPT_ID'].nunique()}")
print(f"  Date range: {test_df['START_DATE'].min()} to {test_df['START_DATE'].max()}")
print(f"\nMissing values:")
print(test_df.isnull().sum())

print(f"\nDays supply distribution:")
print(test_df['DAYS_SUPPLY'].describe())

# %% [markdown]
# ## Step 6: Calculate PDC (Server-Side)
#
# **This is the main analysis!** We calculate PDC for all patients using server-side SQL.
# This is efficient even with millions of rows because computation happens in the database.

# %% Calculate PDC
print("Calculating PDC for all patients (this may take a few minutes for large datasets)...")
print("Processing on server side to handle millions of rows efficiently...")

pdc_results = analyzer.calculate_pdc_server_side(
    start_date=start_date,
    end_date=end_date,
    pdc_threshold=PDC_THRESHOLD,
    min_treatment_days=MIN_TREATMENT_DAYS,
    filter_drug_type=False  # No filter - analyze all drug types
)

print(f"\nPDC calculation complete!")
print(f"  Total patient-drug combinations: {len(pdc_results)}")
print(f"  Unique patients: {pdc_results['PERSON_ID'].nunique()}")
print(f"  Unique drugs: {pdc_results['DRUG_CONCEPT_ID'].nunique()}")

# %% Display Results
print("\nFirst few results:")
print(pdc_results.head(10))

# %% [markdown]
# ## Step 7: Summary Statistics

# %% Overall Statistics
print("\n" + "="*60)
print("OVERALL ADHERENCE STATISTICS")
print("="*60)

total_combinations = len(pdc_results)
total_patients = pdc_results['PERSON_ID'].nunique()
adherent_count = (pdc_results['ADHERENCE_STATUS'] == 'Adherent').sum()
adherent_pct = adherent_count / total_combinations * 100

print(f"\nPatient Metrics:")
print(f"  Total unique patients: {total_patients:,}")
print(f"  Total patient-drug combinations: {total_combinations:,}")
print(f"  Average drugs per patient: {total_combinations/total_patients:.2f}")

print(f"\nAdherence Metrics:")
print(f"  Adherent (PDC >= {PDC_THRESHOLD*100}%): {adherent_count:,} ({adherent_pct:.1f}%)")
print(f"  Non-adherent: {total_combinations - adherent_count:,} ({100-adherent_pct:.1f}%)")

print(f"\nPDC Statistics:")
print(f"  Mean PDC: {pdc_results['PDC'].mean():.3f}")
print(f"  Median PDC: {pdc_results['PDC'].median():.3f}")
print(f"  Std Dev: {pdc_results['PDC'].std():.3f}")
print(f"  Min PDC: {pdc_results['PDC'].min():.3f}")
print(f"  Max PDC: {pdc_results['PDC'].max():.3f}")

print(f"\nGap Statistics:")
print(f"  Average gaps per patient-drug: {pdc_results['NUM_GAPS'].mean():.2f}")
print(f"  Average gap days: {pdc_results['TOTAL_GAP_DAYS'].mean():.1f}")
print(f"  Max gap observed: {pdc_results['MAX_GAP_DAYS'].max():.0f} days")

# %% Adherence Status Distribution
print("\nAdherence Status Distribution:")
adherence_dist = pdc_results['ADHERENCE_STATUS'].value_counts()
print(adherence_dist)
print(f"\nPercentages:")
print(pdc_results['ADHERENCE_STATUS'].value_counts(normalize=True) * 100)

# %% [markdown]
# ## Step 8: Analysis by Drug

# %% Top Drugs Analysis
print("\n" + "="*60)
print("TOP 20 DRUGS BY PATIENT COUNT")
print("="*60)

drug_summary = pdc_results.groupby('DRUG_NAME').agg({
    'PERSON_ID': 'nunique',
    'PDC': ['mean', 'median', 'std'],
    'ADHERENCE_STATUS': lambda x: (x == 'Adherent').sum() / len(x) * 100,
    'NUM_GAPS': 'mean',
    'TOTAL_GAP_DAYS': 'mean',
    'MAX_GAP_DAYS': 'max'
}).round(3)

drug_summary.columns = [
    'unique_patients',
    'mean_pdc',
    'median_pdc',
    'std_pdc',
    'pct_adherent',
    'avg_gaps',
    'avg_gap_days',
    'max_gap'
]

drug_summary = drug_summary.sort_values('unique_patients', ascending=False)

print("\nTop 20 drugs:")
print(drug_summary.head(20))

# %% Drugs with Lowest Adherence
print("\n" + "="*60)
print("DRUGS WITH LOWEST ADHERENCE RATES (min 50 patients)")
print("="*60)

low_adherence_drugs = drug_summary[drug_summary['unique_patients'] >= 50].sort_values('pct_adherent').head(10)
print(low_adherence_drugs)

# %% [markdown]
# ## Step 9: Detailed Gap Analysis

# %% Get Gap Details
print("\nRetrieving detailed gap information (this may take a moment)...")

gap_details = analyzer.get_detailed_gaps(
    start_date=start_date,
    end_date=end_date,
    min_gap_days=MIN_GAP_DAYS,
    filter_drug_type=False  # No filter
)

print(f"\nFound {len(gap_details):,} gaps >= {MIN_GAP_DAYS} days")
print(f"Affecting {gap_details['PERSON_ID'].nunique():,} patients")

# %% Gap Severity Distribution
print("\nGap Severity Distribution:")
gap_severity_dist = gap_details['GAP_SEVERITY'].value_counts()
print(gap_severity_dist)

# %% Gap Statistics by Drug
print("\n" + "="*60)
print("GAP STATISTICS BY DRUG (Top 10 by gap frequency)")
print("="*60)

gap_by_drug = gap_details.groupby('DRUG_NAME').agg({
    'PERSON_ID': 'nunique',
    'GAP_DAYS': ['count', 'mean', 'median', 'max'],
    'GAP_SEVERITY': lambda x: (x.str.contains('Critical')).sum()
}).round(2)

gap_by_drug.columns = [
    'unique_patients',
    'total_gaps',
    'avg_gap_days',
    'median_gap_days',
    'max_gap_days',
    'critical_gaps'
]

gap_by_drug = gap_by_drug.sort_values('total_gaps', ascending=False)
print(gap_by_drug.head(10))

# %% [markdown]
# ## Step 10: Export Results

# %% Save Results
print("\nSaving results to CSV files...")

# Create results directory if it doesn't exist
import os
results_dir = "results"  # Changed from "../results" for notebook compatibility
os.makedirs(results_dir, exist_ok=True)

# Save PDC results
pdc_filename = f"{results_dir}/pdc_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
pdc_results.to_csv(pdc_filename, index=False)
print(f"  Saved PDC results: {pdc_filename}")

# Save drug summary
drug_summary_filename = f"{results_dir}/drug_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
drug_summary.to_csv(drug_summary_filename)
print(f"  Saved drug summary: {drug_summary_filename}")

# Save gap details
gap_filename = f"{results_dir}/gap_details_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
gap_details.to_csv(gap_filename, index=False)
print(f"  Saved gap details: {gap_filename}")

print("\nAll results saved successfully!")

# %% [markdown]
# ## Step 11: Visualizations

# %% Visualization 1: PDC Distribution
print("\nCreating visualizations...")

fig, axes = plt.subplots(2, 2, figsize=(15, 12))

# PDC Histogram
ax1 = axes[0, 0]
ax1.hist(pdc_results['PDC'], bins=50, edgecolor='black', alpha=0.7)
ax1.axvline(PDC_THRESHOLD, color='red', linestyle='--', label=f'Threshold ({PDC_THRESHOLD*100}%)')
ax1.set_xlabel('PDC')
ax1.set_ylabel('Frequency')
ax1.set_title('Distribution of Proportion of Days Covered (PDC)')
ax1.legend()
ax1.grid(True, alpha=0.3)

# Adherence Status Pie Chart
ax2 = axes[0, 1]
adherence_counts = pdc_results['ADHERENCE_STATUS'].value_counts()
colors = ['#2ecc71', '#f39c12', '#e74c3c']
ax2.pie(adherence_counts, labels=adherence_counts.index, autopct='%1.1f%%',
        colors=colors, startangle=90)
ax2.set_title('Adherence Status Distribution')

# Gap Distribution
ax3 = axes[1, 0]
gap_details['GAP_SEVERITY'].value_counts().plot(kind='bar', ax=ax3, color='steelblue')
ax3.set_xlabel('Gap Severity')
ax3.set_ylabel('Count')
ax3.set_title('Distribution of Gap Severity')
ax3.tick_params(axis='x', rotation=45)
ax3.grid(True, alpha=0.3)

# Top 10 Drugs by Patient Count
ax4 = axes[1, 1]
top_drugs = drug_summary.head(10)[['unique_patients']].sort_values('unique_patients')
top_drugs.plot(kind='barh', ax=ax4, legend=False, color='coral')
ax4.set_xlabel('Number of Patients')
ax4.set_title('Top 10 Drugs by Patient Count')
ax4.grid(True, alpha=0.3)

plt.tight_layout()
viz_filename = f"{results_dir}/adherence_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
plt.savefig(viz_filename, dpi=300, bbox_inches='tight')
print(f"  Saved visualization: {viz_filename}")
plt.show()

# %% Visualization 2: PDC by Drug (Top 20)
fig, ax = plt.subplots(figsize=(12, 8))

top_20_drugs = drug_summary.head(20)[['mean_pdc', 'pct_adherent']].sort_values('mean_pdc')

x = np.arange(len(top_20_drugs))
width = 0.35

bars1 = ax.barh(x - width/2, top_20_drugs['mean_pdc'] * 100, width, label='Mean PDC (%)', alpha=0.8)
bars2 = ax.barh(x + width/2, top_20_drugs['pct_adherent'], width, label='% Adherent', alpha=0.8)

ax.set_yticks(x)
ax.set_yticklabels([name[:40] + '...' if len(name) > 40 else name for name in top_20_drugs.index])
ax.set_xlabel('Percentage')
ax.set_title('Top 20 Drugs: Mean PDC vs % Adherent Patients')
ax.axvline(80, color='red', linestyle='--', alpha=0.5, label='80% Threshold')
ax.legend()
ax.grid(True, alpha=0.3, axis='x')

plt.tight_layout()
viz_filename2 = f"{results_dir}/pdc_by_drug_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
plt.savefig(viz_filename2, dpi=300, bbox_inches='tight')
print(f"  Saved visualization: {viz_filename2}")
plt.show()

print("\nAll visualizations created successfully!")

# %% [markdown]
# ## Summary Report

# %% Final Summary
print("\n" + "="*60)
print("MEDICATION ADHERENCE ANALYSIS - FINAL SUMMARY")
print("="*60)
print(f"\nAnalysis Period: {start_date} to {end_date}")
print(f"PDC Threshold: {PDC_THRESHOLD * 100}%")

print(f"\n KEY FINDINGS:")
print(f"  1. Analyzed {total_patients:,} patients with {total_combinations:,} patient-drug combinations")
print(f"  2. Overall adherence rate: {adherent_pct:.1f}%")
print(f"  3. Average PDC: {pdc_results['PDC'].mean():.3f}")
print(f"  4. Identified {len(gap_details):,} significant gaps (>={MIN_GAP_DAYS} days)")
print(f"  5. Average gaps per patient-drug: {pdc_results['NUM_GAPS'].mean():.2f}")

print(f"\n ADHERENCE GAPS:")
critical_gaps = (gap_details['GAP_SEVERITY'] == 'Critical Gap (90+ days)').sum()
major_gaps = (gap_details['GAP_SEVERITY'] == 'Major Gap (30-89 days)').sum()
print(f"  - Critical gaps (90+ days): {critical_gaps:,}")
print(f"  - Major gaps (30-89 days): {major_gaps:,}")

print(f"\n DRUGS WITH LOWEST ADHERENCE:")
for i, (drug_name, row) in enumerate(low_adherence_drugs.head(5).iterrows(), 1):
    print(f"  {i}. {drug_name[:50]}")
    print(f"     Adherence rate: {row['pct_adherent']:.1f}%, Avg PDC: {row['mean_pdc']:.3f}")

print("\n" + "="*60)
print("Analysis complete! Check the results folder for exported files.")
print("="*60)
