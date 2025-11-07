# Quick Start Guide

## Step 1: Upload Files to Hackathon Server

Upload these files to your Jupyter notebook directory on the server:
- `python/airms_helper.py`

## Step 2: Create a New Notebook

Create a new Jupyter notebook and follow these steps:

---

## Cell 1: Connect to Database

```python
# Connect to AIR-MS database
from airms_connect.connection import airms_connection

airms = airms_connection()
airms.on_minerva(login_host_name='li04e04')  # Adjust if needed
airms.connect()

print("✓ Connected to database")
```

---

## Cell 2: Import Libraries

```python
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

print("✓ Libraries imported")
```

---

## Cell 3: Import Helper Module

```python
# Make sure airms_helper.py is in the same directory
from airms_helper import AdherenceAnalyzer, get_date_range

print("✓ Helper module loaded")
```

---

## Cell 4: Configuration

```python
# Configuration
SCHEMA = "CDMDEID"
MEASUREMENT_PERIOD_MONTHS = 12
PDC_THRESHOLD = 0.80
MIN_TREATMENT_DAYS = 30

# Calculate date range
start_date, end_date = get_date_range(MEASUREMENT_PERIOD_MONTHS)

print(f"Analysis Configuration:")
print(f"  Schema: {SCHEMA}")
print(f"  Date Range: {start_date} to {end_date}")
print(f"  PDC Threshold: {PDC_THRESHOLD * 100}%")
```

---

## Cell 5: Initialize Analyzer

```python
analyzer = AdherenceAnalyzer(airms, schema=SCHEMA)
print("✓ Analyzer initialized")
```

---

## Cell 6: Test Query (Small Sample)

```python
# Test with 100 rows first
print("Running test query...")
test_df = analyzer.get_drug_exposures(
    start_date=start_date,
    end_date=end_date,
    limit=100
)

print(f"\n✓ Test successful!")
print(f"  Rows: {len(test_df)}")
print(f"  Patients: {test_df['person_id'].nunique()}")
print(f"  Drugs: {test_df['drug_concept_id'].nunique()}")

test_df.head()
```

---

## Cell 7: Calculate PDC (Main Analysis)

```python
print("Calculating PDC for all patients...")
print("(This may take a few minutes for large datasets)")

pdc_results = analyzer.calculate_pdc_server_side(
    start_date=start_date,
    end_date=end_date,
    pdc_threshold=PDC_THRESHOLD,
    min_treatment_days=MIN_TREATMENT_DAYS
)

print(f"\n✓ PDC calculation complete!")
print(f"  Patient-drug combinations: {len(pdc_results):,}")
print(f"  Unique patients: {pdc_results['person_id'].nunique():,}")
print(f"  Unique drugs: {pdc_results['drug_concept_id'].nunique():,}")
```

---

## Cell 8: View Results

```python
# Display first few results
pdc_results.head(10)
```

---

## Cell 9: Summary Statistics

```python
# Overall statistics
print("="*60)
print("ADHERENCE SUMMARY")
print("="*60)

total = len(pdc_results)
adherent = (pdc_results['adherence_status'] == 'Adherent').sum()
adherent_pct = adherent / total * 100

print(f"\nTotal combinations: {total:,}")
print(f"Adherent (PDC >= 80%): {adherent:,} ({adherent_pct:.1f}%)")
print(f"Non-adherent: {total - adherent:,} ({100-adherent_pct:.1f}%)")

print(f"\nPDC Statistics:")
print(f"  Mean: {pdc_results['pdc'].mean():.3f}")
print(f"  Median: {pdc_results['pdc'].median():.3f}")
print(f"  Std Dev: {pdc_results['pdc'].std():.3f}")

print(f"\nGap Statistics:")
print(f"  Avg gaps per patient: {pdc_results['num_gaps'].mean():.2f}")
print(f"  Avg gap days: {pdc_results['total_gap_days'].mean():.1f}")
print(f"  Max gap: {pdc_results['max_gap_days'].max():.0f} days")
```

---

## Cell 10: Adherence Status Distribution

```python
# Distribution
print("\nAdherence Status Distribution:")
print(pdc_results['adherence_status'].value_counts())
print("\nPercentages:")
print(pdc_results['adherence_status'].value_counts(normalize=True) * 100)

# Pie chart
fig, ax = plt.subplots(figsize=(8, 6))
adherence_counts = pdc_results['adherence_status'].value_counts()
colors = ['#2ecc71', '#f39c12', '#e74c3c']
ax.pie(adherence_counts, labels=adherence_counts.index, autopct='%1.1f%%',
       colors=colors, startangle=90)
ax.set_title('Adherence Status Distribution')
plt.show()
```

---

## Cell 11: Top Drugs Analysis

```python
# Group by drug
drug_summary = pdc_results.groupby('drug_name').agg({
    'person_id': 'nunique',
    'pdc': ['mean', 'median'],
    'adherence_status': lambda x: (x == 'Adherent').sum() / len(x) * 100,
    'num_gaps': 'mean'
}).round(3)

drug_summary.columns = ['patients', 'mean_pdc', 'median_pdc', 'pct_adherent', 'avg_gaps']
drug_summary = drug_summary.sort_values('patients', ascending=False)

print("Top 20 Drugs by Patient Count:")
print(drug_summary.head(20))
```

---

## Cell 12: PDC Distribution Histogram

```python
fig, ax = plt.subplots(figsize=(10, 6))
ax.hist(pdc_results['pdc'], bins=50, edgecolor='black', alpha=0.7, color='steelblue')
ax.axvline(PDC_THRESHOLD, color='red', linestyle='--', linewidth=2,
           label=f'Threshold ({PDC_THRESHOLD*100}%)')
ax.set_xlabel('PDC')
ax.set_ylabel('Frequency')
ax.set_title('Distribution of Proportion of Days Covered (PDC)')
ax.legend()
ax.grid(True, alpha=0.3)
plt.show()
```

---

## Cell 13: Detailed Gap Analysis

```python
print("Retrieving detailed gap information...")

gap_details = analyzer.get_detailed_gaps(
    start_date=start_date,
    end_date=end_date,
    min_gap_days=7
)

print(f"\n✓ Found {len(gap_details):,} gaps >= 7 days")
print(f"  Affecting {gap_details['person_id'].nunique():,} patients")

print("\nGap Severity Distribution:")
print(gap_details['gap_severity'].value_counts())
```

---

## Cell 14: Export Results

```python
import os

# Create results directory
os.makedirs('results', exist_ok=True)

# Save PDC results
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
pdc_filename = f"results/pdc_results_{timestamp}.csv"
pdc_results.to_csv(pdc_filename, index=False)
print(f"✓ Saved: {pdc_filename}")

# Save drug summary
drug_filename = f"results/drug_summary_{timestamp}.csv"
drug_summary.to_csv(drug_filename)
print(f"✓ Saved: {drug_filename}")

# Save gap details
gap_filename = f"results/gap_details_{timestamp}.csv"
gap_details.to_csv(gap_filename, index=False)
print(f"✓ Saved: {gap_filename}")

print("\n✓ All results exported successfully!")
```

---

## Cell 15: Identify High-Risk Patients

```python
# Find patients with critical adherence issues
high_risk = pdc_results[
    (pdc_results['adherence_status'] == 'Non-Adherent') &
    (pdc_results['max_gap_days'] >= 90)
].sort_values('max_gap_days', ascending=False)

print(f"High-Risk Patients: {len(high_risk):,}")
print(f"(Non-adherent with gaps >= 90 days)")
print("\nTop 10:")
print(high_risk[['person_id', 'drug_name', 'pdc', 'max_gap_days', 'num_gaps']].head(10))
```

---

## Cell 16: Drugs with Lowest Adherence

```python
# Find drugs with lowest adherence rates (min 50 patients)
low_adherence = drug_summary[drug_summary['patients'] >= 50].sort_values('pct_adherent')

print("Drugs with Lowest Adherence Rates (min 50 patients):")
print(low_adherence.head(10))
```

---

## That's it!

You now have:
- PDC scores for all patients
- Adherence gap analysis
- Summary statistics
- Visualizations
- Exported CSV files

### Next Steps:
1. Analyze patterns in the data
2. Build predictive models
3. Create intervention strategies
4. Present findings!

For more advanced analysis, see `medication_adherence_analysis.py` for the full workflow.
