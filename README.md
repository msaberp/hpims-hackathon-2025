# Medication Adherence Analysis - HPIMS Hackathon 2025

This project analyzes medication adherence using the **Proportion of Days Covered (PDC)** metric to identify adherence gaps in patients based on OHDSI Common Data Model (CDM) data.

## Overview

### Objective
Detect medication adherence gaps using drug exposure data and calculate PDC for each patient-drug combination to classify patients as adherent (PDC >= 80%) or non-adherent.

### Key Features
- ✅ **Optimized for large datasets** (millions of records) using server-side SQL processing
- ✅ **PDC calculation** with handling of overlapping periods and gaps
- ✅ **End date calculation** using `days_supply` or `refills` as fallback
- ✅ **Detailed gap analysis** with severity classification
- ✅ **Summary statistics** and visualizations
- ✅ **Integration with `airms_connect`** library (hackathon environment)

### Data Sources
- **OHDSI CDM Schema**: CDMDEID
- **Primary Tables**:
  - `DRUG_EXPOSURE` - Medication dispensing/prescription records
  - `CONCEPT` - Drug names and classifications

## Project Structure

```
hpims-hackathon-2025/
├── config/
│   └── db_config.yaml           # Configuration (analysis parameters)
├── sql/
│   ├── 01_calculate_drug_coverage.sql    # Extract drug exposures with end dates
│   ├── 02_calculate_pdc_and_gaps.sql     # PDC calculation with gap detection
│   ├── 03_detailed_gap_analysis.sql      # Detailed gap information
│   └── 04_summary_statistics.sql         # Aggregate statistics
├── python/
│   └── airms_helper.py          # Helper class for airms_connect integration
├── notebooks/
│   └── medication_adherence_analysis.py  # Main analysis script
├── results/                     # Output folder for results (CSVs, plots)
├── requirements.txt             # Python dependencies
└── README.md                    # This file
```

## Setup Instructions

### Option 1: Work in Hackathon Jupyter Notebook (Recommended)

Since you have access to the hackathon server with `airms_connect` already installed:

1. **Upload files to the server**:
   - Upload `python/airms_helper.py` to your notebook directory
   - Upload `notebooks/medication_adherence_analysis.py` (or copy contents into a new notebook)

2. **Create results directory**:
   ```bash
   mkdir -p results
   ```

3. **Open the Jupyter notebook** on the server and run the analysis script

### Option 2: Local Development (if database access is available)

If you can connect from your local machine:

1. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure database connection**:
   Edit `config/db_config.yaml` with your credentials

3. **Run the analysis**:
   ```python
   python notebooks/medication_adherence_analysis.py
   ```

## Usage

### Quick Start in Jupyter Notebook

```python
# 1. Connect to database
from airms_connect.connection import airms_connection

airms = airms_connection()
airms.on_minerva(login_host_name='li04e04')
airms.connect()

# 2. Import helper
from airms_helper import AdherenceAnalyzer, get_date_range

# 3. Initialize analyzer
analyzer = AdherenceAnalyzer(airms, schema="CDMDEID")

# 4. Set analysis period
start_date, end_date = get_date_range(months_back=12)  # Last 12 months

# 5. Calculate PDC (this is the main analysis!)
pdc_results = analyzer.calculate_pdc_server_side(
    start_date=start_date,
    end_date=end_date,
    pdc_threshold=0.80,
    min_treatment_days=30
)

# 6. View results
print(f"Analyzed {pdc_results['person_id'].nunique()} patients")
print(f"Adherence rate: {(pdc_results['adherence_status'] == 'Adherent').sum() / len(pdc_results) * 100:.1f}%")

# 7. Get detailed gaps
gap_details = analyzer.get_detailed_gaps(
    start_date=start_date,
    end_date=end_date,
    min_gap_days=7
)
```

### Full Analysis Workflow

The complete analysis script (`notebooks/medication_adherence_analysis.py`) includes:

1. **Database connection** using `airms_connect`
2. **Data quality checks** on drug exposure data
3. **PDC calculation** for all patient-drug combinations
4. **Summary statistics**:
   - Overall adherence rates
   - Statistics by drug
   - PDC distribution
5. **Gap analysis**:
   - Detailed gap information
   - Gap severity classification
   - Gap patterns by drug
6. **Visualizations**:
   - PDC histogram
   - Adherence status pie chart
   - Gap severity distribution
   - Top drugs analysis
7. **Export results** to CSV files

## PDC Calculation Methodology

### Proportion of Days Covered (PDC)

**Formula**:
```
PDC = (Total Days Covered) / (Treatment Duration) × 100
```

Where:
- **Total Days Covered**: Sum of non-overlapping days with medication
- **Treatment Duration**: Days from first fill to last fill end date

### End Date Calculation Logic

Since `drug_exposure_end_date` may be missing, we use this fallback hierarchy:

1. Use `drug_exposure_end_date` if available
2. Calculate from `days_supply`: `end_date = start_date + days_supply - 1`
3. Calculate from `refills`: `end_date = start_date + (refills × 30) - 1`
4. Default: Assume 30-day supply

### Handling Overlapping Periods

The algorithm:
1. Groups consecutive/overlapping fills using window functions
2. Merges overlapping periods to avoid double-counting
3. Calculates gaps between merged periods
4. Computes PDC based on actual coverage

### Adherence Classification

- **Adherent**: PDC >= 80% (industry standard)
- **Moderately Adherent**: PDC 70-79%
- **Non-Adherent**: PDC < 70%

### Gap Severity Classification

- **Critical Gap**: 90+ days without medication
- **Major Gap**: 30-89 days
- **Moderate Gap**: 14-29 days
- **Minor Gap**: 7-13 days

## Key Functions in `airms_helper.py`

### `AdherenceAnalyzer` Class

#### `get_drug_exposures(start_date, end_date, limit=None)`
Retrieves drug exposure records with calculated end dates.

#### `calculate_pdc_server_side(start_date, end_date, pdc_threshold=0.80)`
**Main function**: Calculates PDC using server-side SQL (handles millions of rows efficiently).

Returns DataFrame with:
- `person_id`, `drug_concept_id`, `drug_name`
- `pdc` (proportion of days covered)
- `adherence_status` (Adherent/Non-Adherent)
- `total_days_covered`, `treatment_duration`
- `num_gaps`, `total_gap_days`, `max_gap_days`

#### `get_detailed_gaps(start_date, end_date, min_gap_days=7)`
Returns detailed information about each adherence gap.

## Configuration

Edit `config/db_config.yaml` to adjust:

```yaml
analysis:
  measurement_period_months: 12      # Analysis window
  pdc_threshold: 0.80                # Adherence threshold
  grace_period_days: 0               # Optional grace period
```

## Performance Optimization

### For Large Datasets (Millions of Rows)

1. **Use server-side SQL** (built into our queries):
   - All PDC calculations use window functions and CTEs
   - Minimal data transfer to Python
   - Leverages database query optimizer

2. **Filter early**:
   - Set appropriate date ranges
   - Focus on specific drug classes if needed
   - Use `min_treatment_days` to exclude short treatments

3. **Process in stages**:
   - Test with small samples first (`limit` parameter)
   - Run full analysis after validation
   - Export intermediate results

## Expected Output

### Files Generated in `results/` folder:

1. **`pdc_results_[timestamp].csv`**:
   - One row per patient-drug combination
   - PDC scores and adherence classification
   - Gap statistics

2. **`drug_summary_[timestamp].csv`**:
   - Aggregated statistics by drug
   - Adherence rates per drug
   - Average gaps and PDC

3. **`gap_details_[timestamp].csv`**:
   - Detailed information about each gap
   - Gap duration and severity
   - Context (fills before/after)

4. **`adherence_analysis_[timestamp].png`**:
   - Multi-panel visualization
   - PDC distribution, adherence status, gaps

5. **`pdc_by_drug_[timestamp].png`**:
   - Drug-level adherence comparison

## Troubleshooting

### Common Issues

**Issue**: "Cannot import airms_helper"
- **Solution**: Upload `airms_helper.py` to the same directory as your notebook, or add the path:
  ```python
  import sys
  sys.path.append('/path/to/python/folder')
  from airms_helper import AdherenceAnalyzer
  ```

**Issue**: Query takes too long
- **Solution**: The queries are optimized for server-side processing. If still slow:
  - Reduce date range
  - Add `LIMIT` for testing
  - Check if database has indexes on `person_id`, `drug_concept_id`, `drug_exposure_start_date`

**Issue**: Memory error
- **Solution**: The queries return aggregated results (one row per patient-drug), not raw records. If still an issue:
  - Process drugs separately
  - Export intermediate results to CSV
  - Use database views/temp tables

**Issue**: "Connection failed"
- **Solution**: Ensure you're running on the hackathon server or have VPN access if connecting locally

## SQL Queries Explained

### `01_calculate_drug_coverage.sql`
- Extracts drug exposures with calculated end dates
- Filters for dispensed/prescribed records (not administered)
- Can be used standalone for data exploration

### `02_calculate_pdc_and_gaps.sql`
- **Main query**: Complete PDC calculation
- Uses CTEs for step-by-step calculation
- Handles overlapping periods with window functions
- Joins with CONCEPT table for drug names
- **This is what `calculate_pdc_server_side()` uses**

### `03_detailed_gap_analysis.sql`
- Identifies gaps between consecutive fills
- Provides context (fills before/after gap)
- Classifies gap severity
- **This is what `get_detailed_gaps()` uses**

### `04_summary_statistics.sql`
- Aggregate statistics (sample query)
- Can be customized for specific reporting needs

## Next Steps for Hackathon

### Analysis Ideas

1. **Identify high-risk patients**:
   ```python
   high_risk = pdc_results[
       (pdc_results['adherence_status'] == 'Non-Adherent') &
       (pdc_results['max_gap_days'] >= 90)
   ]
   ```

2. **Compare adherence across drug classes**:
   - Join with CONCEPT table to get therapeutic classes
   - Group by class and analyze

3. **Time series analysis**:
   - Track adherence trends over time
   - Identify seasonal patterns

4. **Predictive modeling**:
   - Use gap patterns to predict future non-adherence
   - Build risk scores

5. **Intervention targeting**:
   - Prioritize patients with critical gaps
   - Focus on drugs with lowest adherence rates

## References

- [OHDSI Common Data Model v5.4](https://ohdsi.github.io/CommonDataModel/cdm54.html)
- [PDC Measurement Methodology](https://www.pharmacyquality.com/medication-adherence/)
- SAP HANA SQL Reference: [Window Functions](https://help.sap.com/docs/HANA_CLOUD/c1d3f60099654ecfb3fe36ac93c121bb/20a353327519101495dfd0a87060a0d3.html)

## Authors

HPIMS Hackathon 2025 Team

## License

This project is for hackathon use only.
