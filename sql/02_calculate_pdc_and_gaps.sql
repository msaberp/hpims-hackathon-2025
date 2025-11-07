/*
 * Step 2: Calculate Proportion of Days Covered (PDC) and Adherence Gaps
 *
 * This query:
 * - Handles overlapping drug exposure periods
 * - Calculates non-overlapping days covered
 * - Computes PDC per patient per drug
 * - Identifies adherence gaps
 *
 * Optimized for SAP HANA with analytical window functions
 *
 * Parameters to replace:
 * - :schema - Your OHDSI CDM schema name
 * - :start_date - Analysis start date
 * - :end_date - Analysis end date
 * - :pdc_threshold - Adherence threshold (e.g., 0.80)
 */

WITH drug_exposures_cleaned AS (
    -- Step 1: Get drug exposures with calculated end dates
    SELECT
        de.person_id,
        de.drug_concept_id,
        de.drug_exposure_id,
        de.drug_exposure_start_date AS start_date,

        CASE
            WHEN de.drug_exposure_end_date IS NOT NULL
                THEN de.drug_exposure_end_date
            WHEN de.days_supply IS NOT NULL AND de.days_supply > 0
                THEN ADD_DAYS(de.drug_exposure_start_date, de.days_supply - 1)
            WHEN de.refills IS NOT NULL AND de.refills > 0
                THEN ADD_DAYS(de.drug_exposure_start_date, (de.refills * 30) - 1)
            ELSE ADD_DAYS(de.drug_exposure_start_date, 29)
        END AS end_date

    FROM :schema.drug_exposure de

    WHERE de.drug_exposure_start_date >= :start_date
      AND de.drug_exposure_start_date <= :end_date
      AND de.drug_type_concept_id IN (38000175, 38000176, 581373)
),

-- Step 2: Create coverage groups to handle overlapping periods
coverage_groups AS (
    SELECT
        person_id,
        drug_concept_id,
        drug_exposure_id,
        start_date,
        end_date,

        -- Use window function to find the maximum end_date up to current row
        MAX(end_date) OVER (
            PARTITION BY person_id, drug_concept_id
            ORDER BY start_date, end_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS prev_max_end_date,

        -- Create groups for consecutive/overlapping periods
        SUM(CASE
            WHEN start_date <= COALESCE(
                MAX(end_date) OVER (
                    PARTITION BY person_id, drug_concept_id
                    ORDER BY start_date, end_date
                    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                ), TO_DATE('1900-01-01', 'YYYY-MM-DD')
            ) + 1  -- Allow 1 day gap to be considered continuous
            THEN 0
            ELSE 1
        END) OVER (
            PARTITION BY person_id, drug_concept_id
            ORDER BY start_date, end_date
            ROWS UNBOUNDED PRECEDING
        ) AS coverage_group

    FROM drug_exposures_cleaned
),

-- Step 3: Merge overlapping periods within each group
merged_periods AS (
    SELECT
        person_id,
        drug_concept_id,
        coverage_group,
        MIN(start_date) AS period_start,
        MAX(end_date) AS period_end,
        COUNT(*) AS num_fills,

        -- Calculate days covered in this merged period
        DAYS_BETWEEN(MIN(start_date), MAX(end_date)) + 1 AS days_covered

    FROM coverage_groups
    GROUP BY person_id, drug_concept_id, coverage_group
),

-- Step 4: Calculate gaps between coverage periods
gaps AS (
    SELECT
        person_id,
        drug_concept_id,
        period_start,
        period_end,
        days_covered,
        num_fills,

        -- Calculate gap to next period
        LEAD(period_start) OVER (
            PARTITION BY person_id, drug_concept_id
            ORDER BY period_start
        ) AS next_period_start,

        -- Gap duration in days
        CASE
            WHEN LEAD(period_start) OVER (
                PARTITION BY person_id, drug_concept_id
                ORDER BY period_start
            ) IS NOT NULL
            THEN DAYS_BETWEEN(
                period_end,
                LEAD(period_start) OVER (
                    PARTITION BY person_id, drug_concept_id
                    ORDER BY period_start
                )
            ) - 1  -- Subtract 1 because both end and start dates are inclusive
            ELSE 0
        END AS gap_days

    FROM merged_periods
),

-- Step 5: Calculate PDC per patient per drug
patient_drug_pdc AS (
    SELECT
        person_id,
        drug_concept_id,

        -- Total days covered (sum of all merged periods)
        SUM(days_covered) AS total_days_covered,

        -- Total number of fills
        SUM(num_fills) AS total_fills,

        -- Number of coverage periods
        COUNT(*) AS num_periods,

        -- Number of gaps
        SUM(CASE WHEN gap_days > 0 THEN 1 ELSE 0 END) AS num_gaps,

        -- Total gap days
        SUM(gap_days) AS total_gap_days,

        -- Maximum gap
        MAX(gap_days) AS max_gap_days,

        -- Average gap
        CASE
            WHEN SUM(CASE WHEN gap_days > 0 THEN 1 ELSE 0 END) > 0
            THEN AVG(CASE WHEN gap_days > 0 THEN gap_days ELSE NULL END)
            ELSE 0
        END AS avg_gap_days,

        -- First and last exposure dates
        MIN(period_start) AS first_exposure_date,
        MAX(period_end) AS last_exposure_date,

        -- Treatment duration (from first to last exposure)
        DAYS_BETWEEN(MIN(period_start), MAX(period_end)) + 1 AS treatment_duration,

        -- Calculate PDC = (Total Days Covered) / (Treatment Duration)
        CASE
            WHEN DAYS_BETWEEN(MIN(period_start), MAX(period_end)) + 1 > 0
            THEN ROUND(
                CAST(SUM(days_covered) AS DECIMAL(10,4)) /
                CAST(DAYS_BETWEEN(MIN(period_start), MAX(period_end)) + 1 AS DECIMAL(10,4)),
                4
            )
            ELSE 0
        END AS pdc

    FROM gaps
    GROUP BY person_id, drug_concept_id
),

-- Step 6: Join with concept table to get drug names
final_results AS (
    SELECT
        pdc.person_id,
        pdc.drug_concept_id,
        c.concept_name AS drug_name,
        c.concept_class_id,
        pdc.pdc,

        -- Adherence classification
        CASE
            WHEN pdc.pdc >= :pdc_threshold THEN 'Adherent'
            WHEN pdc.pdc >= :pdc_threshold - 0.1 THEN 'Moderately Adherent'
            ELSE 'Non-Adherent'
        END AS adherence_status,

        pdc.total_days_covered,
        pdc.treatment_duration,
        pdc.total_fills,
        pdc.num_periods,
        pdc.num_gaps,
        pdc.total_gap_days,
        pdc.max_gap_days,
        pdc.avg_gap_days,
        pdc.first_exposure_date,
        pdc.last_exposure_date

    FROM patient_drug_pdc pdc
    LEFT JOIN :schema.concept c
        ON pdc.drug_concept_id = c.concept_id

    WHERE pdc.treatment_duration >= 30  -- Filter for patients with at least 30 days of treatment
)

SELECT *
FROM final_results
ORDER BY person_id, drug_concept_id;
