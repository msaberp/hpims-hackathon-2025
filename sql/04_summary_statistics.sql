/*
 * Step 4: Summary Statistics for Adherence Analysis
 *
 * This query generates aggregate statistics:
 * - Overall adherence rates
 * - Distribution by drug
 * - Patient counts by adherence status
 *
 * Optimized for quick dashboard generation without pulling millions of rows
 *
 * Parameters to replace:
 * - :schema - Your OHDSI CDM schema name
 * - :start_date - Analysis start date
 * - :end_date - Analysis end date
 * - :pdc_threshold - Adherence threshold (e.g., 0.80)
 */

-- Re-use PDC calculation logic (can also create as temp table/view)
WITH patient_drug_pdc AS (
    -- [Include the PDC calculation from 02_calculate_pdc_and_gaps.sql]
    -- For brevity, this references the previous query logic
    SELECT
        person_id,
        drug_concept_id,
        pdc,
        CASE
            WHEN pdc >= :pdc_threshold THEN 'Adherent'
            WHEN pdc >= :pdc_threshold - 0.1 THEN 'Moderately Adherent'
            ELSE 'Non-Adherent'
        END AS adherence_status,
        total_days_covered,
        treatment_duration,
        total_fills,
        num_gaps,
        total_gap_days
    FROM (
        -- [PDC calculation subquery would go here]
        -- Simplified for this example
        SELECT
            person_id,
            drug_concept_id,
            0.75 AS pdc,  -- Placeholder
            100 AS total_days_covered,
            120 AS treatment_duration,
            4 AS total_fills,
            2 AS num_gaps,
            20 AS total_gap_days
        FROM :schema.drug_exposure
        WHERE 1=0  -- Placeholder, replace with actual logic
    ) sub
),

-- Overall statistics
overall_stats AS (
    SELECT
        'Overall' AS category,
        'All Drugs' AS subcategory,
        COUNT(DISTINCT person_id) AS unique_patients,
        COUNT(*) AS total_patient_drug_combinations,

        -- Adherence distribution
        SUM(CASE WHEN adherence_status = 'Adherent' THEN 1 ELSE 0 END) AS adherent_count,
        SUM(CASE WHEN adherence_status = 'Moderately Adherent' THEN 1 ELSE 0 END) AS moderately_adherent_count,
        SUM(CASE WHEN adherence_status = 'Non-Adherent' THEN 1 ELSE 0 END) AS non_adherent_count,

        -- Percentages
        ROUND(
            CAST(SUM(CASE WHEN adherence_status = 'Adherent' THEN 1 ELSE 0 END) AS DECIMAL) /
            CAST(COUNT(*) AS DECIMAL) * 100,
            2
        ) AS pct_adherent,

        -- PDC statistics
        ROUND(AVG(pdc), 4) AS avg_pdc,
        ROUND(MIN(pdc), 4) AS min_pdc,
        ROUND(MAX(pdc), 4) AS max_pdc,
        ROUND(STDDEV(pdc), 4) AS stddev_pdc,

        -- Gap statistics
        ROUND(AVG(num_gaps), 2) AS avg_gaps_per_patient,
        ROUND(AVG(total_gap_days), 2) AS avg_gap_days_per_patient,
        MAX(total_gap_days) AS max_total_gap_days

    FROM patient_drug_pdc
),

-- Statistics by drug
drug_stats AS (
    SELECT
        'By Drug' AS category,
        c.concept_name AS subcategory,
        COUNT(DISTINCT pdc.person_id) AS unique_patients,
        COUNT(*) AS total_patient_drug_combinations,

        SUM(CASE WHEN pdc.adherence_status = 'Adherent' THEN 1 ELSE 0 END) AS adherent_count,
        SUM(CASE WHEN pdc.adherence_status = 'Moderately Adherent' THEN 1 ELSE 0 END) AS moderately_adherent_count,
        SUM(CASE WHEN pdc.adherence_status = 'Non-Adherent' THEN 1 ELSE 0 END) AS non_adherent_count,

        ROUND(
            CAST(SUM(CASE WHEN pdc.adherence_status = 'Adherent' THEN 1 ELSE 0 END) AS DECIMAL) /
            CAST(COUNT(*) AS DECIMAL) * 100,
            2
        ) AS pct_adherent,

        ROUND(AVG(pdc.pdc), 4) AS avg_pdc,
        ROUND(MIN(pdc.pdc), 4) AS min_pdc,
        ROUND(MAX(pdc.pdc), 4) AS max_pdc,
        ROUND(STDDEV(pdc.pdc), 4) AS stddev_pdc,

        ROUND(AVG(pdc.num_gaps), 2) AS avg_gaps_per_patient,
        ROUND(AVG(pdc.total_gap_days), 2) AS avg_gap_days_per_patient,
        MAX(pdc.total_gap_days) AS max_total_gap_days

    FROM patient_drug_pdc pdc
    LEFT JOIN :schema.concept c ON pdc.drug_concept_id = c.concept_id
    GROUP BY c.concept_name
    HAVING COUNT(DISTINCT pdc.person_id) >= 10  -- Only show drugs with 10+ patients
),

-- PDC distribution bins
pdc_distribution AS (
    SELECT
        'PDC Distribution' AS category,
        CASE
            WHEN pdc >= 0.9 THEN '90-100%'
            WHEN pdc >= 0.8 THEN '80-89%'
            WHEN pdc >= 0.7 THEN '70-79%'
            WHEN pdc >= 0.6 THEN '60-69%'
            WHEN pdc >= 0.5 THEN '50-59%'
            ELSE '<50%'
        END AS subcategory,
        COUNT(*) AS total_patient_drug_combinations,
        COUNT(DISTINCT person_id) AS unique_patients,

        -- Placeholder columns for consistency
        0 AS adherent_count,
        0 AS moderately_adherent_count,
        0 AS non_adherent_count,
        0.0 AS pct_adherent,
        0.0 AS avg_pdc,
        0.0 AS min_pdc,
        0.0 AS max_pdc,
        0.0 AS stddev_pdc,
        0.0 AS avg_gaps_per_patient,
        0.0 AS avg_gap_days_per_patient,
        0 AS max_total_gap_days

    FROM patient_drug_pdc
    GROUP BY
        CASE
            WHEN pdc >= 0.9 THEN '90-100%'
            WHEN pdc >= 0.8 THEN '80-89%'
            WHEN pdc >= 0.7 THEN '70-79%'
            WHEN pdc >= 0.6 THEN '60-69%'
            WHEN pdc >= 0.5 THEN '50-59%'
            ELSE '<50%'
        END
)

-- Combine all statistics
SELECT * FROM overall_stats
UNION ALL
SELECT * FROM drug_stats
UNION ALL
SELECT * FROM pdc_distribution
ORDER BY
    CASE category
        WHEN 'Overall' THEN 1
        WHEN 'By Drug' THEN 2
        WHEN 'PDC Distribution' THEN 3
    END,
    subcategory;
