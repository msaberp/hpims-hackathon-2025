/*
 * Step 3: Detailed Gap Analysis
 *
 * This query provides detailed information about each adherence gap:
 * - When gaps occur
 * - Duration of each gap
 * - Context around the gap (previous and next fills)
 *
 * Useful for understanding patterns in non-adherence
 *
 * Parameters to replace:
 * - :schema - Your OHDSI CDM schema name
 * - :start_date - Analysis start date
 * - :end_date - Analysis end date
 * - :min_gap_days - Minimum gap duration to report (e.g., 7)
 */

WITH drug_exposures_cleaned AS (
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
        END AS end_date,

        de.days_supply,
        de.quantity

    FROM :schema.drug_exposure de

    WHERE de.drug_exposure_start_date >= :start_date
      AND de.drug_exposure_start_date <= :end_date
      AND de.drug_type_concept_id IN (38000175, 38000176, 581373)
),

-- Identify gaps between consecutive fills
gaps_detail AS (
    SELECT
        person_id,
        drug_concept_id,
        drug_exposure_id,
        start_date,
        end_date,
        days_supply,
        quantity,

        -- Next fill information
        LEAD(drug_exposure_id) OVER (
            PARTITION BY person_id, drug_concept_id
            ORDER BY start_date
        ) AS next_exposure_id,

        LEAD(start_date) OVER (
            PARTITION BY person_id, drug_concept_id
            ORDER BY start_date
        ) AS next_start_date,

        -- Calculate gap
        CASE
            WHEN LEAD(start_date) OVER (
                PARTITION BY person_id, drug_concept_id
                ORDER BY start_date
            ) IS NOT NULL
            THEN DAYS_BETWEEN(
                end_date,
                LEAD(start_date) OVER (
                    PARTITION BY person_id, drug_concept_id
                    ORDER BY start_date
                )
            ) - 1
            ELSE NULL
        END AS gap_days,

        -- Gap start and end dates
        ADD_DAYS(end_date, 1) AS gap_start_date,

        CASE
            WHEN LEAD(start_date) OVER (
                PARTITION BY person_id, drug_concept_id
                ORDER BY start_date
            ) IS NOT NULL
            THEN ADD_DAYS(
                LEAD(start_date) OVER (
                    PARTITION BY person_id, drug_concept_id
                    ORDER BY start_date
                ),
                -1
            )
            ELSE NULL
        END AS gap_end_date,

        -- Sequence number
        ROW_NUMBER() OVER (
            PARTITION BY person_id, drug_concept_id
            ORDER BY start_date
        ) AS fill_sequence

    FROM drug_exposures_cleaned
),

-- Filter for significant gaps and add drug name
significant_gaps AS (
    SELECT
        g.person_id,
        g.drug_concept_id,
        c.concept_name AS drug_name,
        g.fill_sequence,
        g.drug_exposure_id AS fill_before_gap_id,
        g.start_date AS fill_before_gap_date,
        g.end_date AS fill_before_gap_end_date,
        g.gap_start_date,
        g.gap_end_date,
        g.gap_days,
        g.next_exposure_id AS fill_after_gap_id,
        g.next_start_date AS fill_after_gap_date,

        -- Categorize gap severity
        CASE
            WHEN g.gap_days >= 90 THEN 'Critical Gap (90+ days)'
            WHEN g.gap_days >= 30 THEN 'Major Gap (30-89 days)'
            WHEN g.gap_days >= 14 THEN 'Moderate Gap (14-29 days)'
            WHEN g.gap_days >= 7 THEN 'Minor Gap (7-13 days)'
            ELSE 'Minimal Gap (<7 days)'
        END AS gap_severity,

        -- Days supply of the fill before gap
        g.days_supply AS days_supply_before_gap,
        g.quantity AS quantity_before_gap

    FROM gaps_detail g
    LEFT JOIN :schema.concept c
        ON g.drug_concept_id = c.concept_id

    WHERE g.gap_days >= :min_gap_days  -- Only show gaps above threshold
      AND g.gap_days IS NOT NULL
)

SELECT
    person_id,
    drug_concept_id,
    drug_name,
    fill_sequence,
    fill_before_gap_date,
    fill_before_gap_end_date,
    gap_start_date,
    gap_end_date,
    gap_days,
    gap_severity,
    fill_after_gap_date,
    days_supply_before_gap,
    quantity_before_gap

FROM significant_gaps
ORDER BY person_id, drug_concept_id, fill_sequence;
