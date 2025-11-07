/*
 * Step 1: Calculate Drug Exposure Coverage Periods
 *
 * This query extracts drug exposure records and calculates:
 * - End dates using days_supply or refills (with fallback logic)
 * - Standardized coverage periods per patient per drug
 *
 * Optimized for SAP HANA with millions of records
 *
 * Parameters to replace:
 * - :schema - Your OHDSI CDM schema name
 * - :start_date - Analysis start date (YYYY-MM-DD)
 * - :end_date - Analysis end date (YYYY-MM-DD)
 */

WITH drug_exposures_cleaned AS (
    SELECT
        de.person_id,
        de.drug_concept_id,
        de.drug_exposure_id,
        de.drug_exposure_start_date AS start_date,

        -- Calculate end date with fallback logic
        CASE
            -- If end_date is provided and valid, use it
            WHEN de.drug_exposure_end_date IS NOT NULL
                THEN de.drug_exposure_end_date

            -- If days_supply is available, calculate end_date
            WHEN de.days_supply IS NOT NULL AND de.days_supply > 0
                THEN ADD_DAYS(de.drug_exposure_start_date, de.days_supply - 1)

            -- If refills available, estimate days_supply (assume 30 days per fill)
            WHEN de.refills IS NOT NULL AND de.refills > 0
                THEN ADD_DAYS(de.drug_exposure_start_date, (de.refills * 30) - 1)

            -- Default: assume 30 days supply
            ELSE ADD_DAYS(de.drug_exposure_start_date, 29)
        END AS end_date,

        de.days_supply,
        de.refills,
        de.quantity,
        de.drug_type_concept_id,

        -- Calculate actual days covered by this exposure
        CASE
            WHEN de.drug_exposure_end_date IS NOT NULL
                THEN DAYS_BETWEEN(de.drug_exposure_start_date, de.drug_exposure_end_date) + 1
            WHEN de.days_supply IS NOT NULL AND de.days_supply > 0
                THEN de.days_supply
            WHEN de.refills IS NOT NULL AND de.refills > 0
                THEN de.refills * 30
            ELSE 30
        END AS days_covered

    FROM :schema.drug_exposure de

    -- Filter for the analysis period
    WHERE de.drug_exposure_start_date >= :start_date
      AND de.drug_exposure_start_date <= :end_date

      -- Filter for dispensed/prescribed records only (exclude administered)
      -- Adjust drug_type_concept_id values based on your data
      AND de.drug_type_concept_id IN (
          38000175,  -- Prescription dispensed in pharmacy
          38000176,  -- Prescription dispensed through mail order
          581373     -- Prescription written
      )
)

SELECT
    person_id,
    drug_concept_id,
    drug_exposure_id,
    start_date,
    end_date,
    days_covered,
    days_supply,
    refills,
    quantity,
    drug_type_concept_id
FROM drug_exposures_cleaned
ORDER BY person_id, drug_concept_id, start_date;
