"""
AIR-MS Connection Helper (Version 3 - Smart Imputation + Age)

Improvements over v2:
- Smart days_supply imputation using drug-specific medians
- Age calculation from PERSON table
- Handles 90%+ NULL days_supply records intelligently
- Multi-level fallback for end date calculation
"""

import pandas as pd
import logging
from typing import Optional, Dict, Any
from datetime import datetime, timedelta

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class AdherenceAnalyzer:
    """Helper class for medication adherence analysis using airms_connect"""

    def __init__(self, airms_connection, schema: str = "CDMDEID"):
        """
        Initialize analyzer with airms connection

        Args:
            airms_connection: Connected airms object from airms_connect
            schema: Database schema name (default: CDMDEID)
        """
        self.airms = airms_connection
        self.schema = schema
        logger.info(f"AdherenceAnalyzer initialized with schema: {schema}")

    def execute_query(self, query: str, limit: Optional[int] = None) -> pd.DataFrame:
        """
        Execute SQL query and return results as DataFrame

        Args:
            query: SQL query string
            limit: Optional row limit (for testing)

        Returns:
            DataFrame with query results
        """
        try:
            # Add limit if specified
            if limit:
                query = f"SELECT TOP {limit} * FROM ({query}) AS limited_query"

            logger.info(f"Executing query (length: {len(query)} chars)")
            logger.debug(f"Query preview: {query[:200]}...")

            # Execute query - collect() returns a DataFrame directly
            df = self.airms.conn.sql(query).collect()

            logger.info(f"Query returned {len(df)} rows and {len(df.columns)} columns")

            # Convert Decimal columns to float for pandas compatibility
            from decimal import Decimal
            for col in df.columns:
                if len(df) > 0 and isinstance(df[col].iloc[0], Decimal):
                    df[col] = df[col].astype(float)

            return df

        except Exception as e:
            logger.error(f"Query execution error: {e}")
            raise

    def get_drug_exposures(
        self,
        start_date: str,
        end_date: str,
        limit: Optional[int] = None,
        filter_drug_type: bool = False
    ) -> pd.DataFrame:
        """
        Get drug exposure data with calculated end dates

        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            limit: Optional row limit for testing
            filter_drug_type: If True, filter by drug_type_concept_id (default: False)

        Returns:
            DataFrame with drug exposures
        """
        # Build drug_type filter if requested
        drug_type_filter = ""
        if filter_drug_type:
            drug_type_filter = "AND de.drug_type_concept_id IN (38000175, 38000176, 581373)"

        query = f"""
        WITH drug_median_supply AS (
            -- Calculate median days_supply per drug (for imputation)
            SELECT
                drug_concept_id,
                MEDIAN(days_supply) AS median_days_supply
            FROM {self.schema}.DRUG_EXPOSURE
            WHERE days_supply IS NOT NULL AND days_supply > 0
            GROUP BY drug_concept_id
        )

        SELECT
            de.person_id,
            de.drug_concept_id,
            de.drug_exposure_id,
            de.drug_exposure_start_date AS start_date,
            de.drug_exposure_end_date,
            de.days_supply,
            de.refills,
            de.quantity,
            de.drug_type_concept_id,

            dms.median_days_supply,

            -- Smart end date calculation with fallbacks
            CASE
                -- Priority 1: Use actual end_date if available
                WHEN de.drug_exposure_end_date IS NOT NULL
                    THEN de.drug_exposure_end_date

                -- Priority 2: Use days_supply if available
                WHEN de.days_supply IS NOT NULL AND de.days_supply > 0
                    THEN ADD_DAYS(de.drug_exposure_start_date, de.days_supply - 1)

                -- Priority 3: Use refills if available
                WHEN de.refills IS NOT NULL AND de.refills > 0
                    THEN ADD_DAYS(de.drug_exposure_start_date, (de.refills * 30) - 1)

                -- Priority 4: Use drug-specific median
                WHEN dms.median_days_supply IS NOT NULL
                    THEN ADD_DAYS(de.drug_exposure_start_date, dms.median_days_supply - 1)

                -- Priority 5: Default to 30 days
                ELSE ADD_DAYS(de.drug_exposure_start_date, 29)
            END AS calculated_end_date,

            -- Calculated days covered (for reference)
            CASE
                WHEN de.drug_exposure_end_date IS NOT NULL
                    THEN DAYS_BETWEEN(de.drug_exposure_start_date, de.drug_exposure_end_date) + 1
                WHEN de.days_supply IS NOT NULL AND de.days_supply > 0
                    THEN de.days_supply
                WHEN de.refills IS NOT NULL AND de.refills > 0
                    THEN de.refills * 30
                WHEN dms.median_days_supply IS NOT NULL
                    THEN dms.median_days_supply
                ELSE 30
            END AS days_covered,

            -- Flag indicating which method was used for imputation
            CASE
                WHEN de.drug_exposure_end_date IS NOT NULL THEN 'actual_end_date'
                WHEN de.days_supply IS NOT NULL AND de.days_supply > 0 THEN 'days_supply'
                WHEN de.refills IS NOT NULL AND de.refills > 0 THEN 'refills'
                WHEN dms.median_days_supply IS NOT NULL THEN 'drug_median'
                ELSE 'default_30'
            END AS imputation_method

        FROM {self.schema}.DRUG_EXPOSURE de
        LEFT JOIN drug_median_supply dms
            ON de.drug_concept_id = dms.drug_concept_id

        WHERE de.drug_exposure_start_date >= '{start_date}'
          AND de.drug_exposure_start_date <= '{end_date}'
          {drug_type_filter}

        ORDER BY de.person_id, de.drug_concept_id, de.drug_exposure_start_date
        """

        return self.execute_query(query, limit=limit)

    def calculate_pdc_server_side(
        self,
        start_date: str,
        end_date: str,
        pdc_threshold: float = 0.80,
        min_treatment_days: int = 30,
        filter_drug_type: bool = False
    ) -> pd.DataFrame:
        """
        Calculate PDC using server-side SQL with smart imputation and age

        Args:
            start_date: Analysis start date (YYYY-MM-DD)
            end_date: Analysis end date (YYYY-MM-DD)
            pdc_threshold: Adherence threshold (default: 0.80)
            min_treatment_days: Minimum treatment duration to include
            filter_drug_type: If True, filter by drug_type_concept_id (default: False)

        Returns:
            DataFrame with PDC calculations per patient per drug, including age
        """
        # Build drug_type filter if requested
        drug_type_filter = ""
        if filter_drug_type:
            drug_type_filter = "AND de.drug_type_concept_id IN (38000175, 38000176, 581373)"

        query = f"""
        WITH drug_median_supply AS (
            -- Calculate median days_supply per drug (for smart imputation)
            SELECT
                drug_concept_id,
                MEDIAN(days_supply) AS median_days_supply
            FROM {self.schema}.DRUG_EXPOSURE
            WHERE days_supply IS NOT NULL AND days_supply > 0
            GROUP BY drug_concept_id
        ),

        drug_exposures_with_dates AS (
            SELECT
                de.person_id,
                de.drug_concept_id,
                de.drug_exposure_id,
                de.drug_exposure_start_date AS start_date,

                -- Smart end date calculation with multi-level fallback
                CASE
                    -- Priority 1: Actual end_date
                    WHEN de.drug_exposure_end_date IS NOT NULL
                        THEN de.drug_exposure_end_date

                    -- Priority 2: days_supply
                    WHEN de.days_supply IS NOT NULL AND de.days_supply > 0
                        THEN ADD_DAYS(de.drug_exposure_start_date, de.days_supply - 1)

                    -- Priority 3: refills
                    WHEN de.refills IS NOT NULL AND de.refills > 0
                        THEN ADD_DAYS(de.drug_exposure_start_date, (de.refills * 30) - 1)

                    -- Priority 4: Drug-specific median
                    WHEN dms.median_days_supply IS NOT NULL
                        THEN ADD_DAYS(de.drug_exposure_start_date, dms.median_days_supply - 1)

                    -- Priority 5: Default 30 days
                    ELSE ADD_DAYS(de.drug_exposure_start_date, 29)
                END AS end_date,

                -- Days supply for reference
                CASE
                    WHEN de.days_supply IS NOT NULL AND de.days_supply > 0
                        THEN de.days_supply
                    WHEN de.refills IS NOT NULL AND de.refills > 0
                        THEN de.refills * 30
                    WHEN dms.median_days_supply IS NOT NULL
                        THEN dms.median_days_supply
                    ELSE 30
                END AS days_supply

            FROM {self.schema}.DRUG_EXPOSURE de
            LEFT JOIN drug_median_supply dms
                ON de.drug_concept_id = dms.drug_concept_id

            WHERE de.drug_exposure_start_date >= '{start_date}'
              AND de.drug_exposure_start_date <= '{end_date}'
              {drug_type_filter}
        ),

        -- Add previous end date using LAG (FIXED: partition by person AND drug)
        exposure_with_prev AS (
            SELECT
                person_id,
                drug_concept_id,
                drug_exposure_id,
                start_date,
                end_date,
                days_supply,
                LAG(end_date) OVER (
                    PARTITION BY person_id, drug_concept_id  -- FIXED: was missing person_id
                    ORDER BY start_date, end_date
                ) AS prev_end_date,
                ROW_NUMBER() OVER (
                    PARTITION BY person_id, drug_concept_id
                    ORDER BY start_date, end_date
                ) AS fill_number
            FROM drug_exposures_with_dates
        ),

        -- Calculate gaps and adjust coverage
        exposure_with_gaps AS (
            SELECT
                person_id,
                drug_concept_id,
                drug_exposure_id,
                start_date,
                end_date,
                days_supply,
                prev_end_date,
                fill_number,

                -- Gap days (if this fill starts after previous one ended)
                CASE
                    WHEN prev_end_date IS NOT NULL AND start_date > prev_end_date
                        THEN DAYS_BETWEEN(prev_end_date, start_date) - 1
                    ELSE 0
                END AS gap_days,

                -- Overlap days (if this fill starts before previous one ended)
                CASE
                    WHEN prev_end_date IS NOT NULL AND start_date <= prev_end_date
                        THEN DAYS_BETWEEN(start_date, prev_end_date)
                    ELSE 0
                END AS overlap_days,

                -- Adjusted days covered (subtract overlap to avoid double counting)
                CASE
                    WHEN prev_end_date IS NOT NULL AND start_date <= prev_end_date
                        -- Overlap case: only count days AFTER previous fill ended
                        THEN GREATEST(0, DAYS_BETWEEN(start_date, end_date) + 1 - DAYS_BETWEEN(start_date, prev_end_date) - 1)
                    ELSE
                        -- No overlap: count all days
                        DAYS_BETWEEN(start_date, end_date) + 1
                END AS adjusted_days_covered

            FROM exposure_with_prev
        ),

        -- Aggregate per patient-drug
        patient_drug_summary AS (
            SELECT
                person_id,
                drug_concept_id,

                -- Total adjusted days covered (no double counting)
                SUM(adjusted_days_covered) AS total_days_covered,

                -- Total fills
                COUNT(*) AS total_fills,

                -- Number of gaps
                SUM(CASE WHEN gap_days > 0 THEN 1 ELSE 0 END) AS num_gaps,

                -- Total gap days
                SUM(gap_days) AS total_gap_days,

                -- Maximum gap
                MAX(gap_days) AS max_gap_days,

                -- First and last dates
                MIN(start_date) AS first_exposure_date,
                MAX(end_date) AS last_exposure_date,

                -- Treatment duration
                DAYS_BETWEEN(MIN(start_date), MAX(end_date)) + 1 AS treatment_duration

            FROM exposure_with_gaps
            GROUP BY person_id, drug_concept_id
        ),

        -- Calculate PDC
        patient_drug_pdc AS (
            SELECT
                person_id,
                drug_concept_id,
                total_days_covered,
                total_fills,
                num_gaps,
                total_gap_days,
                max_gap_days,
                first_exposure_date,
                last_exposure_date,
                treatment_duration,

                -- PDC = days covered / treatment duration
                CASE
                    WHEN treatment_duration > 0
                        THEN ROUND(
                            CAST(total_days_covered AS DECIMAL(10,4)) /
                            CAST(treatment_duration AS DECIMAL(10,4)),
                            4
                        )
                    ELSE 0
                END AS pdc

            FROM patient_drug_summary
            WHERE treatment_duration >= {min_treatment_days}
        ),

        -- Get patient conditions (simplified - no nested window functions)
        condition_counts AS (
            SELECT
                co.person_id,
                co.condition_concept_id,
                cc.concept_name,
                COUNT(*) AS occurrence_count
            FROM {self.schema}.CONDITION_OCCURRENCE co
            LEFT JOIN {self.schema}.CONCEPT cc
                ON co.condition_concept_id = cc.concept_id
                AND cc.domain_id = 'Condition'
            WHERE co.condition_start_date <= '{end_date}'
              AND cc.concept_name IS NOT NULL
            GROUP BY co.person_id, co.condition_concept_id, cc.concept_name
        ),

        patient_primary_condition AS (
            SELECT
                person_id,
                concept_name AS primary_condition_name,
                ROW_NUMBER() OVER (
                    PARTITION BY person_id
                    ORDER BY occurrence_count DESC
                ) AS rn
            FROM condition_counts
        ),

        patient_condition_summary AS (
            SELECT
                cc.person_id,
                COUNT(DISTINCT cc.condition_concept_id) AS total_conditions,
                pc.primary_condition_name
            FROM condition_counts cc
            LEFT JOIN patient_primary_condition pc
                ON cc.person_id = pc.person_id
                AND pc.rn = 1
            GROUP BY cc.person_id, pc.primary_condition_name
        )

        -- Join with concept table for drug names, person table for age, and conditions
        SELECT
            pdc.person_id,
            pdc.drug_concept_id,
            COALESCE(c.concept_name, 'Unknown Drug') AS drug_name,
            c.concept_class_id,

            -- Age calculation (current year - birth year)
            (YEAR(CURRENT_DATE) - p.year_of_birth) AS age,

            -- Condition information
            COALESCE(pcs.primary_condition_name, 'No Condition Recorded') AS primary_condition,
            COALESCE(pcs.total_conditions, 0) AS comorbidity_count,

            -- Ensure PDC is between 0 and 1 (cap at 1.0 if calculation error)
            CASE
                WHEN pdc.pdc > 1.0 THEN 1.0
                WHEN pdc.pdc < 0.0 THEN 0.0
                ELSE pdc.pdc
            END AS pdc,

            CASE
                WHEN pdc.pdc >= {pdc_threshold} THEN 'Adherent'
                WHEN pdc.pdc >= {pdc_threshold - 0.1} THEN 'Moderately Adherent'
                ELSE 'Non-Adherent'
            END AS adherence_status,

            pdc.total_days_covered,
            pdc.treatment_duration,
            pdc.total_fills,
            pdc.num_gaps,
            pdc.total_gap_days,
            pdc.max_gap_days,
            pdc.first_exposure_date,
            pdc.last_exposure_date

        FROM patient_drug_pdc pdc
        LEFT JOIN {self.schema}.CONCEPT c
            ON pdc.drug_concept_id = c.concept_id
            AND c.domain_id = 'Drug'  -- Only match drug concepts
        LEFT JOIN {self.schema}.PERSON p
            ON pdc.person_id = p.person_id
        LEFT JOIN patient_condition_summary pcs
            ON pdc.person_id = pcs.person_id

        WHERE c.concept_name IS NOT NULL  -- Filter out records with no matching drug concept

        ORDER BY pdc.person_id, pdc.drug_concept_id
        """

        return self.execute_query(query)

    def get_detailed_gaps(
        self,
        start_date: str,
        end_date: str,
        min_gap_days: int = 7,
        filter_drug_type: bool = False
    ) -> pd.DataFrame:
        """
        Get detailed information about adherence gaps with smart imputation

        Args:
            start_date: Analysis start date (YYYY-MM-DD)
            end_date: Analysis end date (YYYY-MM-DD)
            min_gap_days: Minimum gap duration to report
            filter_drug_type: If True, filter by drug_type_concept_id (default: False)

        Returns:
            DataFrame with detailed gap information
        """
        # Build drug_type filter if requested
        drug_type_filter = ""
        if filter_drug_type:
            drug_type_filter = "AND de.drug_type_concept_id IN (38000175, 38000176, 581373)"

        query = f"""
        WITH drug_median_supply AS (
            SELECT
                drug_concept_id,
                MEDIAN(days_supply) AS median_days_supply
            FROM {self.schema}.DRUG_EXPOSURE
            WHERE days_supply IS NOT NULL AND days_supply > 0
            GROUP BY drug_concept_id
        ),

        drug_exposures_cleaned AS (
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
                    WHEN dms.median_days_supply IS NOT NULL
                        THEN ADD_DAYS(de.drug_exposure_start_date, dms.median_days_supply - 1)
                    ELSE ADD_DAYS(de.drug_exposure_start_date, 29)
                END AS end_date,

                de.days_supply,
                de.quantity

            FROM {self.schema}.DRUG_EXPOSURE de
            LEFT JOIN drug_median_supply dms
                ON de.drug_concept_id = dms.drug_concept_id

            WHERE de.drug_exposure_start_date >= '{start_date}'
              AND de.drug_exposure_start_date <= '{end_date}'
              {drug_type_filter}
        ),

        gaps_detail AS (
            SELECT
                person_id,
                drug_concept_id,
                drug_exposure_id,
                start_date,
                end_date,
                days_supply,
                quantity,

                LEAD(start_date) OVER (
                    PARTITION BY person_id, drug_concept_id
                    ORDER BY start_date
                ) AS next_start_date,

                LEAD(drug_exposure_id) OVER (
                    PARTITION BY person_id, drug_concept_id
                    ORDER BY start_date
                ) AS next_exposure_id,

                ROW_NUMBER() OVER (
                    PARTITION BY person_id, drug_concept_id
                    ORDER BY start_date
                ) AS fill_sequence

            FROM drug_exposures_cleaned
        ),

        gaps_calculated AS (
            SELECT
                person_id,
                drug_concept_id,
                drug_exposure_id,
                start_date,
                end_date,
                days_supply,
                quantity,
                next_start_date,
                next_exposure_id,
                fill_sequence,

                -- Calculate gap days
                CASE
                    WHEN next_start_date IS NOT NULL AND next_start_date > end_date
                        THEN DAYS_BETWEEN(end_date, next_start_date) - 1
                    ELSE 0
                END AS gap_days

            FROM gaps_detail
        )

        SELECT
            g.person_id,
            g.drug_concept_id,
            c.concept_name AS drug_name,
            g.fill_sequence,
            g.start_date AS fill_before_gap_date,
            g.end_date AS fill_before_gap_end_date,
            ADD_DAYS(g.end_date, 1) AS gap_start_date,
            ADD_DAYS(g.next_start_date, -1) AS gap_end_date,
            g.gap_days,

            CASE
                WHEN g.gap_days >= 90 THEN 'Critical Gap (90+ days)'
                WHEN g.gap_days >= 30 THEN 'Major Gap (30-89 days)'
                WHEN g.gap_days >= 14 THEN 'Moderate Gap (14-29 days)'
                WHEN g.gap_days >= 7 THEN 'Minor Gap (7-13 days)'
                ELSE 'Minimal Gap (<7 days)'
            END AS gap_severity,

            g.next_start_date AS fill_after_gap_date,
            g.days_supply AS days_supply_before_gap

        FROM gaps_calculated g
        LEFT JOIN {self.schema}.CONCEPT c
            ON g.drug_concept_id = c.concept_id

        WHERE g.gap_days >= {min_gap_days}

        ORDER BY g.person_id, g.drug_concept_id, g.fill_sequence
        """

        return self.execute_query(query)

    def get_database_info(self) -> Dict[str, Any]:
        """
        Get information about the database tables and data availability

        Returns:
            Dictionary with database information
        """
        info = {}

        try:
            # Total rows in DRUG_EXPOSURE
            query = f"SELECT COUNT(*) as total_rows FROM {self.schema}.DRUG_EXPOSURE"
            result = self.airms.conn.sql(query).collect()
            info['total_drug_exposures'] = result['TOTAL_ROWS'].iloc[0]

            # Date range
            query = f"""
            SELECT
                MIN(drug_exposure_start_date) as min_date,
                MAX(drug_exposure_start_date) as max_date
            FROM {self.schema}.DRUG_EXPOSURE
            """
            result = self.airms.conn.sql(query).collect()
            info['date_range'] = {
                'min': result['MIN_DATE'].iloc[0],
                'max': result['MAX_DATE'].iloc[0]
            }

            # Unique patients
            query = f"SELECT COUNT(DISTINCT person_id) as cnt FROM {self.schema}.DRUG_EXPOSURE"
            result = self.airms.conn.sql(query).collect()
            info['unique_patients'] = result['CNT'].iloc[0]

            # Unique drugs
            query = f"SELECT COUNT(DISTINCT drug_concept_id) as cnt FROM {self.schema}.DRUG_EXPOSURE"
            result = self.airms.conn.sql(query).collect()
            info['unique_drugs'] = result['CNT'].iloc[0]

            # Days supply statistics
            query = f"""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN days_supply IS NULL THEN 1 ELSE 0 END) as null_days_supply,
                ROUND(100.0 * SUM(CASE WHEN days_supply IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) as pct_null
            FROM {self.schema}.DRUG_EXPOSURE
            """
            result = self.airms.conn.sql(query).collect()
            info['days_supply_stats'] = {
                'total': result['TOTAL'].iloc[0],
                'null_count': result['NULL_DAYS_SUPPLY'].iloc[0],
                'pct_null': result['PCT_NULL'].iloc[0]
            }

        except Exception as e:
            logger.error(f"Error getting database info: {e}")
            info['error'] = str(e)

        return info


def get_date_range(months_back: int = 12) -> tuple[str, str]:
    """
    Calculate date range for analysis

    Args:
        months_back: Number of months to look back from today

    Returns:
        Tuple of (start_date, end_date) as strings in YYYY-MM-DD format
    """
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=months_back * 30)

    return str(start_date), str(end_date)


def get_actual_date_range(airms_connection, schema: str = "CDMDEID") -> tuple[str, str]:
    """
    Get the actual date range available in the database

    Args:
        airms_connection: Connected airms object
        schema: Database schema name

    Returns:
        Tuple of (start_date, end_date) from the actual data
    """
    query = f"""
    SELECT
        MIN(drug_exposure_start_date) as min_date,
        MAX(drug_exposure_start_date) as max_date
    FROM {schema}.DRUG_EXPOSURE
    """

    result = airms_connection.conn.sql(query).collect()
    start_date = str(result['MIN_DATE'].iloc[0])
    end_date = str(result['MAX_DATE'].iloc[0])

    return start_date, end_date
