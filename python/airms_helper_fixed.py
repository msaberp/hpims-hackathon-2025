"""
AIR-MS Connection Helper (FIXED VERSION)

Fixed to work with airms_connect which returns DataFrames directly from .collect()
Column names are UPPERCASE in the results.
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

            -- Calculate end date with fallback logic
            CASE
                WHEN de.drug_exposure_end_date IS NOT NULL
                    THEN de.drug_exposure_end_date
                WHEN de.days_supply IS NOT NULL AND de.days_supply > 0
                    THEN ADD_DAYS(de.drug_exposure_start_date, de.days_supply - 1)
                WHEN de.refills IS NOT NULL AND de.refills > 0
                    THEN ADD_DAYS(de.drug_exposure_start_date, (de.refills * 30) - 1)
                ELSE ADD_DAYS(de.drug_exposure_start_date, 29)
            END AS calculated_end_date,

            -- Days covered
            CASE
                WHEN de.drug_exposure_end_date IS NOT NULL
                    THEN DAYS_BETWEEN(de.drug_exposure_start_date, de.drug_exposure_end_date) + 1
                WHEN de.days_supply IS NOT NULL AND de.days_supply > 0
                    THEN de.days_supply
                WHEN de.refills IS NOT NULL AND de.refills > 0
                    THEN de.refills * 30
                ELSE 30
            END AS days_covered

        FROM {self.schema}.DRUG_EXPOSURE de

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
        Calculate PDC using server-side SQL (recommended for large datasets)

        Args:
            start_date: Analysis start date (YYYY-MM-DD)
            end_date: Analysis end date (YYYY-MM-DD)
            pdc_threshold: Adherence threshold (default: 0.80)
            min_treatment_days: Minimum treatment duration to include
            filter_drug_type: If True, filter by drug_type_concept_id (default: False)

        Returns:
            DataFrame with PDC calculations per patient per drug
        """
        # Build drug_type filter if requested
        drug_type_filter = ""
        if filter_drug_type:
            drug_type_filter = "AND de.drug_type_concept_id IN (38000175, 38000176, 581373)"

        query = f"""
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
                END AS end_date

            FROM {self.schema}.DRUG_EXPOSURE de

            WHERE de.drug_exposure_start_date >= '{start_date}'
              AND de.drug_exposure_start_date <= '{end_date}'
              {drug_type_filter}
        ),

        coverage_groups AS (
            SELECT
                person_id,
                drug_concept_id,
                drug_exposure_id,
                start_date,
                end_date,

                SUM(CASE
                    WHEN start_date <= COALESCE(
                        MAX(end_date) OVER (
                            PARTITION BY person_id, drug_concept_id
                            ORDER BY start_date, end_date
                            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                        ), TO_DATE('1900-01-01', 'YYYY-MM-DD')
                    ) + 1
                    THEN 0
                    ELSE 1
                END) OVER (
                    PARTITION BY person_id, drug_concept_id
                    ORDER BY start_date, end_date
                    ROWS UNBOUNDED PRECEDING
                ) AS coverage_group

            FROM drug_exposures_cleaned
        ),

        merged_periods AS (
            SELECT
                person_id,
                drug_concept_id,
                coverage_group,
                MIN(start_date) AS period_start,
                MAX(end_date) AS period_end,
                COUNT(*) AS num_fills,
                DAYS_BETWEEN(MIN(start_date), MAX(end_date)) + 1 AS days_covered

            FROM coverage_groups
            GROUP BY person_id, drug_concept_id, coverage_group
        ),

        gaps AS (
            SELECT
                person_id,
                drug_concept_id,
                period_start,
                period_end,
                days_covered,
                num_fills,

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
                    ) - 1
                    ELSE 0
                END AS gap_days

            FROM merged_periods
        ),

        patient_drug_pdc AS (
            SELECT
                person_id,
                drug_concept_id,
                SUM(days_covered) AS total_days_covered,
                SUM(num_fills) AS total_fills,
                COUNT(*) AS num_periods,
                SUM(CASE WHEN gap_days > 0 THEN 1 ELSE 0 END) AS num_gaps,
                SUM(gap_days) AS total_gap_days,
                MAX(gap_days) AS max_gap_days,
                MIN(period_start) AS first_exposure_date,
                MAX(period_end) AS last_exposure_date,
                DAYS_BETWEEN(MIN(period_start), MAX(period_end)) + 1 AS treatment_duration,

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
        )

        SELECT
            pdc.person_id,
            pdc.drug_concept_id,
            c.concept_name AS drug_name,
            c.concept_class_id,
            pdc.pdc,

            CASE
                WHEN pdc.pdc >= {pdc_threshold} THEN 'Adherent'
                WHEN pdc.pdc >= {pdc_threshold - 0.1} THEN 'Moderately Adherent'
                ELSE 'Non-Adherent'
            END AS adherence_status,

            pdc.total_days_covered,
            pdc.treatment_duration,
            pdc.total_fills,
            pdc.num_periods,
            pdc.num_gaps,
            pdc.total_gap_days,
            pdc.max_gap_days,
            pdc.first_exposure_date,
            pdc.last_exposure_date

        FROM patient_drug_pdc pdc
        LEFT JOIN {self.schema}.CONCEPT c
            ON pdc.drug_concept_id = c.concept_id

        WHERE pdc.treatment_duration >= {min_treatment_days}
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
        Get detailed information about adherence gaps

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

            FROM {self.schema}.DRUG_EXPOSURE de

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

                ROW_NUMBER() OVER (
                    PARTITION BY person_id, drug_concept_id
                    ORDER BY start_date
                ) AS fill_sequence

            FROM drug_exposures_cleaned
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

        FROM gaps_detail g
        LEFT JOIN {self.schema}.CONCEPT c
            ON g.drug_concept_id = c.concept_id

        WHERE g.gap_days >= {min_gap_days}
          AND g.gap_days IS NOT NULL

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
