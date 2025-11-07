"""
AIR-MS Connection Helper

Wrapper functions for working with the airms_connect library
to query medication adherence data from OHDSI CDM database.
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

            # Execute query using airms connection
            result = self.airms.conn.sql(query).collect()

            # Convert to DataFrame
            df = pd.DataFrame(result)
            logger.info(f"Query returned {len(df)} rows and {len(df.columns)} columns")

            return df

        except Exception as e:
            logger.error(f"Query execution error: {e}")
            raise

    def get_drug_exposures(
        self,
        start_date: str,
        end_date: str,
        limit: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Get drug exposure data with calculated end dates

        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            limit: Optional row limit for testing

        Returns:
            DataFrame with drug exposures
        """
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
          AND de.drug_type_concept_id IN (38000175, 38000176, 581373)

        ORDER BY de.person_id, de.drug_concept_id, de.drug_exposure_start_date
        """

        return self.execute_query(query, limit=limit)

    def calculate_pdc_server_side(
        self,
        start_date: str,
        end_date: str,
        pdc_threshold: float = 0.80,
        min_treatment_days: int = 30
    ) -> pd.DataFrame:
        """
        Calculate PDC using server-side SQL (recommended for large datasets)

        Args:
            start_date: Analysis start date (YYYY-MM-DD)
            end_date: Analysis end date (YYYY-MM-DD)
            pdc_threshold: Adherence threshold (default: 0.80)
            min_treatment_days: Minimum treatment duration to include

        Returns:
            DataFrame with PDC calculations per patient per drug
        """
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
              AND de.drug_type_concept_id IN (38000175, 38000176, 581373)
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
        min_gap_days: int = 7
    ) -> pd.DataFrame:
        """
        Get detailed information about adherence gaps

        Args:
            start_date: Analysis start date (YYYY-MM-DD)
            end_date: Analysis end date (YYYY-MM-DD)
            min_gap_days: Minimum gap duration to report

        Returns:
            DataFrame with detailed gap information
        """
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
              AND de.drug_type_concept_id IN (38000175, 38000176, 581373)
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

    def get_summary_statistics(
        self,
        start_date: str,
        end_date: str,
        pdc_threshold: float = 0.80
    ) -> Dict[str, pd.DataFrame]:
        """
        Get summary statistics for adherence analysis

        Args:
            start_date: Analysis start date
            end_date: Analysis end date
            pdc_threshold: Adherence threshold

        Returns:
            Dictionary containing different summary DataFrames
        """
        # First, get the full PDC data
        pdc_df = self.calculate_pdc_server_side(start_date, end_date, pdc_threshold)

        # Calculate overall statistics
        overall_stats = {
            'total_patients': pdc_df['person_id'].nunique(),
            'total_patient_drug_combinations': len(pdc_df),
            'adherent_count': (pdc_df['adherence_status'] == 'Adherent').sum(),
            'adherent_percentage': (pdc_df['adherence_status'] == 'Adherent').sum() / len(pdc_df) * 100,
            'avg_pdc': pdc_df['pdc'].mean(),
            'median_pdc': pdc_df['pdc'].median(),
            'avg_gaps': pdc_df['num_gaps'].mean(),
            'avg_gap_days': pdc_df['total_gap_days'].mean()
        }

        # Statistics by drug
        drug_stats = pdc_df.groupby('drug_name').agg({
            'person_id': 'nunique',
            'pdc': ['mean', 'median', 'std'],
            'adherence_status': lambda x: (x == 'Adherent').sum() / len(x) * 100,
            'num_gaps': 'mean',
            'total_gap_days': 'mean'
        }).reset_index()

        # PDC distribution
        pdc_distribution = pd.cut(
            pdc_df['pdc'],
            bins=[0, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0],
            labels=['<50%', '50-59%', '60-69%', '70-79%', '80-89%', '90-100%']
        ).value_counts().sort_index()

        return {
            'overall': pd.DataFrame([overall_stats]),
            'by_drug': drug_stats,
            'pdc_distribution': pd.DataFrame(pdc_distribution),
            'full_data': pdc_df
        }


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
