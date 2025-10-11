import logging
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

from .base import BaseDataSource

try:
    from google.cloud import bigquery
    BIGQUERY_AVAILABLE = True
except ImportError:
    BIGQUERY_AVAILABLE = False

class BigQueryDataSource(BaseDataSource):
    def __init__(self, project_id: Optional[str] = None, region: str = "us"):
        self.project_id = project_id
        self.region = region
        self.client: Optional[bigquery.Client] = None

    async def connect(self) -> None:
        if not BIGQUERY_AVAILABLE:
            logging.warning("google-cloud-bigquery not installed. Using mock data.")
            return

        try:
            self.client = bigquery.Client(project=self.project_id)
            self.project_id = self.client.project
            logging.info(f"Connected to BigQuery project: {self.project_id}")
        except Exception as e:
            logging.error(f"Could not connect to BigQuery: {e}")
            logging.warning("Using mock data instead.")
            self.client = None

    async def get_cost_summary(self) -> Dict[str, Any]:
        if self.client:
            query = f"""
            SELECT
                SUM(total_bytes_billed) / POW(10, 12) * 5 as total_cost_usd,
                COUNT(*) as total_queries,
                AVG(total_bytes_billed) / POW(10, 12) * 5 as avg_cost_per_query
            FROM `{self.project_id}.region-{self.region}.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
            WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
                AND statement_type = 'SELECT'
                AND state = 'DONE'
            """
            try:
                job = self.client.query(query)
                results = list(job.result())
                if results and results[0].total_cost_usd is not None:
                    row = results[0]
                    return {
                        "total_cost": round(row.total_cost_usd, 2),
                        "queries_run": row.total_queries,
                        "avg_cost_per_query": round(row.avg_cost_per_query, 3),
                        "period_days": 30,
                        "last_updated": datetime.now().isoformat()
                    }
            except Exception as e:
                logging.error(f"Error running get_cost_summary query: {e}")

        return {
            "total_cost": 1240.50,
            "queries_run": 8450,
            "avg_cost_per_query": 0.147,
            "potential_savings": 285.40,
            "period_days": 30,
            "last_updated": datetime.now().isoformat()
        }

    async def get_expensive_queries(self) -> Dict[str, Any]:
        if self.client:
            query = f"""
            SELECT
                job_id,
                query,
                user_email,
                TIMESTAMP_MILLIS(creation_time) as timestamp,
                total_bytes_billed / POW(10, 12) * 5 as cost_usd,
                total_bytes_processed / POW(10, 12) as tb_processed,
                total_slot_ms / 1000 as duration_seconds
            FROM `{self.project_id}.region-{self.region}.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
            WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
                AND statement_type = 'SELECT'
                AND state = 'DONE'
                AND total_bytes_billed > 0
            ORDER BY total_bytes_billed DESC
            LIMIT 20
            """
            try:
                job = self.client.query(query)
                results = list(job.result())
                queries = []
                for row in results:
                    optimization = self._analyze_query_for_optimization(row.query)
                    queries.append({
                        "id": row.job_id,
                        "query": row.query[:200] + "..." if len(row.query) > 200 else row.query,
                        "cost": round(row.cost_usd, 2),
                        "bytes_processed": f"{row.tb_processed:.2f} TB",
                        "duration": f"{int(row.duration_seconds)}s",
                        "user": row.user_email,
                        "timestamp": row.timestamp.isoformat(),
                        "optimization": optimization["suggestion"],
                        "potential_savings": round(row.cost_usd * optimization["savings_percent"] / 100, 2),
                        "severity": optimization["severity"]
                    })
                return {"queries": queries}
            except Exception as e:
                logging.error(f"Error running get_expensive_queries query: {e}")

        return {
            "queries": [
                {
                    "id": "job_123",
                    "query": "SELECT * FROM `project.dataset.large_table` WHERE date > '2024-01-01'",
                    "cost": 45.20,
                    "bytes_processed": "2.3 TB",
                    "duration": "45s",
                    "user": "data-team@company.com",
                    "timestamp": datetime.now().isoformat(),
                    "optimization": "Use partitioning filter on date column",
                    "potential_savings": 38.50,
                    "severity": "high"
                }
            ]
        }

    async def get_project_costs(self) -> Dict[str, Any]:
        if self.client:
            query = f"""
            SELECT
                project_id,
                SUM(total_bytes_billed) / POW(10, 12) * 5 as cost_usd
            FROM `{self.project_id}.region-{self.region}.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
            WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
                AND statement_type = 'SELECT'
                AND state = 'DONE'
            GROUP BY project_id
            ORDER BY cost_usd DESC
            """
            try:
                job = self.client.query(query)
                results = list(job.result())
                return {
                    "projects": [
                        {"name": row.project_id, "cost": round(row.cost_usd, 2)}
                        for row in results
                    ]
                }
            except Exception as e:
                logging.error(f"Error running get_project_costs query: {e}")

        return {
            "projects": [
                {"name": "Analytics", "cost": 450.00},
                {"name": "Data Science", "cost": 320.00},
                {"name": "Marketing", "cost": 180.00},
                {"name": "Engineering", "cost": 290.00}
            ]
        }

    async def get_cost_trends(self) -> Dict[str, Any]:
        if self.client:
            query = f"""
            SELECT
                DATE(creation_time) as date,
                SUM(total_bytes_billed) / POW(10, 12) * 5 as cost_usd,
                COUNT(*) as query_count
            FROM `{self.project_id}.region-{self.region}.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
            WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
                AND statement_type = 'SELECT'
                AND state = 'DONE'
            GROUP BY date
            ORDER BY date
            """
            try:
                job = self.client.query(query)
                results = list(job.result())
                return {
                    "trends": [
                        {
                            "date": row.date.strftime("%m/%d"),
                            "cost": round(row.cost_usd, 2),
                            "queries": row.query_count
                        }
                        for row in results
                    ]
                }
            except Exception as e:
                logging.error(f"Error running get_cost_trends query: {e}")

        trends = []
        for i in range(7):
            date = datetime.now() - timedelta(days=6-i)
            trends.append({
                "date": date.strftime("%m/%d"),
                "cost": 150 + (i * 10),
                "queries": 1200 + (i * 100)
            })
        return {"trends": trends}

    async def analyze_query_cost(self, query: str, dry_run: bool = True) -> Dict[str, Any]:
        if self.client and dry_run:
            job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
            try:
                job = self.client.query(query, job_config=job_config)
                bytes_processed = job.total_bytes_processed
                cost = (bytes_processed / (1024**4)) * 5  # $5 per TB
                optimization = self._analyze_query_for_optimization(query)
                return {
                    "bytes_to_process": f"{bytes_processed / (1024**3):.2f} GB",
                    "estimated_cost": round(cost, 4),
                    "optimization": optimization
                }
            except Exception as e:
                logging.error(f"Error running analyze_query_cost: {e}")
                return {"error": str(e)}

        return {
            "bytes_to_process": "1.5 GB",
            "estimated_cost": 0.0075,
            "optimization": {
                "suggestion": "Query analysis requires BigQuery connection",
                "severity": "info",
                "savings_percent": 0
            }
        }

    async def get_cost_by_user(self, days: int) -> Dict[str, Any]:
        if self.client:
            query = f"""
            SELECT
                user_email,
                SUM(total_bytes_billed) / POW(10, 12) * 5 as cost_usd,
                COUNT(*) as query_count
            FROM `{self.project_id}.region-{self.region}.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
            WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @days DAY)
                AND statement_type = 'SELECT'
                AND state = 'DONE'
            GROUP BY user_email
            ORDER BY cost_usd DESC
            LIMIT 20
            """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("days", "INT64", days),
                ]
            )
            try:
                job = self.client.query(query, job_config=job_config)
                results = list(job.result())
                return {
                    "users": [
                        {
                            "email": row.user_email,
                            "cost": round(row.cost_usd, 2),
                            "queries": row.query_count
                        }
                        for row in results
                    ]
                }
            except Exception as e:
                logging.error(f"Error running get_cost_by_user query: {e}")

        return {
            "users": [
                {"email": "data-team@company.com", "cost": 450.50, "queries": 2340},
                {"email": "analytics@company.com", "cost": 320.30, "queries": 1890},
                {"email": "eng-team@company.com", "cost": 290.20, "queries": 1560}
            ]
        }

    def _analyze_query_for_optimization(self, query: str) -> Dict[str, Any]:
        query_upper = query.upper()
        if "SELECT *" in query_upper:
            return {
                "suggestion": "Replace SELECT * with specific columns to reduce data scanned",
                "severity": "high",
                "savings_percent": 40
            }

    async def natural_language_to_sql(self, question: str) -> Dict[str, Any]:
        return {"error": "Natural language to SQL is not supported for BigQuery."}
        if "CROSS JOIN" in query_upper:
            return {
                "suggestion": "Replace CROSS JOIN with proper JOIN condition to avoid cartesian product",
                "severity": "critical",
                "savings_percent": 90
            }
        if "WHERE" not in query_upper and "PARTITION" not in query_upper:
            return {
                "suggestion": "Add WHERE clause with partition filter to reduce data scanned",
                "severity": "high",
                "savings_percent": 70
            }
        if "GROUP BY" in query_upper or "ORDER BY" in query_upper:
            return {
                "suggestion": "Consider adding clustering on frequently filtered columns",
                "severity": "medium",
                "savings_percent": 30
            }
        return {
            "suggestion": "Query appears optimized",
            "severity": "low",
            "savings_percent": 5
        }