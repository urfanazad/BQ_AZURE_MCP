import logging
import os
from datetime import timedelta
from typing import Any, Dict

from azure.core.exceptions import ClientAuthenticationError
from azure.identity import DefaultAzureCredential
from azure.monitor.query import MetricsQueryClient
import openai
from .base import BaseDataSource

try:
    import pyodbc
    AZURESQL_AVAILABLE = True
except ImportError:
    AZURESQL_AVAILABLE = False

class AzureSQLDataSource(BaseDataSource):
    def __init__(self):
        self.conn_str = None
        self.connection = None
        self.monitor_client = None
        self.resource_uri = ""

    async def connect(self) -> None:
        # Connect to Azure SQL DB
        if AZURESQL_AVAILABLE:
            try:
                server = os.getenv("AZURE_SQL_SERVER")
                database = os.getenv("AZURE_SQL_DATABASE")
                username = os.getenv("AZURE_SQL_USERNAME")
                password = os.getenv("AZURE_SQL_PASSWORD")

                if all([server, database, username, password]):
                    self.conn_str = (
                        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                        f"SERVER={server};"
                        f"DATABASE={database};"
                        f"UID={username};"
                        f"PWD={password}"
                    )
                    self.connection = pyodbc.connect(self.conn_str, timeout=5)
                    logging.info("Connected to Azure SQL.")

                    # Prepare resource URI for Azure Monitor
                    subscription_id = os.getenv("AZURE_SUBSCRIPTION_ID")
                    resource_group = os.getenv("AZURE_RESOURCE_GROUP")
                    self.resource_uri = (
                        f"/subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.Sql/"
                        f"servers/{server.split('.')[0]}/databases/{database}"
                    )
                else:
                    logging.warning("Azure SQL environment variables not set. Using mock data for queries.")
            except Exception as e:
                logging.error(f"Could not connect to Azure SQL: {e}")
                self.connection = None
        else:
            logging.warning("pyodbc not installed. Azure SQL data source is not available.")

        # Connect to Azure Monitor
        try:
            credential = DefaultAzureCredential()
            self.monitor_client = MetricsQueryClient(credential)
            logging.info("Connected to Azure Monitor.")
        except (ClientAuthenticationError, ImportError) as e:
            logging.error(f"Could not connect to Azure Monitor: {e}")
            self.monitor_client = None

    async def get_cost_summary(self) -> Dict[str, Any]:
        if not self.monitor_client:
            logging.warning("No Azure Monitor connection. Returning mock data.")
            return {
                "service_tier": "General Purpose",
                "vCores": 4,
                "storage_gb": 512,
                "estimated_monthly_cost": 750.00,
                "description": "Metrics are based on provisioned resources, not per-query cost."
            }

        try:
            response = self.monitor_client.query_resource(
                self.resource_uri,
                metric_names=["cpu_percent", "dtu_used", "storage_percent"],
                timespan=timedelta(hours=1),
                granularity=timedelta(minutes=5),
                aggregations=["average"]
            )

            metrics = {metric.name: metric.timeseries[0].data[-1].average for metric in response.metrics if metric.timeseries}

            return {
                "cpu_percent_avg": metrics.get("cpu_percent"),
                "dtu_used_avg": metrics.get("dtu_used"),
                "storage_percent_avg": metrics.get("storage_percent"),
                "description": "Live metrics from Azure Monitor from the last hour."
            }
        except Exception as e:
            logging.error(f"Error querying Azure Monitor: {e}")
            return {"error": str(e)}

    async def get_expensive_queries(self) -> Dict[str, Any]:
        if not self.connection:
            logging.warning("No Azure SQL connection available. Returning mock data.")
            return {
                "queries": [
                    {
                        "id": "query_hash_1",
                        "query": "SELECT * FROM sales.orders WHERE order_date > '2024-01-01'",
                        "avg_cpu_time_ms": 1200,
                        "avg_duration_ms": 2500,
                        "execution_count": 50,
                        "optimization": "Consider adding an index on order_date.",
                        "severity": "high"
                    }
                ]
            }

        query = """
        SELECT TOP 20
            qt.query_sql_text,
            q.query_id,
            rs.avg_cpu_time,
            rs.avg_duration,
            rs.count_executions
        FROM sys.query_store_query_text AS qt
        JOIN sys.query_store_query AS q
            ON qt.query_text_id = q.query_text_id
        JOIN sys.query_store_runtime_stats AS rs
            ON q.query_id = rs.query_id
        ORDER BY rs.avg_cpu_time DESC;
        """
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            queries = [
                {
                    "id": row.query_id,
                    "query": row.query_sql_text,
                    "avg_cpu_time_ms": row.avg_cpu_time / 1000,
                    "avg_duration_ms": row.avg_duration / 1000,
                    "execution_count": row.count_executions,
                    "optimization": "Analyze query plan for optimization opportunities.",
                    "severity": "medium"
                }
                for row in rows
            ]
            return {"queries": queries}
        except Exception as e:
            logging.error(f"Error querying Query Store: {e}")
            return {"error": str(e)}

    async def get_project_costs(self) -> Dict[str, Any]:
        if not self.connection:
            logging.warning("No Azure SQL connection available. Returning mock data.")
            return {
                "databases": [
                    {"name": "SalesDB", "size_gb": 250, "service_tier": "General Purpose"},
                    {"name": "ReportingDB", "size_gb": 150, "service_tier": "General Purpose"},
                ]
            }

        query = "SELECT name, service_objective, (size * 8) / 1024.0 / 1024.0 AS size_gb FROM sys.database_service_objectives"
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            databases = [
                {
                    "name": row.name,
                    "service_tier": row.service_objective,
                    "size_gb": round(row.size_gb, 2)
                }
                for row in rows
            ]
            return {"databases": databases}
        except Exception as e:
            logging.error(f"Error querying database sizes: {e}")
            return {"error": str(e)}

    async def get_cost_trends(self) -> Dict[str, Any]:
        if not self.monitor_client:
            logging.warning("No Azure Monitor connection. Returning mock data.")
            return {
                "trends": [
                    {"date": "2024/01", "cpu_usage_percent": 60},
                    {"date": "2024/02", "cpu_usage_percent": 65},
                    {"date": "2024/03", "cpu_usage_percent": 70},
                ]
            }

        try:
            response = self.monitor_client.query_resource(
                self.resource_uri,
                metric_names=["cpu_percent"],
                timespan=timedelta(days=30),
                granularity=timedelta(days=1),
                aggregations=["average"]
            )

            trends = [
                {
                    "date": d.timestamp.strftime("%Y/%m/%d"),
                    "cpu_usage_percent": round(d.average, 2)
                }
                for d in response.metrics[0].timeseries[0].data
            ]
            return {"trends": trends}
        except Exception as e:
            logging.error(f"Error querying Azure Monitor for cost trends: {e}")
            return {"error": str(e)}

    async def analyze_query_cost(self, query: str, dry_run: bool) -> Dict[str, Any]:
        if not self.connection:
            logging.warning("No Azure SQL connection available. Returning mock data.")
            return {
                "estimated_impact": "Medium",
                "optimization_suggestion": "Review the query execution plan to identify bottlenecks.",
                "description": "SQL Server cost analysis is based on resource consumption (CPU, I/O), not data scanned."
            }

        try:
            cursor = self.connection.cursor()
            cursor.execute(f"SET SHOWPLAN_XML ON; {query}; SET SHOWPLAN_XML OFF;")
            plan = cursor.fetchone()[0]
            return {"execution_plan": plan}
        except Exception as e:
            logging.error(f"Error getting execution plan: {e}")
            return {"error": str(e)}

    async def get_cost_by_user(self, days: int) -> Dict[str, Any]:
        if not self.connection:
            logging.warning("No Azure SQL connection available. Returning mock data.")
            return {
                "users": [
                    {"name": "sales_app_user", "total_executions": 1200},
                    {"name": "reporting_user", "total_executions": 800},
                ]
            }

        query = "SELECT login_name, COUNT(*) AS session_count FROM sys.dm_exec_sessions GROUP BY login_name"
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            users = [
                {
                    "name": row.login_name,
                    "session_count": row.session_count
                }
                for row in rows
            ]
            return {"users": users}
        except Exception as e:
            logging.error(f"Error querying sessions by user: {e}")
            return {"error": str(e)}

    async def natural_language_to_sql(self, question: str) -> Dict[str, Any]:
        try:
            client = openai.AzureOpenAI(
                api_key=os.getenv("AZURE_OPENAI_API_KEY"),
                api_version="2023-12-01-preview",
                azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT")
            )

            # A simple prompt to get started. In a real application, you'd want to provide
            # more context, such as the database schema.
            prompt = f"Translate the following natural language question into a SQL query for Azure SQL Server:\n\nQuestion: {question}\n\nSQL Query:"

            response = client.completions.create(
                model=os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME"),
                prompt=prompt,
                max_tokens=150,
                temperature=0
            )

            return {"sql_query": response.choices[0].text.strip()}
        except Exception as e:
            logging.error(f"Error calling Azure OpenAI: {e}")
            return {"error": str(e)}