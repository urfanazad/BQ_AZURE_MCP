#!/usr/bin/env python3
"""
BigQuery FinOps MCP Server
Provides cost analytics and query optimization insights from BigQuery
"""

import asyncio
import json
from datetime import datetime, timedelta
from typing import Any, Optional
from mcp.server.models import InitializationOptions
import mcp.types as types
from mcp.server import NotificationOptions, Server
from pydantic import AnyUrl
import mcp.server.stdio

# Optional: Import BigQuery client (install with: pip install google-cloud-bigquery)
try:
    from google.cloud import bigquery
    from google.cloud.bigquery import Client
    BIGQUERY_AVAILABLE = True
except ImportError:
    BIGQUERY_AVAILABLE = False
    print("Warning: google-cloud-bigquery not installed. Using mock data.")

server = Server("bigquery-finops")

# Configuration
PROJECT_ID = None
BQ_CLIENT: Optional[Any] = None

@server.list_resources()
async def handle_list_resources() -> list[types.Resource]:
    """List available BigQuery cost analysis resources"""
    return [
        types.Resource(
            uri=AnyUrl("bigquery://cost-summary"),
            name="Cost Summary",
            description="Overall cost metrics and trends",
            mimeType="application/json",
        ),
        types.Resource(
            uri=AnyUrl("bigquery://expensive-queries"),
            name="Expensive Queries",
            description="Top cost queries with optimization recommendations",
            mimeType="application/json",
        ),
        types.Resource(
            uri=AnyUrl("bigquery://project-costs"),
            name="Project Costs",
            description="Cost breakdown by project",
            mimeType="application/json",
        ),
        types.Resource(
            uri=AnyUrl("bigquery://cost-trends"),
            name="Cost Trends",
            description="Historical cost and query volume trends",
            mimeType="application/json",
        ),
    ]

@server.read_resource()
async def handle_read_resource(uri: AnyUrl) -> str:
    """Read BigQuery cost analysis data"""
    
    if uri.scheme != "bigquery":
        raise ValueError(f"Unsupported URI scheme: {uri.scheme}")
    
    resource_type = str(uri).replace("bigquery://", "")
    
    if resource_type == "cost-summary":
        data = await get_cost_summary()
    elif resource_type == "expensive-queries":
        data = await get_expensive_queries()
    elif resource_type == "project-costs":
        data = await get_project_costs()
    elif resource_type == "cost-trends":
        data = await get_cost_trends()
    else:
        raise ValueError(f"Unknown resource: {resource_type}")
    
    return json.dumps(data, indent=2)

@server.list_tools()
async def handle_list_tools() -> list[types.Tool]:
    """List available BigQuery FinOps tools"""
    return [
        types.Tool(
            name="analyze_query_cost",
            description="Analyze the cost of a BigQuery SQL query",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "The SQL query to analyze"
                    },
                    "dry_run": {
                        "type": "boolean",
                        "description": "Whether to perform a dry run (default: true)",
                        "default": True
                    }
                },
                "required": ["query"]
            },
        ),
        types.Tool(
            name="get_optimization_recommendations",
            description="Get optimization recommendations for expensive queries",
            inputSchema={
                "type": "object",
                "properties": {
                    "days": {
                        "type": "integer",
                        "description": "Number of days to analyze (default: 7)",
                        "default": 7
                    },
                    "min_cost": {
                        "type": "number",
                        "description": "Minimum cost threshold in USD (default: 1.0)",
                        "default": 1.0
                    }
                }
            },
        ),
        types.Tool(
            name="get_cost_by_user",
            description="Get cost breakdown by user",
            inputSchema={
                "type": "object",
                "properties": {
                    "days": {
                        "type": "integer",
                        "description": "Number of days to analyze (default: 30)",
                        "default": 30
                    }
                }
            },
        ),
        types.Tool(
            name="estimate_savings",
            description="Estimate potential savings from query optimizations",
            inputSchema={
                "type": "object",
                "properties": {
                    "optimization_type": {
                        "type": "string",
                        "enum": ["partitioning", "clustering", "materialized_views", "query_optimization", "all"],
                        "description": "Type of optimization to estimate"
                    }
                }
            },
        ),
    ]

@server.call_tool()
async def handle_call_tool(
    name: str, arguments: dict | None
) -> list[types.TextContent | types.ImageContent | types.EmbeddedResource]:
    """Handle tool execution"""
    
    if name == "analyze_query_cost":
        result = await analyze_query_cost(
            arguments.get("query"),
            arguments.get("dry_run", True)
        )
    elif name == "get_optimization_recommendations":
        result = await get_optimization_recommendations(
            arguments.get("days", 7),
            arguments.get("min_cost", 1.0)
        )
    elif name == "get_cost_by_user":
        result = await get_cost_by_user(arguments.get("days", 30))
    elif name == "estimate_savings":
        result = await estimate_savings(arguments.get("optimization_type", "all"))
    else:
        raise ValueError(f"Unknown tool: {name}")
    
    return [types.TextContent(type="text", text=json.dumps(result, indent=2))]

# Implementation functions

async def get_cost_summary() -> dict:
    """Get overall cost summary"""
    if BIGQUERY_AVAILABLE and BQ_CLIENT:
        # Query INFORMATION_SCHEMA.JOBS for actual data
        query = f"""
        SELECT 
            SUM(total_bytes_billed) / POW(10, 12) * 5 as total_cost_usd,
            COUNT(*) as total_queries,
            AVG(total_bytes_billed) / POW(10, 12) * 5 as avg_cost_per_query
        FROM `{PROJECT_ID}.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
        WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
            AND statement_type = 'SELECT'
            AND state = 'DONE'
        """
        job = BQ_CLIENT.query(query)
        results = list(job.result())
        
        if results:
            row = results[0]
            return {
                "total_cost": round(row.total_cost_usd or 0, 2),
                "queries_run": row.total_queries or 0,
                "avg_cost_per_query": round(row.avg_cost_per_query or 0, 3),
                "period_days": 30,
                "last_updated": datetime.now().isoformat()
            }
    
    # Mock data
    return {
        "total_cost": 1240.50,
        "queries_run": 8450,
        "avg_cost_per_query": 0.147,
        "potential_savings": 285.40,
        "period_days": 30,
        "last_updated": datetime.now().isoformat()
    }

async def get_expensive_queries() -> dict:
    """Get list of expensive queries with optimization opportunities"""
    if BIGQUERY_AVAILABLE and BQ_CLIENT:
        query = f"""
        SELECT 
            job_id,
            query,
            user_email,
            TIMESTAMP_MILLIS(creation_time) as timestamp,
            total_bytes_billed / POW(10, 12) * 5 as cost_usd,
            total_bytes_processed / POW(10, 12) as tb_processed,
            total_slot_ms / 1000 as duration_seconds
        FROM `{PROJECT_ID}.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
        WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
            AND statement_type = 'SELECT'
            AND state = 'DONE'
            AND total_bytes_billed > 0
        ORDER BY total_bytes_billed DESC
        LIMIT 20
        """
        job = BQ_CLIENT.query(query)
        results = list(job.result())
        
        queries = []
        for row in results:
            optimization = analyze_query_for_optimization(row.query)
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
    
    # Mock data
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

async def get_project_costs() -> dict:
    """Get cost breakdown by project"""
    if BIGQUERY_AVAILABLE and BQ_CLIENT:
        query = f"""
        SELECT 
            project_id,
            SUM(total_bytes_billed) / POW(10, 12) * 5 as cost_usd
        FROM `{PROJECT_ID}.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
        WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
            AND statement_type = 'SELECT'
            AND state = 'DONE'
        GROUP BY project_id
        ORDER BY cost_usd DESC
        """
        job = BQ_CLIENT.query(query)
        results = list(job.result())
        
        return {
            "projects": [
                {"name": row.project_id, "cost": round(row.cost_usd, 2)}
                for row in results
            ]
        }
    
    # Mock data
    return {
        "projects": [
            {"name": "Analytics", "cost": 450.00},
            {"name": "Data Science", "cost": 320.00},
            {"name": "Marketing", "cost": 180.00},
            {"name": "Engineering", "cost": 290.00}
        ]
    }

async def get_cost_trends() -> dict:
    """Get historical cost trends"""
    if BIGQUERY_AVAILABLE and BQ_CLIENT:
        query = f"""
        SELECT 
            DATE(creation_time) as date,
            SUM(total_bytes_billed) / POW(10, 12) * 5 as cost_usd,
            COUNT(*) as query_count
        FROM `{PROJECT_ID}.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
        WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
            AND statement_type = 'SELECT'
            AND state = 'DONE'
        GROUP BY date
        ORDER BY date
        """
        job = BQ_CLIENT.query(query)
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
    
    # Mock data
    trends = []
    for i in range(7):
        date = datetime.now() - timedelta(days=6-i)
        trends.append({
            "date": date.strftime("%m/%d"),
            "cost": 150 + (i * 10),
            "queries": 1200 + (i * 100)
        })
    return {"trends": trends}

async def analyze_query_cost(query: str, dry_run: bool = True) -> dict:
    """Analyze the cost of a query"""
    if BIGQUERY_AVAILABLE and BQ_CLIENT and dry_run:
        job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        job = BQ_CLIENT.query(query, job_config=job_config)
        
        bytes_processed = job.total_bytes_processed
        cost = (bytes_processed / (1024**4)) * 5  # $5 per TB
        
        optimization = analyze_query_for_optimization(query)
        
        return {
            "bytes_to_process": f"{bytes_processed / (1024**3):.2f} GB",
            "estimated_cost": round(cost, 4),
            "optimization": optimization
        }
    
    # Mock analysis
    return {
        "bytes_to_process": "1.5 GB",
        "estimated_cost": 0.0075,
        "optimization": {
            "suggestion": "Query analysis requires BigQuery connection",
            "severity": "info",
            "savings_percent": 0
        }
    }

def analyze_query_for_optimization(query: str) -> dict:
    """Analyze query and suggest optimizations"""
    query_upper = query.upper()
    
    # Check for SELECT *
    if "SELECT *" in query_upper:
        return {
            "suggestion": "Replace SELECT * with specific columns to reduce data scanned",
            "severity": "high",
            "savings_percent": 40
        }
    
    # Check for CROSS JOIN
    if "CROSS JOIN" in query_upper:
        return {
            "suggestion": "Replace CROSS JOIN with proper JOIN condition to avoid cartesian product",
            "severity": "critical",
            "savings_percent": 90
        }
    
    # Check for missing WHERE on partitioned column
    if "WHERE" not in query_upper and "PARTITION" not in query_upper:
        return {
            "suggestion": "Add WHERE clause with partition filter to reduce data scanned",
            "severity": "high",
            "savings_percent": 70
        }
    
    # Check for missing clustering
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

async def get_optimization_recommendations(days: int, min_cost: float) -> dict:
    """Get optimization recommendations for expensive queries"""
    queries = await get_expensive_queries()
    
    filtered = [
        q for q in queries["queries"]
        if q["cost"] >= min_cost
    ]
    
    recommendations = {
        "total_potential_savings": sum(q["potential_savings"] for q in filtered),
        "queries_analyzed": len(filtered),
        "top_recommendations": [
            {
                "query_id": q["id"],
                "current_cost": q["cost"],
                "optimization": q["optimization"],
                "estimated_savings": q["potential_savings"],
                "severity": q["severity"]
            }
            for q in sorted(filtered, key=lambda x: x["potential_savings"], reverse=True)[:10]
        ]
    }
    
    return recommendations

async def get_cost_by_user(days: int) -> dict:
    """Get cost breakdown by user"""
    if BIGQUERY_AVAILABLE and BQ_CLIENT:
        query = f"""
        SELECT 
            user_email,
            SUM(total_bytes_billed) / POW(10, 12) * 5 as cost_usd,
            COUNT(*) as query_count
        FROM `{PROJECT_ID}.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
        WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
            AND statement_type = 'SELECT'
            AND state = 'DONE'
        GROUP BY user_email
        ORDER BY cost_usd DESC
        LIMIT 20
        """
        job = BQ_CLIENT.query(query)
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
    
    # Mock data
    return {
        "users": [
            {"email": "data-team@company.com", "cost": 450.50, "queries": 2340},
            {"email": "analytics@company.com", "cost": 320.30, "queries": 1890},
            {"email": "eng-team@company.com", "cost": 290.20, "queries": 1560}
        ]
    }

async def estimate_savings(optimization_type: str) -> dict:
    """Estimate potential savings from optimizations"""
    estimates = {
        "partitioning": {"monthly_savings": 120.00, "description": "Add date-based partitioning"},
        "clustering": {"monthly_savings": 70.00, "description": "Add clustering keys on filtered columns"},
        "materialized_views": {"monthly_savings": 55.00, "description": "Cache frequently aggregated results"},
        "query_optimization": {"monthly_savings": 95.00, "description": "Replace SELECT * with specific columns"}
    }
    
    if optimization_type == "all":
        total = sum(e["monthly_savings"] for e in estimates.values())
        return {
            "total_monthly_savings": total,
            "breakdown": estimates
        }
    elif optimization_type in estimates:
        return estimates[optimization_type]
    else:
        return {"error": "Unknown optimization type"}

async def main():
    """Main entry point"""
    global PROJECT_ID, BQ_CLIENT
    
    # Initialize BigQuery client if available
    if BIGQUERY_AVAILABLE:
        try:
            BQ_CLIENT = bigquery.Client()
            PROJECT_ID = BQ_CLIENT.project
            print(f"Connected to BigQuery project: {PROJECT_ID}")
        except Exception as e:
            print(f"Could not connect to BigQuery: {e}")
            print("Using mock data instead")
    
    # Run the server using stdin/stdout streams
    async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="bigquery-finops",
                server_version="0.1.0",
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(),
                    experimental_capabilities={},
                ),
            ),
        )

if __name__ == "__main__":
    asyncio.run(main())