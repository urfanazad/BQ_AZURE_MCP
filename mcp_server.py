#!/usr/bin/env python3
"""
FinOps MCP Server
Provides cost analytics and query optimization insights from various data sources.
"""

import asyncio
import json
import logging
import os
from typing import Any, Optional

import mcp.server.stdio
import mcp.types as types
from mcp.server import NotificationOptions, Server
from mcp.server.models import InitializationOptions
from pydantic import AnyUrl

from data_sources.azuresql import AzureSQLDataSource
from data_sources.base import BaseDataSource
from data_sources.bigquery import BigQueryDataSource
from utils import handle_errors

# ==============================================================================
# Configuration
# ==============================================================================

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize the MCP server
server = Server("azure-bq-mcp-optimiser")

# Global variable for the data source
DATA_SOURCE: Optional[BaseDataSource] = None

# ==============================================================================
# MCP Server Implementation
# ==============================================================================

@server.list_resources()
async def handle_list_resources() -> list[types.Resource]:
    """List available FinOps resources."""
    return [
        types.Resource(
            uri=AnyUrl("finops://cost-summary"),
            name="Cost Summary",
            description="Overall cost metrics and trends",
            mimeType="application/json",
        ),
        types.Resource(
            uri=AnyUrl("finops://expensive-queries"),
            name="Expensive Queries",
            description="Top cost queries with optimization recommendations",
            mimeType="application/json",
        ),
        types.Resource(
            uri=AnyUrl("finops://project-costs"),
            name="Project Costs",
            description="Cost breakdown by project",
            mimeType="application/json",
        ),
        types.Resource(
            uri=AnyUrl("finops://cost-trends"),
            name="Cost Trends",
            description="Historical cost and query volume trends",
            mimeType="application/json",
        ),
    ]

@server.read_resource()
@handle_errors
async def handle_read_resource(uri: AnyUrl) -> str:
    """Read FinOps data from the configured data source."""
    if not DATA_SOURCE:
        raise ConnectionError("Data source is not initialized.")

    if uri.scheme != "finops":
        raise ValueError(f"Unsupported URI scheme: {uri.scheme}")

    resource_type = str(uri).replace("finops://", "")
    data: Dict[str, Any] = {}

    if resource_type == "cost-summary":
        data = await DATA_SOURCE.get_cost_summary()
    elif resource_type == "expensive-queries":
        data = await DATA_SOURCE.get_expensive_queries()
    elif resource_type == "project-costs":
        data = await DATA_SOURCE.get_project_costs()
    elif resource_type == "cost-trends":
        data = await DATA_SOURCE.get_cost_trends()
    else:
        raise ValueError(f"Unknown resource: {resource_type}")

    return json.dumps(data, indent=2)

@server.list_tools()
async def handle_list_tools() -> list[types.Tool]:
    """List available FinOps tools."""
    return [
        types.Tool(
            name="analyze_query_cost",
            description="Analyze the cost of a SQL query",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "The SQL query to analyze"},
                    "dry_run": {"type": "boolean", "description": "Perform a dry run", "default": True},
                },
                "required": ["query"],
            },
        ),
        types.Tool(
            name="get_optimization_recommendations",
            description="Get optimization recommendations for expensive queries",
            inputSchema={
                "type": "object",
                "properties": {
                    "days": {"type": "integer", "description": "Number of days to analyze", "default": 7},
                    "min_cost": {"type": "number", "description": "Minimum cost threshold in USD", "default": 1.0},
                },
            },
        ),
        types.Tool(
            name="get_cost_by_user",
            description="Get cost breakdown by user",
            inputSchema={
                "type": "object",
                "properties": {
                    "days": {"type": "integer", "description": "Number of days to analyze", "default": 30}
                },
            },
        ),
        types.Tool(
            name="natural_language_to_sql",
            description="Translate a natural language question into a SQL query",
            inputSchema={
                "type": "object",
                "properties": {
                    "question": {"type": "string", "description": "The question to translate"},
                },
                "required": ["question"],
            },
        ),
    ]

@server.call_tool()
@handle_errors
async def handle_call_tool(name: str, arguments: dict | None) -> list[types.TextContent]:
    """Handle tool execution."""
    if not DATA_SOURCE:
        raise ConnectionError("Data source is not initialized.")

    if arguments is None:
        arguments = {}

    result: Dict[str, Any] = {}
    if name == "analyze_query_cost":
        result = await DATA_SOURCE.analyze_query_cost(
            arguments.get("query", ""),
            arguments.get("dry_run", True)
        )
    elif name == "get_optimization_recommendations":
        # This is a meta-tool that orchestrates calls to the data source
        expensive_queries = await DATA_SOURCE.get_expensive_queries()
        filtered = [
            q for q in expensive_queries.get("queries", [])
            if q.get("cost", 0) >= arguments.get("min_cost", 1.0)
        ]
        result = {
            "total_potential_savings": sum(q.get("potential_savings", 0) for q in filtered),
            "queries_analyzed": len(filtered),
            "top_recommendations": [
                {
                    "query_id": q.get("id"),
                    "current_cost": q.get("cost"),
                    "optimization": q.get("optimization"),
                    "estimated_savings": q.get("potential_savings"),
                    "severity": q.get("severity"),
                }
                for q in sorted(filtered, key=lambda x: x.get("potential_savings", 0), reverse=True)[:10]
            ],
        }
    elif name == "get_cost_by_user":
        result = await DATA_SOURCE.get_cost_by_user(arguments.get("days", 30))
    elif name == "natural_language_to_sql":
        result = await DATA_SOURCE.natural_language_to_sql(arguments.get("question", ""))
    else:
        raise ValueError(f"Unknown tool: {name}")

    return [types.TextContent(type="text", text=json.dumps(result, indent=2))]

# ==============================================================================
# Main Entry Point
# ==============================================================================

async def main():
    """Main entry point for the FinOps MCP server."""
    global DATA_SOURCE

    # Determine which data source to use based on the environment variable
    data_source_type = os.getenv("DATA_SOURCE_TYPE", "bigquery").lower()

    if data_source_type == "bigquery":
        project_id = os.getenv("GCP_PROJECT_ID")
        region = os.getenv("GCP_REGION", "us")
        DATA_SOURCE = BigQueryDataSource(project_id=project_id, region=region)
    elif data_source_type == "azuresql":
        DATA_SOURCE = AzureSQLDataSource()
    else:
        raise ValueError(f"Unsupported DATA_SOURCE_TYPE: {data_source_type}")

    await DATA_SOURCE.connect()

    # Run the server using stdin/stdout streams
    async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="azure-bq-mcp-optimiser",
                server_version="1.0.0",
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(),
                    experimental_capabilities={},
                ),
            ),
        )

if __name__ == "__main__":
    asyncio.run(main())