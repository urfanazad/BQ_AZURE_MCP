# Azure/BQ MCP Optimiser

The **Azure/BQ MCP Optimiser** is a powerful, extensible MCP server designed to provide FinOps and performance insights for both Google BigQuery and Microsoft Azure SQL Server. It features a modular architecture that allows for easy expansion to other data sources and includes an AI-powered tool to translate natural language questions into SQL queries.

## Key Features

- **Dual Data Source Support**: Connect to either Google BigQuery or Azure SQL Server by simply changing an environment variable.
- **Live Performance Monitoring**: Get real-time insights into your database's performance, including cost analysis for BigQuery and resource usage (CPU, DTU) for Azure SQL.
- **Expensive Query Analysis**: Identify the most resource-intensive queries in your database to pinpoint optimization opportunities.
- **AI-Powered Querying**: Translate natural language questions into SQL queries using Azure OpenAI.
- **Extensible Architecture**: The modular design, built around a `BaseDataSource` class, makes it easy to add support for other databases in the future.
- **Robust and Production-Ready**: With structured logging and comprehensive error handling, the server is built to be reliable and easy to maintain.

## Getting Started

### 1. Prerequisites

- Python 3.8+
- An active Google Cloud Platform account (for BigQuery) or Microsoft Azure account (for Azure SQL).

### 2. Installation

Clone the repository and install the required dependencies:

```bash
git clone <your-repo-url>
cd <your-repo-name>
pip install -r requirements.txt
```

### 3. Configuration

The application is configured using an `.env` file. A sample `.env` file is included in the repository. Rename it from `.env.sample` to `.env` and fill in the required values.

#### a. Choose Your Data Source

First, set the `DATA_SOURCE_TYPE` variable to either `"bigquery"` or `"azuresql"`.

```
DATA_SOURCE_TYPE="bigquery"
```

#### b. Configure BigQuery

If you are using BigQuery, you will need to provide the following:

- `GCP_PROJECT_ID`: Your Google Cloud project ID.
- `GOOGLE_APPLICATION_CREDENTIALS`: The path to your GCP service account key file.

#### c. Configure Azure SQL

If you are using Azure SQL, you will need to provide the following:

- `AZURE_SQL_SERVER`, `AZURE_SQL_DATABASE`, `AZURE_SQL_USERNAME`, `AZURE_SQL_PASSWORD`: Your Azure SQL database connection details.
- `AZURE_SUBSCRIPTION_ID`, `AZURE_RESOURCE_GROUP`: Your Azure subscription and resource group names.
- `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`: Your Azure service principal credentials.
- `AZURE_OPENAI_API_KEY`, `AZURE_OPENAI_ENDPOINT`, `AZURE_OPENAI_DEPLOYMENT_NAME`: Your Azure OpenAI service details.

### 4. Verify Your Configuration

Before running the server, you can use the `verify_env.py` script to check that your environment is configured correctly:

```bash
python verify_env.py
```

### 5. Run the Server

Once your configuration is verified, you can start the MCP server:

```bash
python mcp_server.py
```

## Available Tools

The following tools are available through the MCP server:

- **`get_cost_summary`**: Get a summary of your database's performance and cost.
- **`get_expensive_queries`**: Get a list of the most resource-intensive queries.
- **`get_project_costs`**: Get a breakdown of costs by project (for BigQuery) or database size (for Azure SQL).
- **`get_cost_trends`**: Get historical data on cost or resource usage.
- **`analyze_query_cost`**: Get an estimated execution plan for a query.
- **`get_cost_by_user`**: Get a breakdown of database usage by user.
- **`natural_language_to_sql`**: Translate a natural language question into a SQL query (Azure SQL only).