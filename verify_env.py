#!/usr/bin/env python3
"""
Verify that the environment is properly configured.
Run this before starting the MCP server.
"""

import os
from pathlib import Path

# Helper function to print status
def print_status(var, description, is_set, is_sensitive=False):
    if is_set:
        value = os.getenv(var)
        display_value = ""
        if is_sensitive:
            display_value = f": {value[:4]}..." if value and len(value) > 4 else ": (set)"
        else:
            display_value = f": {value}"
        print(f"✓ {var}{display_value}")
        print(f"  {description}")
    else:
        print(f"✗ {var} - NOT SET")
        print(f"  {description}")
    print()

def main():
    print("="*60)
    print("Environment Variables Verification")
    print("="*60)

    # Check if .env file exists
    env_file = Path(".env")
    if not env_file.exists():
        print(f"\n✗ .env file NOT found at {env_file.absolute()}")
        print("  Please create it in the same directory as the MCP server.")
        return
    else:
        print(f"\n✓ .env file found at: {env_file.absolute()}")

    # Try loading dotenv
    try:
        from dotenv import load_dotenv
        load_dotenv()
        print("✓ python-dotenv is installed and loaded.")
    except ImportError:
        print("✗ python-dotenv is NOT installed. Please run: pip install python-dotenv")
        return

    print("\n" + "-"*60)
    print("Data Source Configuration")
    print("-"*60)

    data_source_type = os.getenv("DATA_SOURCE_TYPE", "bigquery").lower()
    print_status("DATA_SOURCE_TYPE", "The type of data source to use ('bigquery' or 'azuresql')", True)

    if data_source_type == "bigquery":
        print("\n" + "-"*60)
        print("BigQuery Configuration")
        print("-"*60)
        print_status("GCP_PROJECT_ID", "Your Google Cloud Project ID", os.getenv("GCP_PROJECT_ID"))
        print_status("GOOGLE_APPLICATION_CREDENTIALS", "Path to your GCP service account key file (optional)", os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))

        # Test BigQuery connection
        try:
            from google.cloud import bigquery
            print("\nTesting BigQuery connection...")
            client = bigquery.Client()
            print(f"✓ Successfully connected to BigQuery project: {client.project}")
        except Exception as e:
            print(f"✗ BigQuery connection test failed: {e}")

    elif data_source_type == "azuresql":
        print("\n" + "-"*60)
        print("Azure SQL Configuration")
        print("-"*60)
        print_status("AZURE_SQL_SERVER", "Your Azure SQL server address", os.getenv("AZURE_SQL_SERVER"))
        print_status("AZURE_SQL_DATABASE", "Your Azure SQL database name", os.getenv("AZURE_SQL_DATABASE"))
        print_status("AZURE_SQL_USERNAME", "Your Azure SQL username", os.getenv("AZURE_SQL_USERNAME"))
        print_status("AZURE_SQL_PASSWORD", "Your Azure SQL password", os.getenv("AZURE_SQL_PASSWORD"), is_sensitive=True)

        # Test Azure SQL connection
        try:
            import pyodbc
            print("\nTesting Azure SQL connection...")
            conn_str = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={os.getenv('AZURE_SQL_SERVER')};"
                f"DATABASE={os.getenv('AZURE_SQL_DATABASE')};"
                f"UID={os.getenv('AZURE_SQL_USERNAME')};"
                f"PWD={os.getenv('AZURE_SQL_PASSWORD')}"
            )
            with pyodbc.connect(conn_str, timeout=5) as conn:
                print("✓ Successfully connected to Azure SQL.")
        except ImportError:
            print("✗ pyodbc is not installed. Please run: pip install pyodbc")
        except Exception as e:
            print(f"✗ Azure SQL connection test failed: {e}")

    else:
        print(f"\n✗ Invalid DATA_SOURCE_TYPE: '{data_source_type}'. Must be 'bigquery' or 'azuresql'.")


    print("\n" + "="*60)
    print("Next Steps:")
    print("="*60)
    print("1. Ensure all required variables for your chosen data source are set correctly.")
    print("2. Run the MCP server: python mcp_server.py")
    print("="*60 + "\n")

if __name__ == "__main__":
    main()