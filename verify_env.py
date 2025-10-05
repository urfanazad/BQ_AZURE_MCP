#!/usr/bin/env python3
"""
Verify that .env file is properly loaded
Run this before starting the MCP server
"""

import os
from pathlib import Path

print("="*60)
print("Environment Variables Verification")
print("="*60)

# Check if .env file exists
env_file = Path(".env")
if env_file.exists():
    print(f"\n✓ .env file found at: {env_file.absolute()}")
else:
    print(f"\n✗ .env file NOT found")
    print(f"  Expected location: {env_file.absolute()}")
    print(f"  Please create .env file in the same directory as the MCP server")

# Try loading dotenv
try:
    from dotenv import load_dotenv
    load_dotenv()
    print("✓ python-dotenv is installed")
except ImportError:
    print("✗ python-dotenv is NOT installed")
    print("  Install with: pip install python-dotenv")
    exit(1)

print("\n" + "-"*60)
print("Environment Variables Status:")
print("-"*60)

# Check critical variables
critical_vars = {
    "GOOGLE_APPLICATION_CREDENTIALS": "Path to service account key",
    "GCP_PROJECT_ID": "Google Cloud Project ID"
}

for var, description in critical_vars.items():
    value = os.getenv(var)
    if value:
        # Mask sensitive parts
        if "CREDENTIALS" in var or "KEY" in var or "SECRET" in var:
            display_value = value[:20] + "..." if len(value) > 20 else value
        else:
            display_value = value
        print(f"✓ {var}")
        print(f"  {description}")
        print(f"  Value: {display_value}")
        
        # Verify file exists for path variables
        if "CREDENTIALS" in var and value:
            if Path(value).exists():
                print(f"  ✓ File exists")
            else:
                print(f"  ✗ File NOT found at specified path")
    else:
        print(f"✗ {var} - NOT SET")
        print(f"  {description}")
    print()

# Check optional variables
print("-"*60)
print("Optional Variables:")
print("-"*60)

optional_vars = {
    "MONTHLY_BUDGET_USD": "Monthly spending budget",
    "ALERT_THRESHOLD": "Alert threshold (0-1)",
    "LOG_LEVEL": "Logging level",
    "USE_MOCK_DATA": "Use mock data instead of real BigQuery",
    "SLACK_BOT_TOKEN": "Slack integration token",
    "JIRA_URL": "Jira instance URL",
    "ENABLE_SLACK_NOTIFICATIONS": "Enable Slack alerts",
    "ENABLE_JIRA_INTEGRATION": "Enable Jira integration"
}

for var, description in optional_vars.items():
    value = os.getenv(var)
    if value:
        # Mask sensitive parts
        if "TOKEN" in var or "KEY" in var or "SECRET" in var or "PASSWORD" in var:
            display_value = value[:10] + "***" if len(value) > 10 else "***"
        else:
            display_value = value
        print(f"✓ {var}: {display_value}")
    else:
        print(f"  {var}: Not set (optional)")

print("\n" + "="*60)
print("Summary:")
print("="*60)

# Quick test of BigQuery connection
try:
    from google.cloud import bigquery
    
    creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    project_id = os.getenv("GCP_PROJECT_ID")
    
    if creds_path and Path(creds_path).exists():
        print("\nTesting BigQuery connection...")
        client = bigquery.Client(project=project_id)
        print(f"✓ Successfully connected to project: {client.project}")
    elif not creds_path:
        print("\n→ No credentials path set, trying Application Default Credentials...")
        try:
            client = bigquery.Client(project=project_id)
            print(f"✓ Connected using Application Default Credentials")
            print(f"  Project: {client.project}")
        except Exception as e:
            print(f"✗ Could not connect: {e}")
    else:
        print(f"\n✗ Credentials file not found at: {creds_path}")
        
except ImportError:
    print("\n✗ google-cloud-bigquery not installed")
    print("  Install with: pip install google-cloud-bigquery")
except Exception as e:
    print(f"\n✗ BigQuery connection test failed: {e}")

print("\n" + "="*60)
print("Next Steps:")
print("="*60)
print("1. Fix any issues marked with ✗")
print("2. Run the MCP server: python bigquery_finops_mcp.py")
print("3. Check the startup output for configuration details")
print("="*60 + "\n")