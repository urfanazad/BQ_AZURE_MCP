 Google Cloud Service Account (Recommended for Production)
Steps:
bash# Create service account
gcloud iam service-accounts create bigquery-finops \
    --display-name="BigQuery FinOps MCP Server"

# Grant permissions
gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
    --member="serviceAccount:bigquery-finops@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/bigquery.admin"

# Create and download key
gcloud iam service-accounts keys create service-account-key.json \
    --iam-account=bigquery-finops@YOUR_PROJECT_ID.iam.gserviceaccount.com

# Move key to your project folder
move service-account-key.json C:\Users\User\bigquery_MCP\

#########################################################

# Navigate to directory
cd C:\Users\User\bigquery_MCP example

# Install dependencies
python -m pip install --upgrade pip
python -m pip install mcp google-cloud-bigquery google-auth pandas numpy python-dotenv

# Authenticate with Google Cloud
gcloud auth application-default login
gcloud config set project YOUR_PROJECT_ID

# Run test
python test_mcp.py

# If all tests pass, restart Claude Desktop


Summary: Your Next Steps

Create the folder structure in C:\Users\User\bigquery_MCP\
Save the MCP server code as bigquery_finops_mcp.py
Create requirements.txt and run pip install
Set up Google Cloud auth (choose gcloud or service account)
Create config.json with your project settings
Update Claude Desktop config with the correct path
Run the test script to verify everything works
Restart Claude Desktop
Test it out by asking me to show your BigQuery costs!
