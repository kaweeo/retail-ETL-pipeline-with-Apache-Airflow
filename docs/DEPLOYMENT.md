# Deployment Guide

This document describes how to deploy the Retail ETL Data Warehouse pipeline to production environments.

## Table of Contents

-   [Deployment Overview](#deployment-overview)
-   [Prerequisites](#prerequisites)
-   [AWS Setup](#aws-setup)
-   [Snowflake Setup](#snowflake-setup)
-   [Astronomer Cloud Deployment](#astronomer-cloud-deployment)
-   [Docker Deployment](#docker-deployment)
-   [Monitoring & Maintenance](#monitoring--maintenance)

## Deployment Overview

The pipeline can be deployed in three main environments:

1. **Local Development** - Docker + Astronomer CLI (see QUICKSTART.md)
2. **Astronomer Cloud** - Managed Airflow hosting (recommended for production)
3. **Self-Hosted Docker** - Manual deployment on your infrastructure

```
┌─────────────┐      ┌─────────────────┐      ┌──────────────┐
│   GitHub    │─────→│ GitHub Actions  │─────→│  Astronomer  │
│  Repository │      │    CI/CD Tests  │      │    Cloud     │
└─────────────┘      └─────────────────┘      └──────────────┘
                                                      ↓
                                              ┌──────────────┐
                                              │  AWS S3 +    │
                                              │  Snowflake   │
                                              └──────────────┘
```

---

## Prerequisites

### General Requirements

-   Git repository (GitHub recommended)
-   Docker & Docker Compose (for self-hosted)
-   Python 3.10+
-   Astronomer CLI (for Astronomer Cloud deployments)

### AWS Requirements

-   AWS Account with S3 access
-   S3 bucket for raw data and processed data
-   IAM User with S3 read/write permissions
-   AWS Access Key ID and Secret Access Key

### Snowflake Requirements

-   Snowflake Account (Standard or Business Edition)
-   Snowflake User with data warehouse privileges
-   Snowflake Warehouse (minimum XSMALL)
-   Database and schema permissions

---

## AWS Setup

### Step 1: Create S3 Bucket

```bash
# Create bucket for raw and processed data
aws s3 mb s3://retail-etl-data-bucket-$(date +%s) --region us-east-1

# Enable versioning (optional but recommended)
aws s3api put-bucket-versioning \
  --bucket retail-etl-data-bucket \
  --versioning-configuration Status=Enabled
```

### Step 2: Upload Sample Data

```bash
# Create folder structure
aws s3api put-object --bucket retail-etl-data-bucket --key data-wh-etl/

# Upload sample CSV and JSON files
aws s3 cp sales_data.csv s3://retail-etl-data-bucket/data-wh-etl/
aws s3 cp product_data.json s3://retail-etl-data-bucket/data-wh-etl/
```

### Step 3: Create IAM User for Pipeline

```bash
# Create user
aws iam create-user --user-name retail-etl-user

# Create access key
aws iam create-access-key --user-name retail-etl-user

# Create and attach policy
cat > s3-policy.json << EOF
{
  "Version": "2026-01-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::retail-etl-data-bucket",
        "arn:aws:s3:::retail-etl-data-bucket/*"
      ]
    }
  ]
}
EOF

aws iam put-user-policy --user-name retail-etl-user \
  --policy-name s3-access-policy --policy-document file://s3-policy.json
```

### Step 4: Configure AWS Connection in Airflow

Save the IAM credentials (Access Key ID & Secret Access Key) for use in Airflow connections.

---

## Snowflake Setup

### Step 1: Execute Setup Script

```bash
# Connect to Snowflake
snowsql -a <account_identifier> -u <username>

-- Run setup script
!source sql/setup_snowflake.txt
```

This script creates:

-   ETL Role with appropriate permissions
-   Warehouse (ETL_AIRFLOW_EX_WH)
-   Database (RETAIL_DB)
-   Schemas (RAW, CLEANSED, STAR, PRESENTATION)
-   File formats for CSV loading

### Step 2: Create Snowflake Service Account

```sql
CREATE USER IF NOT EXISTS etl_airflow_user
  PASSWORD = '<secure-password>'
  DEFAULT_ROLE = ETL_AIRFLOW_ROLE
  DEFAULT_WAREHOUSE = ETL_AIRFLOW_EX_WH
  DEFAULT_DATABASE = RETAIL_DB;

GRANT ALL PRIVILEGES
  ON WAREHOUSE ETL_AIRFLOW_EX_WH
  TO ROLE ETL_AIRFLOW_ROLE;
```

### Step 3: Test Connection

```bash
# Test connection from local machine
snowsql -a <account_identifier> \
  -u etl_airflow_user \
  -q "SELECT CURRENT_USER(), CURRENT_DATABASE();"
```

---

## Astronomer Cloud Deployment

### Step 1: Create Astronomer Account

1. Go to [app.astronomer.io](https://app.astronomer.io)
2. Sign up for free or paid tier
3. Create a new Deployment

### Step 2: Install Astronomer CLI

```bash
curl -sSL https://install.astronomer.io | bash

# Restart terminal or reload shell
exec $SHELL
```

### Step 3: Initialize Deployment

```bash
# Login to Astronomer
astro login

# List available organizations
astro organization list

# Create new deployment
astro deployment create retail-etl-prod
```

### Step 4: Configure Environment Variables

```bash
# Add Airflow variables and connections
astro deployment variable create \
  --key SNOWFLAKE_ACCOUNT \
  --value "xy12345"

# Deploy from current project
astro deploy
```

### Step 5: Set Up Connections in Airflow UI

1. Open Airflow Web UI (URL from astro deployment list)
2. Navigate to Admin → Connections
3. Add two connections:

**AWS Connection:**

-   Conn ID: `aws_conn_id`
-   Conn Type: Amazon Web Services
-   Access Key ID: `<your-access-key>`
-   Secret Access Key: `<your-secret-key>`
-   Region: `us-east-1`

**Snowflake Connection:**

-   Conn ID: `my_snowflake_conn`
-   Conn Type: Snowflake
-   Login: `etl_airflow_user`
-   Password: `<your-password>`
-   Schema: `RETAIL_DB`
-   Extra: `{"account": "xy12345", "warehouse": "ETL_AIRFLOW_EX_WH"}`

---

## Docker Deployment

### Step 1: Build Docker Image

```bash
# Build image
docker build -t retail-etl:latest .

# Tag for registry
docker tag retail-etl:latest myregistry/retail-etl:latest
```

### Step 2: Push to Container Registry

```bash
# Login to Docker Hub
docker login

# Push image
docker push myregistry/retail-etl:latest
```

### Step 3: Deploy to Docker Host

```bash
# Create Docker Compose override
cat > docker-compose.override.yml << EOF
version: '3.8'

services:
  webserver:
    environment:
      - AIRFLOW__CORE__LOAD_EXAMPLES=False
      - AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
      - AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}

  scheduler:
    environment:
      - AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
      - AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}
EOF

# Start services
docker-compose up -d
```

---

## Monitoring & Maintenance

### Airflow Monitoring

**Check DAG Runs:**

```bash
# Via CLI
astro run retail_etl_pipeline

# Via Web UI
# http://localhost:8080 (or Astronomer Cloud URL)
```

**View Logs:**

```bash
# Follow logs for scheduler
astro dev logs -f scheduler

# Specific task logs
astro dev logs dag_id task_id
```

### Health Checks

**Verify S3 Connectivity:**

```bash
aws s3 ls s3://retail-etl-data-bucket/cleansed/
```

**Verify Snowflake Connectivity:**

```bash
snowsql -a xy12345 -u etl_airflow_user -q \
  "SELECT COUNT(*) FROM RETAIL_DB.CLEANSED.SALES_CLEAN;"
```

### Troubleshooting

| Issue                      | Solution                                               |
| -------------------------- | ------------------------------------------------------ |
| DAG not appearing in UI    | Check DAG parsing: `airflow dags list --repo-type git` |
| S3 connection fails        | Verify AWS credentials and bucket permissions          |
| Snowflake connection fails | Test with `snowsql` CLI first                          |
| Pipeline timeout           | Check task execution time, increase timeout in DAG     |
| Memory errors              | Increase container memory or batch data processing     |

### Auto-Scaling (Astronomer Cloud)

```bash
# Enable auto-scaling for production workloads
astro deployment update --deployment-info \
  '{"scheduler": {"au": 10}, "worker_replicas": "3"}'
```

---

## Rollback & Recovery

### Rollback to Previous Deployment

```bash
# View deployment history
astro deployment list

# Rollback to previous version
astro deployment rollback
```

### Disaster Recovery

**Backup Airflow Metadata:**

```bash
# PostgreSQL backup (if self-hosted)
pg_dump airflow_db > backup_$(date +%Y%m%d).sql
```

**Restore from Backup:**

```bash
psql airflow_db < backup_20260209.sql
```

---

## Security Best Practices

1. **Secrets Management**

    - Use Astronomer's native secret backend
    - Never commit credentials to git
    - Rotate access keys regularly

2. **Network Security**

    - Use VPC endpoints for S3 (AWS)
    - Enable Private Link for Snowflake
    - Restrict security groups/network policies

3. **Audit Logging**

    - Enable CloudTrail for AWS
    - Enable Query History in Snowflake
    - Monitor Airflow logs

4. **Encryption**
    - Enable S3 encryption (SSE-S3 or SSE-KMS)
    - Use Snowflake's native encryption
    - Enable encryption in transit (TLS)

---

## Support & Resources

-   **Astronomer Documentation**: https://docs.astronomer.io
-   **Apache Airflow Docs**: https://airflow.apache.org/docs/
-   **AWS S3 Guide**: https://docs.aws.amazon.com/s3/
-   **Snowflake Docs**: https://docs.snowflake.com/

For issues specific to this project, create a GitHub issue with logs and error messages.
