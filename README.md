# AWS Serverless Data Pipeline — Event-Driven ETL on AWS

A fully serverless, event-driven ETL data pipeline built on AWS using Lambda, S3, SQS, RDS (PostgreSQL), CloudWatch, and Docker. Automatically ingests, processes, and stores structured data triggered by S3 upload events — no servers to manage, scales to zero when idle.

---

## Overview

This pipeline automatically triggers when a file is uploaded to S3, processes and transforms the data using AWS Lambda functions, queues jobs via SQS for reliable delivery, stores results in PostgreSQL (RDS), and monitors everything via CloudWatch. Built for high-volume, fault-tolerant data ingestion workloads.

---

## Architecture

```
Data Source (CSV/JSON upload)
      │
      ▼
  AWS S3 Bucket (raw data)
      │
      └──► S3 Event Notification
                │
                ▼
         SQS Queue (buffer + retry)
                │
                ▼
       Lambda: Data Processor
                │
                ├──► Validate & Transform data
                │
                ├──► Write to RDS PostgreSQL
                │
                ├──► Write processed file to S3 (output bucket)
                │
                └──► Publish metrics to CloudWatch
                           │
                           ▼
                   CloudWatch Alarms
                   (error rate, latency)
                           │
                           ▼
                     SNS Notification
                     (email alert on failure)
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Compute | AWS Lambda (Python 3.11) |
| Storage | AWS S3 (raw + processed buckets) |
| Queue | AWS SQS (Standard + Dead Letter Queue) |
| Database | AWS RDS PostgreSQL 15 |
| Monitoring | AWS CloudWatch + SNS alerts |
| IaC | AWS SAM (Serverless Application Model) |
| Containerization | Docker (local testing via AWS SAM CLI) |
| CI/CD | GitHub Actions + AWS SAM deploy |
| Testing | pytest, moto (AWS mocking) |
| Language | Python 3.11 |

---

## Features

- **Fully serverless** — zero server management, auto-scales with load, pay per execution
- **Event-driven** — S3 upload triggers the entire pipeline automatically
- **Reliable delivery** — SQS with Dead Letter Queue (DLQ) ensures no data loss on Lambda failure
- **Fault-tolerant** — automatic retries with exponential backoff on transient failures
- **Idempotent processing** — duplicate uploads are safely detected and skipped
- **CloudWatch monitoring** — custom metrics, dashboards, and alarms for pipeline health
- **SNS alerting** — email/SMS notifications on pipeline failures or DLQ messages
- **Local testing** — full pipeline testable locally via AWS SAM CLI and Docker
- **Infrastructure as Code** — entire stack defined in `template.yaml` (AWS SAM)

---

## Project Structure

```
aws-serverless-data-pipeline/
├── functions/
│   ├── processor/
│   │   ├── app.py               # Lambda handler — data processing logic
│   │   ├── transformer.py       # Data transformation & validation
│   │   ├── db.py                # RDS PostgreSQL connection & queries
│   │   └── requirements.txt
│   └── notifier/
│       ├── app.py               # Lambda handler — DLQ alert notifier
│       └── requirements.txt
├── tests/
│   ├── unit/
│   │   ├── test_transformer.py  # Unit tests for transformation logic
│   │   └── test_db.py           # Unit tests for DB operations
│   └── integration/
│       ├── test_processor.py    # Integration tests (moto mock AWS)
│       └── conftest.py          # pytest fixtures
├── events/
│   └── s3_event.json            # Sample S3 event for local testing
├── scripts/
│   ├── create_table.sql         # RDS schema setup
│   └── seed_test_data.py        # Test data generator
├── .github/
│   └── workflows/
│       └── deploy.yml           # GitHub Actions CI/CD
├── template.yaml                # AWS SAM infrastructure definition
├── samconfig.toml               # SAM deployment config
└── README.md
```

---

## Setup & Installation

### Prerequisites

- Python 3.11+
- AWS CLI configured (`aws configure`)
- AWS SAM CLI (`brew install aws-sam-cli`)
- Docker (for local Lambda testing)
- AWS account with appropriate IAM permissions

### 1. Clone the repository

```bash
git clone https://github.com/battu2001/aws-serverless-data-pipeline.git
cd aws-serverless-data-pipeline
```

### 2. Set up environment variables

```bash
cp .env.example .env
```

Edit `.env`:

```env
AWS_REGION=us-east-1
S3_RAW_BUCKET=your-raw-data-bucket
S3_PROCESSED_BUCKET=your-processed-data-bucket
SQS_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/123456789/data-pipeline-queue
DB_HOST=your-rds-endpoint.rds.amazonaws.com
DB_NAME=pipeline_db
DB_USER=admin
DB_PASSWORD=your_db_password
SNS_TOPIC_ARN=arn:aws:sns:us-east-1:123456789:pipeline-alerts
```

### 3. Set up the RDS database schema

```bash
psql -h your-rds-endpoint -U admin -d pipeline_db -f scripts/create_table.sql
```

### 4. Install dependencies

```bash
pip install -r functions/processor/requirements.txt
pip install -r functions/notifier/requirements.txt
```

### 5. Run tests locally (with AWS mocking via moto)

```bash
pytest tests/ -v
```

### 6. Test Lambda locally with SAM CLI

```bash
# Build the SAM application
sam build

# Invoke the processor Lambda locally with a sample S3 event
sam local invoke DataProcessorFunction --event events/s3_event.json
```

### 7. Deploy to AWS

```bash
sam deploy --guided
```

Follow the prompts to configure stack name, region, and S3 bucket for artifacts.

---

## Lambda Functions

### DataProcessorFunction

Triggered by SQS (which receives S3 event notifications).

**Flow:**
1. Receives SQS message containing S3 object key
2. Downloads file from S3 raw bucket
3. Validates schema and transforms data
4. Inserts records into RDS PostgreSQL
5. Moves processed file to S3 output bucket
6. Publishes custom CloudWatch metric

```python
def lambda_handler(event, context):
    for record in event['Records']:
        body = json.loads(record['body'])
        s3_key = body['Records'][0]['s3']['object']['key']

        # Download from S3
        raw_data = s3_client.get_object(Bucket=RAW_BUCKET, Key=s3_key)

        # Transform
        processed_data = transformer.transform(raw_data)

        # Store in RDS
        db.insert_records(processed_data)

        # Archive to output bucket
        s3_client.put_object(Bucket=PROCESSED_BUCKET, Key=s3_key, Body=processed_data)
```

### NotifierFunction

Triggered by DLQ — sends SNS alert when processing fails after all retries.

---

## Database Schema

```sql
CREATE TABLE pipeline_records (
    id          SERIAL PRIMARY KEY,
    s3_key      VARCHAR(500) NOT NULL,
    record_data JSONB NOT NULL,
    status      VARCHAR(50) DEFAULT 'processed',
    processed_at TIMESTAMP DEFAULT NOW(),
    created_at  TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_pipeline_records_s3_key ON pipeline_records(s3_key);
CREATE INDEX idx_pipeline_records_status ON pipeline_records(status);
CREATE INDEX idx_pipeline_records_processed_at ON pipeline_records(processed_at);
```

---

## CI/CD Pipeline (GitHub Actions)

```
Push to main
      │
      ▼
1. Run pytest (with moto mocks)
      │
      ▼
2. sam build
      │
      ▼
3. sam deploy --no-confirm-changeset
      │
      ▼
4. Post deployment status to Slack
```

---

## CloudWatch Monitoring

Custom metrics published per Lambda invocation:

| Metric | Description |
|---|---|
| `RecordsProcessed` | Number of records successfully inserted |
| `ProcessingLatencyMs` | Time taken per file processing |
| `ValidationErrors` | Records that failed schema validation |
| `DLQMessageCount` | Failed messages in Dead Letter Queue |

Alarms configured for:
- Error rate > 1% → SNS email alert
- DLQ depth > 0 → immediate SNS alert
- Lambda duration > 25s (near timeout) → SNS alert

---

## Performance

| Metric | Value |
|---|---|
| Cold start latency | ~800ms |
| Warm invocation latency | ~120ms |
| Max throughput | ~1000 records/sec |
| SQS retry attempts | 3 (before DLQ) |
| Test coverage | 88%+ |

---

## Key Engineering Decisions

- **SQS between S3 and Lambda** — decouples ingestion from processing, enables retries and DLQ without data loss
- **SAM over raw CloudFormation** — cleaner syntax for serverless resources, built-in local testing support
- **moto for testing** — mocks all AWS services locally, no real AWS calls in unit/integration tests
- **JSONB in PostgreSQL** — flexible schema for varied record structures without migrations
- **Idempotency via S3 key check** — prevents duplicate processing on SQS redelivery

---

## License

MIT
