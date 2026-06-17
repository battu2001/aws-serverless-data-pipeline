import json
import os
import boto3
import psycopg2
from transformer import transform_record

s3_client = boto3.client("s3")
RAW_BUCKET = os.environ.get("S3_RAW_BUCKET")
PROCESSED_BUCKET = os.environ.get("S3_PROCESSED_BUCKET")

def get_db_connection():
    return psycopg2.connect(
        host=os.environ["DB_HOST"],
        database=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"]
    )

def lambda_handler(event, context):
    """Main Lambda handler — triggered by SQS."""
    processed = 0
    errors = 0

    for record in event.get("Records", []):
        try:
            body = json.loads(record["body"])
            s3_key = body["Records"][0]["s3"]["object"]["key"]

            # Download from S3
            response = s3_client.get_object(Bucket=RAW_BUCKET, Key=s3_key)
            raw_data = json.loads(response["Body"].read())

            # Transform
            transformed = [transform_record(r) for r in raw_data]

            # Insert into RDS PostgreSQL
            conn = get_db_connection()
            insert_records(conn, s3_key, transformed)
            conn.close()

            # Move to processed bucket
            s3_client.put_object(
                Bucket=PROCESSED_BUCKET,
                Key=s3_key,
                Body=json.dumps(transformed)
            )
            processed += 1

        except Exception as e:
            print(f"Error processing record: {e}")
            errors += 1

    return {
        "statusCode": 200,
        "body": json.dumps({"processed": processed, "errors": errors})
    }

def insert_records(conn, s3_key: str, records: list):
    """Insert processed records into RDS PostgreSQL."""
    with conn.cursor() as cur:
        for record in records:
            cur.execute(
                """
                INSERT INTO pipeline_records (s3_key, record_data, status)
                VALUES (%s, %s, %s)
                ON CONFLICT (s3_key) DO NOTHING
                """,
                (s3_key, json.dumps(record), "processed")
            )
    conn.commit()
