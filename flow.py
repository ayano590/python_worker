from google.cloud import bigquery
from datetime import datetime
import uuid
import time
import logging

logging.basicConfig(level=logging.INFO)

PROJECT_ID = "phonic-vortex-437908-s8"
DATASET_ID = "python_raw_data"
TABLE_ID = "events"

TABLE_REF = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

client = bigquery.Client(project=PROJECT_ID)

def prepare_rows(raw_records, source="api"):
    """
    Convert raw extracted data into BigQuery-compatible rows.
    """
    rows = []
    now = datetime.datetime().isoformat()

    for record in raw_records:
        rows.append({
            "event_time": record.get("event_time", now),
            "ingested_at": now,
            "source": source,
            "payload": record,
            "insertId": str(uuid.uuid4())
        })

    return rows

def insert_batch(rows, max_retries=3):
    """
    Insert rows with retry logic
    """
    for attempt in range(max_retries):
        errors = client.insert_rows_json(
            TABLE_REF,
            rows,
            row_ids=[row["insertId"] for row in rows]
        )

        if not errors:
            logging.info(f"Inserted {len(rows)} rows successfully")
            return True
        
        logging.warning(f"Insert error: {errors}")
        time.sleep(60)  # retry after a minute

    logging.error("Failed to insert batch after retries. Tough luck.")
    return False

def run_pipeline():
    raw_data = [
        {"event_time": datetime.datetime().isoformat()},
        {"value": 456},
    ]

    rows = prepare_rows(raw_data)

    success = insert_batch(rows)

    if not success:
        logging.error("Try writing failed batch to cloud storage.")

if __name__ == "__main__":
    run_pipeline()