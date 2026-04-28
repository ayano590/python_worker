from google.cloud import bigquery

client = bigquery.Client()

table = bigquery.Table("phonic-vortex-437908-s8.python_raw_data.events", schema=[
    bigquery.SchemaField("event_time", "TIMESTAMP"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP"),
    bigquery.SchemaField("source", "STRING"),
    bigquery.SchemaField("payload", "JSON")
])

table.time_partitioning = bigquery.TimePartitioning(
    type_=bigquery.TimePartitioningType.DAY,
    field="ingested_at"
)

client.create_table(table)