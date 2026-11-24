import os
from dotenv import load_dotenv

load_dotenv()


def sync_to_bigquery(csv_path="data/clean_transactions.csv"):
    try:
        from google.cloud import bigquery
    except ImportError as exc:
        raise RuntimeError(
            "google-cloud-bigquery is not installed. Run: pip install google-cloud-bigquery"
        ) from exc

    project_id = os.getenv("GCP_PROJECT_ID")
    dataset_id = os.getenv("BQ_DATASET", "autopulse_analytics")
    table_id = os.getenv("BQ_TABLE", "clean_transactions")

    if not project_id:
        raise ValueError("Set GCP_PROJECT_ID in your .env file before running BigQuery sync.")

    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    with open(csv_path, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_ref, job_config=job_config)
    job.result()

    table = client.get_table(table_ref)
    print(f"Loaded {table.num_rows:,} rows into {table_ref}")


if __name__ == "__main__":
    sync_to_bigquery()