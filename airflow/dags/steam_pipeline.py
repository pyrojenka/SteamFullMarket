from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import pandas as pd

# Constants
GCP_PROJECT = "YOUR_GCP_PROJECT_ID"
GCS_BUCKET = "steam-fullmarket-raw"
BQ_DATASET = "steam_fullmarket"
KAGGLE_DATASET = "crainbramp/steam-dataset-2025-multi-modal-gaming-analytics"
LOCAL_DIR = "/tmp/steam_data"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

TARGET_TABLES = [
    "applications",
    "genres",
    "application_genres",
    "categories",
    "application_categories",
    "publishers",
    "application_publishers",
    "developers",
    "application_developers",
]


def find_csv(base_dir: str, table_name: str) -> str:
    """Recursively searches for a CSV file by table name"""
    for root, dirs, files in os.walk(base_dir):
        for file in files:
            if file.lower() == f"{table_name}.csv":
                return os.path.join(root, file)
    raise FileNotFoundError(
        f"File {table_name}.csv not found in {base_dir}"
    )


def download_from_kaggle():
    """Downloads CSV files from Kaggle"""
    import kaggle

    os.makedirs(LOCAL_DIR, exist_ok=True)
    kaggle.api.authenticate()

    csv_files = [
    "steam_dataset_2025_csv_package_v1/steam_dataset_2025_csv/applications.csv",
    "steam_dataset_2025_csv_package_v1/steam_dataset_2025_csv/genres.csv",
    "steam_dataset_2025_csv_package_v1/steam_dataset_2025_csv/application_genres.csv",
    "steam_dataset_2025_csv_package_v1/steam_dataset_2025_csv/categories.csv",
    "steam_dataset_2025_csv_package_v1/steam_dataset_2025_csv/application_categories.csv",
    "steam_dataset_2025_csv_package_v1/steam_dataset_2025_csv/publishers.csv",
    "steam_dataset_2025_csv_package_v1/steam_dataset_2025_csv/application_publishers.csv",
    "steam_dataset_2025_csv_package_v1/steam_dataset_2025_csv/developers.csv",
    "steam_dataset_2025_csv_package_v1/steam_dataset_2025_csv/application_developers.csv",
]

    for file_path in csv_files:
        print(f"Downloading {file_path}...")
        kaggle.api.dataset_download_file(
            KAGGLE_DATASET,
            file_name=file_path,
            path=LOCAL_DIR,
            force=True,
        )
        print(f"  Done!")

    print("All files downloaded!")

def upload_to_gcs():
    """Converts CSV → Parquet and uploads to GCS"""
    from google.cloud import storage

    client = storage.Client(project=GCP_PROJECT)
    bucket = client.bucket(GCS_BUCKET)

    for table_name in TARGET_TABLES:
        csv_path = find_csv(LOCAL_DIR, table_name)

        print(f"Processing {table_name}: {csv_path}")
        df = pd.read_csv(csv_path, low_memory=False)
        print(f"  Rows: {len(df)}, Columns: {len(df.columns)}")

        parquet_path = f"{LOCAL_DIR}/{table_name}.parquet"
        df.to_parquet(parquet_path, index=False)

        blob_name = f"raw/{table_name}/{table_name}.parquet"
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(parquet_path)
        print(f"  Uploaded to gs://{GCS_BUCKET}/{blob_name}")


def load_to_bigquery():
    """Loads Parquet files from GCS into BigQuery"""
    from google.cloud import bigquery

    client = bigquery.Client(project=GCP_PROJECT)

    for table_name in TARGET_TABLES:
        table_id = f"{GCP_PROJECT}.{BQ_DATASET}.raw_{table_name}"
        uri = f"gs://{GCS_BUCKET}/raw/{table_name}/{table_name}.parquet"

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True,
        )

        print(f"Loading {uri} → {table_id}")
        load_job = client.load_table_from_uri(
            uri, table_id, job_config=job_config
        )
        load_job.result()

        table = client.get_table(table_id)
        print(f"  Loaded {table.num_rows} rows into {table_id}")


with DAG(
    dag_id="steam_pipeline",
    default_args=default_args,
    description="Steam Dataset 2025: Kaggle → GCS → BigQuery",
    schedule_interval="@once",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["steam", "de-zoomcamp"],
) as dag:

    t1 = PythonOperator(
        task_id="download_from_kaggle",
        python_callable=download_from_kaggle,
    )

    t2 = PythonOperator(
        task_id="upload_to_gcs",
        python_callable=upload_to_gcs,
    )

    t3 = PythonOperator(
        task_id="load_to_bigquery",
        python_callable=load_to_bigquery,
    )

    t1 >> t2 >> t3