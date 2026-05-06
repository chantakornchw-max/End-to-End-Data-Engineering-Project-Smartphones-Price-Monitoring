import os
import json
import requests
import logging
from datetime import datetime

from airflow import DAG
from airflow.utils import timezone
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig
from cosmos.profiles import GoogleCloudServiceAccountDictProfileMapping



URL = "https://serpapi.com/search.json"
STAGING_PATH = "/opt/airflow/landing_zone"

# Variables Configuration
API_KEY = Variable.get("serp_api_key")
GCP_PROJECT_ID = Variable.get("gcp_project_id")
GCS_BUCKET_NAME = Variable.get("gcs_bucket_name")
RAW_DATASET_ID = Variable.get("raw_dataset_id")
STAGING_DATASET_ID = Variable.get("staging_dataset_id")
MARTS_DATASET_ID = Variable.get("marts_dataset_id")
RAW_TABLE_ID = Variable.get("raw_table_id")


target_products = [
        "iPhone 17 Pro Max",
        "Samsung Galaxy S26 Ultra",
        "Xiaomi 17 Ultra",
        "OPPO Find X9 Pro ",
        "vivo X300 Pro"
]

def _fetch_flagship_smartphone_api(ti, **context):

    date_stamp = context['ds'] 
    time_stamp = context['ts'] 
    file_path = f"{STAGING_PATH}/flagship_smartphones_data_{time_stamp}.json"
    
    os.makedirs(STAGING_PATH, exist_ok=True)

    with open(file_path, "w", encoding="utf-8") as f:
        
        for product in target_products:
            logging.info(f"Fetching data for: {product}")
            
            params = {
                "engine": "google_shopping",
                "q": product,
                "location": "Bangkok, Thailand",
                "google_domain": "google.co.th", 
                "gl": "th",
                "hl": "th",
                "api_key": API_KEY
            }

            try:
                response = requests.get(URL, params=params)
                response.raise_for_status()
                data = response.json()

                shopping_results = data.get("shopping_results", [])

                if not shopping_results:
                    logging.warning(f"No data found for {product}")
                    continue

                product_keyword = product.lower()
                exclude_keywords = ["case", "gift", "เคส", "ฟิล์ม", "used", "มือสอง", "มือ2"] 

                for item in shopping_results:
                    title = item.get("title", "").lower()
                    price = item.get("extracted_price", 0)

                    if product_keyword in title and not any(word in title for word in exclude_keywords) and price > 30000:
                        item["master_product_name"] = product 
                        item["extracted_date"] = date_stamp 

                        json_raw = {
                                "data": item, 
                                "ingested_at": time_stamp
                        }
                        f.write(json.dumps(json_raw, ensure_ascii=False) + "\n")

            except requests.exceptions.RequestException as er:
                logging.error(f"API request error for {product}: {er}")
                continue 
            except Exception as e:
                logging.error(f"Unexpected error occurred: {e}")
                continue

    logging.info(f"Successfully saved all products to {file_path}")
    return file_path


def _upload_from_local_to_gcs(ti, **context):

    date_stamp = context['ds']
    local_file_path = ti.xcom_pull(task_ids='fetch_flagship_smartphone_api')

    if not local_file_path or not os.path.exists(local_file_path):
        raise FileNotFoundError(f"No file found at {local_file_path}")

    gcs_hook = GCSHook(gcp_conn_id = 'gcp_key_conn')
    try:
        gcs_hook.upload(
            bucket_name=GCS_BUCKET_NAME,
            object_name=f"smartphones_data/{date_stamp}/{os.path.basename(local_file_path)}",
            filename=local_file_path 
            )
        logging.info(f"Successfully uploaded {local_file_path} to GCS")
        return local_file_path

    except Exception as e:
        logging.error(f"An error occurred during upload to GCS!: {e}")
        raise


def _remove_file_from_local(ti):
    local_file_path_for_remove = ti.xcom_pull(task_ids='upload_from_local_to_gcs')

    if local_file_path_for_remove and os.path.exists(local_file_path_for_remove):
            os.remove(local_file_path_for_remove)
            logging.info(f"Successfully Cleaned up: {local_file_path_for_remove}")

    else:
        logging.warning(f"File not found for cleanup!: {local_file_path_for_remove}")
    

with DAG(
    dag_id='flagship_smartphones_pipeline',
    start_date=timezone.datetime(2026, 4, 22),
    schedule='0 2,14 * * *',
    catchup=False,
):
    
    start = EmptyOperator(task_id="start")


    fetch_flagship_smartphone_api = PythonOperator(
        task_id='fetch_flagship_smartphone_api',
        python_callable=_fetch_flagship_smartphone_api
    )


    upload_from_local_to_gcs = PythonOperator(
        task_id='upload_from_local_to_gcs',
        python_callable=_upload_from_local_to_gcs
    )


    remove_file_from_local = PythonOperator(
        task_id='remove_file_from_local',
        python_callable=_remove_file_from_local
    )


    create_raw_dataset_in_bq = BigQueryCreateEmptyDatasetOperator(
        task_id='create_raw_dataset_in_bq',
        project_id=GCP_PROJECT_ID,
        dataset_id=RAW_DATASET_ID,
        exists_ok=True, 
        location='us-east1', 
        gcp_conn_id='gcp_key_conn',
    )


    create_raw_table_in_bq = BigQueryCreateEmptyTableOperator(
        task_id="create_raw_table_in_bq",
        project_id=GCP_PROJECT_ID,
        dataset_id=RAW_DATASET_ID,
        table_id=RAW_TABLE_ID,
        exists_ok=True,
        location='us-east1',                      
        gcp_conn_id="gcp_key_conn", 
        schema_fields=[
            {"name": "data", "type": "JSON", "mode": "NULLABLE"},
            {"name": "ingested_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
        ], 
        time_partitioning={
            "type": "DAY",           
            "field": "ingested_at",  
        },
    )


    upload_from_gcs_to_bq = GCSToBigQueryOperator(
        task_id='upload_from_gcs_to_bq',
        bucket=GCS_BUCKET_NAME,
        source_objects=['smartphones_data/{{ ds }}/*.json'],
        destination_project_dataset_table=f"{GCP_PROJECT_ID}.{RAW_DATASET_ID}.{RAW_TABLE_ID}",
        gcp_conn_id='gcp_key_conn',
        write_disposition='WRITE_APPEND', 
        autodetect=False,
        schema_fields=[
            {"name": "data", "type": "JSON", "mode": "NULLABLE"},
            {"name": "ingested_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
        ], 
        source_format='NEWLINE_DELIMITED_JSON',
    )


    create_staging_dataset_in_bq = BigQueryCreateEmptyDatasetOperator(
        task_id="create_staging_dataset_in_bq",
        project_id=GCP_PROJECT_ID,
        dataset_id=STAGING_DATASET_ID, 
        gcp_conn_id="gcp_key_conn",
        exists_ok=True,
        location="us-east1",                       
    )


    create_marts_dataset_in_bq = BigQueryCreateEmptyDatasetOperator(
        task_id="create_marts_dataset_in_bq",
        project_id=GCP_PROJECT_ID,
        dataset_id=MARTS_DATASET_ID,
        location="us-east1",
        gcp_conn_id="gcp_key_conn",
        exists_ok=True,
    )
    

    dbt_transformation_process = DbtTaskGroup(
        group_id="dbt_transformation_process",
        project_config=ProjectConfig("/opt/airflow/dbt/dbt_transformation"),
        profile_config=ProfileConfig(
            profile_name="dbt_transformation", 
            target_name="dev",
            profile_mapping=GoogleCloudServiceAccountDictProfileMapping(
                conn_id="gcp_key_conn", 
                profile_args={
                    "project": GCP_PROJECT_ID, 
                    "dataset": STAGING_DATASET_ID, 
                    "location": "us-east1",
                },
            ),
        ), 
    )


    end = EmptyOperator(task_id="end")


    start >> fetch_flagship_smartphone_api >> upload_from_local_to_gcs

    upload_from_local_to_gcs >> remove_file_from_local

    start >> [create_raw_dataset_in_bq, create_staging_dataset_in_bq, create_marts_dataset_in_bq]

    [upload_from_local_to_gcs, create_raw_dataset_in_bq] >> create_raw_table_in_bq

    create_raw_table_in_bq >> upload_from_gcs_to_bq

    [upload_from_gcs_to_bq, create_staging_dataset_in_bq, create_marts_dataset_in_bq] >> dbt_transformation_process >> end