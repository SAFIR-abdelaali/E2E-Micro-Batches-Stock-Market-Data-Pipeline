import os
import json
import logging
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from cryptography.hazmat.primitives import serialization
import boto3
import snowflake.connector
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ==================== CONFIGURATION ====================
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "test1234"
BUCKET = "raw-transactions"
LOCAL_DIR = "/tmp/minio_downloads"

SNOWFLAKE_USER = "AIRFLOW_SAFIR"
SNOWFLAKE_PRIVATE_KEY_PATH = "/opt/airflow/keys/rsa_key.p8"
SNOWFLAKE_ACCOUNT = "BNAAYAF-MH86132"
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"
SNOWFLAKE_DB = "STOCKS"
SNOWFLAKE_SCHEMA = "COMMON"
SNOWFLAKE_TABLE = "RAW_STOCK_QUOTES"

# ==================== TASK FUNCTIONS ====================

def download_from_minio(**context):
    """Download files from MinIO bucket to local directory."""
    local_files = []
    
    try:
        # Ensure local directory exists
        Path(LOCAL_DIR).mkdir(parents=True, exist_ok=True)
        logger.info(f"Local directory ready: {LOCAL_DIR}")
        
        # Create MinIO client
        s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY
        )
        
        # List objects
        objects = s3.list_objects_v2(Bucket=BUCKET).get("Contents", [])
        
        if not objects:
            logger.warning(f"No objects found in bucket: {BUCKET}")
            context['ti'].xcom_push(key='downloaded_files', value=[])
            return []
        
        logger.info(f"Found {len(objects)} objects in bucket {BUCKET}")
        
        # Download each file
        for obj in objects:
            key = obj["Key"]
            # Skip directories
            if key.endswith('/'):
                continue
                
            local_file = os.path.join(LOCAL_DIR, os.path.basename(key))
            s3.download_file(BUCKET, key, local_file)
            logger.info(f"Downloaded: {key} -> {local_file}")
            local_files.append(local_file)
        
        # Push to XCom
        context['ti'].xcom_push(key='downloaded_files', value=local_files)
        logger.info(f"Successfully downloaded {len(local_files)} files")
        
        return local_files
        
    except Exception as e:
        logger.error(f"Error in download_from_minio: {e}")
        logger.error(traceback.format_exc())
        raise AirflowException(f"Download failed: {e}")


def load_to_snowflake(**context):
    """Load downloaded files from MinIO into Snowflake."""
    conn = None
    cur = None
    
    try:
        # ===== GET XCOM DATA =====
        logger.info("=" * 60)
        logger.info("Getting XCom data from download_minio task...")
        
        local_files = context['ti'].xcom_pull(task_ids='download_minio')
        logger.info(f"XCom data (direct): {local_files}")
        
        if not local_files:
            # Try with specific key
            local_files = context['ti'].xcom_pull(task_ids='download_minio', key='downloaded_files')
            logger.info(f"XCom data (with key): {local_files}")
        
        if not local_files:
            logger.warning("No files to load into Snowflake")
            return "No files to load"
        
        # Convert to list if needed
        if isinstance(local_files, str):
            try:
                local_files = json.loads(local_files)
            except:
                local_files = [local_files]
        elif not isinstance(local_files, list):
            local_files = [local_files]
        
        # Filter valid files
        local_files = [f for f in local_files if f and os.path.exists(f)]
        
        if not local_files:
            logger.warning("No valid files found to load")
            return "No valid files to load"
        
        logger.info(f"Loading {len(local_files)} files into Snowflake table {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE}")
        logger.info("=" * 60)
        
        # ===== SNOWFLAKE CONNECTION =====
        logger.info("Connecting to Snowflake...")
        
        # Based on your screenshot, you're using ACCOUNTADMIN role
        with open(SNOWFLAKE_PRIVATE_KEY_PATH, "rb") as key_file:
            p_key = serialization.load_pem_private_key(
                key_file.read(),
                password=None
            )

        private_key = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            account=SNOWFLAKE_ACCOUNT,
            private_key=private_key,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DB,
            schema=SNOWFLAKE_SCHEMA,
            role="ACCOUNTADMIN",
            client_session_keep_alive=True,
        )
        
        logger.info("Snowflake connection successful!")
        cur = conn.cursor()
        
        # ===== VERIFY CONNECTION =====
        cur.execute("SELECT CURRENT_USER(), CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE(), CURRENT_ROLE()")
        result = cur.fetchone()
        logger.info(f"Connected as: User={result[0]}, DB={result[1]}, Schema={result[2]}, Warehouse={result[3]}, Role={result[4]}")
        
        # ===== VERIFY TABLE STRUCTURE =====
        logger.info(f"Checking table: {SNOWFLAKE_TABLE}")
        try:
            cur.execute(f"DESC TABLE {SNOWFLAKE_TABLE}")
            columns = cur.fetchall()
            logger.info(f"Table columns: {[col[0] for col in columns]}")
        except Exception as e:
            logger.error(f"Table verification failed: {e}")
            logger.info("Creating table if it doesn't exist...")
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {SNOWFLAKE_TABLE} (
                    data VARIANT,
                    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                )
            """)
            logger.info(f"Table {SNOWFLAKE_TABLE} created successfully")
        
        # ===== UPLOAD FILES TO STAGE =====
        uploaded_files = []
        for file_path in local_files:
            file_name = os.path.basename(file_path)
            file_size = os.path.getsize(file_path)
            
            logger.info(f"Uploading: {file_name} ({file_size} bytes)")
            
            try:
                # Use fully qualified stage name
                stage_name = f"@{SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.%{SNOWFLAKE_TABLE}"
                put_query = f"PUT file://{file_path} {stage_name}"
                logger.info(f"Executing: {put_query}")
                
                cur.execute(put_query)
                uploaded_files.append(file_path)
                logger.info(f"Successfully uploaded: {file_name}")
                
                # Check upload status
                result = cur.fetchone()
                if result:
                    logger.info(f"Upload result: {result}")
                    
            except Exception as e:
                logger.error(f"Failed to upload {file_name}: {e}")
                logger.error(traceback.format_exc())
                continue
        
        if not uploaded_files:
            raise AirflowException("No files were uploaded to Snowflake stage")
        
        logger.info(f"Uploaded {len(uploaded_files)} files to stage")
        
        # ===== COPY INTO TABLE =====
        logger.info("=" * 60)
        logger.info("Executing COPY INTO command...")
        
        # Using fully qualified names based on your setup
        copy_query = f"""
            COPY INTO {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE}
            FROM @{SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.%{SNOWFLAKE_TABLE}
            FILE_FORMAT = (TYPE = JSON)
            ON_ERROR = 'CONTINUE'
            PURGE = FALSE
            FORCE = FALSE
        """
        
        logger.info(f"COPY Query: {copy_query}")
        
        try:
            cur.execute(copy_query)
            logger.info("COPY INTO executed successfully!")
            
            # Get results
            results = cur.fetchall()
            for row in results:
                logger.info(f"Copy result: {row}")
            
        except Exception as e:
            logger.error(f"COPY INTO failed: {e}")
            logger.error(traceback.format_exc())
            
            # Try alternative syntax
            logger.info("Trying simplified COPY syntax...")
            simple_copy = f"""
                COPY INTO {SNOWFLAKE_TABLE}
                FROM @%{SNOWFLAKE_TABLE}
                FILE_FORMAT = (TYPE = JSON)
                ON_ERROR = 'CONTINUE'
            """
            try:
                cur.execute(simple_copy)
                logger.info("Simplified COPY executed successfully!")
            except Exception as e2:
                logger.error(f"All COPY attempts failed: {e2}")
                raise
        
        # ===== VERIFY DATA =====
        logger.info("=" * 60)
        logger.info("Verifying data load...")
        
        try:
            cur.execute(f"SELECT COUNT(*) FROM {SNOWFLAKE_TABLE}")
            count = cur.fetchone()[0]
            logger.info(f"Total rows in {SNOWFLAKE_TABLE}: {count}")
            
            # Show sample data
            cur.execute(f"SELECT * FROM {SNOWFLAKE_TABLE} LIMIT 5")
            sample = cur.fetchall()
            logger.info(f"Sample data (first 5 rows): {sample}")
            
        except Exception as e:
            logger.warning(f"Could not verify data: {e}")
        
        # ===== CLEANUP =====
        # Clean up stage
        try:
            cur.execute(f"REMOVE @{SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.%{SNOWFLAKE_TABLE}")
            logger.info("Cleaned up stage files")
        except Exception as e:
            logger.warning(f"Could not clean up stage: {e}")
        
        # Clean up local files
        for file_path in uploaded_files:
            try:
                os.remove(file_path)
                logger.info(f"Removed local file: {file_path}")
            except Exception as e:
                logger.warning(f"Could not remove {file_path}: {e}")
        
        cur.close()
        conn.close()
        
        logger.info("=" * 60)
        logger.info("LOAD TO SNOWFLAKE COMPLETED SUCCESSFULLY!")
        
        return f"Successfully loaded {len(uploaded_files)} files into {SNOWFLAKE_TABLE}"
        
    except Exception as e:
        logger.error(f"Error in load_to_snowflake: {e}")
        logger.error(traceback.format_exc())
        raise AirflowException(f"Data loading failed: {e}")
    finally:
        if cur:
            try:
                cur.close()
            except:
                pass
        if conn:
            try:
                conn.close()
            except:
                pass


# ==================== DAG DEFINITION ====================

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 9, 9),
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}

with DAG(
    "bucket_to_warehouse",
    default_args=default_args,
    description="Load JSON data from MinIO to Snowflake",
    schedule_interval="*/1 * * * *",
    catchup=False,
    max_active_runs=1,
) as dag:

    download_task = PythonOperator(
        task_id="download_minio",
        python_callable=download_from_minio,
        provide_context=True,
    )

    load_task = PythonOperator(
        task_id="load_snowflake",
        python_callable=load_to_snowflake,
        provide_context=True,
    )

    download_task >> load_task