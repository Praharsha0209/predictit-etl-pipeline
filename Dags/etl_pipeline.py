"""
PredictIt ETL Pipeline
Extract data from PredictIt API, load to S3, then to Snowflake
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
import os
import logging

from utils.api_extractor import PredictItExtractor
from utils.s3_loader import S3Loader
from utils.snowflake_loader import SnowflakeLoader

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Environment variables
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION') or 'us-east-1'
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')

SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE')
SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA')
SNOWFLAKE_ROLE = os.getenv('SNOWFLAKE_ROLE')

API_BASE_URL = os.getenv('API_BASE_URL') or 'https://www.predictit.org/api/marketdata/all/'

# Log configuration
logger.info(f"Configuration loaded:")
logger.info(f"  S3_BUCKET_NAME: {S3_BUCKET_NAME}")
logger.info(f"  AWS_REGION: {AWS_REGION}")
logger.info(f"  SNOWFLAKE_ACCOUNT: {SNOWFLAKE_ACCOUNT}")
logger.info(f"  SNOWFLAKE_WAREHOUSE: {SNOWFLAKE_WAREHOUSE}")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='predictit_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for PredictIt market data',
    schedule_interval='0 */6 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'predictit', 'markets']
) as dag:

    def extract_from_api(**context):
        """
        Task 1: Extract data from PredictIt API
        """
        import json
        import glob
        
        logger.info("Starting API extraction")
        
        # Create output directory if it doesn't exist
        output_dir = '/tmp/etl_data'
        os.makedirs(output_dir, exist_ok=True)
        logger.info(f"Output directory: {output_dir}")
        
        # Initialize extractor
        extractor = PredictItExtractor(
            api_url=API_BASE_URL,
            output_dir=output_dir
        )
        
        # List files before extraction
        files_before = set(glob.glob(f'{output_dir}/*.json'))
        logger.info(f"Files before extraction: {len(files_before)} files")
        
        # Extract and save data
        result = extractor.extract_and_save(
            output_format='json',
            filename='predictit_markets'
        )
        
        # List files after extraction
        files_after = set(glob.glob(f'{output_dir}/*.json'))
        new_files = files_after - files_before
        
        logger.info(f"Files after extraction: {len(files_after)} files")
        logger.info(f"New files created: {new_files}")
        
        if new_files:
            # Use the actual file that was created
            file_path = list(new_files)[0]
            logger.info(f"Using file: {file_path}")
        else:
            if isinstance(result, dict):
                # Save the data ourselves
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f'predictit_markets_{timestamp}.json'
                file_path = os.path.join(output_dir, filename)
                
                with open(file_path, 'w') as f:
                    json.dump(result, f)
                
                logger.info(f"Manually saved data to: {file_path}")
            elif isinstance(result, str) and os.path.exists(result):
                # The method returned a file path
                file_path = result
                logger.info(f"Method returned file path: {file_path}")
            else:
                raise ValueError("No new file was created and no valid data was returned")
        
        # Verify file exists and has content
        if not os.path.exists(file_path):
            raise ValueError(f"File does not exist: {file_path}")
        
        file_size = os.path.getsize(file_path)
        if file_size == 0:
            raise ValueError(f"File is empty: {file_path}")
        
        logger.info(f"File verified: {file_path} ({file_size} bytes)")
        
        # Read and log sample of data
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
                if isinstance(data, dict) and 'markets' in data:
                    logger.info(f"Successfully extracted {len(data['markets'])} markets")
                else:
                    logger.info(f"Data structure: {type(data)}")
        except Exception as e:
            logger.warning(f"Could not parse JSON for logging: {e}")
        
        # Push actual file path to XCom
        context['task_instance'].xcom_push(key='json_file_path', value=file_path)
        
        return file_path

    def upload_to_s3(**context):
        """
        Task 2: Upload extracted data to S3
        """
        import boto3
        from datetime import datetime
        
        logger.info("Starting S3 upload")
        
        # Force bucket name if needed
        bucket_name = S3_BUCKET_NAME
        if not bucket_name or bucket_name == 'None':
            bucket_name = 'my-etl-project-bucket-praharshamore'
            logger.warning(f"S3_BUCKET_NAME was not set, using default: {bucket_name}")
        
        logger.info(f"Using S3 bucket: {bucket_name}")
        
        # Get file path from previous task
        ti = context['task_instance']
        file_path = ti.xcom_pull(task_ids='extract_from_api', key='json_file_path')
        
        logger.info(f"Retrieved file path from XCom: {file_path}")
        
        if not file_path:
            raise ValueError("No file path received from extract_from_api task")
        
        if not os.path.exists(file_path):
            import glob
            existing_files = glob.glob('/tmp/etl_data/*.json')
            logger.error(f"File not found: {file_path}")
            logger.error(f"Files that exist: {existing_files}")
            raise ValueError(f"File not found: {file_path}")
        
        logger.info(f"File exists, size: {os.path.getsize(file_path)} bytes")
        
        # Get AWS credentials
        aws_key = AWS_ACCESS_KEY_ID
        aws_secret = AWS_SECRET_ACCESS_KEY
        aws_region = AWS_REGION
        
        if not aws_key or aws_key.startswith('YOUR_'):
            raise ValueError("AWS_ACCESS_KEY_ID not configured!")
        
        if not aws_secret or aws_secret.startswith('YOUR_'):
            raise ValueError("AWS_SECRET_ACCESS_KEY not configured!")
        
        logger.info(f"AWS Region: {aws_region}")
        logger.info(f"AWS Key starts with: {aws_key[:4]}...")
        
        # Create S3 client directly
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_key,
            aws_secret_access_key=aws_secret,
            region_name=aws_region
        )
        
        # Create S3 key with date partitioning
        timestamp = datetime.now()
        year = timestamp.strftime('%Y')
        month = timestamp.strftime('%m')
        day = timestamp.strftime('%d')
        filename = os.path.basename(file_path)
        
        s3_key = f'predictit/raw/year={year}/month={month}/day={day}/{filename}'
        
        logger.info(f"Uploading to S3 key: {s3_key}")
        
        # Upload the file
        try:
            s3_client.upload_file(file_path, bucket_name, s3_key)
            logger.info(f"Successfully uploaded to s3://{bucket_name}/{s3_key}")
        except Exception as e:
            logger.error(f"Failed to upload to S3: {e}")
            raise
        
        # Push S3 key to XCom
        ti.xcom_push(key='s3_key', value=s3_key)
        
        return s3_key

    def create_snowflake_stage(**context):
        """
        Task 3: Create external stage in Snowflake pointing to S3
        """
        logger.info("Creating Snowflake external stage")
        
        # Check Snowflake credentials
        if not SNOWFLAKE_USER or SNOWFLAKE_USER.startswith('YOUR_'):
            raise ValueError("SNOWFLAKE_USER not configured! Please update etl_pipeline.py")
        
        if not SNOWFLAKE_PASSWORD or SNOWFLAKE_PASSWORD.startswith('YOUR_'):
            raise ValueError("SNOWFLAKE_PASSWORD not configured! Please update etl_pipeline.py")
        
        # Initialize Snowflake loader
        sf_loader = SnowflakeLoader(
            account=SNOWFLAKE_ACCOUNT,
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
            role=SNOWFLAKE_ROLE
        )
        
        # Get bucket name
        bucket_name = S3_BUCKET_NAME or 'my-etl-project-bucket-praharshamore'
        
        # Create stage
        stage_name = 'PREDICTIT_S3_STAGE'
        s3_path = 'predictit/raw/'  # Just the prefix, not the full s3:// URL
        
        logger.info(f"Creating stage: {stage_name}")
        logger.info(f"S3 bucket: {bucket_name}")
        logger.info(f"S3 path: {s3_path}")
        
        try:
            result = sf_loader.create_stage(
                stage_name=stage_name,
                s3_bucket=bucket_name,
                s3_path=s3_path,
                aws_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_key=AWS_SECRET_ACCESS_KEY
            )
            
            logger.info(f"Stage created/updated successfully: {stage_name}")
            logger.info(f"Result: {result}")
            
        except Exception as e:
            logger.error(f"Failed to create stage: {e}")
            raise
        
        return stage_name

    def load_to_snowflake(**context):
        """
        Task 4: Load data from S3 to Snowflake using correct JSON structure
        """
        logger.info("Loading data to Snowflake")
        
        # Initialize Snowflake loader
        sf_loader = SnowflakeLoader(
            account=SNOWFLAKE_ACCOUNT,
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
            role=SNOWFLAKE_ROLE
        )
        
        bucket_name = S3_BUCKET_NAME or 'my-etl-project-bucket-praharshamore'
        stage_name = 'PREDICTIT_S3_STAGE'
        
        # Step 1: Create or replace external stage
        logger.info("Creating Snowflake external stage")
        
        create_stage_sql = f"""
        CREATE OR REPLACE STAGE ETL_DB.RAW_DATA.{stage_name}
        URL = 's3://{bucket_name}/predictit/raw/'
        CREDENTIALS = (
            AWS_KEY_ID = '{AWS_ACCESS_KEY_ID}'
            AWS_SECRET_KEY = '{AWS_SECRET_ACCESS_KEY}'
        )
        FILE_FORMAT = (TYPE = 'JSON');
        """
        
        sf_loader.execute_query(create_stage_sql)
        logger.info(f"Stage {stage_name} created successfully")
        
        # Step 2: Create permanent staging table (not temp)
        logger.info("Creating staging table for raw JSON")
        
        create_staging_sql = """
        CREATE TABLE IF NOT EXISTS ETL_DB.RAW_DATA.RAW_JSON_STAGING (
            RAW_DATA VARIANT,
            FILE_NAME VARCHAR,
            LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        );
        """
        
        sf_loader.execute_query(create_staging_sql)
        
        # Truncate staging table
        sf_loader.execute_query("TRUNCATE TABLE ETL_DB.RAW_DATA.RAW_JSON_STAGING;")
        logger.info("Staging table ready")
        
        # Step 3: Load raw JSON into staging table
        logger.info("Loading raw JSON from S3")
        
        load_json_sql = f"""
        COPY INTO ETL_DB.RAW_DATA.RAW_JSON_STAGING (RAW_DATA, FILE_NAME)
        FROM (
            SELECT 
                $1,
                METADATA$FILENAME
            FROM @ETL_DB.RAW_DATA.{stage_name}
        )
        FILE_FORMAT = (TYPE = 'JSON')
        PATTERN = '.*\.json'
        FORCE = TRUE;
        """
        
        result = sf_loader.execute_query(load_json_sql)
        logger.info(f"Raw JSON loaded: {result}")
        
        # Step 4: Verify staging data
        count_staging = sf_loader.execute_query("SELECT COUNT(*) FROM ETL_DB.RAW_DATA.RAW_JSON_STAGING;")
        logger.info(f"Staging table has {count_staging[0][0]} rows")
        
        if count_staging[0][0] == 0:
            raise ValueError("No data loaded to staging table!")
        
        # Step 5: Truncate target table
        logger.info("Truncating PREDICTIT_RAW table")
        
        truncate_sql = "TRUNCATE TABLE ETL_DB.RAW_DATA.PREDICTIT_RAW;"
        sf_loader.execute_query(truncate_sql)
        
        # Step 6: Parse and insert with correct JSON structure
        logger.info("Parsing JSON and inserting into PREDICTIT_RAW")
        
        insert_sql = """
        INSERT INTO ETL_DB.RAW_DATA.PREDICTIT_RAW (
            MARKET_ID,
            MARKET_NAME,
            SHORT_NAME,
            MARKET_URL,
            MARKET_STATUS,
            CONTRACT_DATA,
            EXTRACTION_TIMESTAMP
        )
        SELECT
            market.value:id::INTEGER,
            market.value:name::VARCHAR,
            market.value:shortName::VARCHAR,
            market.value:url::VARCHAR,
            COALESCE(market.value:status::VARCHAR, 'Unknown'),
            market.value:contracts::VARIANT,
            r.RAW_DATA:extracted_at::TIMESTAMP_NTZ
        FROM ETL_DB.RAW_DATA.RAW_JSON_STAGING r,
        LATERAL FLATTEN(input => r.RAW_DATA:data:markets) market;
        """
        
        result = sf_loader.execute_query(insert_sql)
        logger.info(f"Data parsed and inserted: {result}")
        
        # Step 7: Verify the load
        count_sql = "SELECT COUNT(*) as row_count FROM ETL_DB.RAW_DATA.PREDICTIT_RAW;"
        count_result = sf_loader.execute_query(count_sql)
        row_count = count_result[0][0] if count_result else 0
        
        logger.info(f"Successfully loaded {row_count} rows to Snowflake")
        
        if row_count == 0:
            raise ValueError("No data was loaded to Snowflake")
        
        # Step 8: Cleanup - truncate staging table
        sf_loader.execute_query("TRUNCATE TABLE ETL_DB.RAW_DATA.RAW_JSON_STAGING;")
        logger.info("Staging table cleaned up")
        
        return True

    def run_transformations(**context):
        """
        Task 5: Run SQL transformations - populate analytics tables
        """
        logger.info("Running transformations")
        
        sf_loader = SnowflakeLoader(
            account=SNOWFLAKE_ACCOUNT,
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema='ANALYTICS',
            role=SNOWFLAKE_ROLE
        )
        
        # Transform 1: Populate MARKET_SUMMARY
        logger.info("Populating MARKET_SUMMARY")
        
        # Truncate first
        sf_loader.execute_query("TRUNCATE TABLE ETL_DB.ANALYTICS.MARKET_SUMMARY;")
        
        # Then insert
        market_summary_sql = """
        INSERT INTO ETL_DB.ANALYTICS.MARKET_SUMMARY (
            MARKET_ID, MARKET_NAME, SHORT_NAME, MARKET_URL, MARKET_STATUS,
            TOTAL_CONTRACTS, TOTAL_VOLUME, LAST_UPDATED
        )
        SELECT DISTINCT
            market_id, market_name, short_name, market_url, market_status,
            CASE WHEN contract_data IS NOT NULL THEN ARRAY_SIZE(contract_data) ELSE 0 END,
            0.00,
            extraction_timestamp
        FROM ETL_DB.RAW_DATA.PREDICTIT_RAW
        WHERE market_id IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (PARTITION BY market_id ORDER BY extraction_timestamp DESC) = 1;
        """
        
        sf_loader.execute_query(market_summary_sql)
        logger.info("MARKET_SUMMARY populated")
        
        # Transform 2: Populate CONTRACT_DETAILS
        logger.info("Populating CONTRACT_DETAILS")
        
        # Truncate first
        sf_loader.execute_query("TRUNCATE TABLE ETL_DB.ANALYTICS.CONTRACT_DETAILS;")
        
        # Then insert
        contract_details_sql = """
        INSERT INTO ETL_DB.ANALYTICS.CONTRACT_DETAILS (
            CONTRACT_ID, MARKET_ID, CONTRACT_NAME, CONTRACT_STATUS, LAST_TRADE_PRICE,
            BEST_BUY_YES_COST, BEST_SELL_YES_COST, BEST_BUY_NO_COST, BEST_SELL_NO_COST,
            LAST_CLOSE_PRICE, DISPLAY_ORDER, LAST_UPDATED
        )
        SELECT
            contract.value:id::INTEGER,
            raw.market_id,
            contract.value:name::VARCHAR,
            contract.value:status::VARCHAR,
            contract.value:lastTradePrice::DECIMAL(10,4),
            contract.value:bestBuyYesCost::DECIMAL(10,4),
            contract.value:bestSellYesCost::DECIMAL(10,4),
            contract.value:bestBuyNoCost::DECIMAL(10,4),
            contract.value:bestSellNoCost::DECIMAL(10,4),
            contract.value:lastClosePrice::DECIMAL(10,4),
            contract.value:displayOrder::INTEGER,
            raw.extraction_timestamp
        FROM ETL_DB.RAW_DATA.PREDICTIT_RAW raw,
        LATERAL FLATTEN(input => raw.contract_data) contract
        WHERE raw.market_id IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (PARTITION BY contract.value:id ORDER BY raw.extraction_timestamp DESC) = 1;
        """
        
        sf_loader.execute_query(contract_details_sql)
        logger.info("CONTRACT_DETAILS populated")
        
        # Transform 3: Populate DAILY_MARKET_METRICS
        logger.info("Populating DAILY_MARKET_METRICS")
        
        daily_metrics_sql = """
        MERGE INTO ETL_DB.ANALYTICS.DAILY_MARKET_METRICS AS target
        USING (
            SELECT
                m.MARKET_ID,
                CURRENT_DATE() AS METRIC_DATE,
                COUNT(DISTINCT c.CONTRACT_ID) AS TOTAL_CONTRACTS,
                COALESCE(SUM(c.LAST_TRADE_PRICE), 0) AS TOTAL_VOLUME,
                COALESCE(AVG(c.LAST_TRADE_PRICE), 0) AS AVG_TRADE_PRICE,
                COALESCE(STDDEV(c.LAST_TRADE_PRICE), 0) AS PRICE_VOLATILITY
            FROM ETL_DB.ANALYTICS.MARKET_SUMMARY m
            LEFT JOIN ETL_DB.ANALYTICS.CONTRACT_DETAILS c ON m.MARKET_ID = c.MARKET_ID
            GROUP BY m.MARKET_ID
        ) AS source
        ON target.METRIC_DATE = source.METRIC_DATE AND target.MARKET_ID = source.MARKET_ID
        WHEN MATCHED THEN UPDATE SET
            target.TOTAL_CONTRACTS = source.TOTAL_CONTRACTS,
            target.TOTAL_VOLUME = source.TOTAL_VOLUME,
            target.AVG_TRADE_PRICE = source.AVG_TRADE_PRICE,
            target.PRICE_VOLATILITY = source.PRICE_VOLATILITY
        WHEN NOT MATCHED THEN INSERT (
            MARKET_ID, METRIC_DATE, TOTAL_CONTRACTS, TOTAL_VOLUME, AVG_TRADE_PRICE, PRICE_VOLATILITY
        ) VALUES (
            source.MARKET_ID, source.METRIC_DATE, source.TOTAL_CONTRACTS, source.TOTAL_VOLUME,
            source.AVG_TRADE_PRICE, source.PRICE_VOLATILITY
        );
        """
        
        sf_loader.execute_query(daily_metrics_sql)
        logger.info("DAILY_MARKET_METRICS populated")
        
        logger.info("All transformations completed successfully")
        return True

    def data_quality_checks(**context):
        """
        Task 6: Run data quality checks
        """
        logger.info("Running data quality checks")
        
        sf_loader = SnowflakeLoader(
            account=SNOWFLAKE_ACCOUNT,
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
            role=SNOWFLAKE_ROLE
        )
        
        # Check 1: Row count
        logger.info("Check 1: Verifying data in PREDICTIT_RAW table")
        count_query = "SELECT COUNT(*) FROM ETL_DB.RAW_DATA.PREDICTIT_RAW;"
        results = sf_loader.execute_query(count_query)
        row_count = results[0][0] if results else 0
        
        if row_count > 0:
            logger.info(f" Data quality check passed: {row_count} rows found")
        else:
            raise ValueError(" Data quality check failed: No rows in PREDICTIT_RAW table")
        
        # Check 2: Check for nulls in key columns
        logger.info("Check 2: Checking for null values in key columns")
        null_check_query = """
        SELECT COUNT(*) 
        FROM ETL_DB.RAW_DATA.PREDICTIT_RAW 
        WHERE market_id IS NULL OR market_name IS NULL;
        """
        results = sf_loader.execute_query(null_check_query)
        null_count = results[0][0] if results else 0
        
        if null_count == 0:
            logger.info(" No null values in key columns")
        else:
            logger.warning(f"  Found {null_count} rows with null values in key columns")
        
        # Check 3: Sample data
        logger.info("Check 3: Sampling data")
        sample_query = "SELECT * FROM ETL_DB.RAW_DATA.PREDICTIT_RAW LIMIT 3;"
        sample = sf_loader.execute_query(sample_query)
        logger.info(f"Sample data retrieved: {len(sample)} rows")
        
        logger.info(" All data quality checks completed successfully")
        return True

    # Define tasks
    start_pipeline = DummyOperator(
        task_id='start_pipeline'
    )

    extract_task = PythonOperator(
        task_id='extract_from_api',
        python_callable=extract_from_api,
        provide_context=True
    )

    upload_task = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
        provide_context=True
    )

    create_stage_task = PythonOperator(
        task_id='create_snowflake_stage',
        python_callable=create_snowflake_stage,
        provide_context=True
    )

    load_task = PythonOperator(
        task_id='load_to_snowflake',
        python_callable=load_to_snowflake,
        provide_context=True
    )

    transform_task = PythonOperator(
        task_id='run_transformations',
        python_callable=run_transformations,
        provide_context=True
    )

    quality_check_task = PythonOperator(
        task_id='data_quality_checks',
        python_callable=data_quality_checks,
        provide_context=True
    )

    end_pipeline = DummyOperator(
        task_id='end_pipeline'
    )

    # Define task dependencies
    start_pipeline >> extract_task >> upload_task >> create_stage_task >> load_task >> transform_task >> quality_check_task >> end_pipeline