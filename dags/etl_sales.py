from airflow.sdk import dag, task
from airflow.exceptions import AirflowSkipException
from datetime import datetime, timedelta

from etl.extract import DataLakeExtractor
from etl.transform import DataTransformer
from etl.load import DataLoader
from utils.logger import get_logger

logger = get_logger(__name__)

default_args = {
    'owner': 'luis',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

@dag(
    dag_id='etl_sales',
    default_args=default_args,
    description='ETL pipeline for sales data',
    schedule='0 9-18 * * 1-5',
    start_date=datetime(2026, 1, 1),
    catchup=False
)

def etl_sales_pipeline():
    
    @task
    def check_new_files():
        logger.info("ETL Started")
        logger.info("Checking new files...")
        extractor = DataLakeExtractor()
        files = extractor.list_files()
        if not files:
            raise AirflowSkipException("No new files to process")
        
    @task
    def extract_transform_load():
        try:
            logger.info("Extraction initiated")
            extractor = DataLakeExtractor()
            transformer = DataTransformer()
            loader = DataLoader()
            files = extractor.list_files()
            for file in files:
                df = extractor.extract_file(file)
                logger.info(f"Extraction finished - shape: {df.shape}")

                transformed = transformer.transform(df)
                logger.info("Transformed finished")

                loader.load(transformed)
                extractor.move_files(file, "processed")

            logger.info("ETL Finished Successfully")
        except Exception as e:
            if 'file' in locals():
                extractor.move_files(file, "error")
            logger.exception(f"Pipeline failed {e}")
            raise
        
    check_new_files() >> extract_transform_load()
    
etl_sales_pipeline()