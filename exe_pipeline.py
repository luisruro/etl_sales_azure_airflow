import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'plugins'))

from plugins.utils.logger import get_logger
from plugins.etl.extract import DataLakeExtractor
from plugins.etl.transform import DataTransformer
from plugins.etl.load import DataLoader

logger = get_logger(__name__)


def run_pipeline():
    try:
        logger.info("ETL Started")
        logger.info("Extraction initiated")

        extractor = DataLakeExtractor()
        transformer = DataTransformer()
        loader = DataLoader()
        raw_files = extractor.list_files()
        
        if not raw_files:
            logger.info("No new files to process")
            return
        
        for file in raw_files:
            df = extractor.extract_file(file)
            logger.info(f"Extraction finished - shape: {df.shape}")
            
            transformed = transformer.transform(df)
            logger.info(f"Transformed finished")
            
            loader.load(transformed)
            
            extractor.move_files(file, "processed")
        
        logger.info("ETL Finished Successfully")
    
    except Exception as e:
        if 'file' in locals():
            extractor.move_files(file, "error")
        logger.exception(f"Pipeline failed {e}")
        raise
    


if __name__ == "__main__":
    run_pipeline()