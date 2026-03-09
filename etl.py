from utils.logger import get_logger
from etl.extract import DataLakeExtractor
from etl.transform import DataTransformer

logger = get_logger(__name__)


def run_pipeline():
    try:
        logger.info("ETL Started")
        logger.info("Extraction initiated")

        extractor = DataLakeExtractor()
        transformer = DataTransformer()
        raw_files = extractor.list_files()
        
        for file in raw_files:
            df = extractor.extract_file(file)
            #extractor.move_files(file)
            logger.info(f"Extraction finished \n {df.info()}")
            
            transformed = transformer.transform(df)
            logger.info(f"Transformed finished")
            
            logger.info("ETL Finished Successfully")

        return transformed
    except Exception:
        #extractor.move_files(file)
        logger.exception("Pipeline failed")
        raise
    


if __name__ == "__main__":
    run_pipeline()