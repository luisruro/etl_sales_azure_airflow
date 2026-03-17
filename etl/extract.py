import os
import pandas as pd
from io import BytesIO
from dotenv import load_dotenv
from azure.storage.filedatalake import DataLakeServiceClient

from utils.logger import get_logger

load_dotenv()

logger = get_logger(__name__)

class DataLakeExtractor:
    
    def __init__(self):
        
        self.connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        self.container_name = os.getenv("AZURE_CONTAINER_NAME")
        self.new_files_path = os.getenv("NEW_DATA_DIRECTORY")
        self.processed_file_path = os.getenv("PROCESSED_DATA_DESTINATION")
        self.error_file_path = os.getenv("ERROR_DATA_DESTINATION")
        self.datalake_service_client = DataLakeServiceClient.from_connection_string(self.connection_string)
        
    def list_files(self) -> list[str]:
        """
        List new files to process in 'new' directory
        """
        
        try:
            file_system_client = self.datalake_service_client.get_file_system_client(self.container_name)
            files = []
            
            for path in file_system_client.get_paths(path=self.new_files_path):
                if path.is_directory == False:
                    files.append(path.name)
            
            logger.info(f"Found {len(files)} new files to process")
            logger.info(f"Files: {','.join(files)}")
            return files
        except Exception as e:
            logger.error(f"Error listing new files: {e}")
            raise
        
    def extract_file(self, filepath:str) -> pd.DataFrame:
        """
        Extract the information from the new files to process
        """
        
        try:
            file_system_client = self.datalake_service_client.get_file_system_client(self.container_name)
            file_client = file_system_client.get_file_client(filepath)
            data = file_client.download_file().readall()
            df = pd.read_csv(BytesIO(data))
            logger.info(f"File {filepath} extracted successfully.")
            logger.info(f"Shape: {df.shape}")
            return df
        except Exception as e:
            logger.error(f'Error extracting file {filepath}: {e}')
            raise
        
    def move_files(self, filename:str, destination:str):
        """
        destination: 'processed' or 'error' 
        """
        
        try:
            file_system_client = self.datalake_service_client.get_file_system_client(self.container_name)
            
            base_filename = os.path.basename(filename)
            
            source_path = f"{self.new_files_path}/{base_filename}"
            
            if destination == 'processed':
                destination_path = f"{self.processed_file_path}/{base_filename}"
            else:
                destination_path = f"{self.error_file_path}/{base_filename}"
            
            source_client = file_system_client.get_file_client(source_path)
            
            source_client.rename_file(
                f"{self.container_name}/{destination_path}"
            )
            logger.info(f"File {base_filename} moved to {destination_path}")
        except Exception as e:
            logger.error(f"Error moving file {filename} to {destination_path}: {e}")
            raise
        