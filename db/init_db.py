import os
import pyodbc
from dotenv import load_dotenv
import logging

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

class DatabaseConnection:
    
    def __init__(self):
      self.server = os.getenv("AZURE_SQL_SERVER")
      self.username = os.getenv("AZURE_SQL_USERNAME")
      self.password = os.getenv("AZURE_SQL_PASSWORD")
      self.target_db = os.getenv("TARGET_DB")
      
    
    def get_connection(self, database:str):
        
        try:
            conn = pyodbc.connect(
                f"DRIVER={{ODBC Driver 18 for SQL Server}};"
                f"SERVER={self.server};"
                f"DATABASE={database};"
                f"UID={self.username};"
                f"PWD={self.password};"
                "Encrypt=yes;"
            )
            
            logger.info(f"Connection successfully to {database}")
            return conn
        except Exception as e:
            logger.warning(f"Error connecting the database: {e}")
            raise
        
if __name__ == "__main__":
    connection = DatabaseConnection()
    
    connection.get_connection("master")