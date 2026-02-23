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
        
    
    def run_sql_query_file(self, cursor, filepath):
        try:
            with open(filepath, "r") as f:
                sql_query = f.read()
            
            cursor.execute(sql_query)
            
            logger.info(f"Query executed in: {filepath}")
        except FileNotFoundError as e:
            logger.error(f'File not found in {filepath}: {e}')
            raise
        except Exception as e:
            logger.error(f'Error executing query in {filepath}: {e}')
            
    def create_database(self, filepath):
        try:
            conn = self.get_connection("master")
            conn.autocommit = True
            cursor = conn.cursor()
            self.run_sql_query_file(cursor, filepath)
            logger.info(f"Database created successfully")
        except Exception as e:
            logger.error(f'Error creating database in {filepath}: {e}')
        finally:
            cursor.close()
            conn.close()
            
    def create_schema(self, filepath):
        try:
            conn = self.get_connection(self.target_db)
            cursor = conn.cursor()
            self.run_sql_query_file(cursor, filepath)
            logger.info(f"Schema created successfully")
        except Exception as e:
            logger.error(f'Error creating schema: {e}')
        finally:
            cursor.close()
            conn.close()
        
            
            
    
    
        
if __name__ == "__main__":
    connection = DatabaseConnection()
    
    connection.create_schema("db/migrations/02_create_schema.sql")