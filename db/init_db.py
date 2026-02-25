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

class Database:
    
    def __init__(self):
      self.server = os.getenv("AZURE_SQL_SERVER")
      self.username = os.getenv("AZURE_SQL_USERNAME")
      self.password = os.getenv("AZURE_SQL_PASSWORD")
      self.target_db = os.getenv("TARGET_DB")
      
    
    def get_connection(self, database:str = None):
        
        if database is None:
            database = self.target_db
        
        try:
            conn = pyodbc.connect(
                f"DRIVER={{ODBC Driver 18 for SQL Server}};"
                f"SERVER={self.server};"
                f"DATABASE={database};"
                f"UID={self.username};"
                f"PWD={self.password};"
                "Encrypt=yes;"
                "Connection Timeout=60;"
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
            raise
            
    def create_database(self, filepath):
        conn = None
        cursor = None
        try:
            conn = self.get_connection("master")
            conn.autocommit = True
            cursor = conn.cursor()
            self.run_sql_query_file(cursor, filepath)
            logger.info(f"Database created successfully")
        except Exception as e:
            logger.error(f'Error creating database in {filepath}: {e}')
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
            
    def create_schema(self, filepath):
        cursor = None
        conn = None
        try:
            conn = self.get_connection(self.target_db)
            cursor = conn.cursor()
            self.run_sql_query_file(cursor, filepath)
            logger.info(f"Schema created successfully")
            conn.commit()
        except Exception as e:
            logger.error(f'Error creating schema: {e}')
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
        
    def create_table(self, filepath:list[str]):
        cursor = None
        conn = None
        try:
            conn = self.get_connection(self.target_db)
            cursor = conn.cursor()
            for script in filepath:
                self.run_sql_query_file(cursor, script)
                logger.info(f"Table created successfully from file: {script}")
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f'Error creating table in file: {e}')
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()     
               
if __name__ == "__main__":
    db = Database()
    
    db.create_schema("db/migrations/02_create_schema.sql")
    
    db.create_table([
        "db/migrations/03_create_dim_tables.sql",
        "db/migrations/04_create_fact_table.sql"
    ])