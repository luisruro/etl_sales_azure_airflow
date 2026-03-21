from sqlalchemy import create_engine
import pandas as pd
import os
from dotenv import load_dotenv
from utils.logger import get_logger

load_dotenv()
logger = get_logger(__name__)

class DataLoader:
    
    def __init__(self):
        self.engine = self.get_engine()
        
    def get_engine(self):
        try:
            logger.info("Creating database engine...")
            
            conn_url = (
                f"mssql+pyodbc://{os.getenv('AZURE_SQL_USERNAME')}:{os.getenv('AZURE_SQL_PASSWORD')}"
                f"@{os.getenv('AZURE_SQL_SERVER')}/{os.getenv('TARGET_DB')}"
                "?driver=ODBC+Driver+18+for+SQL+Server"
                "&Encrypt=yes"
                "&TrustServerCertificate=no"
                "&Connection Timeout=30"
            )
            
            engine = create_engine(conn_url)
            
            with engine.connect() as conn:
                logger.info("Database connection test successful")
            
            return engine
        except Exception as e:
            logger.error(f'Error creating database engine: {e}')
            raise
    
    def load_dimension(self, df: pd.DataFrame, table_name: str, unique_column:str):
        """
        Insert new records into the dimension tables and return the complete dimension with IDs
        """
        df = df.drop_duplicates(subset=[unique_column])
        
        # Read what already exists in the database
        existing_records = pd.read_sql(f"SELECT * FROM gold.{table_name}", self.engine)
        
        if unique_column == 'order_date':
            existing_records['order_date'] = pd.to_datetime(existing_records['order_date'])
            df['order_date'] = pd.to_datetime(df['order_date'])
        
        # Filter only new records
        new_records = df[~df[unique_column].isin(existing_records[unique_column])]
        
        if not new_records.empty:
            new_records.to_sql(
                name=table_name,
                con=self.engine,
                schema='gold',
                if_exists='append',
                index=False
            )
            logger.info(f"Inserted {len(new_records)} new records into {table_name}")
        else:
            logger.info(f"No new records for {table_name}")
            
        # Return the complete dimension table with IDs from database
        return pd.read_sql(f"SELECT * FROM gold.{table_name}", self.engine)
    
    def load_fact(self, fact_df: pd.DataFrame, dim_product: pd.DataFrame, dim_region: pd.DataFrame, dim_payment: pd.DataFrame, dim_date: pd.DataFrame):
        """
        Merge with dimensions Ids and load into fact table
        """
        fact = fact_df.copy()
        
        logger.info(f"Fact rows before insert: {len(fact)}")
        
        fact['order_date'] = pd.to_datetime(fact['order_date'])
        dim_date['order_date'] = pd.to_datetime(dim_date['order_date'])
        
        dim_product = dim_product.drop_duplicates(subset=['product_category'])
        dim_region = dim_region.drop_duplicates(subset=['customer_region'])
        dim_payment = dim_payment.drop_duplicates(subset=['payment_method'])
        dim_date = dim_date.drop_duplicates(subset=['order_date'])

        # Merge to get the real IDs from database
        fact = fact.merge(dim_product[['product_id', 'product_category']], on='product_category', how='left')
        fact = fact.merge(dim_region[['region_id', 'customer_region']], on='customer_region', how='left')
        fact = fact.merge(dim_date[['date_id', 'order_date']], on='order_date', how='left')
        fact = fact.merge(dim_payment[['payment_method_id', 'payment_method']], on='payment_method', how='left')
        
        logger.info(f"Fact rows after joins: {len(fact)}")
        
        # Get the fact table columns
        fact = fact[[
            'order_id', 'product_id', 'region_id', 'payment_method_id',
            'date_id', 'price', 'discount_percent', 'discounted_price',
            'quantity_sold', 'total_revenue', 'rating', 'review_count'
        ]]
        
        if len(fact) != len(fact_df):
            raise ValueError(
                f"Row count mismatch after joins. Expected {len(fact_df)}, got {len(fact)}"
    )
        
        fact.to_sql(
            name='fact_sales',
            con=self.engine,
            schema='gold',
            if_exists='append',
            index=False
        )
        
        logger.info(f"Inserted {len(fact)} records into fact_sales")
        
    def load(self, tables: dict):
        """
        Orchestrate the full load
        """
        
        try:
            logger.info("Starting data load...")
        
            # Load dimension tables and get IDs from database
            dim_product = self.load_dimension(tables['dim_product'], 'dim_product', 'product_category')
            dim_region = self.load_dimension(tables['dim_region'], 'dim_region', 'customer_region')
            dim_payment = self.load_dimension(tables['dim_payment'], 'dim_payment', 'payment_method')
            dim_date = self.load_dimension(tables['dim_date'], 'dim_date', 'order_date')
            
            # Load fact table with real IDs
            self.load_fact(tables['fact_sales'], dim_product, dim_region, dim_payment, dim_date)
            
            logger.info("Data load completed successfully")
        except Exception as e:
            logger.error(f"An error loading data: {e}")
            raise
        