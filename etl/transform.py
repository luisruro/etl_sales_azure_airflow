import pandas as pd
import numpy as np

from utils.logger import get_logger

logger = get_logger(__name__)

class DataTransformer:
    
    def transform(self, df: pd.DataFrame):
        """Transform: Data cleaning and validation"""
        
        logger.info("Initiating data transformation...")
        df_transformed = df.copy()
        
        # Columns name standardization
        logger.info("Step 1: Renaming columns...")
        df_transformed.columns = [
            'order_id', 
            'order_date', 
            'product_id', 
            'product_category', 
            'price', 
            'discount_percent', 
            'quantity_sold', 
            'customer_region', 
            'payment_method',
            'rating',
            'review_count',
            'discounted_price',
            'total_revenue'
        ]
        
        # Data type conversion
        logger.info("Step 2: Data type conversion...")
        
        # Convert date
        df_transformed['order_date'] = pd.to_datetime(df_transformed['order_date'], errors='coerce')
        
        # Verify invalid date
        invalid_dates = df_transformed['order_date'].isnull().sum()
        if invalid_dates > 0:
            logger.warning(f"{invalid_dates} Invalid dates found")
            df_transformed = df_transformed.dropna(subset=['order_date'])
        
        # Convert numerical columns
        numerical_columns = ['price', 'discount_percent', 'quantity_sold', 'rating', 'review_count', 'discounted_price', 'total_revenue']
        
        for col in numerical_columns:
            df_transformed[col] = pd.to_numeric(df_transformed[col], errors='coerce')
            
        # Data cleaning
        logger.info("Step 3: Data cleaning...")
        
        # Remove spaces
        df_transformed = df_transformed.apply(
            lambda col: col.str.strip() if col.dtype == 'object' else col
        )
        
        # Replace empty strings with NaN
        df_transformed = df_transformed.replace('', np.nan)
        
        # Verify null values
        nulls_per_column = df_transformed.isnull().sum()
        logger.info(f"Total null values: {nulls_per_column.sum()}")
        if nulls_per_column.sum() > 0:
            logger.warning(f"Null values found: \n{nulls_per_column[nulls_per_column > 0]}")
            
        # Critical columns
        df_transformed = df_transformed.dropna(subset=[
            'order_id', 'product_id', 'price', 'total_revenue', 'product_category', 'customer_region', 'payment_method', 'quantity_sold'
        ])
        
        # Non-critical columns
        df_transformed['rating'] = df_transformed['rating'].fillna(0)
        df_transformed['review_count'] = df_transformed['review_count'].fillna(0)
        df_transformed['discount_percent'] = df_transformed['discount_percent'].fillna(0)
        df_transformed['discounted_price'] = df_transformed['discounted_price'].fillna(0)
        
        # Business validation
        
        logger.info("Step 3: Business validation...")
        
        # Verify the price is positive
        
        invalid_prices = (df_transformed['price'] <= 0).sum()
        if invalid_prices > 0:
            logger.warning(f"{invalid_prices} records with price <= 0 ")
            df_transformed = df_transformed[df_transformed['price'] > 0]
            
        # Verify discount between 0 to 100
        
        df_transformed.loc[df_transformed['discount_percent'] < 0, 'discount_percent'] = 0
        df_transformed.loc[df_transformed['discount_percent'] > 100, 'discount_percent'] = 100
        
        # Verify rating between 1 to 5
        
        df_transformed.loc[df_transformed['rating'] < 0, 'rating'] = 0
        df_transformed.loc[df_transformed['rating'] > 5, 'rating'] = 0
        
        # Delete duplicates
        duplicates = len(df_transformed)
        df_transformed = df_transformed.drop_duplicates()
        deleted_duplicates = duplicates - len(df_transformed)
        if deleted_duplicates > 0:
            logger.warning(f"Deleted {deleted_duplicates} duplicated records")
            logger.info(f"Shape after cleaning: {df_transformed.shape}")
        
        # Columns for date dimension
        
        df_transformed['day'] = df_transformed['order_date'].dt.day
        df_transformed['month'] = df_transformed['order_date'].dt.month
        df_transformed['quarter'] = df_transformed['order_date'].dt.quarter
        df_transformed['year'] = df_transformed['order_date'].dt.year
        df_transformed['day_of_week'] = df_transformed['order_date'].dt.day_name()
        
        return self.build_star_schema(df_transformed)
        
        #logger.info(f"{df_transformed.info()}")
        
    def build_dim_product(self, df: pd.DataFrame) -> pd.DataFrame:
        dim_product = df[['product_category']].drop_duplicates().reset_index(drop=True)
        return dim_product
    
    def build_dim_region(self, df: pd.DataFrame) -> pd.DataFrame:
        dim_region = df[['customer_region']].drop_duplicates().reset_index(drop=True)
        return dim_region
    
    def build_dim_payment(self, df: pd.DataFrame) -> pd.DataFrame:
        dim_payment = df[['payment_method']].drop_duplicates().reset_index(drop=True)
        return dim_payment
    
    def build_dim_date(self, df: pd.DataFrame) -> pd.DataFrame:
        dim_date = df[['order_date', 'day', 'month', 'quarter', 'year', 'day_of_week']].drop_duplicates().reset_index(drop=True)
        return dim_date
    
    def build_fact_sales(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[[
            'order_id', 'product_category', 'customer_region',
            'payment_method', 'order_date', 'price', 'discount_percent',
            'discounted_price', 'quantity_sold', 'total_revenue',
            'rating', 'review_count'
        ]]
        
    def build_star_schema(self, df: pd.DataFrame) -> dict:
        dim_product = self.build_dim_product(df)
        dim_region = self.build_dim_region(df)
        dim_payment = self.build_dim_payment(df)
        dim_date = self.build_dim_date(df)
        fact_sales = self.build_fact_sales(df)

        return {
            'dim_product': dim_product,
            'dim_region': dim_region,
            'dim_payment': dim_payment,
            'dim_date': dim_date,
            'fact_sales': fact_sales
        }