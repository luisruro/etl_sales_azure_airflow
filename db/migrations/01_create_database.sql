IF NOT EXISTS (
    SELECT name
    FROM sys.databases
    WHERE name = 'sales_etl'
)
BEGIN
    CREATE DATABASE sales_etl;
END