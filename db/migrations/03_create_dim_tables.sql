CREATE TABLE gold.dim_product(
    product_id INT PRIMARY KEY,
    product_category VARCHAR(50),
    created_at DATETIME2 DEFAULT GETUTCDATE(),
    updated_at DATETIME2 DEFAULT GETUTCDATE()
);

CREATE TABLE gold.dim_region(
    region_id INT PRIMARY KEY,
    customer_region VARCHAR(100),
    created_at DATETIME2 DEFAULT GETUTCDATE(),
    updated_at DATETIME2 DEFAULT GETUTCDATE()
);

CREATE TABLE gold.dim_payment(
    payment_method_id INT PRIMARY KEY,
    payment_method VARCHAR(50),
    created_at DATETIME2 DEFAULT GETUTCDATE(),
    updated_at DATETIME2 DEFAULT GETUTCDATE()
);

CREATE TABLE gold.dim_date(
    date_id INT PRIMARY KEY,
    order_date DATE,
    day_num INT,
    month_num INT,
    quarter_num INT,
    year_num INT,
    day_of_week VARCHAR(20)
);