CREATE TABLE gold.fact_sales(
    sale_id INT PRIMARY KEY IDENTITY(1,1),
    order_id INT,
    product_id INT,
    region_id INT,
    payment_method_id INT,
    date_id INT,
    price DECIMAL(10,2),
    discount_percent DECIMAL(5,2),
    discount_price DECIMAL(10,2),
    quantity_sold INT,
    total_revenue DECIMAL(10,2),
    rating DECIMAL(3,2),
    review_count INT,
    created_at DATETIME2 DEFAULT GETUTCDATE(),
    updated_at DATETIME2 DEFAULT GETUTCDATE()

    CONSTRAINT fk_product FOREIGN KEY (product_id) REFERENCES gold.dim_product(product_id),
    CONSTRAINT fk_region FOREIGN KEY (region_id) REFERENCES gold.dim_region(region_id),
    CONSTRAINT fk_payment FOREIGN KEY (payment_method_id) REFERENCES gold.dim_payment(payment_method_id),
    CONSTRAINT fk_date FOREIGN KEY (date_id) REFERENCES gold.dim_date(date_id)
)