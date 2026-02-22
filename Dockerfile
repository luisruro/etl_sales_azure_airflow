FROM apache/airflow:3.1.7

# Dependencies file
COPY requirements.txt .

# Install additional dependencies
RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt