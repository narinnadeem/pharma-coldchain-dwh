FROM apache/airflow:3.4.0
RUN pip install --no-cache-dir --default-timeout=120 --retries 10 duckdb pandas psycopg2-binary openpyxl watchdog