import os
import time
import duckdb
import pandas as pd

BASE_DIR = os.getenv("AIRFLOW_HOME", os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
DUCKDB_PATH = os.path.join(BASE_DIR, "data", "warehouse.duckdb")

def init_query_log(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS Query_Execution_Log (
            log_id INTEGER,
            query_name VARCHAR,
            execution_time_ms DOUBLE,
            rows_returned BIGINT,
            executed_at TIMESTAMP
        );
        CREATE SEQUENCE IF NOT EXISTS seq_query_log_id START 1;
    """)

def log_query_metric(con, query_name, duration_ms, rows_returned):
    con.execute("""
        INSERT INTO Query_Execution_Log VALUES 
        (nextval('seq_query_log_id'), ?, ?, ?, CURRENT_TIMESTAMP);
    """, [query_name, duration_ms, rows_returned])

def run_analytical_layer():
    con = duckdb.connect(DUCKDB_PATH)
    init_query_log(con)

    # Pre-aggregated Summary View to accelerate reporting
    con.execute("""
        CREATE OR REPLACE VIEW Summary_Hospital_Fulfillment AS
        SELECT 
            h.HospitalName,
            h.HospitalType,
            SUM(f.ForecastDemandPacks) AS TotalForecastPacks,
            SUM(f.ConsumedQuantityPacks) AS TotalConsumedPacks,
            ROUND(SUM(f.ConsumedQuantityPacks) * 100.0 / NULLIF(SUM(f.ForecastDemandPacks), 0), 2) AS ConsumptionVsForecastPct,
            SUM(f.UnfulfilledDemandPacks) AS TotalUnfulfilledPacks,
            ROUND(AVG(f.ServiceLevelPct), 2) AS AvgServiceLevelPct
        FROM Fact_HospitalDemand f
        JOIN Dim_Hospital h ON f.HospitalID = h.HospitalID
        GROUP BY h.HospitalName, h.HospitalType;
    """)

    # Query 1
    t0 = time.perf_counter()
    q1_res = con.execute("SELECT * FROM Summary_Hospital_Fulfillment ORDER BY TotalUnfulfilledPacks DESC LIMIT 10;").df()
    d1 = (time.perf_counter() - t0) * 1000.0
    log_query_metric(con, "Hospital Demand Fulfillment (Summary)", d1, len(q1_res))

    # Query 2
    q2 = """
        SELECT 
            c.SalesChannel,
            COUNT(s.SalesTransactionID) AS TransactionCount,
            ROUND(SUM(s.GrossSalesAmountUSD), 2) AS GrossRevenueUSD,
            ROUND(SUM(s.NetSalesAmountUSD), 2) AS NetRevenueUSD,
            ROUND(SUM(s.GrossMarginUSD), 2) AS TotalGrossMarginUSD,
            ROUND((SUM(s.GrossMarginUSD) * 100.0 / NULLIF(SUM(s.NetSalesAmountUSD), 0)), 2) AS GrossMarginPct
        FROM Fact_Sales s
        JOIN Dim_Channel c ON s.ChannelKey = c.ChannelKey
        GROUP BY c.SalesChannel
        ORDER BY NetRevenueUSD DESC;
    """
    t0 = time.perf_counter()
    q2_res = con.execute(q2).df()
    d2 = (time.perf_counter() - t0) * 1000.0
    log_query_metric(con, "Channel Margin Comparison", d2, len(q2_res))

    # Query 3
    q3 = """
        WITH ColdChainSales AS (
            SELECT 
                m.GenericName,
                m.TherapeuticClass,
                m.StorageMinTempC,
                m.StorageMaxTempC,
                SUM(s.QuantitySoldPacks) AS TotalPacksSold,
                ROUND(SUM(s.NetSalesAmountUSD), 2) AS TotalSalesUSD
            FROM Fact_Sales s
            JOIN Dim_Medicine m ON s.MedicineSK = m.MedicineSK
            WHERE m.ColdChainFlag = 'Y'
            GROUP BY m.GenericName, m.TherapeuticClass, m.StorageMinTempC, m.StorageMaxTempC
        )
        (SELECT 'FAST-MOVING (TOP 5)' AS MovementType, * FROM ColdChainSales ORDER BY TotalPacksSold DESC LIMIT 5)
        UNION ALL
        (SELECT 'SLOW-MOVING (BOTTOM 5)' AS MovementType, * FROM ColdChainSales ORDER BY TotalPacksSold ASC LIMIT 5);
    """
    t0 = time.perf_counter()
    q3_res = con.execute(q3).df()
    d3 = (time.perf_counter() - t0) * 1000.0
    log_query_metric(con, "Cold Chain Velocity", d3, len(q3_res))

    # Storage Metrics Query
    storage_df = con.execute("""
        SELECT 
            table_name, 
            estimated_size AS EstimatedRows
        FROM duckdb_tables() 
        WHERE schema_name = 'main';
    """).df()
    db_size_mb = os.path.getsize(DUCKDB_PATH) / (1024 * 1024) if os.path.exists(DUCKDB_PATH) else 0.0

    print("=" * 65)
    print("ANALYTICAL EXECUTION BENCHMARKS:")
    print(f"  * Query 1 (Hospital Fulfillment): {d1:.2f} ms")
    print(f"  * Query 2 (Channel Revenue):     {d2:.2f} ms")
    print(f"  * Query 3 (Cold Chain Velocity): {d3:.2f} ms")
    print(f"\nSTORAGE METRICS (DuckDB Size: {db_size_mb:.2f} MB):")
    print(storage_df.to_string(index=False))
    print("=" * 65)

    con.close()

if __name__ == "__main__":
    run_analytical_layer()