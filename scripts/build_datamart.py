import os
import duckdb
import psycopg2
import pandas as pd
import time

BASE_DIR = os.getenv("AIRFLOW_HOME", os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
DUCKDB_PATH = os.path.join(BASE_DIR, "data", "warehouse.duckdb")

PG_CONFIG = {
    "host": os.getenv("WAREHOUSE_HOST", "warehouse-db" if os.path.exists("/.dockerenv") else "localhost"),
    "port": int(os.getenv("WAREHOUSE_PORT", 5432 if os.path.exists("/.dockerenv") else 5433)),
    "dbname": "staging",
    "user": "dwh",
    "password": "dwh123"
}

def build_star_schema():
    print("=" * 65)
    print("TRANSFORMING STAR SCHEMA (WITH SCD TYPE 2 & REFINED DIMS)")
    print("=" * 65)

    pg_conn = psycopg2.connect(**PG_CONFIG)
    df_master = pd.read_sql("SELECT * FROM staging.stg_master_data;", pg_conn)
    df_sales = pd.read_sql("SELECT * FROM staging.stg_pharmacy_sales;", pg_conn)
    df_hosp = pd.read_sql("SELECT * FROM staging.stg_hospital_demand;", pg_conn)
    pg_conn.close()

    con = duckdb.connect(DUCKDB_PATH)

    # 1. Dim_Medicine with SCD Type 2
    con.execute("DROP TABLE IF EXISTS Dim_Medicine;")
    # Base version
    con.execute("""
        CREATE TABLE Dim_Medicine AS
        SELECT 
            ROW_NUMBER() OVER () AS MedicineSK,
            MedicineID,
            InternalSKU,
            GTIN14,
            GenericName,
            FictionalBrandName,
            DiseaseCategory,
            TherapeuticClass,
            DosageForm,
            Strength,
            ColdChainFlag,
            StorageMinTempC,
            StorageMaxTempC,
            UnitManufacturingCostUSD,
            WholesalePriceUSD,
            RecommendedRetailPriceUSD,
            CAST('2023-01-01' AS DATE) AS RowEffectiveDate,
            CAST('9999-12-31' AS DATE) AS RowExpiryDate,
            TRUE AS IsCurrent
        FROM df_master;
    """)

    # Simulate an SCD2 revision for MED001 to demonstrate historical versioning
    con.execute("""
        -- Expire older record
        UPDATE Dim_Medicine 
        SET RowExpiryDate = CAST('2024-06-30' AS DATE), IsCurrent = FALSE 
        WHERE MedicineID = 'MED001';

        -- Insert newly revised record
        INSERT INTO Dim_Medicine
        SELECT 
            (SELECT MAX(MedicineSK) + 1 FROM Dim_Medicine),
            MedicineID,
            InternalSKU,
            GTIN14,
            GenericName,
            FictionalBrandName,
            DiseaseCategory,
            TherapeuticClass,
            DosageForm,
            Strength,
            ColdChainFlag,
            StorageMinTempC,
            StorageMaxTempC,
            UnitManufacturingCostUSD * 1.10,
            WholesalePriceUSD * 1.08,
            RecommendedRetailPriceUSD * 1.08,
            CAST('2024-07-01' AS DATE) AS RowEffectiveDate,
            CAST('9999-12-31' AS DATE) AS RowExpiryDate,
            TRUE AS IsCurrent
        FROM df_master
        WHERE MedicineID = 'MED001';
    """)
    print("    -> Dim_Medicine (SCD Type 2) built.")

    # 2. Dim_Hospital (Full list from Hospital Demand + Pharmacy Sales)
    con.execute("DROP TABLE IF EXISTS Dim_Hospital;")
    con.execute("""
        CREATE TABLE Dim_Hospital AS
        SELECT DISTINCT
            HospitalID,
            HospitalGLN,
            HospitalName,
            HospitalType,
            City,
            State,
            CountryCode,
            LicensedBeds,
            ICUBeds,
            TraumaLevel,
            CatchmentPopulation
        FROM df_hosp
        WHERE HospitalID IS NOT NULL;
    """)
    print("    -> Dim_Hospital built.")

    # 3. Dim_Channel (Captures all fulfillment/sales avenues)
    con.execute("DROP TABLE IF EXISTS Dim_Channel;")
    con.execute("""
        CREATE TABLE Dim_Channel AS
        WITH channels AS (
            SELECT DISTINCT SalesChannel, PharmacyType FROM df_sales WHERE SalesChannel IS NOT NULL
            UNION
            SELECT 'Hospital Direct Demand' AS SalesChannel, HospitalType AS PharmacyType FROM df_hosp
        )
        SELECT 
            ROW_NUMBER() OVER () AS ChannelKey,
            SalesChannel,
            PharmacyType
        FROM channels;
    """)
    print("    -> Dim_Channel built.")

    # 4. Dim_Date
    con.execute("DROP TABLE IF EXISTS Dim_Date;")
    con.execute("""
        CREATE TABLE Dim_Date AS
        WITH all_dates AS (
            SELECT DISTINCT CAST(SalesDate AS DATE) AS FullDate FROM df_sales
            UNION
            SELECT DISTINCT CAST(DemandMonth AS DATE) AS FullDate FROM df_hosp
        )
        SELECT 
            CAST(strftime(FullDate, '%Y%m%d') AS INT) AS DateKey,
            FullDate,
            EXTRACT(YEAR FROM FullDate) AS Year,
            EXTRACT(QUARTER FROM FullDate) AS Quarter,
            EXTRACT(MONTH FROM FullDate) AS Month,
            strftime(FullDate, '%B') AS MonthName,
            EXTRACT(DAY FROM FullDate) AS Day,
            strftime(FullDate, '%A') AS DayName
        FROM all_dates;
    """)
    print("    -> Dim_Date built.")

    # 5. Fact_Sales
    con.execute("DROP TABLE IF EXISTS Fact_Sales;")
    con.execute("""
        CREATE TABLE Fact_Sales AS
        SELECT 
            s.SalesTransactionID,
            CAST(strftime(CAST(s.SalesDate AS DATE), '%Y%m%d') AS INT) AS DateKey,
            m.MedicineSK,
            s.MedicineID,
            s.HospitalID,
            c.ChannelKey,
            s.PharmacyID,
            s.QuantitySoldPacks,
            s.UnitRetailPriceUSD,
            s.GrossSalesAmountUSD,
            s.DiscountAmountUSD,
            s.NetSalesAmountUSD,
            s.CostOfGoodsSoldUSD,
            s.GrossMarginUSD
        FROM df_sales s
        LEFT JOIN Dim_Channel c 
            ON s.SalesChannel = c.SalesChannel 
           AND s.PharmacyType = c.PharmacyType
        LEFT JOIN Dim_Medicine m 
            ON s.MedicineID = m.MedicineID 
           AND m.IsCurrent = TRUE;
    """)
    print("    -> Fact_Sales built.")

    # 6. Fact_HospitalDemand
    con.execute("DROP TABLE IF EXISTS Fact_HospitalDemand;")
    con.execute("""
        CREATE TABLE Fact_HospitalDemand AS
        SELECT 
            h.HospitalDemandRecordID,
            CAST(strftime(CAST(h.DemandMonth AS DATE), '%Y%m%d') AS INT) AS DateKey,
            h.HospitalID,
            m.MedicineSK,
            h.MedicineID,
            h.ForecastDemandPacks,
            h.RequestedQuantityPacks,
            h.ApprovedQuantityPacks,
            h.ReceivedQuantityPacks,
            h.ConsumedQuantityPacks,
            h.UnfulfilledDemandPacks,
            h.StockoutDays,
            h.ServiceLevelPct
        FROM df_hosp h
        LEFT JOIN Dim_Medicine m
            ON h.MedicineID = m.MedicineID 
           AND m.IsCurrent = TRUE;
    """)
    print("    -> Fact_HospitalDemand built.")
    con.close()

if __name__ == "__main__":
    build_star_schema()