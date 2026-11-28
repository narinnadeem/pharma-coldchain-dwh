"""
BI layer for the Phase 1 SDI mart (Milestone 2).  Task 6 of the DAG: publish_bi_tables.

What it does, in plain words:
  1. Opens the DuckDB star schema (read-only).
  2. Runs 4 aggregate queries (one per business question + a refresh history row).
  3. Writes the small results into Postgres schema "bi" (same database as the DE metrics),
     so Grafana can read them.  Snapshot tables are replaced every run; bi.refresh_history
     gets ONE NEW ROW per run, so the dashboards visibly change after every DAG run.
  4. Exports the same tables to an Excel workbook (data/bi_exports/Milestone.1.SDI.BI.Excel.xlsx).

Run by hand (needs only the warehouse-db container):  python scripts\\build_bi_layer.py
"""
import os
import time
from datetime import datetime, timezone

import duckdb
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

BASE_DIR = os.getenv("AIRFLOW_HOME", os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
DUCKDB_PATH = os.path.join(BASE_DIR, "data", "warehouse.duckdb")
EXCEL_PATH = os.path.join(BASE_DIR, "data", "bi_exports", "Milestone.1.SDI.BI.Excel.xlsx")
IN_DOCKER = os.path.exists("/.dockerenv")
DB_CONFIG = {
    "host": os.getenv("WAREHOUSE_HOST", "warehouse-db" if IN_DOCKER else "localhost"),
    "port": int(os.getenv("WAREHOUSE_PORT", 5432 if IN_DOCKER else 5433)),
    "dbname": "staging",
    "user": "dwh",
    "password": "dwh123",
}
SCHEMA = "bi"

# Targets are OUR assumptions (the data set has no targets). Change here, re-run generate_grafana_bi_dashboards.py.
TARGETS = {"consumption_ratio_min_pct": 95.0, "unfulfilled_share_max_pct": 2.5, "gross_margin_min_pct": 21.0}

# "Metric contract" (Lecture 11, slide 14): every KPI is defined before it is drawn.
METRICS = [
    ("Consumption ratio", "Consumed packs / forecast packs x 100", "Percent; hospital x medicine x month, summed", "All months in the warehouse (history is kept)", ">= 95% (assumed)", "Rows with null IDs or negative consumption are rejected at staging", "Data engineering; every DAG run"),
    ("Unfulfilled share of forecast", "Unfulfilled packs / forecast packs x 100", "Percent; hospital x medicine x month, summed", "All months in the warehouse", "<= 2.5% (assumed)", "Same as above", "Data engineering; every DAG run"),
    ("Net sales", "Gross sales minus discount", "USD; one row per sales transaction, summed", "All sales dates in the warehouse", "None set", "Rows with null IDs or price <= 0 are rejected at staging", "Data engineering; every DAG run"),
    ("Gross margin %", "(Net sales - cost of goods sold) / net sales x 100", "Percent; per channel group and month", "All sales dates in the warehouse", ">= 21% (assumed)", "Unmatched channel keys would show as Unknown (none found)", "Data engineering; every DAG run"),
    ("Channel group", "E-commerce if SalesChannel contains 'commerce'; Hospital if PharmacyType contains 'hospital'; otherwise Retail", "Label derived from Dim_Channel", "n/a", "n/a", "n/a", "BI layer"),
    ("Fast / slow medicine", "Rank of all medicines by packs sold: top 10 = Fast, bottom 10 = Slow", "Packs; one row per medicine (SCD2 versions merged by MedicineID)", "All sales dates in the warehouse", "None set", "Only 3 of 63 medicines have ColdChainFlag = Y, so cold chain is shown as a flag", "Data engineering; every DAG run"),
    ("Change vs previous refresh", "Value after the latest BI refresh minus value after the refresh before it", "Same unit as the KPI", "Between two consecutive DAG runs", "n/a", "First refresh has no previous value", "BI layer"),
]
LIMITS_NOTE = ("Records from 2026-01 onward are synthetic refresh batches made by our oversampling step. "
               "Step changes in early 2026 come from that generator, not from business performance.")

# ---------------------------------------------------------------------------
# Table specification: (column, Postgres type, business meaning).
# The same spec creates the tables AND fills the data dictionary sheet in Excel.
# ---------------------------------------------------------------------------
SPEC = {
    "hospital_monthly": [
        ("month", "DATE", "First day of the demand month"),
        ("hospital_id", "TEXT", "Hospital identifier"),
        ("hospital_name", "TEXT", "Hospital name"),
        ("hospital_type", "TEXT", "Type of hospital"),
        ("forecast_packs", "BIGINT", "Forecast demand in packs"),
        ("requested_packs", "BIGINT", "Packs requested by the hospital"),
        ("received_packs", "BIGINT", "Packs received by the hospital"),
        ("consumed_packs", "BIGINT", "Packs consumed (actual consumption)"),
        ("unfulfilled_packs", "BIGINT", "Demand that could not be fulfilled, in packs"),
        ("stockout_days", "BIGINT", "Total days out of stock (sum over medicines)"),
        ("avg_service_level_pct", "NUMERIC(8,2)", "Average service level in percent"),
        ("consumption_ratio_pct", "NUMERIC(8,2)", "Consumed / forecast x 100"),
        ("unfulfilled_pct", "NUMERIC(8,2)", "Unfulfilled / forecast x 100"),
    ],
    "hospital_medicine_gap": [
        ("medicine_id", "TEXT", "Medicine identifier"),
        ("generic_name", "TEXT", "Generic drug name"),
        ("therapeutic_class", "TEXT", "Therapeutic class"),
        ("forecast_packs", "BIGINT", "Total forecast demand in packs"),
        ("consumed_packs", "BIGINT", "Total packs consumed"),
        ("unfulfilled_packs", "BIGINT", "Total unfulfilled demand in packs"),
        ("consumption_ratio_pct", "NUMERIC(8,2)", "Consumed / forecast x 100"),
        ("unfulfilled_pct", "NUMERIC(8,2)", "Unfulfilled / forecast x 100"),
    ],
    "channel_monthly": [
        ("month", "DATE", "First day of the sales month"),
        ("channel_group", "TEXT", "Retail, Hospital or E-commerce (derived from SalesChannel + PharmacyType)"),
        ("sales_channel", "TEXT", "SalesChannel from Dim_Channel"),
        ("pharmacy_type", "TEXT", "PharmacyType from Dim_Channel"),
        ("transactions", "BIGINT", "Number of sales transactions"),
        ("units_sold", "BIGINT", "Packs sold"),
        ("gross_sales", "NUMERIC(16,2)", "Sales before discount, USD"),
        ("discount", "NUMERIC(16,2)", "Discount given, USD"),
        ("net_sales", "NUMERIC(16,2)", "Sales after discount, USD"),
        ("cogs", "NUMERIC(16,2)", "Cost of goods sold, USD"),
        ("gross_margin", "NUMERIC(16,2)", "Net sales minus COGS, USD"),
        ("gross_margin_pct", "NUMERIC(8,2)", "Gross margin / net sales x 100"),
    ],
    "medicine_velocity": [
        ("medicine_id", "TEXT", "Medicine identifier (all SCD2 versions merged)"),
        ("generic_name", "TEXT", "Generic drug name"),
        ("brand_name", "TEXT", "Fictional brand name"),
        ("therapeutic_class", "TEXT", "Therapeutic class"),
        ("cold_chain_flag", "TEXT", "Y if the medicine needs cold-chain storage"),
        ("units_sold", "BIGINT", "Total packs sold"),
        ("net_sales", "NUMERIC(16,2)", "Total net sales, USD"),
        ("gross_margin", "NUMERIC(16,2)", "Total gross margin, USD"),
        ("overall_rank", "INT", "Rank by units sold among all medicines (1 = fastest)"),
        ("rank_in_class", "INT", "Rank by units sold inside its therapeutic class (1 = fastest)"),
        ("movement", "TEXT", "Fast (top 10), Slow (bottom 10) or Medium"),
    ],
    "refresh_history": [
        ("refresh_no", "INT", "Running number of the BI refresh"),
        ("refreshed_at", "TIMESTAMP", "UTC time of the BI refresh"),
        ("sales_rows", "BIGINT", "Rows in Fact_Sales"),
        ("hospital_rows", "BIGINT", "Rows in Fact_HospitalDemand"),
        ("units_sold", "BIGINT", "Total packs sold"),
        ("net_sales", "NUMERIC(16,2)", "Total net sales, USD"),
        ("gross_margin", "NUMERIC(16,2)", "Total gross margin, USD"),
        ("gross_margin_pct", "NUMERIC(8,2)", "Gross margin / net sales x 100"),
        ("forecast_packs", "BIGINT", "Total hospital forecast, packs"),
        ("consumed_packs", "BIGINT", "Total hospital consumption, packs"),
        ("unfulfilled_packs", "BIGINT", "Total unfulfilled demand, packs"),
        ("consumption_ratio_pct", "NUMERIC(8,2)", "Consumed / forecast x 100"),
        ("unfulfilled_pct", "NUMERIC(8,2)", "Unfulfilled / forecast x 100"),
        ("sales_data_to", "DATE", "Latest sales date in the mart"),
        ("hospital_data_to", "DATE", "Latest hospital demand month in the mart"),
    ],
}
SNAPSHOT_TABLES = [t for t in SPEC if t != "refresh_history"]

# Hospital dimension de-duplicated by HospitalID so a join can never double count.
HOSP_DIM = ("(SELECT HospitalID, MAX(HospitalName) AS HospitalName, MAX(HospitalType) AS HospitalType "
            "FROM Dim_Hospital GROUP BY HospitalID)")

CHANNEL_GROUP = ("CASE WHEN c.ChannelKey IS NULL THEN 'Unknown' "
                 "WHEN lower(c.SalesChannel) LIKE '%commerce%' THEN 'E-commerce' "
                 "WHEN lower(c.PharmacyType) LIKE '%hospital%' THEN 'Hospital' "
                 "ELSE 'Retail' END")


def _pct(num, den):
    return (num * 100.0 / den.where(den != 0)).round(2)


def build_frames(con):
    """Run the aggregate queries on the DuckDB star schema; returns {table: DataFrame}."""
    F = {}

    # ---- BI 1: hospital forecast vs actual consumption + unfulfilled demand
    h = con.execute(f"""
        SELECT CAST(date_trunc('month', d.FullDate) AS DATE) AS month,
               f.HospitalID AS hospital_id, MAX(h.HospitalName) AS hospital_name, MAX(h.HospitalType) AS hospital_type,
               CAST(SUM(f.ForecastDemandPacks) AS BIGINT) AS forecast_packs,
               CAST(SUM(f.RequestedQuantityPacks) AS BIGINT) AS requested_packs,
               CAST(SUM(f.ReceivedQuantityPacks) AS BIGINT) AS received_packs,
               CAST(SUM(f.ConsumedQuantityPacks) AS BIGINT) AS consumed_packs,
               CAST(SUM(f.UnfulfilledDemandPacks) AS BIGINT) AS unfulfilled_packs,
               CAST(SUM(f.StockoutDays) AS BIGINT) AS stockout_days,
               ROUND(CAST(AVG(f.ServiceLevelPct) AS DOUBLE), 2) AS avg_service_level_pct
        FROM Fact_HospitalDemand f
        JOIN Dim_Date d ON d.DateKey = f.DateKey
        LEFT JOIN {HOSP_DIM} h ON h.HospitalID = f.HospitalID
        GROUP BY 1, 2 ORDER BY 1, 2""").df()
    h["consumption_ratio_pct"] = _pct(h.consumed_packs, h.forecast_packs)
    h["unfulfilled_pct"] = _pct(h.unfulfilled_packs, h.forecast_packs)
    F["hospital_monthly"] = h

    g = con.execute("""
        SELECT f.MedicineID AS medicine_id, MAX(m.GenericName) AS generic_name, MAX(m.TherapeuticClass) AS therapeutic_class,
               CAST(SUM(f.ForecastDemandPacks) AS BIGINT) AS forecast_packs,
               CAST(SUM(f.ConsumedQuantityPacks) AS BIGINT) AS consumed_packs,
               CAST(SUM(f.UnfulfilledDemandPacks) AS BIGINT) AS unfulfilled_packs
        FROM Fact_HospitalDemand f
        LEFT JOIN Dim_Medicine m ON m.MedicineSK = f.MedicineSK
        GROUP BY f.MedicineID ORDER BY unfulfilled_packs DESC""").df()
    g["consumption_ratio_pct"] = _pct(g.consumed_packs, g.forecast_packs)
    g["unfulfilled_pct"] = _pct(g.unfulfilled_packs, g.forecast_packs)
    F["hospital_medicine_gap"] = g

    # ---- BI 2: channel revenue split + gross margin (LEFT JOIN so totals always reconcile)
    ch = con.execute(f"""
        SELECT CAST(date_trunc('month', d.FullDate) AS DATE) AS month,
               {CHANNEL_GROUP} AS channel_group,
               COALESCE(c.SalesChannel, 'Unknown') AS sales_channel,
               COALESCE(c.PharmacyType, 'Unknown') AS pharmacy_type,
               CAST(COUNT(*) AS BIGINT) AS transactions,
               CAST(SUM(s.QuantitySoldPacks) AS BIGINT) AS units_sold,
               ROUND(CAST(SUM(s.GrossSalesAmountUSD) AS DOUBLE), 2) AS gross_sales,
               ROUND(CAST(SUM(s.DiscountAmountUSD) AS DOUBLE), 2) AS discount,
               ROUND(CAST(SUM(s.NetSalesAmountUSD) AS DOUBLE), 2) AS net_sales,
               ROUND(CAST(SUM(s.CostOfGoodsSoldUSD) AS DOUBLE), 2) AS cogs,
               ROUND(CAST(SUM(s.GrossMarginUSD) AS DOUBLE), 2) AS gross_margin
        FROM Fact_Sales s
        JOIN Dim_Date d ON d.DateKey = s.DateKey
        LEFT JOIN Dim_Channel c ON c.ChannelKey = s.ChannelKey
        GROUP BY 1, 2, 3, 4 ORDER BY 1, 2, 3, 4""").df()
    ch["gross_margin_pct"] = _pct(ch.gross_margin, ch.net_sales)
    F["channel_monthly"] = ch

    # ---- BI 3: fast vs slow moving medicines (cold chain), by therapeutic class
    v = con.execute("""
        SELECT f.MedicineID AS medicine_id, MAX(m.GenericName) AS generic_name, MAX(m.FictionalBrandName) AS brand_name,
               MAX(m.TherapeuticClass) AS therapeutic_class, MAX(m.ColdChainFlag) AS cold_chain_flag,
               CAST(SUM(f.QuantitySoldPacks) AS BIGINT) AS units_sold,
               ROUND(CAST(SUM(f.NetSalesAmountUSD) AS DOUBLE), 2) AS net_sales,
               ROUND(CAST(SUM(f.GrossMarginUSD) AS DOUBLE), 2) AS gross_margin
        FROM Fact_Sales f
        JOIN Dim_Medicine m ON m.MedicineSK = f.MedicineSK
        GROUP BY f.MedicineID""").df()
    # Only 3 of the 63 medicines have ColdChainFlag = 'Y', so a top-10 / bottom-10 inside that group is meaningless.
    # We rank ALL medicines and keep cold_chain_flag as a column so cold-chain items stay visible.
    n, k = len(v), min(10, len(v) // 2)
    v["overall_rank"] = v.units_sold.rank(ascending=False, method="first").astype("Int64")
    v["rank_in_class"] = v.groupby("therapeutic_class").units_sold.rank(ascending=False, method="first").astype("Int64")
    v["movement"] = "Medium"
    v.loc[v.overall_rank <= k, "movement"] = "Fast (Top 10)"
    v.loc[v.overall_rank > n - k, "movement"] = "Slow (Bottom 10)"
    F["medicine_velocity"] = v.sort_values(["overall_rank", "units_sold"], ascending=[True, False], na_position="last")

    # ---- refresh history row (one per run); totals taken straight from the facts
    s = con.execute("""SELECT COUNT(*), CAST(SUM(QuantitySoldPacks) AS BIGINT),
                              ROUND(CAST(SUM(NetSalesAmountUSD) AS DOUBLE), 2), ROUND(CAST(SUM(GrossMarginUSD) AS DOUBLE), 2)
                       FROM Fact_Sales""").fetchone()
    hd = con.execute("""SELECT COUNT(*), CAST(SUM(ForecastDemandPacks) AS BIGINT), CAST(SUM(ConsumedQuantityPacks) AS BIGINT),
                               CAST(SUM(UnfulfilledDemandPacks) AS BIGINT) FROM Fact_HospitalDemand""").fetchone()
    s_to = con.execute("SELECT MAX(d.FullDate) FROM Fact_Sales f JOIN Dim_Date d ON d.DateKey = f.DateKey").fetchone()[0]
    h_to = con.execute("SELECT MAX(d.FullDate) FROM Fact_HospitalDemand f JOIN Dim_Date d ON d.DateKey = f.DateKey").fetchone()[0]
    F["refresh_history"] = pd.DataFrame([{
        "refresh_no": None, "refreshed_at": datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0),
        "sales_rows": s[0], "hospital_rows": hd[0], "units_sold": s[1], "net_sales": s[2], "gross_margin": s[3],
        "gross_margin_pct": round(s[3] * 100.0 / s[2], 2) if s[2] else None,
        "forecast_packs": hd[1], "consumed_packs": hd[2], "unfulfilled_packs": hd[3],
        "consumption_ratio_pct": round(hd[2] * 100.0 / hd[1], 2) if hd[1] else None,
        "unfulfilled_pct": round(hd[3] * 100.0 / hd[1], 2) if hd[1] else None,
        "sales_data_to": s_to, "hospital_data_to": h_to}])
    return F


def _rows(df, cols):
    """DataFrame -> list of tuples with plain Python values (NaN/NA -> None)."""
    d = df[cols].astype(object).where(df[cols].notna(), None)
    return [tuple(r) for r in d.itertuples(index=False, name=None)]


def write_postgres(F):
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn, conn.cursor() as cur:                       # one transaction: all or nothing
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
            for t, cols in SPEC.items():
                ddl = ", ".join(f"{c} {typ}" for c, typ, _ in cols)
                cur.execute(f"CREATE TABLE IF NOT EXISTS {SCHEMA}.{t} ({ddl})")
            cur.execute(f"SELECT COALESCE(MAX(refresh_no), 0) + 1 FROM {SCHEMA}.refresh_history")
            F["refresh_history"].loc[:, "refresh_no"] = cur.fetchone()[0]
            for t in SNAPSHOT_TABLES:                          # snapshots: replace with the full current mart
                cur.execute(f"TRUNCATE {SCHEMA}.{t}")
            for t, cols in SPEC.items():
                names = [c for c, _, _ in cols]
                execute_values(cur, f"INSERT INTO {SCHEMA}.{t} ({', '.join(names)}) VALUES %s",
                               _rows(F[t], names), page_size=2000)
            cur.execute(f"SELECT * FROM {SCHEMA}.refresh_history ORDER BY refresh_no")
            hist = pd.DataFrame(cur.fetchall(), columns=[c[0] for c in cur.description])
    finally:
        conn.close()
    return hist


def export_excel(F, hist, path=EXCEL_PATH):
    from openpyxl.chart import BarChart, LineChart, Reference
    from openpyxl.styles import Alignment, Font, PatternFill
    os.makedirs(os.path.dirname(path), exist_ok=True)

    h, ch, v = F["hospital_monthly"], F["channel_monthly"], F["medicine_velocity"]
    hm = h.groupby("month", as_index=False)[["forecast_packs", "consumed_packs", "unfulfilled_packs"]].sum()
    hm["consumption_ratio_pct"] = _pct(hm.consumed_packs, hm.forecast_packs)
    hh = h.groupby(["hospital_name", "hospital_type"], as_index=False)[["forecast_packs", "consumed_packs", "unfulfilled_packs"]].sum()
    hh["consumption_ratio_pct"] = _pct(hh.consumed_packs, hh.forecast_packs)
    hh["unfulfilled_pct"] = _pct(hh.unfulfilled_packs, hh.forecast_packs)
    hh = hh.sort_values("unfulfilled_packs", ascending=False)
    cs = ch.groupby("channel_group", as_index=False)[["transactions", "units_sold", "gross_sales", "discount", "net_sales", "cogs", "gross_margin"]].sum()
    cs["gross_margin_pct"] = _pct(cs.gross_margin, cs.net_sales)
    cs["revenue_share_pct"] = (cs.net_sales * 100.0 / cs.net_sales.sum()).round(2)
    cs = cs.sort_values("net_sales", ascending=False)
    vc = v.groupby("therapeutic_class", as_index=False).agg(
        medicines=("medicine_id", "count"), cold_chain_medicines=("cold_chain_flag", lambda x: int((x == "Y").sum())),
        units_sold=("units_sold", "sum"), net_sales=("net_sales", "sum")).sort_values("units_sold", ascending=False)
    top10 = v[v.movement == "Fast (Top 10)"].sort_values("overall_rank")

    latest = hist.iloc[-1]
    readme = pd.DataFrame({"Item": [
        "Project", "Mart", "Refresh number", "Refreshed at (UTC)", "Source", "Sheets",
        "KPI definitions", "Channel groups", "Fast / slow", "Medicines"], "Description": [
        "Intelligent Data Architectures, Fall 2026 - Pharma Cold Chain DWH + BI (Narin Nadeem 29167, Ayesha Salahuddin 29157)",
        "Phase 1: Sales & Demand Intelligence (SDI) - BI layer",
        str(latest["refresh_no"]), str(latest["refreshed_at"]),
        "DuckDB star schema (Fact_Sales, Fact_HospitalDemand, Dim_*) -> Airflow task publish_bi_tables -> Postgres schema bi -> Grafana + this file",
        "Metric_Definitions, Refresh_History, Hospital_Monthly, Hospital_ByHospital, Hospital_Medicine_Gap, Channel_Summary, Channel_Monthly, Velocity_Medicines, Velocity_ByClass, BI_Data_Dictionary",
        "Consumption ratio = consumed / forecast x 100. Unfulfilled % = unfulfilled / forecast x 100. Gross margin % = gross margin / net sales x 100.",
        "E-commerce = SalesChannel contains 'commerce'; Hospital = PharmacyType contains 'hospital'; everything else = Retail.",
        "All 63 medicines ranked by packs sold: top 10 = Fast, bottom 10 = Slow (only 3 medicines carry ColdChainFlag = Y, so cold-chain is shown as a flag column). The two SCD2 versions of a medicine are merged by MedicineID.",
        "Packs sold and amounts are summed over the full history in the mart (data is never deleted). " + LIMITS_NOTE + " Targets are assumptions: " + str(TARGETS)]})
    dd = pd.DataFrame([(t, c, typ, desc) for t, cols in SPEC.items() for c, typ, desc in cols],
                      columns=["bi_table", "column", "type", "description"])

    metric_df = pd.DataFrame(METRICS, columns=["metric", "definition", "unit_and_grain", "time_rule", "target", "exclusions", "owner_and_refresh"])
    sheets = {"README": readme, "Metric_Definitions": metric_df, "Refresh_History": hist, "Hospital_Monthly": hm, "Hospital_ByHospital": hh,
              "Hospital_Medicine_Gap": F["hospital_medicine_gap"], "Channel_Summary": cs, "Channel_Monthly": ch,
              "Velocity_Medicines": v, "Velocity_ByClass": vc, "BI_Data_Dictionary": dd}

    def col(df, name):                                   # 1-based column index for charts
        return list(df.columns).index(name) + 1

    def chart(kind, title, ws, df, ycol, xcol, anchor, horizontal=False, rows=None, w=17, hgt=8.5):
        c = LineChart() if kind == "line" else BarChart()
        if kind == "bar" and horizontal:
            c.type = "bar"
        c.title, c.width, c.height = title, w, hgt
        last = (rows or len(df)) + 1
        c.add_data(Reference(ws, min_col=col(df, ycol), min_row=1, max_row=last), titles_from_data=True)
        c.set_categories(Reference(ws, min_col=col(df, xcol), min_row=2, max_row=last))
        ws.add_chart(c, anchor)

    with pd.ExcelWriter(path, engine="openpyxl", datetime_format="yyyy-mm-dd", date_format="yyyy-mm-dd") as xw:
        for name, df in sheets.items():
            df.to_excel(xw, sheet_name=name, index=False)
        wb = xw.book
        for ws in wb.worksheets:
            for c in ws[1]:
                c.font, c.alignment = Font(bold=True, color="FFFFFF"), Alignment(horizontal="center")
                c.fill = PatternFill("solid", fgColor="1F4E78")
            for cc in ws.columns:
                w = max(len(str(c.value)) if c.value is not None else 0 for c in cc[:80])
                ws.column_dimensions[cc[0].column_letter].width = min(max(12, w + 2), 30)
            ws.freeze_panes = "A2"
        wb["README"].column_dimensions["B"].width = 130
        wb["BI_Data_Dictionary"].column_dimensions["D"].width = 80
        for r in wb["README"].iter_rows(min_row=2):
            r[1].alignment = Alignment(wrap_text=True, vertical="top")

        chart("line", "Consumption ratio % by month", wb["Hospital_Monthly"], hm, "consumption_ratio_pct", "month", "H2")
        chart("bar", "Unfulfilled demand by hospital (packs)", wb["Hospital_ByHospital"], hh, "unfulfilled_packs", "hospital_name", "J2", horizontal=True)
        chart("bar", "Net sales by channel group (USD)", wb["Channel_Summary"], cs, "net_sales", "channel_group", "N2", w=13)
        chart("bar", "Gross margin % by channel group", wb["Channel_Summary"], cs, "gross_margin_pct", "channel_group", "N20", w=13)
        if len(top10):
            tv = wb["Velocity_Medicines"]
            chart("bar", "Top 10 fast-moving medicines (packs)", tv, v, "units_sold", "generic_name", "N2", horizontal=True, rows=len(top10), hgt=10)
        chart("line", "Fact_Sales rows per BI refresh", wb["Refresh_History"], hist, "sales_rows", "refresh_no", "Q2", w=13)
    return path


def export_excel_only():
    """Rebuild only the Excel file from the current mart + stored refresh history (no new refresh row)."""
    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    try:
        F = build_frames(con)
    finally:
        con.close()
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        hist = pd.read_sql(f"SELECT * FROM {SCHEMA}.refresh_history ORDER BY refresh_no", conn)
    finally:
        conn.close()
    print("Excel   :", export_excel(F, hist))


def run_bi_layer():
    print("=" * 65)
    print("BUILDING BI LAYER (DuckDB star schema -> Postgres schema bi + Excel)")
    print("=" * 65)
    t0 = time.time()
    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    try:
        F = build_frames(con)
    finally:
        con.close()
    for t, df in F.items():
        print(f"  {t:<24}: {len(df):>6,} rows")

    # reconciliation: channel table must add up to the fact table
    fact_net = float(F["refresh_history"].net_sales.iloc[0])
    chan_net = float(F["channel_monthly"].net_sales.sum())
    print(f"  check: net sales in Fact_Sales {fact_net:,.2f} vs channel table {chan_net:,.2f} "
          f"-> {'OK' if abs(fact_net - chan_net) < 1 else 'MISMATCH'}")
    print("  channel groups used:")
    print(F["channel_monthly"].groupby(["channel_group", "sales_channel", "pharmacy_type"]).net_sales.sum().round(0).to_string())

    hist = write_postgres(F)
    print(f"  Postgres: schema {SCHEMA} written, refresh number {int(hist.refresh_no.iloc[-1])}")
    xlsx = export_excel(F, hist)
    print(f"  Excel   : {xlsx}")
    print(f"Done in {time.time() - t0:.1f} s")


if __name__ == "__main__":
    import sys
    if "--excel-only" in sys.argv:
        export_excel_only()
    else:
        run_bi_layer()
