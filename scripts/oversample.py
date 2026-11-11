"""
Oversampling layer: synthesises ONE new time-period batch per call (per DAG run)
for Sales (05) and Hospital Demand (06), and drops the CSVs in data/landing.

Guarantees across repeated runs:
  * IDs continue from the max ID found in raw + landing (no collisions).
  * Dates continue from the latest date found in raw + landing (no overlapping periods).
  * Hospital demand keeps its grain: one row per hospital x medicine x month.
  * COGS is rescaled with the regenerated quantity, so margins stay realistic.
"""
import os
import glob
import numpy as np
import pandas as pd
from datetime import datetime

BASE_DIR = os.getenv("AIRFLOW_HOME", os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
LANDING_DIR = os.path.join(BASE_DIR, "data", "landing")
LOG_PATH = os.path.join(BASE_DIR, "data", "oversampling_log.csv")
os.makedirs(LANDING_DIR, exist_ok=True)

SALES_RAW = "05_pharmacy_sales_data.csv"
HOSP_RAW = "06_hospital_demand_data.csv"


def _landing_files(kind):
    return sorted(glob.glob(os.path.join(LANDING_DIR, f"incremental_{kind}_*.csv")))


def _all_sources(kind, raw_file):
    return [os.path.join(RAW_DIR, raw_file)] + _landing_files(kind)


def get_max_id(kind, raw_file, id_col, pattern):
    """Highest numeric ID across the raw file AND every landing file."""
    max_id = 0
    for path in _all_sources(kind, raw_file):
        ids = (pd.read_csv(path, usecols=[id_col])[id_col]
               .astype(str).str.extract(pattern)[0].dropna().astype(int))
        if not ids.empty:
            max_id = max(max_id, int(ids.max()))
    return max_id


def get_latest_date(kind, raw_file, date_col):
    """Latest date across the raw file AND every landing file."""
    latest = None
    for path in _all_sources(kind, raw_file):
        d = pd.to_datetime(pd.read_csv(path, usecols=[date_col])[date_col]).max()
        latest = d if latest is None else max(latest, d)
    return latest


def _find_col(df, name):
    for c in df.columns:
        if c.lower() == name.lower():
            return c
    raise KeyError(f"Column '{name}' not found in {list(df.columns)}")


def _append_log(rows):
    df = pd.DataFrame(rows)
    df.to_csv(LOG_PATH, mode="a", header=not os.path.exists(LOG_PATH), index=False)


def make_sales_batch(raw, start_id, latest_date, multiplier):
    n = int(len(raw) * multiplier)
    s = raw.sample(n=n, replace=True).copy()

    # per-unit cost from the template row, so COGS scales with the new quantity
    orig_qty = s["QuantitySoldPacks"].replace(0, np.nan)
    unit_cost = (s["CostOfGoodsSoldUSD"] / orig_qty).fillna(0)

    qty = np.random.randint(1, 15, size=n)
    days = np.random.randint(1, 31, size=n)                    # next 30 days after latest date
    secs = np.random.randint(8 * 3600, 20 * 3600, size=n)      # business hours
    ts = latest_date.normalize() + pd.to_timedelta(days, unit="D") + pd.to_timedelta(secs, unit="s")

    s["SalesTransactionID"] = [f"SALE-{i:09d}" for i in range(start_id + 1, start_id + n + 1)]
    s["SalesDate"] = ts.strftime("%Y-%m-%d")
    s["SalesTimestampUTC"] = ts.strftime("%Y-%m-%dT%H:%M:%SZ")
    s["QuantitySoldPacks"] = qty
    s["GrossSalesAmountUSD"] = (qty * s["UnitRetailPriceUSD"]).round(2)
    disc = np.random.choice([0.0, 0.05, 0.10], size=n)
    s["DiscountAmountUSD"] = (s["GrossSalesAmountUSD"] * disc).round(2)
    s["NetSalesAmountUSD"] = (s["GrossSalesAmountUSD"] - s["DiscountAmountUSD"]).round(2)
    s["CostOfGoodsSoldUSD"] = (unit_cost * qty).round(2)
    s["GrossMarginUSD"] = (s["NetSalesAmountUSD"] - s["CostOfGoodsSoldUSD"]).round(2)
    return s, ts.min(), ts.max()


def make_hospital_batch(raw, start_id, latest_month):
    """One new month: exactly one row per hospital x medicine pair (preserves grain)."""
    hcol = _find_col(raw, "HospitalID")
    mcol = _find_col(raw, "MedicineID")

    tmp = raw.copy()
    tmp["_m"] = pd.to_datetime(tmp["DemandMonth"])
    base = (tmp.sort_values("_m").drop_duplicates([hcol, mcol], keep="last")
               .drop(columns="_m").reset_index(drop=True))
    n = len(base)

    forecast = (base["ForecastDemandPacks"] * np.random.uniform(0.9, 1.1, n)).round().astype(int).clip(lower=1)
    requested = (forecast * np.random.uniform(0.9, 1.2, n)).astype(int)
    approved = (requested * np.random.uniform(0.85, 1.0, n)).astype(int)
    received = approved
    consumed = (received * np.random.uniform(0.80, 0.98, n)).astype(int)

    new_month = latest_month + pd.DateOffset(months=1)
    base["HospitalDemandRecordID"] = [f"HDEM-{i:08d}" for i in range(start_id + 1, start_id + n + 1)]
    base["DemandMonth"] = new_month.strftime("%Y-%m-%d")
    base["ForecastDemandPacks"] = forecast
    base["RequestedQuantityPacks"] = requested
    base["ApprovedQuantityPacks"] = approved
    base["ReceivedQuantityPacks"] = received
    base["ConsumedQuantityPacks"] = consumed
    base["UnfulfilledDemandPacks"] = np.maximum(0, requested - received)
    return base, new_month


def generate_batches(num_batches=1, sales_multiplier=0.02, **_ignored):
    """num_batches=1 -> one DAG run = one new time period = one refresh."""
    print("=" * 65)
    print("GENERATING INCREMENTAL BATCH(ES)")
    print("=" * 65)

    sales_raw = pd.read_csv(os.path.join(RAW_DIR, SALES_RAW))
    hosp_raw = pd.read_csv(os.path.join(RAW_DIR, HOSP_RAW))

    sale_id = get_max_id("sales", SALES_RAW, "SalesTransactionID", r"SALE-(\d+)")
    hosp_id = get_max_id("hosp", HOSP_RAW, "HospitalDemandRecordID", r"HDEM-(\d+)")
    sales_date = get_latest_date("sales", SALES_RAW, "SalesDate")
    hosp_month = get_latest_date("hosp", HOSP_RAW, "DemandMonth")

    raw_margin = sales_raw["GrossMarginUSD"].sum() / sales_raw["NetSalesAmountUSD"].sum()
    now = datetime.now()

    for b in range(1, num_batches + 1):
        ts = now.strftime("%Y%m%d_%H%M%S") + f"_b{b}"

        s, s_start, s_end = make_sales_batch(sales_raw, sale_id, sales_date, sales_multiplier)
        sale_id += len(s)
        sales_date = s_end
        s_path = os.path.join(LANDING_DIR, f"incremental_sales_{ts}.csv")
        s.to_csv(s_path, index=False)
        batch_margin = s["GrossMarginUSD"].sum() / s["NetSalesAmountUSD"].sum()
        print(f"Batch {b} Sales   : {len(s):,} rows | {s_start.date()} -> {s_end.date()} | "
              f"margin {batch_margin:.1%} (raw {raw_margin:.1%}) -> {os.path.basename(s_path)}")

        h, h_month = make_hospital_batch(hosp_raw, hosp_id, hosp_month)
        hosp_id += len(h)
        hosp_month = h_month
        h_path = os.path.join(LANDING_DIR, f"incremental_hosp_{ts}.csv")
        h.to_csv(h_path, index=False)
        print(f"Batch {b} Hospital: {len(h):,} rows | month {h_month.date()} -> {os.path.basename(h_path)}")

        _append_log([
            dict(generated_at=now.isoformat(timespec="seconds"), source="sales", file=os.path.basename(s_path),
                 rows=len(s), period_start=s_start.date(), period_end=s_end.date(),
                 total_value=round(s["NetSalesAmountUSD"].sum(), 2)),
            dict(generated_at=now.isoformat(timespec="seconds"), source="hospital_demand", file=os.path.basename(h_path),
                 rows=len(h), period_start=h_month.date(), period_end=h_month.date(),
                 total_value=int(h["ForecastDemandPacks"].sum())),
        ])


if __name__ == "__main__":
    generate_batches()