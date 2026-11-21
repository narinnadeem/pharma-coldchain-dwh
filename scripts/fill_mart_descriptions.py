import pandas as pd
from openpyxl import load_workbook

M = {
"Dim_Channel": {
 "channelkey": "Surrogate key of the sales channel (primary key)",
 "saleschannel": "Sales channel: Physical Pharmacy or Ecommerce",
 "pharmacytype": "Type of pharmacy (chain, independent or hospital pharmacy)"},
"Dim_Date": {
 "datekey": "Date key in YYYYMMDD format (primary key)",
 "fulldate": "Calendar date",
 "year": "Calendar year", "quarter": "Calendar quarter (1 to 4)",
 "month": "Month number (1 to 12)", "monthname": "Month name",
 "day": "Day of the month", "dayname": "Day of the week name"},
"Dim_Hospital": {
 "hospitalid": "Hospital identifier (primary key)",
 "hospitalgln": "GS1 Global Location Number of the hospital",
 "hospitalname": "Name of the hospital", "hospitaltype": "Type of hospital",
 "city": "City of the hospital", "state": "State of the hospital",
 "countrycode": "ISO country code", "licensedbeds": "Number of licensed beds",
 "icubeds": "Number of intensive care beds",
 "traumalevel": "Trauma centre level; blank if none",
 "catchmentpopulation": "Population served by the hospital"},
"Dim_Medicine": {
 "medicinesk": "Surrogate key of the medicine version (primary key, SCD Type 2)",
 "medicineid": "Business identifier of the medicine",
 "internalsku": "Internal stock keeping unit code",
 "gtin14": "14-digit GS1 Global Trade Item Number",
 "genericname": "Generic drug name",
 "fictionalbrandname": "Fictional brand name",
 "diseasecategory": "Disease area the medicine is used for",
 "therapeuticclass": "Therapeutic drug class",
 "dosageform": "Pharmaceutical form of the medicine",
 "strength": "Dose strength per unit",
 "coldchainflag": "Y/N: needs cold-chain storage",
 "storagemintempc": "Minimum storage temperature in degrees Celsius",
 "storagemaxtempc": "Maximum storage temperature in degrees Celsius",
 "unitmanufacturingcostusd": "Manufacturing cost per pack in US dollars",
 "wholesalepriceusd": "Wholesale price per pack in US dollars",
 "recommendedretailpriceusd": "Recommended retail price per pack in US dollars",
 "roweffectivedate": "SCD Type 2: date this version became valid",
 "rowexpirydate": "SCD Type 2: date this version stopped being valid",
 "iscurrent": "SCD Type 2: true for the current version of the medicine"},
"Fact_HospitalDemand": {
 "hospitaldemandrecordid": "Hospital demand record identifier (grain of the fact)",
 "datekey": "Foreign key to Dim_Date (demand month)",
 "hospitalid": "Foreign key to Dim_Hospital",
 "medicinesk": "Foreign key to Dim_Medicine (version valid at that time)",
 "medicineid": "Business identifier of the medicine",
 "forecastdemandpacks": "Forecast demand in packs",
 "requestedquantitypacks": "Packs requested by the hospital",
 "approvedquantitypacks": "Packs approved for supply",
 "receivedquantitypacks": "Packs received by the hospital",
 "consumedquantitypacks": "Packs consumed in the month",
 "unfulfilleddemandpacks": "Demand in packs that could not be fulfilled",
 "stockoutdays": "Days the medicine was out of stock",
 "servicelevelpct": "Share of demand fulfilled, in percent"},
"Fact_Sales": {
 "salestransactionid": "Sales transaction identifier (grain of the fact)",
 "datekey": "Foreign key to Dim_Date (sale date)",
 "medicinesk": "Foreign key to Dim_Medicine (version valid at sale)",
 "medicineid": "Business identifier of the medicine",
 "hospitalid": "Linked hospital; blank if not hospital-linked",
 "channelkey": "Foreign key to Dim_Channel",
 "pharmacyid": "Identifier of the selling pharmacy",
 "quantitysoldpacks": "Packs sold",
 "unitretailpriceusd": "Retail price per pack in US dollars",
 "grosssalesamountusd": "Sales amount before discount in US dollars",
 "discountamountusd": "Discount in US dollars",
 "netsalesamountusd": "Sales amount after discount in US dollars",
 "costofgoodssoldusd": "Cost of goods sold in US dollars",
 "grossmarginusd": "Net sales minus cost of goods sold in US dollars"},
"Query_Execution_Log": {
 "log_id": "Running identifier of the log row",
 "query_name": "Name of the analytical query that was run",
 "execution_time_ms": "Query run time in milliseconds",
 "rows_returned": "Number of rows the query returned",
 "executed_at": "Timestamp when the query ran"},
"Summary_Hospital_Fulfillment": {
 "hospitalname": "Name of the hospital",
 "hospitaltype": "Type of hospital",
 "totalforecastpacks": "Total forecast demand in packs",
 "totalconsumedpacks": "Total packs consumed",
 "consumptionvsforecastpct": "Consumed packs as a percentage of forecast",
 "totalunfulfilledpacks": "Total unfulfilled demand in packs",
 "avgservicelevelpct": "Average service level in percent"},
}

def get(t, c):
    return M.get(t, {}).get(str(c).strip().lower(), "")

df = pd.read_csv("data_dictionary/mart_tables.csv", dtype=str, keep_default_na=False)
df["description"] = [get(t, c) for t, c in zip(df["table"], df["column"])]
print("CSV rows:", len(df), "| empty:", int((df["description"] == "").sum()))
df.to_csv("data_dictionary/mart_tables.csv", index=False)

xl = "data_dictionary/Milestone.1.SDI.DWH.DataDictionary.xlsx"
wb = load_workbook(xl)
ws = wb["mart_tables"]
head = [str(c.value).strip().lower() if c.value else "" for c in ws[1]]
ti, ci, di = head.index("table") + 1, head.index("column") + 1, head.index("description") + 1
n = 0
for r in range(2, ws.max_row + 1):
    v = get(ws.cell(r, ti).value, ws.cell(r, ci).value)
    if v:
        ws.cell(r, di).value = v
        n += 1
print("Excel mart_tables filled", n, "rows")
wb.save(xl)
