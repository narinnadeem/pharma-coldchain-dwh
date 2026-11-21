import pandas as pd
from openpyxl import load_workbook

D = {
# master data
"MedicineID": "Unique medicine identifier (primary key of the product master)",
"InternalSKU": "Internal stock keeping unit code of the medicine",
"GTIN14": "14-digit GS1 Global Trade Item Number (barcode) of the product",
"GenericName": "Generic (non-branded) drug name",
"FictionalBrandName": "Fictional brand name used for the medicine in this dataset",
"DiseaseCategory": "Disease area the medicine is used for",
"ICD10Category": "ICD-10 code group of the disease",
"ATCCode": "WHO Anatomical Therapeutic Chemical (ATC) classification code",
"TherapeuticClass": "Therapeutic drug class of the medicine",
"DosageForm": "Pharmaceutical form (for example tablet or injection)",
"Strength": "Dose strength per unit (for example 500 mg)",
"RouteOfAdministration": "Route by which the medicine is given to the patient",
"PackSize": "Number of units contained in one pack",
"BaseUnitOfMeasure": "Base unit in which stock and sales are counted",
"PrescriptionClass": "Whether the medicine needs a prescription or is over the counter",
"ControlledMedicineFlag": "Y/N: medicine is a controlled substance",
"ColdChainFlag": "Y/N: medicine needs cold-chain storage and transport",
"StorageMinTempC": "Minimum allowed storage temperature in degrees Celsius",
"StorageMaxTempC": "Maximum allowed storage temperature in degrees Celsius",
"MaxHumidityPct": "Maximum allowed storage humidity in percent",
"ShelfLifeMonths": "Total shelf life of the product in months",
"MinimumRemainingShelfLifeDays": "Minimum remaining shelf life in days required at delivery",
"PrimarySupplierID": "Identifier of the main supplier of the medicine",
"PrimaryPlantID": "Identifier of the main manufacturing plant",
"ManufacturingLeadTimeDays": "Days needed to manufacture a batch",
"StandardBatchSizePacks": "Standard production batch size in packs",
"SafetyStockDays": "Safety stock target expressed in days of demand",
"ReorderReviewCycleDays": "Days between inventory reorder reviews",
"UnitManufacturingCostUSD": "Manufacturing cost per pack in US dollars",
"WholesalePriceUSD": "Wholesale price per pack in US dollars",
"RecommendedRetailPriceUSD": "Recommended retail price per pack in US dollars",
"ProductStatus": "Lifecycle status of the product (for example Active)",
"EffectiveFromDate": "Date from which this product record is valid (used for SCD Type 2 history)",
"SourceSystem": "Source system that produced the record",
# sales
"SalesTransactionID": "Unique identifier of the sales transaction (primary key)",
"SalesDate": "Calendar date of the sale",
"SalesTimestampUTC": "Exact date and time of the sale in UTC",
"SalesChannel": "Sales channel: Physical Pharmacy or Ecommerce",
"PharmacyID": "Identifier of the pharmacy that made the sale",
"PharmacyGLN": "GS1 Global Location Number of the pharmacy",
"PharmacyName": "Name of the pharmacy",
"PharmacyType": "Type of pharmacy (chain, independent or hospital pharmacy)",
"HospitalID": "Identifier of the linked hospital; blank when the sale is not hospital-linked",
"PharmacyCity": "City where the pharmacy is located",
"PharmacyState": "State where the pharmacy is located",
"CountryCode": "ISO country code (US)",
"PharmacyOrderID": "Identifier of the pharmacy order that supplied the stock",
"PharmacyDeliveryID": "Identifier of the delivery that brought the stock to the pharmacy",
"PharmacyDeliveryDate": "Date the stock was delivered to the pharmacy",
"DeliveryQuantityPacks": "Quantity of packs in the delivery",
"DeliveryOTIFFlag": "Y/N: delivery was on time and in full",
"WarehouseLotID": "Identifier of the warehouse lot the stock came from",
"SourceWarehouseID": "Identifier of the warehouse that shipped the stock",
"ProductionBatchID": "Identifier of the production batch of the medicine",
"BatchExpiryDate": "Expiry date of the production batch",
"DaysToExpiryAtSale": "Days remaining until expiry on the day of sale",
"QuantitySoldPacks": "Number of packs sold in the transaction",
"UnitRetailPriceUSD": "Retail price per pack in US dollars",
"GrossSalesAmountUSD": "Sales amount before discount (quantity x unit price) in US dollars",
"DiscountAmountUSD": "Discount applied to the sale in US dollars",
"NetSalesAmountUSD": "Sales amount after discount (gross minus discount) in US dollars",
"SalesTaxUSD": "Sales tax charged in US dollars",
"CostOfGoodsSoldUSD": "Cost of the goods sold in US dollars",
"GrossMarginUSD": "Gross margin (net sales minus cost of goods sold) in US dollars",
"PromotionCode": "Promotion code used; blank when no promotion applied",
"PrescriptionRequiredFlag": "Y/N: the sale required a prescription",
"PrescriptionID": "Prescription identifier; blank when no prescription was used",
"CustomerSegment": "Customer segment of the buyer",
"PayerType": "Who pays for the medicine (cash, insurance, government program, hospital formulary)",
"PaymentMethod": "Payment method used (cash, card, insurance claim, etc.)",
"EcommerceDeliveryStatus": "Delivery status for ecommerce orders; Not applicable for in-store sales",
"ReturnFlag": "Y/N: the sale was returned",
"ReturnedQuantityPacks": "Number of packs returned",
"SaleStatus": "Status of the sale (for example Completed)",
# hospital demand
"HospitalDemandRecordID": "Unique identifier of the hospital demand record (primary key)",
"DemandMonth": "First day of the month the demand record refers to",
"HospitalGLN": "GS1 Global Location Number of the hospital",
"HospitalName": "Name of the hospital",
"HospitalType": "Type of hospital (for example academic or community)",
"HospitalPharmacyID": "Identifier of the hospital's own pharmacy",
"City": "City where the hospital is located",
"State": "State where the hospital is located",
"LicensedBeds": "Number of licensed hospital beds",
"ICUBeds": "Number of intensive care unit beds",
"AverageOccupancyRatePct": "Average bed occupancy rate in percent",
"TraumaLevel": "Trauma centre level; blank when the hospital has no trauma designation",
"CatchmentPopulation": "Population served by the hospital",
"EstimatedDiseaseCases": "Estimated number of disease cases in the catchment area for the month",
"DiseaseRelatedAdmissions": "Hospital admissions related to the disease in the month",
"OutpatientEncounters": "Outpatient visits related to the disease in the month",
"ForecastDemandPacks": "Forecast demand for the medicine in packs",
"RequestedQuantityPacks": "Quantity of packs requested by the hospital",
"ApprovedQuantityPacks": "Quantity of packs approved for supply",
"ReceivedQuantityPacks": "Quantity of packs actually received by the hospital",
"EmergencyRequestQuantityPacks": "Packs requested as emergency orders",
"OpeningStockPacks": "Stock in packs at the start of the month",
"DispensedQuantityPacks": "Packs dispensed to patients and wards",
"ConsumedQuantityPacks": "Packs consumed during the month",
"WastageQuantityPacks": "Packs wasted (expired or damaged)",
"ClosingStockPacks": "Stock in packs at the end of the month",
"UnfulfilledDemandPacks": "Demand in packs that could not be fulfilled",
"StockoutDays": "Number of days the medicine was out of stock",
"ServiceLevelPct": "Share of demand fulfilled, in percent",
"MedicationRequestStatus": "Status of the medication request (for example Completed)",
"FormularyStatus": "Whether the medicine is on the hospital formulary",
"AverageLeadTimeDays": "Average supply lead time in days",
"DemandTrendIndex": "Relative demand trend index (1.0 means baseline demand)",
}

df = pd.read_csv("data_dictionary/raw_files.csv", dtype=str, keep_default_na=False)
df["description"] = df["column"].map(D).fillna("")
missing = sorted(set(df.loc[df["description"] == "", "column"]))
df.to_csv("data_dictionary/raw_files.csv", index=False)
print("CSV rows:", len(df), "| still empty:", missing)

xl = "data_dictionary/Milestone.1.SDI.DWH.DataDictionary.xlsx"
wb = load_workbook(xl)
for ws in wb.worksheets:
    head = [str(c.value).strip().lower() if c.value else "" for c in ws[1]]
    if "column" in head and "description" in head:
        ci, di = head.index("column") + 1, head.index("description") + 1
        n = 0
        for r in range(2, ws.max_row + 1):
            v = D.get(str(ws.cell(r, ci).value))
            if v:
                ws.cell(r, di).value = v
                n += 1
        print("Excel sheet", ws.title, "filled", n, "rows")
wb.save(xl)
print("Saved", xl)
