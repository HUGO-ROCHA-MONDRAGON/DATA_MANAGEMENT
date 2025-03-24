proet

import pandas as pd
from openpyxl import load_workbook
from openpyxl import Workbook

excel_ratings = "C:/Users/h24826/BNP Paribas/GFI - GCC - Structured Credit - Documents/Portfolio Management/Hugo/Ratings/RATINGS.xlsx"
excel_output = "C:/Users/h24826/BNP Paribas/GFI - GCC - Structured Credit - Documents/Portfolio Management/Hugo/Ratings/RATINGS_output.xlsx"
output_wsname = "ratings"

sheet_pairs = [
    ("T_MINUS_ONE_A", "T_MINUS_TWO_A"),
    ("T_MINUS_ONE_B", "T_MINUS_TWO_B"),
    ("T_MINUS_ONE_C", "T_MINUS_TWO_C"),
]

def get_rating_change(row, agency):
    old_rating = row[f"{agency}_change"].split(" → ")[0].strip()
    new_rating = row[f"{agency}_change"].split(" → ")[-1].strip()

    if pd.isna(old_rating) or pd.isna(new_rating):
        return ""
    if old_rating == new_rating:
        return ""
    return f"{old_rating} → {new_rating}"

# Open or create workbook
try:
    wb = load_workbook(excel_output)
    if output_wsname in wb.sheetnames:
        ws = wb[output_wsname]
        ws.delete_rows(2, ws.max_row)  # Clear old data
    else:
        ws = wb.create_sheet(output_wsname)
except FileNotFoundError:
    wb = Workbook()
    ws = wb.active
    ws.title = output_wsname

start_row = 2

for sheet_1, sheet_2 in sheet_pairs:
    t_minus_1 = pd.read_excel(excel_ratings, sheet_name=sheet_1)
    t_minus_2 = pd.read_excel(excel_ratings, sheet_name=sheet_2)

    # Clean string columns
    for df in [t_minus_1, t_minus_2]:
        df["ISIN"] = df["ISIN"].astype(str).str.strip()
        df["Security_description"] = df["Security_description"].astype(str).str.strip()

    comparison = t_minus_1.merge(t_minus_2, on=["ISIN", "Security_description"], suffixes=("_minus1", "_minus2"))

    for agency in ["Moody's", "Fitch", "S&P"]:
        def get_change(row):
            old_rating = row[f"{agency}_rating_minus2"]
            new_rating = row[f"{agency}_rating_minus1"]
            if pd.isna(old_rating) or pd.isna(new_rating):
                return ""
            if old_rating == new_rating:
                return ""
            return f"{old_rating} → {new_rating}"
        comparison[f"{agency}_change"] = comparison.apply(get_change, axis=1)

    rating_changes = comparison[
        (comparison[["Moody's_change", "Fitch_change", "S&P_change"]] != "").any(axis=1)
    ][["ISIN", "Security_description", "Moody's_change", "Fitch_change", "S&P_change"]]

    # Write to Excel sheet
    for row_idx, row in enumerate(rating_changes.itertuples(index=False), start=start_row):
        for col_idx, value in enumerate(row, start=1):
            ws.cell(row=row_idx, column=col_idx, value=value)

    start_row = row_idx + 2  # Leave one row between tables

wb.save(excel_output)
print(f"Data inserted into '{output_wsname}' in {excel_output}")