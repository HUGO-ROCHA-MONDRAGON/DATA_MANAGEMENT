import pandas as pd
from openpyxl import load_workbook
from openpyxl import Workbook

excel_ratings = "C:/Users/h24826/BNP Paribas/GFI - GCC - Structured Credit - Documents/Portfolio Management/Hugo/Ratings/RATINGS.xlsx"
excel_output = "C:/Users/h24826/BNP Paribas/GFI - GCC - Structured Credit - Documents/Portfolio Management/Hugo/Ratings/RATINGS_output.xlsx"
output_wsname = "ratings"

# Sheet pairs
sheet_pairs = [
    ("T_MINUS_ONE_A", "T_MINUS_TWO_A"),
    ("T_MINUS_ONE_B", "T_MINUS_TWO_B"),
    ("T_MINUS_ONE_C", "T_MINUS_TWO_C")
]

def get_rating_change(row, agency):
    old_rating = row[f"{agency}_minus2"]
    new_rating = row[f"{agency}_minus1"]

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
        ws.delete_rows(2, ws.max_row)  # Clear old content, keep header if needed
    else:
        ws = wb.create_sheet(output_wsname)
except FileNotFoundError:
    wb = Workbook()
    ws = wb.active
    ws.title = output_wsname

start_row = 2

# Loop through each pair of sheets
for sheet1, sheet2 in sheet_pairs:
    t_minus_1 = pd.read_excel(excel_ratings, sheet_name=sheet1)
    t_minus_2 = pd.read_excel(excel_ratings, sheet_name=sheet2)

    # Strip and clean identifiers
    for df in [t_minus_1, t_minus_2]:
        df["ISIN"] = df["ISIN"].astype(str).str.strip()
        df["Security_description"] = df["Security_description"].astype(str).str.strip()

    # Rename rating columns to avoid clashes on merge
    t_minus_1 = t_minus_1.rename(columns={
        "Moodys": "Moodys_minus1",
        "Fitch": "Fitch_minus1",
        "S&P": "S&P_minus1"
    })
    t_minus_2 = t_minus_2.rename(columns={
        "Moodys": "Moodys_minus2",
        "Fitch": "Fitch_minus2",
        "S&P": "S&P_minus2"
    })

    # Merge
    comparison = t_minus_1.merge(t_minus_2, on=["ISIN", "Security_description"], how="inner")

    # Calculate changes
    for agency in ["Moodys", "Fitch", "S&P"]:
        comparison[f"{agency}_change"] = comparison.apply(lambda row: get_rating_change(row, agency), axis=1)

    # Filter only rows with a change
    rating_changes = comparison[
        (comparison[["Moodys_change", "Fitch_change", "S&P_change"]] != "").any(axis=1)
    ][["ISIN", "Security_description", "Moodys_change", "Fitch_change", "S&P_change"]]

    # Optional: Add a header to label the table section
    ws.cell(row=start_row, column=1, value=f"Results from {sheet1} vs {sheet2}")
    start_row += 1

    # Write results
    for row_idx, row in enumerate(rating_changes.itertuples(index=False), start=start_row):
        for col_idx, value in enumerate(row, start=1):
            ws.cell(row=row_idx, column=col_idx, value=value)

    start_row = row_idx + 2  # Leave one empty row before the next table

# Save
wb.save(excel_output)
print(f"Data inserted into '{output_wsname}' in {excel_output}")