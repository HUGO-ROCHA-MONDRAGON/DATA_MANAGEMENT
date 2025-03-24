import pandas as pd
from openpyxl import load_workbook, Workbook

# Input and output files
excel_ratings = "C:/Users/h24826/BNP Paribas/GFI - GCC - Structured Credit - Documents/Portfolio Management/Hugo/Ratings/RATINGS.xlsx"
excel_output = "C:/Users/h24826/BNP Paribas/GFI - GCC - Structured Credit - Documents/Portfolio Management/Hugo/Ratings/RATINGS_output.xlsx"
output_wsname = "ratings"

# Sheet pairs to process
sheet_pairs = [
    ("T_MINUS_ONE_A", "T_MINUS_TWO_A"),
    ("T_MINUS_ONE_B", "T_MINUS_TWO_B"),
    ("T_MINUS_ONE_C", "T_MINUS_TWO_C")
]

# Function to detect change
def get_rating_change(row, agency):
    old_rating = row[f"{agency}_minus2"]
    new_rating = row[f"{agency}_minus1"]
    if pd.isna(old_rating) or pd.isna(new_rating):
        return ""
    if old_rating == new_rating:
        return ""
    return f"{old_rating} → {new_rating}"

# Prepare Excel output sheet
try:
    wb = load_workbook(excel_output)
    if output_wsname in wb.sheetnames:
        ws = wb[output_wsname]
    else:
        ws = wb.create_sheet(output_wsname)
except FileNotFoundError:
    wb = Workbook()
    ws = wb.active
    ws.title = output_wsname

# Clear existing content (except header row if any)
ws.delete_rows(2, ws.max_row)

# Start writing from row 2
start_row = 2

# Loop through each pair of sheets
for sheet1, sheet2 in sheet_pairs:
    # Load sheets
    t1 = pd.read_excel(excel_ratings, sheet_name=sheet1)
    t2 = pd.read_excel(excel_ratings, sheet_name=sheet2)

    # Clean ID columns
    for df in [t1, t2]:
        df["ISIN"] = df["ISIN"].astype(str).str.strip()
        df["Security_description"] = df["Security_description"].astype(str).str.strip()

    # Rename columns to prevent merge conflict
    t1 = t1.rename(columns={
        "Moodys": "Moodys_minus1",
        "Fitch": "Fitch_minus1",
        "S&P": "S&P_minus1"
    })
    t2 = t2.rename(columns={
        "Moodys": "Moodys_minus2",
        "Fitch": "Fitch_minus2",
        "S&P": "S&P_minus2"
    })

    # Merge on ISIN and description
    df = t1.merge(t2, on=["ISIN", "Security_description"], how="inner")

    # Create change columns
    for agency in ["Moodys", "Fitch", "S&P"]:
        df[f"{agency}_change"] = df.apply(lambda row: get_rating_change(row, agency), axis=1)

    # Keep only rows with at least one change
    changes = df[
        (df[["Moodys_change", "Fitch_change", "S&P_change"]] != "").any(axis=1)
    ][["ISIN", "Security_description", "Moodys_change", "Fitch_change", "S&P_change"]]

    # Write section header
    if changes.empty:
        ws.cell(row=start_row, column=1, value=f"No changes for {sheet1} vs {sheet2}")
        start_row += 2  # one empty row below
    else:
        # Title row
        ws.cell(row=start_row, column=1, value=f"Results from {sheet1} vs {sheet2}")
        start_row += 1

        # Column headers
        headers = ["ISIN", "Security_description", "Moodys_change", "Fitch_change", "S&P_change"]
        for col_idx, header in enumerate(headers, start=1):
            ws.cell(row=start_row, column=col_idx, value=header)
        start_row += 1

        # Write data rows
        for row in changes.itertuples(index=False):
            for col_idx, value in enumerate(row, start=1):
                ws.cell(row=start_row, column=col_idx, value=value)
            start_row += 1

        # Add one blank row between blocks
        start_row += 1

# Save file
wb.save(excel_output)
print(f"Data inserted into '{output_wsname}' in {excel_output}")