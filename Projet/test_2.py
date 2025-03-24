import pandas as pd
from openpyxl import load_workbook, Workbook
from openpyxl.styles import Font, PatternFill, Border, Side, Alignment
from openpyxl.utils import get_column_letter
from datetime import date, timedelta

# Input/output paths
excel_ratings = "C:/Users/h24826/BNP Paribas/GFI - GCC - Structured Credit - Documents/Portfolio Management/Hugo/Ratings/RATINGS.xlsx"
excel_output = "C:/Users/h24826/BNP Paribas/GFI - GCC - Structured Credit - Documents/Portfolio Management/Hugo/Ratings/RATINGS_output.xlsx"
output_wsname = "ratings"

# Sheet pairs
sheet_pairs = [
    ("T_MINUS_ONE_A", "T_MINUS_TWO_A"),
    ("T_MINUS_ONE_B", "T_MINUS_TWO_B"),
    ("T_MINUS_ONE_C", "T_MINUS_TWO_C")
]

# Labels for headers
label_map = {
    "T_MINUS_ONE_A": "FLABSA",
    "T_MINUS_ONE_B": "FLABSB",
    "T_MINUS_ONE_C": "FLABSC"
}

# Dates
today = date.today()
minus_1 = today - timedelta(days=1)
minus_2 = today - timedelta(days=2)

# Styles
bold_font = Font(bold=True)
header_fill = PatternFill(start_color="D9E1F2", end_color="D9E1F2", fill_type="solid")
thin_border = Border(
    left=Side(style="thin"), right=Side(style="thin"),
    top=Side(style="thin"), bottom=Side(style="thin")
)
center_align = Alignment(horizontal="center", vertical="center")

# Rating change function
def get_rating_change(row, agency):
    old_rating = row[f"{agency}_minus2"]
    new_rating = row[f"{agency}_minus1"]
    if pd.isna(old_rating) or pd.isna(new_rating):
        return ""
    if old_rating == new_rating:
        return ""
    return f"{old_rating} → {new_rating}"

# Open or create output sheet
try:
    wb = load_workbook(excel_output)
    if output_wsname in wb.sheetnames:
        del wb[output_wsname]  # Overwrite sheet
    ws = wb.create_sheet(output_wsname)
except FileNotFoundError:
    wb = Workbook()
    ws = wb.active
    ws.title = output_wsname

start_row = 2

# Loop over sheet pairs
for sheet1, sheet2 in sheet_pairs:
    # Load sheets
    t1 = pd.read_excel(excel_ratings, sheet_name=sheet1)
    t2 = pd.read_excel(excel_ratings, sheet_name=sheet2)

    # Clean columns
    for df in [t1, t2]:
        df["ISIN"] = df["ISIN"].astype(str).str.strip()
        df["Security_description"] = df["Security_description"].astype(str).str.strip()

    # Rename to avoid merge conflict
    t1 = t1.rename(columns={
        "Moodys": "Moodys_minus1", "Fitch": "Fitch_minus1", "S&P": "S&P_minus1"
    })
    t2 = t2.rename(columns={
        "Moodys": "Moodys_minus2", "Fitch": "Fitch_minus2", "S&P": "S&P_minus2"
    })

    # Merge and compare
    df = t1.merge(t2, on=["ISIN", "Security_description"], how="inner")
    for agency in ["Moodys", "Fitch", "S&P"]:
        df[f"{agency}_change"] = df.apply(lambda row: get_rating_change(row, agency), axis=1)

    changes = df[
        (df[["Moodys_change", "Fitch_change", "S&P_change"]] != "").any(axis=1)
    ][["ISIN", "Security_description", "Moodys_change", "Fitch_change", "S&P_change"]]

    # Title row
    label = label_map.get(sheet1, "Unknown")
    header_text = f"{label}: {minus_1.strftime('%d/%m/%Y')} vs {minus_2.strftime('%d/%m/%Y')}"
    ws.cell(row=start_row, column=1, value=header_text).font = bold_font
    start_row += 1

    if changes.empty:
        ws.cell(row=start_row, column=1, value="No changes").font = Font(italic=True)
        start_row += 2
    else:
        # Column headers
        headers = ["ISIN", "Security_description", "Moodys_change", "Fitch_change", "S&P_change"]
        for col_idx, header in enumerate(headers, start=1):
            cell = ws.cell(row=start_row, column=col_idx, value=header)
            cell.font = bold_font
            cell.fill = header_fill
            cell.border = thin_border
            cell.alignment = center_align
        start_row += 1

        # Table rows
        for row in changes.itertuples(index=False):
            for col_idx, value in enumerate(row, start=1):
                cell = ws.cell(row=start_row, column=col_idx, value=value)
                cell.border = thin_border
                cell.alignment = center_align
            start_row += 1
        start_row += 1  # Blank line

# Autofit columns
for col_cells in ws.columns:
    max_len = 0
    col_letter = get_column_letter(col_cells[0].column)
    for cell in col_cells:
        if cell.value:
            max_len = max(max_len, len(str(cell.value)))
    ws.column_dimensions[col_letter].width = max_len + 2

# Save
wb.save(excel_output)
print(f"Data inserted into '{output_wsname}' in {excel_output}")