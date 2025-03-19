# DATA_MANAGEMENT
prjet


import pandas as pd
import ace_tools as tools  # If running in an assistant environment

# Load today's and yesterday's ratings
file_path = "portfolio_ratings.xlsx"  # Change to your actual file path
today_df = pd.read_excel(file_path, sheet_name="Today")
yesterday_df = pd.read_excel(file_path, sheet_name="Yesterday")

# Merge on ISIN and Asset for better identification
comparison_df = today_df.merge(yesterday_df, on=["ISIN", "Asset"], suffixes=("_today", "_yesterday"))

# Function to check for rating changes
def get_rating_change(row, agency):
    old_rating = row[f"{agency}_yesterday"]
    new_rating = row[f"{agency}_today"]
    
    if old_rating != new_rating:
        return f"{old_rating} → {new_rating}"
    return "-"

# Apply function to each rating agency
for agency in ["Moody's", "S&P", "Fitch"]:
    comparison_df[f"{agency}_Change"] = comparison_df.apply(lambda row: get_rating_change(row, agency), axis=1)

# Filter only assets with rating changes
rating_changes = comparison_df[["ISIN", "Asset", "Moody's_Change", "S&P_Change", "Fitch_Change"]]
rating_changes = rating_changes[(rating_changes.iloc[:, 2:] != "-").any(axis=1)]

# Display result
tools.display_dataframe_to_user(name="Rating Changes", dataframe=rating_changes)

# Save the output
output_path = "rating_changes_report.xlsx"
rating_changes.to_excel(output_path, index=False)
print(f"Report saved to {output_path}")