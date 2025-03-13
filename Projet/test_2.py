import pandas as pd
from data_collector import get_data_yf

tickers = ["AAPL", "GOOGL", "AMZN", "MSFT", "TSLA", "META", "NVDA", "PYPL", "ADBE", "INTC"]  # Replaced "FB" with "META"
start_date = "2023-01-01"
end_date = "2024-12-31"

# Initialize an empty DataFrame
all_data = None

for ticker in tickers: 
    try:
        df = get_data_yf(ticker, start_date, end_date)  # Now returns a clean structure
        
        if all_data is None:
            all_data = df  # Start with first ticker's data
        else:
            all_data = all_data.merge(df, on='Date', how='outer')  # Merge instead of join()
    except Exception as e:
        print(f"Failed to fetch data for {ticker}: {e}")  # Catch any errors

# Print DataFrame structure
print(all_data.head())
print(all_data.columns)
