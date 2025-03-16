import pandas as pd
from data_collector import get_data_yf
from datetime import datetime, timedelta
import sqlite3


class BaseBuilder: 
    def __init__(self, tickers, start_date, end_date):
        self.tickers = tickers
        self.start_date = start_date
        self.end_date = end_date
        self.all_data = None
    
    def main_data_frame(self):
        data_frames = []
    
        for ticker in self.tickers:
            try:
                df = get_data_yf(ticker, self.start_date, self.end_date)
                if not df.empty:
                    data_frames.append(df)
            except Exception as e:
                print(f"Failed to fetch data for {ticker}: {e}")
    
        if data_frames:
            self.all_data = pd.concat(data_frames, ignore_index=True)
        else:
            self.all_data = pd.DataFrame(columns=["IMPORT_DATE", "TICKER", "SECTOR", "PRICE"])


    def weekly_update(self, db_file):
        if self.all_data is None: 
            print("No data available, please make sure all_data is populated")
            return
        
        start_date = datetime.strptime(start_date, "%d/%m/%Y")
        end_date = datetime.strptime(end_date, "%d/%m/%Y")

        current_date = start_date#Verifier que le premier jour est bien un lundi
        while current_date <= end_date: 
            last_week_date = current_date - timedelta(days=7)
            weekly_data = self.all_data[(self.all_data['Date'] <= current_date) & (self.all_data['Date'] >= last_week_date)]

            try: 
                conn = sqlite3.connect(db_file)
                weekly_data.to_sql('Products', conn, if_exists='append', index=False)
                conn.commit()
                print(f"Data for the week starting {last_week_date.strftime("%d/%m/%Y")} inserted successfully.")
            except sqlite3.Error as e:
                print(f"SQLite error: {e}")
            finally:
                if conn:
                    conn.close()
            current_date += timedelta(days=7)



# #if __name__ == "__main__":
#  #   tickers = ["AAPL", "GOOGL", "AMZN", "MSFT", "TSLA", "META", "NVDA", "PYPL", "ADBE", "INTC"]
#   #  start_date = "2023-01-01"
#     end_date = "2024-12-31"

#     base_builder = BaseBuilder(tickers, start_date, end_date)
#     base_builder.main_data_frame()
#     base_builder.weekly_update("Fund.db")