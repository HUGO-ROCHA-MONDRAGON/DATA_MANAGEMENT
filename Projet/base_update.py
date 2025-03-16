import pandas as pd
from data_collector import get_data_yf
from datetime import datetime, timedelta
import sqlite3


class BaseUpdate: 
    def __init__(self, tickers, start_date, end_date):
        self.tickers = tickers
        self.start_date = datetime.strptime(start_date, "%d/%m/%Y")
        self.end_date = datetime.strptime(end_date, "%d/%m/%Y")
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
        

        current_date = self.start_date#Verifier que le premier jour est bien un lundi
        while current_date <= self.end_date: 
            last_week_date = current_date - timedelta(days=7)
            weekly_data = self.all_data[(self.all_data['IMPORT_DATE'] <= current_date) & (self.all_data['IMPORT_DATE'] >= last_week_date)]

            try: 
                conn = sqlite3.connect(db_file)
                weekly_data.to_sql('PRODUCTS', conn, if_exists='append', index=False)
                conn.commit()
                print(f"Data for the week starting {last_week_date.strftime("%d/%m/%Y")} inserted successfully.")
            except sqlite3.Error as e:
                print(f"SQLite error: {e}")
            finally:
                if conn:
                    conn.close()
            current_date += timedelta(days=7)



# if __name__ == "__main__":
#     tickers = ["AAPL", "GOOGL", "AMZN", "MSFT", "TSLA", "META", "NVDA", "PYPL", "ADBE", "INTC"]
#     start_date = "01/01/2023"
#     end_date = "31/12/2023"

#     base_update = BaseUpdate(tickers, start_date, end_date)
#     base_update.main_data_frame()
#     base_update.weekly_update("Fund.db")