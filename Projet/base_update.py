import pandas as pd
from data_collector import GetData
from datetime import datetime, timedelta
import sqlite3


class BaseUpdate: 
    def __init__(self, tickers, start_date, end_date):
        self.tickers = tickers
        self.start_date = datetime.strptime(start_date, "%d/%m/%Y")
        self.end_date = datetime.strptime(end_date, "%d/%m/%Y")
        self.all_data = None
        self.data_collector = GetData()

    def update_products(self, db_file):
        """
        Updates the Products table with new market data for a specific date range.
        
        Args:
            db_file (str): Path to the SQLite database file
        """
        try:
            # Configure the data collector
            self.data_collector.tickers = self.tickers
            self.data_collector.start_date = self.start_date.strftime("%d/%m/%Y")
            self.data_collector.end_date = self.end_date.strftime("%d/%m/%Y")
            
            # Get the market data
            df = self.data_collector.main_data_frame()
            
            if df.empty:
                print("No data to update.")
                return
            
            # Convert IMPORT_DATE to datetime for filtering
            df['IMPORT_DATE'] = pd.to_datetime(df['IMPORT_DATE'])
            
            # Filter data for the specified date range
            mask = (df['IMPORT_DATE'] >= self.start_date) & (df['IMPORT_DATE'] <= self.end_date)
            df_filtered = df[mask]
            
            if df_filtered.empty:
                print(f"No data available for the specified date range ({self.start_date.strftime('%d/%m/%Y')} to {self.end_date.strftime('%d/%m/%Y')})")
                return
            
            # Convert IMPORT_DATE back to string format for SQLite
            df_filtered['IMPORT_DATE'] = df_filtered['IMPORT_DATE'].dt.strftime('%d/%m/%Y')
            
            # Connect to the database
            conn = sqlite3.connect(db_file)
            cursor = conn.cursor()
            
            # Prepare the insert query
            insert_query = """
            INSERT INTO Products (TICKER, SECTOR, PRICE, IMPORT_DATE)
            VALUES (?, ?, ?, ?)
            """
            
            # Insert the filtered data
            records_inserted = 0
            for _, row in df_filtered.iterrows():
                try:
                    cursor.execute(insert_query, (
                        row['TICKER'],
                        row['SECTOR'],
                        float(row['PRICE']),
                        row['IMPORT_DATE']
                    ))
                    records_inserted += 1
                except sqlite3.Error as e:
                    print(f"Error inserting row for {row['TICKER']}: {e}")
                    continue
            
            conn.commit()
            print(f"Successfully inserted {records_inserted} new records into Products table for the period {self.start_date.strftime('%d/%m/%Y')} to {self.end_date.strftime('%d/%m/%Y')}")
            
        except Exception as e:
            print(f"Error updating Products table: {e}")
        finally:
            if 'conn' in locals():
                conn.close()
