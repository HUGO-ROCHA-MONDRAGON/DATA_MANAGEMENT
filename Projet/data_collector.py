import yfinance as yf
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime

class GetData: 
    def __init__(self, tickers=None, start_date=None, end_date=None):
        """
        Initialize the GetData class.
        
        Args:
            tickers (list): List of stock ticker symbols
            start_date (str): Start date in DD/MM/YYYY format
            end_date (str): End date in DD/MM/YYYY format
        """
        self.tickers = tickers or []
        self.start_date = start_date
        self.end_date = end_date
        self.all_data = None
        
    def _convert_date_format(self, date_str):
        """
        Convert date from DD/MM/YYYY to YYYY-MM-DD format for Yahoo Finance API.
        
        Args:
            date_str (str): Date string in DD/MM/YYYY format
            
        Returns:
            str: Date string in YYYY-MM-DD format
        """
        try:
            date_obj = datetime.strptime(date_str, "%d/%m/%Y")
            return date_obj.strftime("%Y-%m-%d")
        except ValueError as e:
            raise ValueError(f"Invalid date format. Please use DD/MM/YYYY format. Error: {e}")
        
    def get_data_yf(self, ticker, start_date, end_date):
        """
        Get stock data from Yahoo Finance.
        
        Args:
            ticker (str): Stock ticker symbol
            start_date (str): Start date in DD/MM/YYYY format
            end_date (str): End date in DD/MM/YYYY format
            
        Returns:
            pandas.DataFrame: DataFrame containing stock data
        """
        try:
            # Convert dates to YYYY-MM-DD format for Yahoo Finance API
            yf_start_date = self._convert_date_format(start_date)
            yf_end_date = self._convert_date_format(end_date)
            
            df = yf.download(ticker, start=yf_start_date, end=yf_end_date, auto_adjust=True)[['Close']].ffill()
            df.columns = ['PRICE']  
            df.index.name = 'IMPORT_DATE'  
            
            stock = yf.Ticker(ticker)
            sector = stock.info.get("sector", "Unknown")  
            
            df["TICKER"] = ticker
            df["SECTOR"] = sector
            
            return df.reset_index()

        except Exception as e:
            print(f"Failed to fetch data for {ticker}: {e}")
            return pd.DataFrame()

    def get_data_sql(self, ticker, start_date, end_date):
        """
        Get stock data from SQL database.
        
        Args:
            ticker (str): Stock ticker symbol
            start_date (str): Start date in DD/MM/YYYY format
            end_date (str): End date in DD/MM/YYYY format
            
        Returns:
            pandas.DataFrame: DataFrame containing stock data
        """
        # Convert dates to YYYY-MM-DD format for SQL query
        sql_start_date = self._convert_date_format(start_date)
        sql_end_date = self._convert_date_format(end_date)
        
        query = """
        SELECT * FROM fund 
        WHERE ticker = ? 
        AND date BETWEEN ? AND ?
        """
        conn = sqlite3.connect("database.db")  # Ensure connection is established
        df = pd.read_sql_query(query, conn, params=(ticker, sql_start_date, sql_end_date))
        conn.close()  # Close connection after query
        return df

    def main_data_frame(self):
        """
        Create a main DataFrame by fetching data for all tickers.
        
        Returns:
            pandas.DataFrame: Combined DataFrame with data for all tickers
        """
        if not self.tickers or not self.start_date or not self.end_date:
            raise ValueError("Please set tickers, start_date, and end_date before calling main_data_frame")
            
        data_frames = []
    
        for ticker in self.tickers:
            try:
                df = self.get_data_yf(ticker, self.start_date, self.end_date)
                if not df.empty:
                    data_frames.append(df)
            except Exception as e:
                print(f"Failed to fetch data for {ticker}: {e}")
    
        if data_frames:
            self.all_data = pd.concat(data_frames, ignore_index=True)
        else:
            self.all_data = pd.DataFrame(columns=["IMPORT_DATE", "TICKER", "SECTOR", "PRICE"])
        
        return self.all_data



class OutlierRemover:
    def __init__(self, method="zscore", threshold=3):
        """
        Initialise l'outil de suppression des valeurs aberrantes.
        
        :param method: str, méthode de détection ('zscore', 'iqr', 'percentile')
        :param threshold: float, seuil pour identifier une valeur aberrante (ex: 3 pour z-score, 1.5 pour IQR)
        """
        self.method = method
        self.threshold = threshold
    
    def remove_outliers(self, df):
        """
        Supprime les valeurs aberrantes d'un DataFrame en fonction de la méthode choisie.
        
        :param df: pd.DataFrame, le dataset à traiter
        :return: pd.DataFrame, dataset nettoyé
        """
        df_cleaned = df.copy()
        
        for col in df.select_dtypes(include=[np.number]).columns:
            if self.method == "zscore":
                df_cleaned = self._remove_by_zscore(df_cleaned, col)
            elif self.method == "iqr":
                df_cleaned = self._remove_by_iqr(df_cleaned, col)
            elif self.method == "percentile":
                df_cleaned = self._remove_by_percentile(df_cleaned, col)
            else:
                raise ValueError("Méthode inconnue. Choisissez parmi 'zscore', 'iqr' ou 'percentile'.")
        
        return df_cleaned
    
    def _remove_by_zscore(self, df, col):
        """ Supprime les valeurs aberrantes en utilisant le z-score. """
        z_scores = np.abs((df[col] - df[col].mean()) / df[col].std())
        return df[z_scores < self.threshold]
    
    def _remove_by_iqr(self, df, col):
        """ Supprime les valeurs aberrantes en utilisant l'IQR (Interquartile Range). """
        Q1 = df[col].quantile(0.25)
        Q3 = df[col].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - self.threshold * IQR
        upper_bound = Q3 + self.threshold * IQR
        return df[(df[col] >= lower_bound) & (df[col] <= upper_bound)]
    
    def _remove_by_percentile(self, df, col):
        """ Supprime les valeurs aberrantes en utilisant les percentiles (1% et 99%). """
        lower_bound = df[col].quantile(0.01)
        upper_bound = df[col].quantile(0.99)
        return df[(df[col] >= lower_bound) & (df[col] <= upper_bound)]
    