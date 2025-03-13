import yfinance as yf
import pandas as pd
import numpy as np
import sqlite3



def get_data_yf(ticker, start_date, end_date):
    df = yf.download(ticker, start=start_date, end=end_date, auto_adjust=True)[['Close']].ffill()
    df.columns = [ticker]  # Rename the 'Close' column to the ticker name
    df.index.name = 'Date'  # Ensure 'Date' is the index
    return df.reset_index()  # Reset index to avoid MultiIndex issues


def get_data_sql(ticker, start_date, end_date):
    query = """
    SELECT * FROM fund 
    WHERE ticker = ? 
    AND date BETWEEN ? AND ?
    """
    conn = sqlite3.connect("database.db")  # Ensure connection is established
    df = pd.read_sql_query(query, conn, params=(ticker, start_date, end_date))
    conn.close()  # Close connection after query
    return df
