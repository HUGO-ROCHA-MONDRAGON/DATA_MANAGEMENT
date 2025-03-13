import yfinance as yf
import pandas as pd
import numpy as np
import sqlite3


def get_data_yf(ticker):
    df = yf.download(ticker, start='2023-01-01', end='2024-12-31')[['Close']].ffill()    
    return df
    
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
