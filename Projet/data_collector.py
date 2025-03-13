import yfinance as yf
import pandas as pd
import numpy as np
import sqlite3


def get_data_yf(ticker):
    df = yf.download(ticker, start='2023-01-01', end='2024-12-31')[['Close']].ffill()    
    return df
def calculate_daily_returns(prices):
    """
    Calculate daily percentage returns.
    """
    return prices.pct_change().dropna()
    

def annualized_volatility(returns):
    """
    Compute the annualized volatility from daily returns.
    """
    return returns.std() * np.sqrt(252)
    

def sharpe_ratio(returns):
    """
    Calculate a simple Sharpe ratio (mean return divided by standard deviation).
    """
    rf_rate = get_data_yf("^IRX")
    risk_free_rate = rf_rate.mean()#A ameliorer, ici on prend la moyenne de toute la periode, faudrait prendre la moyenne d'un an ou un truc
    std = returns.std()
    return (returns.mean() - risk_free_rate)/ std if std != 0 else 0
    
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
