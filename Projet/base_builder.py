import pandas as pd
import sqlite3

db_file = "Fund.db"

create_table_query = """CREATE TABLE IF NOT EXISTS market_data(
    id INTEGER PRIMARY KEY AUTOINCREMENT,  
    asset_ticker TEXT NOT NULL,           
    asset_name TEXT,                  
    asset_class TEXT CHECK(asset_class IN ('Equity', 'Fixed Income', 'Commodity', 'Crypto', 'Derivative', 'Other')), 
    currency TEXT NOT NULL,                 
    trade_date DATE NOT NULL,               
    open_price REAL,                        
    high_price REAL,                         
    low_price REAL,                     
    close_price REAL NOT NULL,             
    adjusted_close REAL,                  
    volume INTEGER,                         
    returns_daily REAL,                  
    volatility_rolling_30d REAL,             
    market_cap REAL,                        
    sector TEXT,                           
    benchmark_index TEXT
)
"""

try:
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    
    cursor.execute(create_table_query)
    conn.commit()
except sqlite3.Error as e:
    print(f"Erreur SQLite : {e}")
    
finally: 
    conn.close()
