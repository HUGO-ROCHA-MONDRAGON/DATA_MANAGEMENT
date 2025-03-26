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
def update_portefeuille1(db_file,tickers,risk_type,quantité_initiale):
    
    # Connexion à la base
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Étape 1 : récupérer le MANAGER_ID de la table Managers
    cursor.execute("SELECT MANAGER_ID FROM Managers ORDER BY MANAGER_ID LIMIT 1")
    manager_id = cursor.fetchone()[0]  # prend le premier manager

    # Étape 2 : insérer chaque ticker avec les données de dernière date
    for ticker in tickers:
        cursor.execute("""
            SELECT PRICE, IMPORT_DATE 
            FROM Products 
            WHERE TICKER = ?
            ORDER BY IMPORT_DATE DESC 
            LIMIT 1
        """, (ticker,))

        row = cursor.fetchone()
        if row is None:
            continue  # passe si le ticker n'existe pas dans Products

        spot_price, last_date = row

        # Insertion dans Portfolios
        cursor.execute("""
            INSERT INTO Portfolios (RISK_TYPE, TICKER, QUANTITY, MANAGER_ID, LAST_UPDATED, SPOT_PRICE)
            VALUES ( ?, ?, ?, ?, ?, ?)
        """, (risk_type ,ticker, quantité_initiale, manager_id, last_date, spot_price))

    # Valider les insertions
    conn.commit()
    conn.close()
    print("Portfolios mis à jour avecc succés")
def update_portefeuille2(db_file, tickers, risk_type):
    """
    Initialise un portefeuille LOW_RISK avec les tickers macro,
    en assignant quantité = 0, tout en stockant le dernier spot connu.

    Args:
        db_file (str): chemin vers la base de données
        tickers (list): liste de tickers macro (ETF, obligations, etc.)
        risk_type (str): ici on utilisera toujours 'LOW_RISK'
    """
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Récupérer le MANAGER_ID (le premier dispo)
    cursor.execute("SELECT MANAGER_ID FROM Managers ORDER BY MANAGER_ID LIMIT 1")
    result = cursor.fetchone()
    if result is None:
        print("❌ Aucun manager trouvé dans la table Managers.")
        conn.close()
        return
    manager_id = result[0]

    for ticker in tickers:
        cursor.execute("""
            SELECT PRICE, IMPORT_DATE
            FROM Products
            WHERE TICKER = ?
            ORDER BY IMPORT_DATE DESC
            LIMIT 1
        """, (ticker,))
        row = cursor.fetchone()

        if row:
            spot_price, last_date = row
            cursor.execute("""
                INSERT INTO Portfolios (RISK_TYPE, TICKER, QUANTITY, MANAGER_ID, LAST_UPDATED, SPOT_PRICE)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                risk_type, ticker, 0, manager_id,
                last_date, spot_price
            ))

    conn.commit()
    conn.close()
    print("✅ Portefeuille LOW_RISK initialisé avec quantité = 0 pour tous les actifs.")


#abdel
