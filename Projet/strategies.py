import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sqlite3
from scipy.optimize import minimize
import schedule
import time
from base_update import BaseUpdate

class RunAllStrat:
    def __init__(self, db_file, start_date, end_date, tickers):
        self.db_file = db_file
        self.start_date = datetime.strptime(start_date, "%d/%m/%Y")
        self.end_date = datetime.strptime(end_date, "%d/%m/%Y")
        self.tickers = tickers

    def update_products_for_last_week(self):
        """Updates the Products table with data from the previous week."""
        current_date = datetime.now()
        # Calculate last week's date range
        week_end = current_date - timedelta(days=current_date.weekday() + 1)  # Previous Sunday
        week_start = week_end - timedelta(days=6)  # Previous Monday
        
        # Format dates for BaseUpdate
        week_start_str = week_start.strftime("%d/%m/%Y")
        week_end_str = week_end.strftime("%d/%m/%Y")
        
        print(f"Updating products for last week: {week_start_str} to {week_end_str}")
        
        # Create BaseUpdate instance and update products
        updater = BaseUpdate(
            tickers=self.tickers,
            start_date=week_start_str,
            end_date=week_end_str
        )
        updater.update_products(self.db_file)

    def update_strategy(self):
        # First update products with last week's data
        self.update_products_for_last_week()
        
        # Then run the strategies
        print("Running strategies...")
        self.strategy_one()
        self.strategy_two()

    def strategy_one(self):
        print("Running strategy one...")

        # Connexion à la base
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        risk_type = 'HY_EQUITY'

        # Quantités initiales
        quantites = {}
        query = "SELECT TICKER, QUANTITY FROM Portfolios WHERE RISK_TYPE = ?"
        cursor.execute(query, (risk_type,))
        rows = cursor.fetchall()
        for ticker, qte in rows:
            quantites[ticker] = qte

        # Données depuis la table Products
        df_sql = pd.read_sql_query("SELECT IMPORT_DATE, TICKER, PRICE FROM Products", conn)
        df = df_sql.pivot(index='IMPORT_DATE', columns='TICKER', values='PRICE').reset_index()

        historique = []

        for ticker in df.columns[1:]:
            cursor.execute("""
                SELECT MANAGER_ID, ROWID FROM Portfolios
                WHERE TICKER = ? AND RISK_TYPE = ?
            """, (ticker, risk_type))
            result = cursor.fetchone()
            if result:
                manager_id, portfolio_id = result
                df[f"rend_{ticker}"] = None

                for i in range(6, len(df), 7):
                    price_today = df[ticker].iloc[i]
                    price_7_days_ago = df[ticker].iloc[i - 6]
                    rendement = (price_today - price_7_days_ago) / price_7_days_ago
                    df.loc[df.index[i], f"rend_{ticker}"] = rendement

                rendements = df[f"rend_{ticker}"].dropna().tolist()
                quantite = quantites.get(ticker, 50)

                for j, r in enumerate(rendements):
                    variation = quantite * r
                    quantite += variation
                    spot = df.loc[df.index[j * 7 + 6], ticker]
                    trade_date = df.loc[df.index[j * 7 + 6], "IMPORT_DATE"]
                    trade_type = "Buy" if variation > 0 else "Sell"

                    historique.append({
                        "TICKER": ticker,
                        "spot": round(spot, 2),
                        "Rendement": round(r, 4),
                        "Variation_stock": int(variation),
                        "Quantité_totale": int(quantite),
                        "depense/revenu": round(-variation * spot, 2),
                        "trade": trade_type,
                        "trade_date": trade_date
                    })

        df_result = pd.DataFrame(historique)
        print(df_result)  # Pour voir dans la console
        conn.close()
        return df_result


    def strategy_two(self):
        # Placeholder for the second strategy logic
        print("Running strategy two...")

    def run(self):
        # Schedule the update_strategy method to run every Monday
        schedule.every().monday.do(self.check_and_run_strategy)

        while True:
            schedule.run_pending()
            time.sleep(1)

    def check_and_run_strategy(self):
        current_date = datetime.now()
        #if self.start_date <= current_date <= self.end_date:
        self.update_strategy()
        #else:
            #print("Current date is outside the specified range. Skipping update.")

    def stop_update_strategy(self):
        # Placeholder for stopping the strategy update logic
        print("Stopping strategy update...")

# Example usage
#if __name__ == "__main__":
#    start_date = "31/12/2023"
#    end_date = "31/05/2024"
#    tickers = ['AAPL', 'MSFT', 'GOOGL']  # Example tickers
#    strategy_runner = RunAllStrat("Fund.db", start_date, end_date, tickers)
#    strategy_runner.run()
