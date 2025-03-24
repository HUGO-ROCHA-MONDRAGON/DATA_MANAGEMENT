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
        self.strategy_two()


    def strategy_two(self):
        print("Running HY_EQUITY strategy...")
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        risk_type = "HY_EQUITY" # Adapter au profil de risque adapté

        # Étape 1 : Quantités initiales
        quantites = {}
        query = "SELECT TICKER, QUANTITY FROM Portfolios WHERE RISK_TYPE = ?"
        cursor.execute(query, (risk_type,))
        for ticker, qte in cursor.fetchall():
            quantites[ticker] = qte

        # Étape 2 : Données depuis Products
        df_sql = pd.read_sql_query("SELECT IMPORT_DATE, TICKER, PRICE FROM Products", conn)
        df_sql["IMPORT_DATE"] = pd.to_datetime(df_sql["IMPORT_DATE"], dayfirst=True)
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
                print(f"✓ Ticker trouvé : {ticker} avec Portfolio ID {portfolio_id}, Manager ID {manager_id}")
                df[f"rend_{ticker}"] = None
                for i in range(6, len(df), 7):
                    price_today = df[ticker].iloc[i]
                    price_7_days_ago = df[ticker].iloc[i - 6]
                    rendement = (price_today - price_7_days_ago) / price_7_days_ago
                    df.loc[df.index[i], f"rend_{ticker}"] = rendement

                rendements = df[f"rend_{ticker}"].dropna().tolist()
                quantite = quantites.get(ticker, 50)
                print(f"→ Rendements calculés pour {ticker} : {rendements}")
                for j, r in enumerate(rendements):
                    variation = quantite * r
                    quantite += variation
                    spot = df.loc[df.index[j * 7 + 6], ticker]
                    trade_date = df.loc[df.index[j * 7 + 6], "IMPORT_DATE"]
                    trade_type = "Buy" if variation > 0 else "Sell"
                    quantity_traded = abs(int(variation))

                    # ✅ Sécurité contre les erreurs d'intégrité
                    if quantity_traded == 0 or spot is None or pd.isna(spot) or spot < 0:
                        print(f"⚠️ Trade ignoré pour {ticker} à la date {trade_date} (quantité={quantity_traded}, spot={spot})")
                        continue

                    print(f"→ Insertion validée : {trade_type} {quantity_traded} x {ticker} @ {spot} le {trade_date}")

                    # 🔁 Insertion dans Deals
                    cursor.execute("""
                        INSERT INTO Deals (
                            PORTFOLIO_ID, TICKER, EXECUTION_DATE, MANAGER_ID,
                            TRADE_TYPE, QUANTITY, BUY_PRICE
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (
                        portfolio_id,
                        ticker,
                        trade_date.strftime("%Y-%m-%d"),
                        manager_id,
                        trade_type,
                        quantity_traded,
                        round(spot, 2)
                    ))


        conn.commit()
        conn.close()
        df_resultat = pd.DataFrame(historique)
        print(df_resultat)
        return df_resultat

    def run(self):
        # Schedule the update_strategy method to run every Monday
        schedule.every().monday.do(self.check_and_run_strategy)

        while True:
            schedule.run_pending()
            time.sleep(1)

    def check_and_run_strategy(self):
        current_date = datetime.now()
        if self.start_date <= current_date <= self.end_date:
            self.update_strategy()
        else:
            print("Current date is outside the specified range. Skipping update.")

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
