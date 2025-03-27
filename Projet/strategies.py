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
        self.stop_flag = False  # Ajouter un flag pour arrêter proprement

    def update_products_for_last_week(self, simulation_date):
        """Updates the Products table with data from the previous week of the simulation date."""
        # Utiliser la date de simulation au lieu de datetime.now()
        current_date = simulation_date
        
        # Calculer la semaine précédente par rapport à la date simulée
        week_end = current_date - timedelta(days=1)  # Dimanche précédent
        week_start = week_end - timedelta(days=6)  # Lundi précédent
        
        # Format dates for BaseUpdate
        week_start_str = week_start.strftime("%d/%m/%Y")
        week_end_str = week_end.strftime("%d/%m/%Y")
        
        print(f"Updating products for historical week: {week_start_str} to {week_end_str}")
        
        # Create BaseUpdate instance and update products
        updater = BaseUpdate(
            tickers=self.tickers,
            start_date=week_start_str,
            end_date=week_end_str
        )
        updater.update_products(self.db_file)

    def update_strategy(self, simulation_date):
        self.update_products_for_last_week(simulation_date)
        print("Running strategies...")
        self.strategy_two(simulation_date)

    def strategy_two(self, simulation_date):
        print("Running HY_EQUITY strategy...")
        try:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            risk_type = "HY_EQUITY"

            # Étape 1 : Récupérer les portfolios existants
            portfolios = {}
            query = """
                SELECT p.TICKER, p.QUANTITY, p.ROWID, p.MANAGER_ID 
                FROM Portfolios p 
                WHERE p.RISK_TYPE = ?
            """
            cursor.execute(query, (risk_type,))
            for ticker, quantity, rowid, manager_id in cursor.fetchall():
                portfolios[ticker] = {
                    'quantity': quantity,
                    'ROWID': rowid,
                    'manager_id': manager_id,
                    'risk_type': risk_type
                }

            # Étape 2 : Données depuis Products (seulement la dernière semaine)
            df_sql = pd.read_sql_query("""
                SELECT IMPORT_DATE, TICKER, PRICE 
                FROM Products 
                WHERE IMPORT_DATE >= date('now', '-7 days')
                ORDER BY IMPORT_DATE, TICKER
            """, conn)
            
            df_sql["IMPORT_DATE"] = pd.to_datetime(df_sql["IMPORT_DATE"], dayfirst=True)

            # Pour chaque ticker dans le portfolio
            for ticker, portfolio_info in portfolios.items():
                print(f"✓ Ticker trouvé : {ticker} avec Le risque {portfolio_info['risk_type']}, Manager ID {portfolio_info['manager_id']}")
                
                ticker_data = df_sql[df_sql['TICKER'] == ticker].sort_values('IMPORT_DATE')
                if len(ticker_data) < 2:
                    print(f"⚠️ Pas assez de données pour {ticker} sur la dernière semaine, ignoré")
                    continue

                try:
                    price_today = ticker_data.iloc[-1]['PRICE']
                    price_week_ago = ticker_data.iloc[0]['PRICE']
                    rendement = (price_today - price_week_ago) / price_week_ago
                    current_quantity = portfolio_info['quantity']

                    # Calculer la variation
                    variation = int(current_quantity * rendement)

                    if variation == 0:
                        print(f"⚠️ Trade ignoré pour {ticker} (variation nulle)")
                        continue

                    # Utiliser la date de simulation pour l'insertion
                    cursor.execute("""
                        INSERT INTO Deals (
                            RISK_TYPE, TICKER, EXECUTION_DATE, MANAGER_ID,
                            TRADE_TYPE, QUANTITY, BUY_PRICE
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (
                        portfolio_info['risk_type'],  # Utiliser INPUT_ID comme PORTFOLIO_ID
                        ticker,
                        simulation_date.strftime("%Y-%m-%d"),
                        portfolio_info['manager_id'],
                        "Buy" if variation > 0 else "Sell",
                        abs(variation),
                        price_today
                    ))

                    # Mettre à jour le Portfolio avec la date de simulation
                    new_quantity = current_quantity + variation
                    cursor.execute("""
                        UPDATE Portfolios 
                        SET QUANTITY = ?,
                            LAST_UPDATED = ?,
                            SPOT_PRICE = ?
                        WHERE ROWID = ?
                    """, (
                        new_quantity,
                        simulation_date.strftime("%Y-%m-%d"),
                        price_today,
                        portfolio_info['ROWID']
                    ))

                    print(f"→ Insertion validée : {'Buy' if variation > 0 else 'Sell'} {abs(variation)} x {ticker} @ {price_today} le {simulation_date.strftime('%Y-%m-%d')}")
                    print(f"✓ Portfolio mis à jour pour {ticker}: nouvelle quantité ajustée de {variation}")

                    conn.commit()

                except Exception as e:
                    print(f"Erreur lors du traitement de {ticker}: {e}")
                    continue

            # Ajouter au début de strategy_two
            cursor.execute("SELECT DISTINCT RISK_TYPE FROM Portfolios")
            risk_types = cursor.fetchall()
            print(f"Risk types disponibles dans la base : {risk_types}")

        except Exception as e:
            print(f"Erreur globale dans strategy_two: {e}")
            conn.rollback()
        finally:
            conn.close()

    def run(self):
        try:
            print(f"\nDébut de la simulation historique du {self.start_date.strftime('%d/%m/%Y')} au {self.end_date.strftime('%d/%m/%Y')}")
            
            current_date = self.start_date
            lundis_simulés = 0
            
            while current_date <= self.end_date:
                if current_date.weekday() == 0:
                    print(f"\n=== Simulation pour le lundi {current_date.strftime('%d/%m/%Y')} ===")
                    self.check_and_run_strategy(simulation_date=current_date)
                    lundis_simulés += 1
                
                current_date += timedelta(days=1)
                
            print(f"\nSimulation terminée !")
            print(f"Nombre de lundis simulés : {lundis_simulés}")
            print(f"Période couverte : {self.start_date.strftime('%d/%m/%Y')} au {self.end_date.strftime('%d/%m/%Y')}")
                
        except Exception as e:
            print(f"Erreur dans la simulation : {e}")

    def check_and_run_strategy(self, simulation_date):
        """
        Exécute la stratégie pour une date historique donnée
        """
        print(f"Exécution de la stratégie pour la date historique : {simulation_date.strftime('%d/%m/%Y')}")
        self.update_strategy(simulation_date)

    def stop_update_strategy(self):
        self.stop_flag = True
        print("Arrêt propre de la stratégie...")
        # Nettoyage des tâches planifiées
        schedule.clear()

# Example usage
#if __name__ == "__main__":
#    start_date = "31/12/2023"
#    end_date = "31/05/2024"
#    tickers = ['AAPL', 'MSFT', 'GOOGL']  # Example tickers
#    strategy_runner = RunAllStrat("Fund.db", start_date, end_date, tickers)
#    strategy_runner.run()
