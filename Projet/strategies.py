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
            end_date=week_end_str,
            db_file=self.db_file
        )
        updater.update_products()

    def update_strategy(self, simulation_date):
        self.update_products_for_last_week(simulation_date)
        print("Running strategies...")
        self.strategy_two(simulation_date)

    def strategy_two(self, simulation_date):
        print("Running HY_EQUITY strategy...")
        try:
            # S'assurer que simulation_date est un objet datetime
            if isinstance(simulation_date, str):
                simulation_date = datetime.strptime(simulation_date, "%d/%m/%Y")
            
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            risk_type = "HY_EQUITY"

            # Étape 1 : Récupérer le cash disponible
            cursor.execute("""
                SELECT QUANTITY FROM Portfolios 
                WHERE RISK_TYPE = ? AND TICKER = 'CASH'
            """, (risk_type,))
            cash_result = cursor.fetchone()
            if not cash_result:
                print("Pas de cash disponible pour HY_EQUITY")
                return
            available_cash = cash_result[0]

            # Étape 2 : Récupérer les portfolios existants
            portfolios = {}
            query = """
                SELECT p.TICKER, p.QUANTITY, p.ROWID, p.MANAGER_ID, p.SPOT_PRICE
                FROM Portfolios p 
                WHERE p.RISK_TYPE = ? AND p.TICKER != 'CASH'
            """
            cursor.execute(query, (risk_type,))
            for ticker, quantity, rowid, manager_id, spot_price in cursor.fetchall():
                portfolios[ticker] = {
                    'quantity': quantity,
                    'ROWID': rowid,
                    'manager_id': manager_id,
                    'risk_type': risk_type,
                    'spot_price': spot_price
                }

            # Étape 3 : Données depuis Products (seulement la dernière semaine)
            df_sql = pd.read_sql_query("""
                SELECT IMPORT_DATE, TICKER, PRICE 
                FROM Products 
                WHERE IMPORT_DATE >= date('now', '-7 days')
                ORDER BY IMPORT_DATE, TICKER
            """, conn)
            
            df_sql["IMPORT_DATE"] = pd.to_datetime(df_sql["IMPORT_DATE"], dayfirst=True)

            # Étape 4 : Calculer les variations et les prioriser
            variations = []
            for ticker, portfolio_info in portfolios.items():
                ticker_data = df_sql[df_sql['TICKER'] == ticker].sort_values('IMPORT_DATE')
                if len(ticker_data) < 2:
                    continue

                try:
                    price_today = ticker_data.iloc[-1]['PRICE']
                    price_week_ago = ticker_data.iloc[0]['PRICE']
                    rendement = (price_today - price_week_ago) / price_week_ago
                    current_quantity = portfolio_info['quantity']
                    current_value = current_quantity * price_today

                    # Calculer la variation souhaitée
                    variation = int(current_quantity * rendement)
                    variation_value = abs(variation * price_today)

                    if variation != 0:
                        variations.append({
                            'ticker': ticker,
                            'variation': variation,
                            'variation_value': variation_value,
                            'price': price_today,
                            'rendement': rendement,
                            'portfolio_info': portfolio_info
                        })

                except Exception as e:
                    print(f"Erreur lors du calcul pour {ticker}: {e}")
                    continue

            # Trier les variations par rendement absolu (prioriser les plus importantes)
            variations.sort(key=lambda x: abs(x['rendement']), reverse=True)

            # Étape 5 : Exécuter les transactions en tenant compte du cash
            for variation in variations:
                ticker = variation['ticker']
                variation_value = variation['variation_value']
                price_today = variation['price']
                portfolio_info = variation['portfolio_info']

                # Vérifier si on peut effectuer la transaction
                if variation['variation'] > 0:  # Achat
                    if variation_value > available_cash:
                        # Ajuster la variation pour utiliser tout le cash disponible
                        variation['variation'] = int(available_cash / price_today)
                        variation_value = variation['variation'] * price_today
                        if variation['variation'] == 0:
                            continue

                # Mettre à jour le cash disponible
                available_cash -= variation_value if variation['variation'] > 0 else -variation_value

                # Insérer le deal
                cursor.execute("""
                    INSERT INTO Deals (
                        RISK_TYPE, TICKER, EXECUTION_DATE, MANAGER_ID,
                        TRADE_TYPE, QUANTITY, BUY_PRICE
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    portfolio_info['risk_type'],
                    ticker,
                    simulation_date.strftime("%Y-%m-%d"),
                    portfolio_info['manager_id'],
                    "Buy" if variation['variation'] > 0 else "Sell",
                    abs(variation['variation']),
                    price_today
                ))

                # Mettre à jour le Portfolio
                new_quantity = portfolio_info['quantity'] + variation['variation']
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

                print(f"→ Transaction effectuée : {'Buy' if variation['variation'] > 0 else 'Sell'} {abs(variation['variation'])} x {ticker} @ {price_today:.2f}")

            # Mettre à jour le cash restant
            cursor.execute("""
                UPDATE Portfolios 
                SET QUANTITY = ?
                WHERE RISK_TYPE = ? AND TICKER = 'CASH'
            """, (available_cash, risk_type))

            conn.commit()
            print(f"Cash restant : {available_cash:,.2f}")

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

