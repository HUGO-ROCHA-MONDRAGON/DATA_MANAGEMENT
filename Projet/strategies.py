import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sqlite3
from scipy.optimize import minimize
import schedule
import time
from base_update import BaseUpdate

class RunAllStrat:
    def __init__(self, db_file, start_date, end_date, tickers, existing_data=None):
        self.db_file = db_file
        self.start_date = datetime.strptime(start_date, "%d/%m/%Y")
        self.end_date = datetime.strptime(end_date, "%d/%m/%Y")
        self.tickers = tickers
        self.stop_flag = False  # Ajouter un flag pour arrêter proprement
        self.existing_data = existing_data
        

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
            db_file=self.db_file,
            existing_data=self.existing_data
        )
        updater.update_products()

    def update_strategy(self, simulation_date):
        self.update_products_for_last_week(simulation_date)
        print("Running strategies...")
        self.strategy_one(simulation_date)
        self.strategy_two(simulation_date)
        self.strategy_three(simulation_date)

    def strategy_one(self, simulation_date):
        print("Running breakout weekly LOW_TURNOVER strategy...")

        conn = sqlite3.connect(self.db_file)
        tickers = ["BTC-USD", "ETH-USD"]

        # 🔹 1. Récupérer les données de prix (20 derniers jours)
        price_data = {}
        for ticker in tickers:
            df = pd.read_sql_query("""
                SELECT IMPORT_DATE, PRICE FROM Products
                WHERE TICKER = ? ORDER BY IMPORT_DATE DESC LIMIT 20
            """, conn, params=(ticker,))
            if not df.empty:
                df['IMPORT_DATE'] = pd.to_datetime(df['IMPORT_DATE'], dayfirst=True)
                df.sort_values("IMPORT_DATE", inplace=True)
                price_data[ticker] = df

        if len(price_data) < 2:
            print("⚠️ Pas assez de données pour les tickers.")
            conn.close()
            return

        # 🔹 2. Récupérer le cash disponible
        cash_df = pd.read_sql_query("""
            SELECT QUANTITY FROM Portfolios
            WHERE RISK_TYPE = 'LOW_TURNOVER' AND TICKER = 'CASH'
        """, conn)
        cash_available = float(cash_df['QUANTITY'].iloc[0]) if not cash_df.empty else 0

        # 🔹 3. Récupérer nombre de deals effectués ce mois-ci
        current_month = simulation_date.strftime("%Y-%m")
        deals_df = pd.read_sql_query("""
            SELECT COUNT(*) as count FROM Deals
            WHERE RISK_TYPE = 'LOW_TURNOVER' AND strftime('%Y-%m', EXECUTION_DATE) = ?
        """, conn, params=(current_month,))
        deals_this_month = int(deals_df['count'].iloc[0]) if not deals_df.empty else 0
        remaining_deals = 2 - deals_this_month

        decisions = []

        # ✅ Pas de trades si limite atteinte
        if remaining_deals <= 0:
            print("🚫 Limite de transactions mensuelles atteinte pour LOW_TURNOVER.")
            conn.close()
            return

        # 🔹 4. Appliquer la stratégie breakout inversée
        for ticker, df in price_data.items():
            if len(df) < 15:
                continue

            last_price = df['PRICE'].iloc[-1]
            ma20 = df['PRICE'].rolling(window=20).mean().iloc[-1]
            last_week = df.iloc[-8:-1]  # 7 jours avant
            high_last_week = last_week['PRICE'].max()
            low_last_week = last_week['PRICE'].min()

            # ✅ Signal d'achat inversé (prix sous plus bas de la semaine + sous MA20)
            if last_price < low_last_week and last_price < ma20 and remaining_deals > 0 and cash_available > last_price:
                decisions.append({'ticker': ticker, 'action': 'Buy', 'price': last_price})
                remaining_deals -= 1
                cash_available -= last_price

            # ✅ Signal de vente inversé (prix au-dessus plus haut + au-dessus MA20)
            elif last_price > high_last_week and last_price > ma20 and remaining_deals > 0:
                decisions.append({'ticker': ticker, 'action': 'Sell', 'price': last_price})
                remaining_deals -= 1

        # 🔹 5. Fallback : stratégie basée sur performance
        if len(decisions) < 2 and remaining_deals >= 2:
            performances = []
            for ticker, df in price_data.items():
                rendement = (df['PRICE'].iloc[-1] - df['PRICE'].iloc[0]) / df['PRICE'].iloc[0]
                performances.append((ticker, rendement, df['PRICE'].iloc[-1]))

            sorted_perf = sorted(performances, key=lambda x: x[1])  # tri croissant

            buy_candidate = sorted_perf[0]
            sell_candidate = sorted_perf[-1]

            if cash_available > buy_candidate[2]:
                decisions.append({'ticker': buy_candidate[0], 'action': 'Buy', 'price': buy_candidate[2]})
                decisions.append({'ticker': sell_candidate[0], 'action': 'Sell', 'price': sell_candidate[2]})
                remaining_deals -= 2

        # 🔹 6. Exécution des décisions (Deals)
        for decision in decisions:
            conn.execute("""
                INSERT INTO Deals (RISK_TYPE, TICKER, EXECUTION_DATE, MANAGER_ID, TRADE_TYPE, QUANTITY, BUY_PRICE)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                'LOW_TURNOVER',
                decision['ticker'],
                simulation_date.strftime("%Y-%m-%d"),
                2,
                decision['action'],
                1,
                decision['price']
            ))

        conn.commit()
        conn.close()

        if decisions:
            print(f"✅ {len(decisions)} deal(s) exécuté(s) pour LOW_TURNOVER :")
            for d in decisions:
                print(f"→ {d['action']} {d['ticker']} @ {d['price']:.2f}")
        else:
            print("ℹ️ Aucune condition remplie pour un breakout cette semaine.")


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


    def strategy_three(self, simulation_date):
        print("Running Monte Carlo robust strategy for LOW_RISK...")

        db_file = self.db_file
        risk_type = "LOW_RISK"
        num_simulations = 200
        num_days = 7
        target_volatility = 0.10

        conn = sqlite3.connect(db_file)

        # 🔹 Récupérer automatiquement le MANAGER_ID
        manager_query = """
            SELECT MANAGER_ID FROM Portfolios
            WHERE RISK_TYPE = ? LIMIT 1
        """
        manager_row = pd.read_sql_query(manager_query, conn, params=(risk_type,))
        if manager_row.empty:
            print(f"Aucun manager trouvé pour le portefeuille {risk_type}.")
            conn.close()
            return
        manager_id = int(manager_row['MANAGER_ID'].iloc[0])

        # 🔹 Récupérer le budget disponible
        cash_query = """
            SELECT QUANTITY FROM Portfolios
            WHERE RISK_TYPE = ? AND TICKER = 'CASH'
        """
        cash_row = pd.read_sql_query(cash_query, conn, params=(risk_type,))
        if cash_row.empty:
            print("Pas de ligne 'CASH' dans le portefeuille.")
            conn.close()
            return
        max_budget = float(cash_row['QUANTITY'].iloc[0])

        # 🔹 Récupérer les tickers présents dans le portefeuille
        initial_query = """
            SELECT TICKER, QUANTITY FROM Portfolios
            WHERE RISK_TYPE = ? AND TICKER != 'CASH'
        """
        initial_df = pd.read_sql_query(initial_query, conn, params=(risk_type,))
        portfolio_tickers = initial_df['TICKER'].tolist()

        if not portfolio_tickers:
            print("Aucun actif à simuler.")
            conn.close()
            return

        # 🔹 Récupérer les données de prix
        price_data = {}
        for ticker in portfolio_tickers:
            df = pd.read_sql_query("""
                SELECT IMPORT_DATE, PRICE FROM Products
                WHERE TICKER = ? ORDER BY IMPORT_DATE ASC
            """, conn, params=(ticker,))
            if not df.empty:
                df['IMPORT_DATE'] = pd.to_datetime(df['IMPORT_DATE'], dayfirst=True)
                df.set_index('IMPORT_DATE', inplace=True)
                price_data[ticker] = df['PRICE']

        if not price_data:
            print("Données de prix manquantes.")
            conn.close()
            return

        # 🔹 Préparation des matrices
        prices_df = pd.concat(price_data.values(), axis=1)
        prices_df.columns = list(price_data.keys())
        prices_df.dropna(inplace=True)

        log_returns = np.log(prices_df / prices_df.shift(1)).dropna()
        mean_returns = log_returns.mean()
        cov_matrix = log_returns.cov()
        latest_prices = prices_df.iloc[-1].values
        tickers = prices_df.columns.tolist()
        nb_assets = len(tickers)

        # 🔹 Quantités initiales
        initial_quantities = np.zeros(nb_assets)
        for i, ticker in enumerate(tickers):
            row = initial_df[initial_df['TICKER'] == ticker]
            if not row.empty:
                initial_quantities[i] = float(row['QUANTITY'].iloc[0])
        initial_cost = np.sum(initial_quantities * latest_prices)

        # 🔹 Simulation Monte Carlo de la valeur future
        def simulate_gain(q):
            final_values = []
            for _ in range(30):
                future_prices = latest_prices.copy()
                for _ in range(num_days):
                    daily_returns = np.random.multivariate_normal(mean_returns, cov_matrix)
                    future_prices *= np.exp(daily_returns)
                final_values.append(np.sum(future_prices * q))
            return np.mean(final_values)

        # 🔹 Portefeuille actuel
        initial_gain = simulate_gain(initial_quantities)
        initial_result = {
            'tickers': tickers,
            'quantities': dict(zip(tickers, initial_quantities.astype(int))),
            'volatility': np.sqrt(np.dot((initial_quantities / np.sum(initial_quantities)).T,
                                        np.dot(cov_matrix * num_days, (initial_quantities / np.sum(initial_quantities))))),
            'expected_return': np.sum(mean_returns * initial_quantities / np.sum(initial_quantities)) * num_days,
            'total_cost': initial_cost,
            'gain_or_loss': initial_gain - initial_cost,
            'performance': 'gain' if initial_gain - initial_cost > 0 else 'loss'
        }

        best_result = initial_result

        # 🔁 Simulations
        for _ in range(num_simulations):
            weights = np.random.random(nb_assets)
            weights /= np.sum(weights)
            candidate_quantities = np.floor((weights * max_budget) / latest_prices)

            # ✅ Contraintes : vente max -10000 et achat max +150000
            diff = candidate_quantities - initial_quantities
            if any(diff < -10000) or any(diff > 150000):
                continue

            port_vol = np.sqrt(np.dot((candidate_quantities / np.sum(candidate_quantities)).T,
                                    np.dot(cov_matrix * num_days, (candidate_quantities / np.sum(candidate_quantities)))))
            total_cost = np.sum(candidate_quantities * latest_prices)

            # ✅ Filtrer selon budget et volatilité cible
            if port_vol <= target_volatility and total_cost <= max_budget:
                future_value = simulate_gain(candidate_quantities)
                gain_or_loss = future_value - total_cost

                if gain_or_loss > best_result['gain_or_loss']:
                    best_result = {
                        'tickers': tickers,
                        'quantities': dict(zip(tickers, candidate_quantities.astype(int))),
                        'volatility': port_vol,
                        'expected_return': np.sum(mean_returns * candidate_quantities / np.sum(candidate_quantities)) * num_days,
                        'total_cost': total_cost,
                        'gain_or_loss': gain_or_loss,
                        'performance': 'gain' if gain_or_loss > 0 else 'loss'
                    }

        # ⚠️ Si aucune meilleure solution, on ne change rien
        if best_result == initial_result:
            print("⚠️ Aucune stratégie optimale trouvée. Portefeuille conservé.")
            conn.close()
            return

        # 🔹 Mise à jour de la base : supprimer les lignes du portefeuille LOW_RISK
        cursor = conn.cursor()
        cursor.execute("DELETE FROM Portfolios WHERE RISK_TYPE = ?", (risk_type,))
        conn.commit()

        # 🔹 Réinsérer les nouvelles positions
        for ticker in best_result['tickers']:
            quantity = int(best_result['quantities'][ticker])
            spot = float(prices_df[ticker].iloc[-1])
            cursor.execute("""
                INSERT INTO Portfolios (RISK_TYPE, TICKER, QUANTITY, MANAGER_ID, LAST_UPDATED, SPOT_PRICE)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (risk_type, ticker, quantity, manager_id, simulation_date.strftime("%Y-%m-%d"), spot))

        # 🔹 Réinjecter le cash restant
        cash_remaining = max_budget - best_result['total_cost']
        cursor.execute("""
            INSERT INTO Portfolios (RISK_TYPE, TICKER, QUANTITY, MANAGER_ID, LAST_UPDATED, SPOT_PRICE)
            VALUES (?, 'CASH', ?, ?, ?, 1)
        """, (risk_type, cash_remaining, manager_id, simulation_date.strftime("%Y-%m-%d")))

        # 🔹 Historique des deals
        for i, ticker in enumerate(best_result['tickers']):
            old_qty = initial_quantities[i]
            new_qty = best_result['quantities'][ticker]
            delta = int(new_qty - old_qty)
            if delta != 0:
                trade_type = 'Buy' if delta > 0 else 'Sell'
                cursor.execute("""
                    INSERT INTO Deals (RISK_TYPE, TICKER, MANAGER_ID, TRADE_TYPE, QUANTITY, BUY_PRICE)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    risk_type,
                    ticker,
                    manager_id,
                    trade_type,
                    abs(delta),
                    float(prices_df[ticker].iloc[-1])
                ))

        conn.commit()
        conn.close()
        print("✅ Monte Carlo strategy executed with result:")
        print(best_result)


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

