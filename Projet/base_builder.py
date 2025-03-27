import sqlite3
from faker import Faker
import random
import pandas as pd
from datetime import date, timedelta
from collections import defaultdict

fake = Faker()
#test abdel
class DatabaseBuilder:
    def __init__(self, db_file):
        self.db_file = db_file
 
    def create_tables(self):
        Query_products = """CREATE TABLE IF NOT EXISTS Products (
            PRODUCT_ID INTEGER PRIMARY KEY AUTOINCREMENT,
            TICKER TEXT NOT NULL,
            SECTOR TEXT NOT NULL,
            PRICE REAL CHECK(PRICE >= 0) NOT NULL,
            IMPORT_DATE DATE NOT NULL
        );"""
 
        Query_returns = """CREATE TABLE IF NOT EXISTS Returns (
            TICKER TEXT PRIMARY KEY,
            FIVE_YEAR_PERFORMANCE REAL,
            TRADING_VOLUME INTEGER CHECK(TRADING_VOLUME >= 0),
            L_RETURN REAL
        );"""
 
        Query_managers = """CREATE TABLE IF NOT EXISTS Managers (
            MANAGER_ID INTEGER PRIMARY KEY AUTOINCREMENT,
            FIRST_NAME TEXT NOT NULL,
            LAST_NAME TEXT NOT NULL,
            BIRTH_DATE DATE NOT NULL,
            EMAIL TEXT UNIQUE NOT NULL,
            PHONE TEXT,
            SENIORITY INTEGER CHECK(SENIORITY >= 0),
            RISK_TYPE TEXT CHECK(RISK_TYPE IN ('LOW_RISK', 'LOW_TURNOVER', 'HY_EQUITY')) NOT NULL
        );"""
 
        Query_clients = """CREATE TABLE IF NOT EXISTS Clients (
            CLIENTS_ID INTEGER PRIMARY KEY AUTOINCREMENT,
            FIRST_NAME TEXT NOT NULL,
            LAST_NAME TEXT NOT NULL,
            EMAIL TEXT NOT NULL UNIQUE,
            BIRTH_DATE DATE NOT NULL,
            PHONE TEXT,
            REGISTRATION_DATE DATE DEFAULT CURRENT_TIMESTAMP NOT NULL,
            RISK_TYPE TEXT CHECK(RISK_TYPE IN ('LOW_RISK', 'LOW_TURNOVER', 'HY_EQUITY')) NOT NULL,
            INVESTMENT_AMOUNT REAL CHECK(INVESTMENT_AMOUNT >= 100000) NOT NULL,
            INVESTMENT_KNOWLEDGE TEXT CHECK(INVESTMENT_KNOWLEDGE IN ('Low', 'Medium', 'High')) NOT NULL,
            ASSET_PREFERENCE TEXT CHECK(ASSET_PREFERENCE IN ('Stocks', 'Bonds', 'Commodities', 'Real Estate', 'Cryptocurrency')) NOT NULL,
            INVESTMENT_GOAL TEXT CHECK(INVESTMENT_GOAL IN ('Retirement', 'Education', 'Wealth Preservation', 'Wealth Accumulation', 'Other')) NOT NULL,
            AGE INTEGER
        );"""
 
        Query_portfolio = """CREATE TABLE IF NOT EXISTS Portfolios (
            INPUT_ID INTEGER PRIMARY KEY AUTOINCREMENT,
            RISK_TYPE TEXT CHECK(RISK_TYPE IN ('LOW_RISK', 'LOW_TURNOVER', 'HY_EQUITY')) NOT NULL,
            TICKER TEXT NOT NULL,
            QUANTITY INTEGER CHECK(QUANTITY >= 0) NOT NULL,
            MANAGER_ID INTEGER NOT NULL,
            LAST_UPDATED DATE NOT NULL,
            SPOT_PRICE REAL CHECK(SPOT_PRICE >= 0) NOT NULL,
            VALUE REAL GENERATED ALWAYS AS (QUANTITY * SPOT_PRICE) STORED,
            FOREIGN KEY(MANAGER_ID) REFERENCES Managers(MANAGER_ID) ON DELETE SET NULL
        );"""
 
        Query_deals = """CREATE TABLE IF NOT EXISTS Deals (
            DEAL_ID INTEGER PRIMARY KEY AUTOINCREMENT,
            RISK_TYPE TEXT CHECK(RISK_TYPE IN ('LOW_RISK', 'LOW_TURNOVER', 'HY_EQUITY')) NOT NULL,
            TICKER TEXT NOT NULL,
            EXECUTION_DATE TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            MANAGER_ID INTEGER NOT NULL,
            TRADE_TYPE TEXT CHECK(TRADE_TYPE IN ('Buy', 'Sell')) NOT NULL,
            QUANTITY INTEGER CHECK(QUANTITY > 0) NOT NULL,
            BUY_PRICE REAL CHECK(BUY_PRICE >= 0) NOT NULL,
            FOREIGN KEY(RISK_TYPE) REFERENCES Portfolios(RISK_TYPE) ON DELETE CASCADE,
            FOREIGN KEY(MANAGER_ID) REFERENCES Managers(MANAGER_ID) ON DELETE SET NULL
        );"""

        create_history_table_query = """CREATE TABLE IF NOT EXISTS Portfolio_History (
             HISTORY_ID INTEGER PRIMARY KEY AUTOINCREMENT,
             MANAGER_ID INTEGER NOT NULL,
             TICKER TEXT NOT NULL,
             QUANTITY INTEGER NOT NULL,
             DATE_SNAPSHOT DATE NOT NULL,
             FOREIGN KEY(MANAGER_ID) REFERENCES Managers(MANAGER_ID) ON DELETE CASCADE
         );"""
 
        try:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            cursor.execute(Query_products)
            cursor.execute(Query_returns)
            cursor.execute(Query_managers)
            cursor.execute(Query_clients)
            cursor.execute(Query_portfolio)
            cursor.execute(Query_deals)
            cursor.execute(create_history_table_query)
            conn.commit()
            print("Tables created successfully.")
        except sqlite3.Error as e:
            print(f"SQLite error: {e}")
        finally:
            if conn:
                conn.close()
 
    def determine_risk_type(self, amount, knowledge, preference, goal, age):
        """Détermine le type de risque en fonction des caractéristiques du client."""
        if amount > 50000 and knowledge == 'High' and preference in ['Stocks', 'Cryptocurrency']:
            return 'HY_EQUITY'
        elif amount < 20000 and knowledge == 'Low' and goal == 'Wealth Preservation':
            return 'LOW_RISK'
        elif age > 60 and goal in ['Retirement', 'Wealth Preservation']:
            return 'LOW_RISK'
        elif knowledge in ['Medium', 'High'] and preference in ['Bonds', 'Real Estate']:
            return 'LOW_TURNOVER'
        else:
            return random.choice(['LOW_RISK', 'LOW_TURNOVER'])
 
    def generate_clients_data(self, n):
        """Génère des données fictives pour les clients."""
        if n < 3:
            raise ValueError("Il faut au moins 3 clients pour couvrir tous les risk types (LOW_RISK, LOW_TURNOVER, HY_EQUITY)")
        
        clients_data = []
        risk_types = ['LOW_RISK', 'LOW_TURNOVER', 'HY_EQUITY']
        
        # Générer d'abord un client pour chaque risk type
        for risk_type in risk_types:
            # Générer des attributs qui correspondent au risk type souhaité
            if risk_type == 'HY_EQUITY':
                INVESTMENT_AMOUNT = round(random.uniform(500000, 10000000), 2)
                INVESTMENT_KNOWLEDGE = 'High'
                ASSET_PREFERENCE = random.choice(['Stocks', 'Cryptocurrency'])
                INVESTMENT_GOAL = random.choice(['Wealth Accumulation', 'Education'])
                AGE = random.randint(25, 55)
            elif risk_type == 'LOW_RISK':
                INVESTMENT_AMOUNT = round(random.uniform(100000, 500000), 2)
                INVESTMENT_KNOWLEDGE = 'Low'
                ASSET_PREFERENCE = random.choice(['Bonds', 'Real Estate'])
                INVESTMENT_GOAL = 'Wealth Preservation'
                AGE = random.randint(60, 80)
            else:  # LOW_TURNOVER
                INVESTMENT_AMOUNT = round(random.uniform(100000, 1000000), 2)
                INVESTMENT_KNOWLEDGE = random.choice(['Medium', 'High'])
                ASSET_PREFERENCE = random.choice(['Bonds', 'Real Estate'])
                INVESTMENT_GOAL = random.choice(['Retirement', 'Wealth Preservation'])
                AGE = random.randint(35, 65)
            
            # Générer les autres attributs
            FIRST_NAME = fake.first_name()
            LAST_NAME = fake.last_name()
            EMAIL = FIRST_NAME + "_" + LAST_NAME + "@gmail.com"
            PHONE = fake.phone_number()
            
            # Générer une date de naissance qui permet une date d'enregistrement valide
            max_birth_date = date(2005, 12, 31)  # 18 ans en 2023
            BIRTH_DATE = fake.date_of_birth(minimum_age=18, maximum_age=80)
            BIRTH_DATE_STR = BIRTH_DATE.strftime("%d/%m/%Y")
            
            # S'assurer que la date d'enregistrement est après la date de naissance + 18 ans
            min_registration_date = BIRTH_DATE + timedelta(days=18*365)
            max_registration_date = date(2023, 12, 31)
            
            # Si la date minimale est après la date maximale, ajuster la date de naissance
            if min_registration_date > max_registration_date:
                BIRTH_DATE = fake.date_between(start_date=date(1943, 1, 1), end_date=max_birth_date)
                BIRTH_DATE_STR = BIRTH_DATE.strftime("%d/%m/%Y")
                min_registration_date = BIRTH_DATE + timedelta(days=18*365)
            
            REGISTRATION_DATE = fake.date_between(start_date=min_registration_date, end_date=max_registration_date)
            REGISTRATION_DATE_STR = REGISTRATION_DATE.strftime("%d/%m/%Y")
            
            clients_data.append((FIRST_NAME, LAST_NAME, EMAIL, BIRTH_DATE_STR, PHONE, REGISTRATION_DATE_STR, risk_type, INVESTMENT_AMOUNT, INVESTMENT_KNOWLEDGE, ASSET_PREFERENCE, INVESTMENT_GOAL, AGE))
        
        # Générer les clients restants avec des attributs aléatoires
        for _ in range(3, n):
            FIRST_NAME = fake.first_name()
            LAST_NAME = fake.last_name()
            EMAIL = FIRST_NAME + "_" + LAST_NAME + "@gmail.com"
            PHONE = fake.phone_number()
            
            # Générer une date de naissance qui permet une date d'enregistrement valide
            max_birth_date = date(2005, 12, 31)  # 18 ans en 2023
            BIRTH_DATE = fake.date_of_birth(minimum_age=18, maximum_age=80)
            BIRTH_DATE_STR = BIRTH_DATE.strftime("%d/%m/%Y")
            
            # S'assurer que la date d'enregistrement est après la date de naissance + 18 ans
            min_registration_date = BIRTH_DATE + timedelta(days=18*365)
            max_registration_date = date(2023, 12, 31)
            
            # Si la date minimale est après la date maximale, ajuster la date de naissance
            if min_registration_date > max_registration_date:
                BIRTH_DATE = fake.date_between(start_date=date(1943, 1, 1), end_date=max_birth_date)
                BIRTH_DATE_STR = BIRTH_DATE.strftime("%d/%m/%Y")
                min_registration_date = BIRTH_DATE + timedelta(days=18*365)
            
            REGISTRATION_DATE = fake.date_between(start_date=min_registration_date, end_date=max_registration_date)
            REGISTRATION_DATE_STR = REGISTRATION_DATE.strftime("%d/%m/%Y")
            
            INVESTMENT_AMOUNT = round(random.uniform(100000,10000000), 2)
            INVESTMENT_KNOWLEDGE = random.choice(['Low', 'Medium', 'High'])
            ASSET_PREFERENCE = random.choice(['Stocks', 'Bonds', 'Commodities', 'Real Estate', 'Cryptocurrency'])
            INVESTMENT_GOAL = random.choice(['Retirement', 'Education', 'Wealth Preservation', 'Wealth Accumulation', 'Other'])
            AGE = (date.today() - BIRTH_DATE).days // 365
            RISK_TYPE = self.determine_risk_type(INVESTMENT_AMOUNT, INVESTMENT_KNOWLEDGE, ASSET_PREFERENCE, INVESTMENT_GOAL, AGE)
            
            clients_data.append((FIRST_NAME, LAST_NAME, EMAIL, BIRTH_DATE_STR, PHONE, REGISTRATION_DATE_STR, RISK_TYPE, INVESTMENT_AMOUNT, INVESTMENT_KNOWLEDGE, ASSET_PREFERENCE, INVESTMENT_GOAL, AGE))
        
        return clients_data
 
    def generate_managers_data(self, l):
        """Génère des données fictives pour les managers."""
        if l < 3:
            raise ValueError("Il faut au moins 3 managers pour couvrir tous les risk types (LOW_RISK, LOW_TURNOVER, HY_EQUITY)")
        
        managers_data = []
        risk_types = ['LOW_RISK', 'LOW_TURNOVER', 'HY_EQUITY']
        
        # Pour les 3 premiers managers, on attribue un risk type différent
        for i in range(3):
            FIRST_NAME = fake.first_name()
            LAST_NAME = fake.last_name()
            EMAIL = FIRST_NAME + "_" + LAST_NAME + "@aeou_management.com"
            PHONE = fake.phone_number()
            BIRTH_DATE_OBJ = fake.date_of_birth(minimum_age=18, maximum_age=65)
            BIRTH_DATE = BIRTH_DATE_OBJ.isoformat()              
            reference_date = date(2024, 12, 31)
            age_at_reference = (reference_date - BIRTH_DATE_OBJ).days // 365
            max_seniority = max(age_at_reference - 18, 0)
            SENIORITY = int(random.randint(0, max_seniority))
            RISK_TYPE = risk_types[i]
            
            manager_tuple = (FIRST_NAME, LAST_NAME, BIRTH_DATE, EMAIL, PHONE, SENIORITY, RISK_TYPE)
            managers_data.append(manager_tuple)
        
        # Pour les managers supplémentaires, on attribue un risk type aléatoire
        for _ in range(3, l):
            FIRST_NAME = fake.first_name()
            LAST_NAME = fake.last_name()
            EMAIL = FIRST_NAME + "_" + LAST_NAME + "@aeou_management.com"
            PHONE = fake.phone_number()
            BIRTH_DATE_OBJ = fake.date_of_birth(minimum_age=18, maximum_age=65)
            BIRTH_DATE = BIRTH_DATE_OBJ.isoformat()              
            reference_date = date(2024, 12, 31)
            age_at_reference = (reference_date - BIRTH_DATE_OBJ).days // 365
            max_seniority = max(age_at_reference - 18, 0)
            SENIORITY = int(random.randint(0, max_seniority))
            RISK_TYPE = random.choice(risk_types)
            
            manager_tuple = (FIRST_NAME, LAST_NAME, BIRTH_DATE, EMAIL, PHONE, SENIORITY, RISK_TYPE)
            managers_data.append(manager_tuple)
        
        return managers_data
 
    def insert_clients_data(self, num_clients):
        """Insère des données de clients dans la base de données."""
        try:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            insert_query = """
            INSERT INTO Clients (FIRST_NAME, LAST_NAME, EMAIL, BIRTH_DATE, PHONE, REGISTRATION_DATE, RISK_TYPE, INVESTMENT_AMOUNT, INVESTMENT_KNOWLEDGE, ASSET_PREFERENCE, INVESTMENT_GOAL, AGE)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            clients_data = self.generate_clients_data(num_clients)
            cursor.executemany(insert_query, clients_data)
            conn.commit()
            print(f"{num_clients} clients insérés avec succès dans la table 'Clients'.")
        except sqlite3.Error as e:
            print(f"Erreur SQLite : {e}")
        finally:
            if conn:
                conn.close()
 
    def insert_managers_data(self, num_managers):
        """Insère des données de managers dans la base de données."""
        try:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            insert_query = """
            INSERT INTO Managers (FIRST_NAME, LAST_NAME, BIRTH_DATE, EMAIL, PHONE, SENIORITY, RISK_TYPE)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """
            managers_data = self.generate_managers_data(num_managers)
            cursor.executemany(insert_query, managers_data)
            conn.commit()
            print(f"{num_managers} managers insérés avec succès dans la table 'Managers'.")
        except ValueError as e:
            print(f"Erreur de validation : {e}")
        except sqlite3.Error as e:
            print(f"Erreur SQLite : {e}")
        finally:
            if conn:
                conn.close()

    def get_investment_amount_by_risk_type(self):
        """Calcule la somme des montants d'investissement par risk type."""
        try:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            
            query = """
            SELECT RISK_TYPE, COUNT(*) as client_count, SUM(INVESTMENT_AMOUNT) as total_amount
            FROM Clients
            GROUP BY RISK_TYPE
            ORDER BY RISK_TYPE
            """
            
            cursor.execute(query)
            results = cursor.fetchall()
            
            print("\nRésumé des investissements par risk type:")
            print("-" * 50)
            for risk_type, count, total in results:
                print(f"Risk Type: {risk_type}")
                print(f"Nombre de clients: {count}")
                print(f"Montant total investi: {total:,.2f} €")
                print("-" * 50)
            
            return results
            
        except sqlite3.Error as e:
            print(f"Erreur SQLite : {e}")
            return None
        finally:
            if conn:
                conn.close()

    def insert_initial_cash_portfolios(self, start_date):
        """Insère les montants initiaux en cash dans la table Portfolios."""
        try:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            
            # Récupérer les montants par risk type
            query_amounts = """
            SELECT RISK_TYPE, SUM(INVESTMENT_AMOUNT) as total_amount
            FROM Clients
            GROUP BY RISK_TYPE
            """
            cursor.execute(query_amounts)
            risk_amounts = cursor.fetchall()
            
            # Récupérer les managers par risk type
            query_managers = """
            SELECT MANAGER_ID, RISK_TYPE
            FROM Managers
            WHERE RISK_TYPE IN ('LOW_RISK', 'LOW_TURNOVER', 'HY_EQUITY')
            """
            cursor.execute(query_managers)
            managers = cursor.fetchall()
            
            # Créer un dictionnaire pour accéder facilement aux managers par risk type
            manager_by_risk = {risk: manager_id for manager_id, risk in managers}
            
            # Insérer les lignes de cash pour chaque risk type
            insert_query = """
            INSERT INTO Portfolios (RISK_TYPE, TICKER, QUANTITY, MANAGER_ID, LAST_UPDATED, SPOT_PRICE)
            VALUES (?, 'CASH', ?, ?, ?, 1)
            """
            
            for risk_type, amount in risk_amounts:
                manager_id = manager_by_risk.get(risk_type)
                if manager_id:
                    cursor.execute(insert_query, (risk_type, amount, manager_id, start_date))
            
            conn.commit()
            print("Montants initiaux en cash insérés avec succès dans la table Portfolios.")
            
        except sqlite3.Error as e:
            print(f"Erreur SQLite : {e}")
        finally:
            if conn:
                conn.close()
            

    def rebuild_portfolio_history_by_risk_type(self, risk_type):
        try:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()

            # 1. Composition initiale des portefeuilles ayant ce type de risque
            cursor.execute("""
                SELECT MANAGER_ID, TICKER, QUANTITY
                FROM Portfolios
                WHERE RISK_TYPE = ?
            """, (risk_type,))
            initial_assets = cursor.fetchall()

            # Dictionnaire de portefeuilles par manager
            portfolios = defaultdict(dict)
            for manager_id, ticker, qty in initial_assets:
                portfolios[manager_id][ticker] = qty

            # 2. Récupérer les deals triés par date pour ce risk_type
            cursor.execute("""
                SELECT DATE(EXECUTION_DATE), MANAGER_ID, TICKER, TRADE_TYPE, QUANTITY
                FROM Deals
                WHERE RISK_TYPE = ?
                ORDER BY EXECUTION_DATE ASC
            """, (risk_type,))
            deals = cursor.fetchall()

            # 3. Grouper les deals [par manager_id][par date]
            grouped_deals = defaultdict(lambda: defaultdict(list))
            for date, manager_id, ticker, trade_type, qty in deals:
                grouped_deals[manager_id][date].append((ticker, trade_type, qty))

            # 4. Appliquer les deals par manager et par date
            for manager_id, deals_by_date in grouped_deals.items():
                portfolio = portfolios.get(manager_id, {})  # récupérer ou init portefeuille

                for date in sorted(deals_by_date.keys()):
                    for ticker, trade_type, qty in deals_by_date[date]:
                        if trade_type == 'Buy':
                            portfolio[ticker] = portfolio.get(ticker, 0) + qty
                        elif trade_type == 'Sell':
                            portfolio[ticker] = max(portfolio.get(ticker, 0) - qty, 0)

                    # Nettoyage des actifs à 0
                    portfolio = {k: v for k, v in portfolio.items() if v > 0}

                    # Sauvegarde du snapshot pour ce manager à cette date
                    for tick, qte in portfolio.items():
                        cursor.execute("""
                            INSERT INTO Portfolio_History (MANAGER_ID, TICKER, QUANTITY, DATE_SNAPSHOT)
                            VALUES (?, ?, ?, ?)
                        """, (manager_id, tick, qte, date))

            conn.commit()
            print(f"✅ Historique reconstruit pour les portefeuilles de type {risk_type}")
        except sqlite3.Error as e:
            print(f"❌ Erreur SQLite : {e}")
        finally:
            if conn:
                conn.close()

