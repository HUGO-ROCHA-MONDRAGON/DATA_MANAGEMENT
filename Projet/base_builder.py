import sqlite3
from faker import Faker
import random
import pandas as pd
from datetime import date, timedelta
 
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
            ASSET_UNDER_MANAGEMENT REAL DEFAULT 0
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
 
        try:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            cursor.execute(Query_products)
            cursor.execute(Query_returns)
            cursor.execute(Query_managers)
            cursor.execute(Query_clients)
            cursor.execute(Query_portfolio)
            cursor.execute(Query_deals)
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
        clients_data = []
        for _ in range(n):
            FIRST_NAME = fake.first_name()
            LAST_NAME = fake.last_name()
            EMAIL = FIRST_NAME + "_" + LAST_NAME + "@gmail.com"
            PHONE = fake.phone_number()
            BIRTH_DATE = fake.date_of_birth(minimum_age=18, maximum_age=80)
            BIRTH_DATE_STR = BIRTH_DATE.strftime("%d/%m/%Y")
            min_registration_date = BIRTH_DATE + timedelta(days=18*365)
            max_registration_date = date(2023, 12, 31)
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
        managers_data = []
        for _ in range(l):
            FIRST_NAME = fake.first_name()
            LAST_NAME = fake.last_name()
            EMAIL = FIRST_NAME + "_" + LAST_NAME + "@aeou_management.com"
            PHONE = fake.phone_number()
            BIRTH_DATE_OBJ = fake.date_of_birth(minimum_age=18, maximum_age=65)
            BIRTH_DATE = BIRTH_DATE_OBJ.isoformat()              
            # Calcul de l'âge à la date de référence
            reference_date = date(2024, 12, 31)
            age_at_reference = (reference_date - BIRTH_DATE_OBJ).days // 365
            # Séniorité possible = âge - 18 (car on suppose début de carrière à 18 ans)
            max_seniority = max(age_at_reference - 18, 0)
            SENIORITY = int(random.randint(0, max_seniority))
            # Génération d'un montant réaliste d'AUM (entre 1M et 100M)
            ASSET_UNDER_MANAGEMENT = 0   
            # Création du tuple dans l'ordre exact attendu par la requête SQL
            manager_tuple = (FIRST_NAME, LAST_NAME, BIRTH_DATE, EMAIL, PHONE, SENIORITY, ASSET_UNDER_MANAGEMENT)
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
            INSERT INTO Managers (FIRST_NAME, LAST_NAME, BIRTH_DATE, EMAIL, PHONE, SENIORITY, ASSET_UNDER_MANAGEMENT)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """
            managers_data = self.generate_managers_data(num_managers)
            cursor.executemany(insert_query, managers_data)
            conn.commit()
            print(f"{num_managers} managers insérés avec succès dans la table 'Managers'.")
        except sqlite3.Error as e:
            print(f"Erreur SQLite : {e}")
        finally:
            if conn:
                conn.close()
 
   
def update_asset_under_management(self):
    """Calcule les AUM de chaque manager en fonction des clients ayant le même RISK_TYPE que ses portefeuilles."""
    try:
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
 
        # Étape 1 : Récupérer tous les managers et leurs RISK_TYPE (via Portfolios)
        cursor.execute("""
            SELECT DISTINCT MANAGER_ID, RISK_TYPE
            FROM Portfolios
        """)
        manager_risk_map = cursor.fetchall()
 
        # Étape 2 : Dictionnaire pour stocker la somme par manager
        manager_aum = {}
 
        for manager_id, risk_type in manager_risk_map:
            # Étape 3 : Somme des montants investis par les clients ayant ce RISK_TYPE
            cursor.execute("""
                SELECT SUM(INVESTMENT_AMOUNT)
                FROM Clients
                WHERE RISK_TYPE = ?
            """, (risk_type,))
            total_invested = cursor.fetchone()[0] or 0  # évite None
 
            # Si un manager gère plusieurs types de risque, on cumule
            if manager_id in manager_aum:
                manager_aum[manager_id] += total_invested
            else:
                manager_aum[manager_id] = total_invested
 
        # Étape 4 : Mise à jour de la colonne ASSET_UNDER_MANAGEMENT dans Managers
        for manager_id, aum in manager_aum.items():
            cursor.execute("""
                UPDATE Managers
                SET ASSET_UNDER_MANAGEMENT = ?
                WHERE MANAGER_ID = ?
            """, (aum, manager_id))
 
        conn.commit()
    except sqlite3.Error as e:
        print(f"❌ Erreur SQLite : {e}")
    finally:
        if conn:
            conn.close()
 