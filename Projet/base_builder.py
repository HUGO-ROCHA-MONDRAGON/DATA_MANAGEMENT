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
            SENIORITY INTEGER CHECK(SENIORITY >= 0)
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
            INVESTMENT_AMOUNT REAL CHECK(INVESTMENT_AMOUNT >= 0) NOT NULL,
            INVESTMENT_KNOWLEDGE TEXT CHECK(INVESTMENT_KNOWLEDGE IN ('Low', 'Medium', 'High')) NOT NULL,
            ASSET_PREFERENCE TEXT CHECK(ASSET_PREFERENCE IN ('Stocks', 'Bonds', 'Commodities', 'Real Estate', 'Cryptocurrency')) NOT NULL,
            INVESTMENT_GOAL TEXT CHECK(INVESTMENT_GOAL IN ('Retirement', 'Education', 'Wealth Preservation', 'Wealth Accumulation', 'Other')) NOT NULL,
            AGE INTEGER
        );"""

        Query_portfolio = """CREATE TABLE IF NOT EXISTS Portfolios (
            PORTFOLIO_ID INTEGER PRIMARY KEY AUTOINCREMENT,
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
            PORTFOLIO_ID INTEGER NOT NULL,
            TICKER TEXT NOT NULL,
            EXECUTION_DATE TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            MANAGER_ID INTEGER NOT NULL,
            TRADE_TYPE TEXT CHECK(TRADE_TYPE IN ('Buy', 'Sell')) NOT NULL,
            QUANTITY INTEGER CHECK(QUANTITY > 0) NOT NULL,
            BUY_PRICE REAL CHECK(BUY_PRICE >= 0) NOT NULL,
            FOREIGN KEY(PORTFOLIO_ID) REFERENCES Portfolios(PORTFOLIO_ID) ON DELETE CASCADE,
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
            EMAIL = fake.email()
            PHONE = fake.phone_number()
            BIRTH_DATE = fake.date_of_birth(minimum_age=18, maximum_age=80)
            BIRTH_DATE_STR = BIRTH_DATE.strftime("%d/%m/%Y")
            min_registration_date = BIRTH_DATE + timedelta(days=18*365)
            max_registration_date = date(2024, 12, 31)
            REGISTRATION_DATE = fake.date_between(start_date=min_registration_date, end_date=max_registration_date)
            REGISTRATION_DATE_STR = REGISTRATION_DATE.strftime("%d/%m/%Y")
            INVESTMENT_AMOUNT = round(random.uniform(0, 100000), 2)
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
            BIRTH_DATE = fake.date_of_birth(minimum_age=18, maximum_age=80).strftime("%d/%m/%Y")
            EMAIL = fake.email()
            PHONE = fake.phone_number()
            SENIORITY = random.randint(0, 40)
            managers_data.append((FIRST_NAME, LAST_NAME, BIRTH_DATE, EMAIL, PHONE, SENIORITY))
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
            INSERT INTO Managers (FIRST_NAME, LAST_NAME, BIRTH_DATE, EMAIL, PHONE, SENIORITY)
            VALUES (?, ?, ?, ?, ?, ?)
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

    
