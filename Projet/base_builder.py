import sqlite3
from faker import Faker
import random
import pandas as pd
fake = Faker()
from datetime import date, timedelta


db_file = "Fund.db"
Query_products = """CREATE TABLE IF NOT EXISTS Products (
    ISIN TEXT PRIMARY KEY,
    TICKER TEXT NOT NULL,
    ASSET_NAME TEXT NOT NULL,
    ASSET_CLASS TEXT NOT NULL
);"""

Query_returns = """CREATE TABLE IF NOT EXISTS Returns (
    ISIN TEXT PRIMARY KEY,
    FIVE_YEAR_PERFORMANCE REAL,
    TRADING_VOLUME INTEGER CHECK(TRADING_VOLUME >= 0),
    L_RETURN REAL,
    LAST_UPDATED DATE NOT NULL,
    FOREIGN KEY(ISIN) REFERENCES Products(ISIN) ON DELETE CASCADE
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
    ISIN TEXT NOT NULL,
    QUANTITY INTEGER CHECK(QUANTITY >= 0) NOT NULL,
    MANAGER_ID INTEGER NOT NULL,
    LAST_UPDATED DATE NOT NULL,
    SPOT_PRICE REAL CHECK(SPOT_PRICE >= 0) NOT NULL,
    FOREIGN KEY(ISIN) REFERENCES Products(ISIN) ON DELETE CASCADE,
    FOREIGN KEY(MANAGER_ID) REFERENCES Managers(MANAGER_ID) ON DELETE SET NULL
);"""

Query_deals = """CREATE TABLE IF NOT EXISTS Deals (
    DEAL_ID INTEGER PRIMARY KEY AUTOINCREMENT,
    PORTFOLIO_ID INTEGER NOT NULL,
    ISIN TEXT NOT NULL,
    EXECUTION_DATE TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    MANAGER_ID INTEGER NOT NULL,
    TRADE_TYPE TEXT CHECK(TRADE_TYPE IN ('Buy', 'Sell')) NOT NULL,
    QUANTITY INTEGER CHECK(QUANTITY > 0) NOT NULL,
    BUY_PRICE REAL CHECK(BUY_PRICE >= 0) NOT NULL,
    FOREIGN KEY(PORTFOLIO_ID) REFERENCES Portfolios(PORTFOLIO_ID) ON DELETE CASCADE,
    FOREIGN KEY(ISIN) REFERENCES Products(ISIN) ON DELETE CASCADE,
    FOREIGN KEY(MANAGER_ID) REFERENCES Managers(MANAGER_ID) ON DELETE SET NULL
);"""

try:
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    
    # Execute each query in order
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


def determine_risk_type(amount, knowledge, preference, goal, age):
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
    
def generate_clients_data(n: int):
    """Génère des données fictives pour les clients."""
    clients_data = []
    
    for _ in range(n):
        FIRST_NAME = fake.first_name()
        LAST_NAME = fake.last_name()
        EMAIL = fake.email()
        PHONE = fake.phone_number()

        # Génération de la date de naissance
        BIRTH_DATE = fake.date_of_birth(minimum_age=18, maximum_age=80)
        BIRTH_DATE_STR = BIRTH_DATE.strftime('%Y-%m-%d')

        # Définition des bornes pour la date d'inscription
        min_registration_date = BIRTH_DATE + timedelta(days=18*365)  # Après 18 ans minimum
        max_registration_date = date(2024, 12, 31)  # Pas après cette date
        REGISTRATION_DATE = fake.date_between(start_date=min_registration_date, end_date=max_registration_date)
        REGISTRATION_DATE_STR = REGISTRATION_DATE.strftime('%Y-%m-%d')

        # Choix aléatoire des autres variables
        INVESTMENT_AMOUNT = round(random.uniform(0, 100000), 2)
        INVESTMENT_KNOWLEDGE = random.choice(['Low', 'Medium', 'High'])
        ASSET_PREFERENCE = random.choice(['Stocks', 'Bonds', 'Commodities', 'Real Estate', 'Cryptocurrency'])
        INVESTMENT_GOAL = random.choice(['Retirement', 'Education', 'Wealth Preservation', 'Wealth Accumulation', 'Other'])
        AGE = (date.today() - BIRTH_DATE).days // 365

        #DETERMINER RISK TYPE EN FCT DES TRUCS DONNES 
        RISK_TYPE = determine_risk_type(INVESTMENT_AMOUNT, INVESTMENT_KNOWLEDGE, ASSET_PREFERENCE, INVESTMENT_GOAL, AGE)

        # Ajout des données à la liste
        clients_data.append((
            FIRST_NAME, LAST_NAME, EMAIL, BIRTH_DATE_STR, PHONE, REGISTRATION_DATE_STR, 
            RISK_TYPE, INVESTMENT_AMOUNT, INVESTMENT_KNOWLEDGE, ASSET_PREFERENCE, INVESTMENT_GOAL, AGE
        ))
    
    return clients_data


def generate_managers_data(l): #  l = 3 managers car 3 profils de risque 
    managers_data = []
    for _ in range(l):
        FIRST_NAME = fake.first_name()
        LAST_NAME = fake.last_name()
        BIRTH_DATE = fake.date_of_birth(minimum_age=18, maximum_age=80).strftime('%Y-%m-%d')
        EMAIL = fake.email()
        PHONE = fake.phone_number()
        SENIORITY = random.randint(0, 40)
        managers_data.append((FIRST_NAME, LAST_NAME, BIRTH_DATE, EMAIL, PHONE, SENIORITY))
    return managers_data


# Nombre de clients à générer
num_clients = 3

try:
    # Connexion à la base de données SQLite
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    # Insertion des données dans la table Clients
    insert_query = """
    INSERT INTO Clients (FIRST_NAME, LAST_NAME, EMAIL, BIRTH_DATE, PHONE, REGISTRATION_DATE, RISK_TYPE, INVESTMENT_AMOUNT, INVESTMENT_KNOWLEDGE, ASSET_PREFERENCE, INVESTMENT_GOAL, AGE)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    clients_data = generate_clients_data(num_clients)
    cursor.executemany(insert_query, clients_data)
    conn.commit()
    print(f"{num_clients} clients insérés avec succès dans la table 'Clients'.")

except sqlite3.Error as e:
    print(f"Erreur SQLite : {e}")

finally:
    # Fermeture de la connexion
    if conn:
        conn.close()


# Nombre de Managers à générer
num_managers = 3

try:
    # Connexion à la base de données SQLite
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    # Insertion des données dans la table Clients
    insert_query = """
    INSERT INTO Managers (FIRST_NAME, LAST_NAME, BIRTH_DATE, EMAIL, PHONE, SENIORITY)
    VALUES (?, ?, ?, ?, ?, ?)
    """
    Managers_data = generate_managers_data(num_managers)
    cursor.executemany(insert_query, Managers_data)
    conn.commit()
    print(f"{num_managers} clients insérés avec succès dans la table 'Managers'.")

except sqlite3.Error as e:
    print(f"Erreur SQLite : {e}")

finally:
    # Fermeture de la connexion
    if conn:
        conn.close()