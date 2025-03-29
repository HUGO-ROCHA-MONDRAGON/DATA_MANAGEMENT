import pandas as pd
from data_collector import GetData
from datetime import datetime, timedelta
import sqlite3


class BaseUpdate: 
    def __init__(self, tickers, start_date, end_date, db_file, existing_data=None):
        self.tickers = tickers
        self.start_date = datetime.strptime(start_date, "%d/%m/%Y")
        self.end_date = datetime.strptime(end_date, "%d/%m/%Y")
        self.db_file = db_file
        self.all_data = existing_data

    def update_products(self):
        """
        Updates the Products table with new market data for a specific date range.
        Uses the existing DataFrame passed during initialization.
        """
        try:
            if self.all_data is None:
                print("No data available. Please provide existing_data during initialization.")
                return
            
            # Convert IMPORT_DATE to datetime for filtering
            df = self.all_data.copy()
            
            # Convertir les dates au format DD/MM/YYYY
            try:
                # Si les dates sont déjà au format datetime
                if pd.api.types.is_datetime64_any_dtype(df['IMPORT_DATE']):
                    df['IMPORT_DATE'] = df['IMPORT_DATE'].dt.strftime('%d/%m/%Y')
                else:
                    # Si les dates sont au format YYYY-MM-DD
                    if df['IMPORT_DATE'].str.contains('-').all():
                        df['IMPORT_DATE'] = pd.to_datetime(df['IMPORT_DATE']).dt.strftime('%d/%m/%Y')
                    # Si les dates sont déjà au format DD/MM/YYYY
                    elif df['IMPORT_DATE'].str.contains('/').all():
                        pass  # Ne rien faire, déjà au bon format
                    else:
                        print("Format de date non reconnu")
                        return
            except Exception as e:
                print(f"Erreur lors de la conversion des dates: {e}")
                return
            
            # Filter data for the specified date range
            mask = (pd.to_datetime(df['IMPORT_DATE'], format='%d/%m/%Y') >= self.start_date) & \
                   (pd.to_datetime(df['IMPORT_DATE'], format='%d/%m/%Y') <= self.end_date)
            df_filtered = df[mask].copy()
            
            if df_filtered.empty:
                print(f"No data available for the specified date range ({self.start_date.strftime('%d/%m/%Y')} to {self.end_date.strftime('%d/%m/%Y')})")
                return
            
            # S'assurer que PRICE est un float
            df_filtered.loc[:, 'PRICE'] = df_filtered['PRICE'].astype(float)
            
            # Connect to the database
            conn = sqlite3.connect(self.db_file)
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
                        str(row['TICKER']),
                        str(row['SECTOR']),
                        float(row['PRICE']),
                        str(row['IMPORT_DATE'])  # Déjà au format DD/MM/YYYY
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
                    
    def initialisation_portefeuille_HY(self):
        """Initialise le portefeuille HY_EQUITY en achetant les 5 tickers avec les meilleurs rendements"""
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        risk_type = "HY_EQUITY"
        
        try:
            # Vérifier si le portefeuille est déjà initialisé
            cursor.execute("""
                SELECT COUNT(*) FROM Portfolios 
                WHERE RISK_TYPE = ? AND TICKER != 'CASH'
            """, (risk_type,))
            if cursor.fetchone()[0] > 0:
                print(f"Le portefeuille {risk_type} est déjà initialisé")
                return

            # Vérifier le cash disponible
            cursor.execute("""
                SELECT QUANTITY FROM Portfolios 
                WHERE RISK_TYPE = ? AND TICKER = 'CASH'
            """, (risk_type,))
            cash_result = cursor.fetchone()
            if not cash_result or cash_result[0] < 1000:
                print(f"Pas assez de cash disponible pour {risk_type} (minimum 1000)")
                return
            cash_amount = cash_result[0]
            print(f"Cash disponible : {cash_amount:,.2f}")

            # Trouver les 5 meilleurs tickers
            cursor.execute("""
                WITH StartPrices AS (
                    SELECT TICKER, PRICE
                    FROM Products
                    WHERE IMPORT_DATE = (
                        SELECT IMPORT_DATE
                        FROM Products
                        ORDER BY strftime('%d/%m/%Y', IMPORT_DATE) ASC
                        LIMIT 1
                    )
                ),
                EndPrices AS (
                    SELECT TICKER, PRICE
                    FROM Products
                    WHERE IMPORT_DATE = (
                        SELECT IMPORT_DATE
                        FROM Products
                        ORDER BY strftime('%d/%m/%Y', IMPORT_DATE) DESC
                        LIMIT 1
                    )
                )
                SELECT sp.TICKER, ep.PRICE, 
                       ((ep.PRICE - sp.PRICE) / sp.PRICE) * 100 as RETURN
                FROM StartPrices sp
                JOIN EndPrices ep ON sp.TICKER = ep.TICKER
                ORDER BY RETURN DESC
                LIMIT 5
            """)
            top_tickers = cursor.fetchall()
            
            if not top_tickers:
                print("Aucun ticker trouvé pour l'initialisation")
                return

            print("\nTickers sélectionnés pour l'initialisation :")
            for ticker, price, return_value in top_tickers:
                print(f"{ticker}: {price:.2f} (Rendement: {return_value:.2f}%)")

            # Calculer le montant maximum par ticker (cash/5)
            max_amount_per_ticker = cash_amount / 5
            
            # Insérer les positions
            total_invested = 0
            for ticker, price, _ in top_tickers:
                # Calculer la quantité maximale possible pour ce ticker
                quantity = int(max_amount_per_ticker / price)
                if quantity > 0:  # Ne pas insérer si la quantité est 0
                    cursor.execute("""
                        INSERT INTO Portfolios (RISK_TYPE, TICKER, QUANTITY, MANAGER_ID, LAST_UPDATED, SPOT_PRICE)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, ("HY_EQUITY", ticker, quantity, 1, 
                          (self.end_date - timedelta(days=1)).strftime("%d/%m/%Y"), price))
                    print(f"→ Achat de {quantity} unités de {ticker} @ {price:.2f}")
                    total_invested += quantity * price

            # Mettre à jour le cash restant
            remaining_cash = cash_amount - total_invested
            cursor.execute("""
                UPDATE Portfolios 
                SET QUANTITY = ?
                WHERE RISK_TYPE = ? AND TICKER = 'CASH'
            """, (remaining_cash, risk_type))
            
            print(f"\nCash restant : {remaining_cash:,.2f}")
            conn.commit()

        except Exception as e:
            print(f"Erreur lors de l'initialisation du portefeuille : {e}")
            conn.rollback()
        finally:
            conn.close()


    #abdel
    #abdel

    def initialisation_portefeuille_LR(self, tickers):
        """
        Initialise un portefeuille LOW_RISK avec les tickers macro,
        en assignant quantité = 0, tout en stockant le dernier spot connu.

        Args:
            db_file (str): chemin vers la base de données
            tickers (list): liste de tickers macro (ETF, obligations, etc.)
            risk_type (str): ici on utilisera toujours 'LOW_RISK'
        """
        risk_type = "LOW_RISK"
        conn = sqlite3.connect(self.db_file)
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
