import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

class PortfolioAnalyzer:
    def __init__(self, db_path="Fund.db", freq="D", base_value=100):
        self.db_path = db_path
        self.freq = freq
        self.base_value = base_value
        self.performance_df = None
        self.metrics_df = None

    def plot_portfolio_performance(self):
        conn = sqlite3.connect(self.db_path)
        portfolio_df = pd.read_sql_query("SELECT * FROM Portfolio_History", conn)
        products_df = pd.read_sql_query("SELECT * FROM Products", conn)
        conn.close()

        portfolio_df["DATE_SNAPSHOT"] = pd.to_datetime(portfolio_df["DATE_SNAPSHOT"])
        products_df["IMPORT_DATE"] = pd.to_datetime(products_df["IMPORT_DATE"], dayfirst=True)

        all_dates = products_df["IMPORT_DATE"].sort_values().unique()
        portfolio_df = portfolio_df.sort_values(["RISK_TYPE", "DATE_SNAPSHOT"])

        expanded = (
            portfolio_df
            .groupby(["RISK_TYPE", "TICKER"])
            .apply(lambda group: group.set_index("DATE_SNAPSHOT")
                                    .reindex(pd.date_range(group.DATE_SNAPSHOT.min(), all_dates[-1], freq=self.freq))
                                    .ffill()
                                    .assign(TICKER=group["TICKER"].iloc[0])
                                    .reset_index())
            .reset_index(drop=True)
        )

        expanded.rename(columns={"index": "DATE"}, inplace=True)
        expanded = expanded[expanded["DATE"].isin(products_df["IMPORT_DATE"])]

        merged_df = pd.merge(
            expanded,
            products_df,
            left_on=["TICKER", "DATE"],
            right_on=["TICKER", "IMPORT_DATE"],
            how="inner"
        )

        merged_df["POSITION_VALUE"] = merged_df["QUANTITY"] * merged_df["PRICE"]
        portfolio_value = merged_df.groupby(["DATE", "RISK_TYPE"])["POSITION_VALUE"].sum().reset_index()
        pivot_df = portfolio_value.pivot(index="DATE", columns="RISK_TYPE", values="POSITION_VALUE")
        self.performance_df = pivot_df / pivot_df.iloc[0] * self.base_value

        self.performance_df.plot(figsize=(12, 6))
        plt.title(f"Performance journalière des portefeuilles (base {self.base_value})")
        plt.xlabel("Date")
        plt.ylabel("Valeur du portefeuille")
        plt.grid(True)
        plt.legend(title="RISK_TYPE")
        plt.tight_layout()
        plt.show()

        return self.performance_df

    def compute_portfolio_metrics(self):
        if self.performance_df is None:
            raise ValueError("Vous devez d'abord exécuter plot_portfolio_performance()")

        daily_returns = self.performance_df.pct_change().dropna()
        weekly_returns = self.performance_df.resample('W').last().pct_change().dropna()
        metrics = {}

        for col in self.performance_df.columns:
            series = self.performance_df[col]
            returns = daily_returns[col]
            weekly = weekly_returns[col]

            cumulative_return = series.iloc[-1] / series.iloc[0] - 1
            annual_volatility = returns.std() * (252**0.5)
            sharpe_ratio = returns.mean() / returns.std() * (252**0.5)
            running_max = series.cummax()
            drawdown = (series - running_max) / running_max
            max_drawdown = drawdown.min()
            days = (series.index[-1] - series.index[0]).days
            cagr = (series.iloc[-1] / series.iloc[0])**(365 / days) - 1

            best_weekly_perf = weekly.max() * 100
            worst_weekly_perf = weekly.min() * 100

            metrics[col] = {
                "Performance Totale (%)": cumulative_return * 100,
                "Volatilité Annuelle (%)": annual_volatility * 100,
                "Sharpe Ratio": sharpe_ratio,
                "Max Drawdown (%)": max_drawdown * 100,
                "CAGR (%)": cagr * 100,
                "Meilleure Perf Hebdo (%)": best_weekly_perf,
                "Pire Perf Hebdo (%)": worst_weekly_perf
            }

        self.metrics_df = pd.DataFrame(metrics).T.round(2)
        return self.metrics_df

    def describe_best_portfolios(self):
        """
        Compare les portefeuilles pour chaque métrique et génère une phrase descriptive
        en mentionnant le nom du manager lié au portefeuille le plus performant.
        """
        if self.metrics_df is None:
            raise ValueError("Il faut d'abord exécuter compute_portfolio_metrics().")

        # Connexion à la base
        conn = sqlite3.connect(self.db_path)
        managers_df = pd.read_sql_query("SELECT * FROM Managers", conn)
        conn.close()

        managers_df = managers_df.set_index("RISK_TYPE")

        for metric in self.metrics_df.columns:
            if "Drawdown" in metric or "Pire" in metric:
                best_risk_type = self.metrics_df[metric].idxmin()
            else:
                best_risk_type = self.metrics_df[metric].idxmax()

            value = self.metrics_df.loc[best_risk_type, metric]
            manager = managers_df.loc[best_risk_type]

            first = manager["FIRST_NAME"]
            last = manager["LAST_NAME"]

            # Formulation personnalisée
            if "Sharpe" in metric:
                phrase = f"présente le meilleur Sharpe Ratio ({value}), indiquant le couple rendement/risque le plus efficace."
            elif "Volatilité" in metric:
                phrase = f"a la volatilité annuelle la plus faible ({value}%), illustrant une meilleure stabilité."
            elif "Drawdown" in metric:
                phrase = f"présente le plus faible drawdown ({value}%), signe d'une bonne résistance en période de baisse."
            elif "CAGR" in metric:
                phrase = f"affiche le meilleur taux de croissance annualisé ({value}%)."
            elif "Performance Totale" in metric:
                phrase = f"affiche la performance totale la plus élevée ({value}%)."
            elif "Meilleure Perf Hebdo" in metric:
                phrase = f"a enregistré la meilleure performance hebdomadaire ({value}%)."
            elif "Pire Perf Hebdo" in metric:
                phrase = f"a subi la pire performance hebdomadaire ({value}%)."
            else:
                phrase = f"obtient le meilleur score pour l’indicateur {metric} ({value})."

            print(f"Le portefeuille {best_risk_type} du manager {first} {last} {phrase}")
