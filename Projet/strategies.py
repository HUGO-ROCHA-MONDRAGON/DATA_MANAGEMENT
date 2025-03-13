import pandas as pd
import numpy as np
from datetime import datetime
import sqlite3
from scipy.optimize import minimize
from data_collector import get_data_sql, calculate_daily_returns, annualized_volatility, sharpe_ratio

class LowRiskFundStrategy:
    """
    Implements a multi-phase strategy:
      Phase 1 (Crypto Phase): Start fully in crypto and, each week, shift a portion of the allocation 
                              from the worst (lowest Sharpe ratio) crypto to the least volatile asset 
                              from the global database.
      Phase 2 (Low-Vol Phase): Exit crypto and construct a portfolio of low-volatility assets targeting
                               an annualized volatility of ~10%.
    """
    
    def __init__(self, crypto_tickers, global_tickers, low_vol_tickers, 
                 crypto_start, crypto_end, 
                 lowvol_start, lowvol_end, 
                 rebalancing_freq='W-FRI', crypto_shift=0.1, 
                 target_vol=0.10):
        """
        :param crypto_tickers: List of crypto tickers (initial positions).
        :param global_tickers: List of all tickers in the database used to find the least volatile asset.
        :param low_vol_tickers: List of low-volatility asset tickers for Phase 2.
        :param crypto_start: Start date for the crypto phase (e.g. "2023-01-01").
        :param crypto_end: End date for the crypto phase (e.g. "2023-03-31").
        :param lowvol_start: Start date for the low-volatility phase (e.g. "2023-04-01").
        :param lowvol_end: End date for the low-volatility phase (e.g. "2023-12-31").
        :param rebalancing_freq: Frequency for rebalancing (default weekly on Fridays).
        :param crypto_shift: Fraction of allocation to shift each week.
        :param target_vol: Target annualized volatility for Phase 2 portfolio.
        """
        self.crypto_tickers = crypto_tickers
        self.global_tickers = global_tickers
        self.low_vol_tickers = low_vol_tickers
        self.crypto_start = crypto_start
        self.crypto_end = crypto_end
        self.lowvol_start = lowvol_start
        self.lowvol_end = lowvol_end
        self.rebalancing_freq = rebalancing_freq
        self.crypto_shift = crypto_shift
        self.target_vol = target_vol
        # Start with 100% allocation equally among crypto tickers.
        self.portfolio = {ticker: 1/len(crypto_tickers) for ticker in crypto_tickers}

    
    def run_crypto_phase(self):
        """
        Executes the crypto phase with weekly rebalancing:
          - For each week, compute metrics for your crypto positions.
          - Identify the worst crypto (lowest Sharpe ratio).
          - Over the global set of assets, find the asset with the lowest annualized volatility during the week.
          - Shift a fraction (crypto_shift) of allocation from the worst crypto to the least volatile asset.
        """
        rebalancing_dates = pd.date_range(start=self.crypto_start, end=self.crypto_end, freq=self.rebalancing_freq)
        print("Rebalancing dates (Crypto Phase):", rebalancing_dates)
        
        for i in range(1, len(rebalancing_dates)):
            week_start = rebalancing_dates[i-1].strftime("%Y-%m-%d")
            week_end   = rebalancing_dates[i].strftime("%Y-%m-%d")
            
            # Evaluate crypto positions (only those with allocation > 0)
            crypto_metrics = {}
            for ticker in self.crypto_tickers:
                if ticker not in self.portfolio or self.portfolio[ticker] <= 0:
                    continue
                prices = get_data_sql(ticker, week_start, week_end)
                if prices.empty:
                    continue
                returns = calculate_daily_returns(prices)
                if returns.empty:
                    continue
                vol = annualized_volatility(returns)
                sr = sharpe_ratio(returns)
                crypto_metrics[ticker] = {"vol": vol, "sr": sr}
            
            if not crypto_metrics:
                continue
            
            # Worst-performing crypto: lowest Sharpe ratio.
            worst_crypto = min(crypto_metrics, key=lambda t: crypto_metrics[t]["sr"])
            
            # Find the least volatile asset in the global database.
            global_metrics = {}
            for ticker in self.global_tickers:
                prices = get_data_sql(ticker, week_start, week_end)
                if prices.empty:
                    continue
                returns = calculate_daily_returns(prices)
                if returns.empty:
                    continue
                vol = annualized_volatility(returns)
                global_metrics[ticker] = vol
            
            if not global_metrics:
                continue
            
            best_asset = min(global_metrics, key=global_metrics.get)
            
            print(f"Week ending {week_end}:")
            print(f"  Worst crypto: {worst_crypto} (Sharpe: {crypto_metrics[worst_crypto]['sr']:.4f})")
            print(f"  Least volatile asset (global): {best_asset} (Vol: {global_metrics[best_asset]:.4f})")
            
            # Shift a fraction of the allocation from worst_crypto to best_asset.
            shift_amount = self.crypto_shift * self.portfolio.get(worst_crypto, 0)
            self.portfolio[worst_crypto] -= shift_amount
            if best_asset in self.portfolio:
                self.portfolio[best_asset] += shift_amount
            else:
                self.portfolio[best_asset] = shift_amount
            
            # Normalize portfolio (ensure sum to 1 and no negative weights)
            total_weight = sum(self.portfolio.values())
            self.portfolio = {t: max(w, 0) / total_weight for t, w in self.portfolio.items()}
            
            print("  Updated portfolio weights:", self.portfolio)
        
        print("\nEnd of Crypto Phase. Final portfolio:", self.portfolio)
        return self.portfolio
    
    def run_lowvol_phase(self):
        """
        Constructs a low-volatility portfolio for Phase 2.
        Downloads data for low-volatility assets, calculates returns and the covariance matrix,
        and optimizes weights to target an annualized volatility near self.target_vol.
        """
        returns_data = {}
        for ticker in self.low_vol_tickers:
            prices = get_data_sql(ticker, self.lowvol_start, self.lowvol_end)
            if prices.empty:
                continue
            returns_data[ticker] = calculate_daily_returns(prices)
        
        returns_df = pd.DataFrame(returns_data).dropna()
        cov_matrix = returns_df.cov() * 252
        
        def portfolio_volatility(weights):
            return np.sqrt(np.dot(weights.T, np.dot(cov_matrix, weights)))
        
        def objective(weights):
            return (portfolio_volatility(weights) - self.target_vol) ** 2
        
        num_assets = len(self.low_vol_tickers)
        initial_weights = np.array([1/num_assets] * num_assets)
        constraints = {'type': 'eq', 'fun': lambda w: np.sum(w) - 1}
        bounds = tuple((0, 1) for _ in range(num_assets))
        
        result = minimize(objective, initial_weights, method='SLSQP', bounds=bounds, constraints=constraints)
        
        if result.success:
            optimized_weights = result.x
            final_vol = portfolio_volatility(optimized_weights)
            low_vol_portfolio = {ticker: weight for ticker, weight in zip(self.low_vol_tickers, optimized_weights)}
            print("\nOptimized Low-Volatility Portfolio (Phase 2):")
            for ticker, weight in low_vol_portfolio.items():
                print(f"  {ticker}: {weight:.4f}")
            print(f"Portfolio annualized volatility: {final_vol:.4f}")
            return low_vol_portfolio, final_vol
        else:
            print("Low volatility portfolio optimization failed.")
            return None, None
    
    def run_strategy(self):
        """
        Executes the complete strategy:
          1. Runs the crypto phase with weekly rebalancing.
          2. Exits crypto positions and transitions to the low-volatility portfolio phase.
        Returns the final crypto phase portfolio, the low-vol phase portfolio, and the final portfolio volatility.
        """
        print("Starting Crypto Phase...")
        crypto_portfolio = self.run_crypto_phase()
        print("\nExiting crypto positions. Transitioning to Low-Volatility Portfolio Phase...")
        lowvol_portfolio, final_vol = self.run_lowvol_phase()
        return crypto_portfolio, lowvol_portfolio, final_vol

# Example usage:
if __name__ == "__main__":
    # Define your ticker lists.
    crypto_tickers = ["BTC-USD", "ETH-USD", "LTC-USD", "XRP-USD"]
    # Global tickers include all assets from your database (crypto + others).
    global_tickers = ["BTC-USD", "ETH-USD", "LTC-USD", "XRP-USD", "TLT", "IEF", "SPLV"]
    low_vol_tickers = ["TLT", "IEF", "SPLV"]
    
    # Define date ranges for each phase.
    crypto_start = "2023-01-01"
    crypto_end   = "2023-03-31"
    lowvol_start = "2023-04-01"
    lowvol_end   = "2023-12-31"
    
    # Initialize and run the strategy.
    strategy = LowRiskFundStrategy(crypto_tickers, global_tickers, low_vol_tickers, 
                                   crypto_start, crypto_end, lowvol_start, lowvol_end,
                                   rebalancing_freq='W-FRI', crypto_shift=0.1, target_vol=0.10)
    
    crypto_final, lowvol_final, final_port_vol = strategy.run_strategy()
