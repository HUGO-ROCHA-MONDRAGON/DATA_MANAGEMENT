import numpy as np
import pandas as pd

class PortfolioPerformance:
    def __init__(self, returns, risk_free_rate=0.02):
        """
        Classe pour calculer les indicateurs de performance d'un portefeuille.
        
        :param returns: pd.Series ou pd.DataFrame, rendements du portefeuille (fréquence hebdomadaire).
        :param risk_free_rate: float, taux sans risque annuel (ex: 2%).
        """
        self.returns = returns.dropna()
        self.risk_free_rate = risk_free_rate / 52  # Ajusté pour une base hebdomadaire
    
    def sharpe_ratio(self):
        """ Calcule le ratio de Sharpe. """
        excess_returns = self.returns - self.risk_free_rate
        return np.sqrt(52) * excess_returns.mean() / excess_returns.std()
    
    def sortino_ratio(self):
        """ Calcule le ratio de Sortino (ne pénalise que la volatilité négative). """
        negative_returns = self.returns[self.returns < 0]
        downside_std = negative_returns.std()
        return np.sqrt(52) * (self.returns.mean() - self.risk_free_rate) / downside_std
    
    def max_drawdown(self):
        """ Calcule le Maximum Drawdown (perte maximale subie). """
        cumulative_returns = (1 + self.returns).cumprod()
        peak = cumulative_returns.cummax()
        drawdown = (cumulative_returns - peak) / peak
        return drawdown.min()
    
    def calmar_ratio(self):
        """ Calcule le ratio de Calmar (rendement annuel / max drawdown). """
        annual_return = (1 + self.returns.mean())**52 - 1
        return annual_return / abs(self.max_drawdown())
    
    def beta(self, market_returns):
        """ Calcule le Beta du portefeuille par rapport au marché. """
        covariance = np.cov(self.returns, market_returns)[0, 1]
        market_variance = np.var(market_returns)
        return covariance / market_variance
    
    def alpha(self, market_returns):
        """ Calcule l'Alpha de Jensen. """
        market_sharpe = self.beta(market_returns) * (market_returns.mean() - self.risk_free_rate)
        return (self.returns.mean() - self.risk_free_rate) - market_sharpe
    
    def worst_performance(self):
        """ Retourne la pire performance hebdomadaire. """
        return self.returns.min()
    
    def best_performance(self):
        """ Retourne la meilleure performance hebdomadaire. """
        return self.returns.max()
    
    def volatility(self):
        """ Calcule la volatilité hebdomadaire. """
        return self.returns.std()
    
    def total_return(self):
        """ Calcule le rendement total du portefeuille. """
        return (1 + self.returns).prod() - 1
    
    def annualized_return(self):
        """ Calcule le rendement annualisé. """
        return (1 + self.total_return())**(52 / len(self.returns)) - 1
    
    def annualized_volatility(self):
        """ Calcule la volatilité annualisée. """
        return self.volatility() * np.sqrt(52)
    
    def summary(self, market_returns=None):
        """ Retourne un résumé des indicateurs de performance. """
        performance = {
            "Sharpe Ratio": self.sharpe_ratio(),
            "Sortino Ratio": self.sortino_ratio(),
            "Max Drawdown": self.max_drawdown(),
            "Calmar Ratio": self.calmar_ratio(),
            "Worst Weekly Performance": self.worst_performance(),
            "Best Weekly Performance": self.best_performance(),
            "Volatility": self.volatility(),
            "Total Return": self.total_return(),
            "Annualized Return": self.annualized_return(),
            "Annualized Volatility": self.annualized_volatility()
        }
        if market_returns is not None:
            performance["Beta"] = self.beta(market_returns)
            performance["Alpha"] = self.alpha(market_returns)
        return pd.DataFrame.from_dict(performance, orient='index', columns=['Value'])
    