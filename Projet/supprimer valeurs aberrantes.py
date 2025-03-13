import numpy as np
import pandas as pd

class OutlierRemover:
    def __init__(self, method="zscore", threshold=3):
        """
        Initialise l'outil de suppression des valeurs aberrantes.
        
        :param method: str, méthode de détection ('zscore', 'iqr', 'percentile')
        :param threshold: float, seuil pour identifier une valeur aberrante (ex: 3 pour z-score, 1.5 pour IQR)
        """
        self.method = method
        self.threshold = threshold
    
    def remove_outliers(self, df):
        """
        Supprime les valeurs aberrantes d'un DataFrame en fonction de la méthode choisie.
        
        :param df: pd.DataFrame, le dataset à traiter
        :return: pd.DataFrame, dataset nettoyé
        """
        df_cleaned = df.copy()
        
        for col in df.select_dtypes(include=[np.number]).columns:
            if self.method == "zscore":
                df_cleaned = self._remove_by_zscore(df_cleaned, col)
            elif self.method == "iqr":
                df_cleaned = self._remove_by_iqr(df_cleaned, col)
            elif self.method == "percentile":
                df_cleaned = self._remove_by_percentile(df_cleaned, col)
            else:
                raise ValueError("Méthode inconnue. Choisissez parmi 'zscore', 'iqr' ou 'percentile'.")
        
        return df_cleaned
    
    def _remove_by_zscore(self, df, col):
        """ Supprime les valeurs aberrantes en utilisant le z-score. """
        z_scores = np.abs((df[col] - df[col].mean()) / df[col].std())
        return df[z_scores < self.threshold]
    
    def _remove_by_iqr(self, df, col):
        """ Supprime les valeurs aberrantes en utilisant l'IQR (Interquartile Range). """
        Q1 = df[col].quantile(0.25)
        Q3 = df[col].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - self.threshold * IQR
        upper_bound = Q3 + self.threshold * IQR
        return df[(df[col] >= lower_bound) & (df[col] <= upper_bound)]
    
    def _remove_by_percentile(self, df, col):
        """ Supprime les valeurs aberrantes en utilisant les percentiles (1% et 99%). """
        lower_bound = df[col].quantile(0.01)
        upper_bound = df[col].quantile(0.99)
        return df[(df[col] >= lower_bound) & (df[col] <= upper_bound)]
    