from data_collector import get_data_yf
from strategies import LowRiskFundStrategy
import pandas as pd


crypto_tickers = ["BTC-USD", "ETH-USD", "LTC-USD", "XRP-USD"]
global_tickers = ["BTC-USD", "ETH-USD", "LTC-USD", "XRP-USD", "TLT", "IEF", "SPLV"]
low_vol_tickers = ["TLT", "IEF", "SPLV"]
    

crypto_start = "2023-01-01"
crypto_end   = "2023-03-31"
lowvol_start = "2023-04-01"
lowvol_end   = "2023-12-31"
    

strategy = LowRiskFundStrategy(crypto_tickers, global_tickers, low_vol_tickers, 
                                   crypto_start, crypto_end, lowvol_start, lowvol_end,
                                   rebalancing_freq='W-FRI', crypto_shift=0.1, target_vol=0.10)

crypto_final, lowvol_final, final_port_vol = strategy.run_strategy()