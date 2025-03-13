import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sqlite3
from scipy.optimize import minimize
import schedule
import time

class RunAllStrat:
    def __init__(self, db_file, start_date, end_date):
        self.db_file = db_file
        self.start_date = datetime.strptime(start_date, "%d/%m/%Y")
        self.end_date = datetime.strptime(end_date, "%d/%m/%Y")

    def update_strategy(self):
        # Placeholder for the strategy update logic
        print("Updating strategy...")

        # Call other strategies here
        self.strategy_one()
        self.strategy_two()

    def strategy_one(self):
        # Placeholder for the first strategy logic
        print("Running strategy one...")

    def strategy_two(self):
        # Placeholder for the second strategy logic
        print("Running strategy two...")

    def run(self):
        # Schedule the update_strategy method to run every Monday
        schedule.every().monday.do(self.check_and_run_strategy)

        while True:
            schedule.run_pending()
            time.sleep(1)

    def check_and_run_strategy(self):
        current_date = datetime.now()
        if self.start_date <= current_date <= self.end_date:
            self.update_strategy()
        else:
            print("Current date is outside the specified range. Skipping update.")

    def stop_update_strategy(self):
        # Placeholder for stopping the strategy update logic
        print("Stopping strategy update...")

# Example usage
#if __name__ == "__main__":
#    start_date = "31/12/2023"
#    end_date = "31/05/2024"
#    strategy_runner = RunAllStrat("Fund.db", start_date, end_date)
#   strategy_runner.run()