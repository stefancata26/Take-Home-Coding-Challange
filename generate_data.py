import pandas as pd
import numpy as np
from datetime import datetime, timedelta, time
import random

# Configs
STOCK_SYMBOLS = ["AAPL", "GOOG", "MSFT", "AMZN", "TSLA", "NFLX", "NVDA", "META"]
NUM_DAYS = 45
START_DATE = datetime(2024, 7, 1)
TRADES_PER_DAY = 500
OUTPUT_FILE = "trades.csv"

# Helper Functions
# Return a random time between 09:30 and 16:00.
def random_time_in_market_hours():
    market_open = time(9, 30)
    market_close = time(16, 0)

    start_seconds = market_open.hour * 3600 + market_open.minute * 60
    end_seconds = market_close.hour * 3600 + market_close.minute * 60
    rand_seconds = random.randint(start_seconds, end_seconds)
    hours = rand_seconds // 3600
    minutes = (rand_seconds % 3600) // 60
    seconds = rand_seconds % 60
    return time(hours, minutes, seconds)

# Generate trades for a single trading day.
def generate_trades_for_day(current_date, num_trades):
    rows = []
    for _ in range(num_trades):
        symbol = random.choice(STOCK_SYMBOLS) # random stock
        side = random.choice(["BUY", "SELL"]) # random action
        quantity = int(np.random.randint(10, 1000)) # random qauantity
        base_price = {
            "AAPL": 190, "GOOG": 130, "MSFT": 340, "AMZN": 135,
            "TSLA": 250, "NFLX": 450, "NVDA": 420, "META": 300
        }[symbol] # base price for each stock
        # random price around base price 
        price = round(np.random.normal(base_price, base_price * 0.02), 2)

        # anomalies
        if random.random() < 0.01:
            price *= random.uniform(0.5, 1.5)  # price spike/drop
        if random.random() < 0.01:
            quantity *= random.randint(5, 10)  # anomaly large trade

        timestamp = datetime.combine(current_date, random_time_in_market_hours())
        rows.append({
            "Timestamp": timestamp,
            "Symbol": symbol,
            "Side": side,
            "Quantity": quantity,
            "Price": price
        })
    return rows

# Generate the full dataset.
def generate_dataset(start_date, num_days, trades_per_day):
    all_trades = []
    for i in range(num_days):
        # Generate trades for each day
        day = start_date + timedelta(days=i)
        day_trades = generate_trades_for_day(day, trades_per_day)
        all_trades.extend(day_trades)
    return pd.DataFrame(all_trades)

# Main
def main():
    print("Generating trade dataset...")
    df = generate_dataset(START_DATE, NUM_DAYS, TRADES_PER_DAY)
    df.sort_values(by="Timestamp", inplace=True)
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"Dataset saved to {OUTPUT_FILE}")
    print(df.head(10))

if __name__ == "__main__":
    main()