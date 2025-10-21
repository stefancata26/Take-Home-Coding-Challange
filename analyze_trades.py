import sqlite3
import polars as pl
from pathlib import Path

# Configs
DB_FILE = Path("trades.db")
CSV_FILE = Path("trades.csv")

# Data Ingestion
# Read CSV using Polars and insert into SQLite database.
def load_into_sqlite(csv_file: Path, db_file: Path):
    print("Loading data started...")

    # read CSV with Polars
    df = pl.read_csv(csv_file, try_parse_dates=True)

    # Ensure correct types
    df = df.with_columns([
        pl.col("Timestamp").cast(pl.Datetime),
        pl.col("Quantity").cast(pl.Int64),
        pl.col("Price").cast(pl.Float64),
    ])

    # Connection with SQLite
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Create the table trades if it doesn’t exist.
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            Timestamp TEXT,
            Symbol TEXT,
            Side TEXT,
            Quantity INTEGER,
            Price REAL
        )
    """)
    conn.commit() # save changes

    # Insert data into the trades table
    df.to_pandas().to_sql("trades", conn, if_exists="replace", index=False)

    print(f"Loaded {len(df)} rows into trades table")
    conn.close()
    return df

# Financial Analysis
def analyze_trades(df: pl.DataFrame):
    print("\nRunning analysis...")

    # Total traded volume and trade value per stock (symbol)
    volume_value = (
        df.group_by("Symbol")
          .agg([
              pl.sum("Quantity").alias("TotalVolume"),
              (pl.col("Quantity") * pl.col("Price")).sum().alias("TotalTradeValue"),
          ])
          .sort("TotalTradeValue", descending=True)
    )

    # Net position per stock ((total shares bought minus sold) 
    net_position = (
        df.with_columns([
            pl.when(pl.col("Side") == "BUY").then(pl.col("Quantity"))
              .otherwise(-pl.col("Quantity"))
              .alias("NetQty")
        ])
        .group_by("Symbol")
        .agg(pl.sum("NetQty").alias("NetPosition"))
        .sort("NetPosition", descending=True)
    )

    # Day(s) with highest trading volume overall and per symbol
    df = df.with_columns([
        pl.col("Timestamp").dt.date().alias("TradeDate")
    ])

    total_volume_per_day = (
        df.group_by("TradeDate")
          .agg(pl.sum("Quantity").alias("TotalVolume"))
          .sort("TotalVolume", descending=True)
    )
    top_day = total_volume_per_day.head(1)

    volume_per_day_symbol = (
        df.group_by(["TradeDate", "Symbol"])
          .agg(pl.sum("Quantity").alias("TotalVolume"))
          .sort("TotalVolume", descending=True)
    )

    return {
        "volume_value": volume_value,
        "net_position": net_position,
        "top_day": top_day,
        "volume_per_day_symbol": volume_per_day_symbol,
    }

# main
def main():
    if not CSV_FILE.exists():
        print("Trades.csv not found.")
        return

    # Load data into SQLite
    df = load_into_sqlite(CSV_FILE, DB_FILE)
    results = analyze_trades(df)

    print("\n Results summary:")

    # Results 
    print("\nTotal volume & value per symbol")
    print(results["volume_value"])

    print("\nNet position per symbol")
    print(results["net_position"])

    print("\nDay with highest total volume")
    print(results["top_day"])

    print("\n Highest volume per symbol / day")
    print(results["volume_per_day_symbol"].head(10)) # only top 10

if __name__ == "__main__":
    main()