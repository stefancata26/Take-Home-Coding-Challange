from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import List
import sqlite3
import pandas as pd

# Config and setup
DB_PATH = "trades.db"
app = FastAPI(title="LinkFinance Trading API")

# schemas
class SymbolSummary(BaseModel):
    symbol: str
    total_volume: int
    total_value: float
    net_position: int

class DailyTrend(BaseModel):
    date: str
    total_volume: int
    avg_price: float

# Helper functions
# Get database connection
def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

# Load trades from the database
def load_trades() -> pd.DataFrame:
    conn = get_db_connection()
    df = pd.read_sql("SELECT * FROM trades", conn, parse_dates=["Timestamp"])
    conn.close()
    return df

# API Endpoints
@app.get("/symbols/summary", response_model=List[SymbolSummary])
def api_summary():
    df = load_trades()

    # Check if data exists
    if df.empty:
        raise HTTPException(status_code=404, detail="No data found")

    # Group by symbol and calculate summary
    summary = df.groupby("Symbol").agg(
        total_volume=pd.NamedAgg(column="Quantity", aggfunc="sum"),
        total_value=pd.NamedAgg(column="Price", aggfunc=lambda x: (x * df.loc[x.index, "Quantity"]).sum()),
        net_position=pd.NamedAgg(column="Quantity", aggfunc=lambda x: (x.where(df.loc[x.index, "Side"]=="BUY", -x)).sum())
    ).reset_index()

    # Match python types in response model
    result = []
    for _, row in summary.iterrows():
        result.append({
            "symbol": row["Symbol"],
            "total_volume": int(row["total_volume"]),
            "total_value": float(row["total_value"]),
            "net_position": int(row["net_position"])
        })
    return result

app.mount("/", StaticFiles(directory="static", html=True), name="static")