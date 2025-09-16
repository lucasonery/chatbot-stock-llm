from app.core.db import get_conn
import pandas as pd



def insert_data(ticker: str, df: pd.DataFrame) -> None:
    """Insere dados de um DataFrame no banco de forma otimizada (usando itertuples)."""
    rows = [
        (
            ticker.upper(),
            row.date,
            row.open,
            row.high,
            row.low,
            row.close,
            row.adj_close,
            row.volume,
            row.avg_price,
            row.variation_pct,
        )
        for row in df.itertuples(index=False)
    ]

    conn = get_conn()
    cursor = conn.cursor()
    cursor.executemany(
        """
        INSERT OR IGNORE INTO ativos
        (ticker, date, open, high, low, close, adj_close, volume, avg_price, variation_pct)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
        rows,
    )
    conn.commit()
    conn.close()



def get_watchlist():
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT ticker FROM ativos ORDER BY ticker")
    tickers = [row[0] for row in cursor.fetchall()]
    conn.close()
    return tickers


def get_ativos_filtered(
    ticker,
    limit=5,
    start_date=None,
    end_date=None,
    min_price=None,
    max_price=None,
    min_volume=None,
    max_volume=None,
):
    conn = get_conn()
    cursor = conn.cursor()

    query = """
        SELECT date, open, high, low, close, adj_close, volume, avg_price, variation_pct
        FROM ativos
        WHERE ticker = ?
    """
    params = [ticker.upper()]

    if start_date:
        query += " AND date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND date <= ?"
        params.append(end_date)
    if min_price:
        query += " AND close >= ?"
        params.append(min_price)
    if max_price:
        query += " AND close <= ?"
        params.append(max_price)
    if min_volume:
        query += " AND volume >= ?"
        params.append(min_volume)
    if max_volume:
        query += " AND volume <= ?"
        params.append(max_volume)

    query += " ORDER BY date DESC LIMIT ?"
    params.append(limit)

    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()
    return rows


def get_highest_volume(ticker, start_date=None, end_date=None):
    conn = get_conn()
    cursor = conn.cursor()

    query = """
        SELECT date, volume FROM ativos
        WHERE ticker = ?
    """
    params = [ticker.upper()]

    if start_date:
        query += " AND date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND date <= ?"
        params.append(end_date)

    query += " ORDER BY volume DESC LIMIT 1"

    cursor.execute(query, params)
    row = cursor.fetchone()
    conn.close()
    return row



def get_lowest_closing(ticker, start_date=None, end_date=None):
    conn = get_conn()
    cursor = conn.cursor()

    query = """
        SELECT date, close FROM ativos
        WHERE ticker = ?
    """
    params = [ticker.upper()]

    if start_date:
        query += " AND date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND date <= ?"
        params.append(end_date)

    query += " ORDER BY close ASC LIMIT 1"

    cursor.execute(query, params)
    row = cursor.fetchone()
    conn.close()
    return row



def get_consolidated_metrics(
    ticker,
    start_date=None,
    end_date=None,
    min_price=None,
    max_price=None,
    min_volume=None,
    max_volume=None,
):
    conn = get_conn()
    cursor = conn.cursor()

    query = """
        SELECT 
            AVG(avg_price),     -- preço médio geral
            MAX(close),         -- maior fechamento
            MIN(close),         -- menor fechamento
            AVG(volume)         -- volume médio
        FROM ativos
        WHERE ticker = ?
    """
    params = [ticker.upper()]

    if start_date:
        query += " AND date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND date <= ?"
        params.append(end_date)
    if min_price:
        query += " AND close >= ?"
        params.append(min_price)
    if max_price:
        query += " AND close <= ?"
        params.append(max_price)
    if min_volume:
        query += " AND volume >= ?"
        params.append(min_volume)
    if max_volume:
        query += " AND volume <= ?"
        params.append(max_volume)

    cursor.execute(query, params)
    row = cursor.fetchone()
    conn.close()
    return row


def get_consolidated_table(ticker=None, start_date=None, end_date=None):
    conn = get_conn()
    cursor = conn.cursor()

    query = """
        SELECT ticker, date, avg_price, variation_pct
        FROM ativos
        WHERE 1=1
    """
    params = []

    if ticker:
        query += " AND ticker = ?"
        params.append(ticker.upper())
    if start_date:
        query += " AND date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND date <= ?"
        params.append(end_date)

    query += " ORDER BY ticker, date ASC"

    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()
    return rows
