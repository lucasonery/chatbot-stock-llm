import os
import sqlite3

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_DIR = os.path.join(os.path.dirname(BASE_DIR), "..", "data")
DB_PATH = os.path.join(DB_DIR, "aboda.db")

os.makedirs(DB_DIR, exist_ok=True)

def get_conn():
    """Retorna conexão com o banco SQLite."""
    return sqlite3.connect(DB_PATH)

def init_db():
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS ativos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ticker TEXT NOT NULL,
        date TEXT NOT NULL,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        adj_close REAL,
        volume REAL,
        avg_price REAL,
        variation_pct REAL,
        UNIQUE(ticker, date)
    )
    """)
    conn.commit()
    conn.close()

# Inicializa o banco ao importar
init_db()
