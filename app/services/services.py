import pandas as pd
from typing import Optional, Dict, Any
from app.repository.repository import (
    insert_data,
    get_watchlist,
    get_ativos_filtered,
    get_highest_volume,
    get_lowest_closing,
    get_consolidated_metrics,
    get_consolidated_table,
)


REQUIRED_COLUMNS = ["Date", "Open", "High", "Low", "Close", "Adj Close", "Volume"]


def normalize_csv(file) -> pd.DataFrame:
    """Lê CSV, valida colunas obrigatórias, trata erros e cria métricas calculadas."""
    try:
        df = pd.read_csv(file)
    except Exception as e:
        raise ValueError(f"Erro ao ler o CSV: {e}")

    if df.empty:
        raise ValueError("O arquivo CSV está vazio.")

    # Colunas obrigatórias
    missing_cols = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Colunas obrigatórias ausentes no CSV: {', '.join(missing_cols)}")

    # Padronizar colunas
    df.rename(columns={
        "Date": "date",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Adj Close": "adj_close",
        "Volume": "volume",
    }, inplace=True)

    # Conversão segura
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    for col in ["open", "high", "low", "close", "adj_close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Remove linhas inválidas
    df.dropna(subset=["date", "open", "close"], inplace=True)
    if df.empty:
        raise ValueError("Nenhuma linha válida encontrada após validação.")

    # Colunas calculadas
    df["avg_price"] = (df["open"] + df["close"]) / 2
    df["variation_pct"] = ((df["close"] - df["open"]) / df["open"]) * 100
    # evitar divisão por zero
    df.loc[df["open"] == 0, "variation_pct"] = None
    return df


# Services 

def service_insert_csv(ticker: str, file) -> None:
    df = normalize_csv(file)
    insert_data(ticker, df)


def service_watchlist() -> Dict[str, list]:
    return {"tickers": get_watchlist()}


def service_ativos(
    ticker: str,
    limit: int = 5,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    min_price: Optional[float] = None,
    max_price: Optional[float] = None,
    min_volume: Optional[int] = None,
    max_volume: Optional[int] = None,
) -> Dict[str, Any]:
    rows = get_ativos_filtered(
        ticker, limit, start_date, end_date, min_price, max_price, min_volume, max_volume
    )
    return {
        "ticker": ticker.upper(),
        "data": [
            {
                "date": r[0],
                "open": r[1],
                "high": r[2],
                "low": r[3],
                "close": r[4],
                "adj_close": r[5],
                "volume": r[6],
                "avg_price": r[7],
                "variation_pct": r[8],
            }
            for r in rows
        ],
    }


def service_highest_volume(ticker: str, start_date=None, end_date=None) -> Dict[str, Any]:
    row = get_highest_volume(ticker, start_date, end_date)
    if row:
        return {"ticker": ticker.upper(), "date": row[0], "highest_volume": row[1]}
    return {"error": f"Nenhum dado encontrado para {ticker.upper()}."}


def service_lowest_closing(ticker: str, start_date=None, end_date=None) -> Dict[str, Any]:
    row = get_lowest_closing(ticker, start_date, end_date)
    if row:
        return {"ticker": ticker.upper(), "date": row[0], "lowest_closing_price": row[1]}
    return {"error": f"Nenhum dado encontrado para {ticker.upper()}."}



# Resumo simples
def service_consolidated_summary(
    ticker: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    min_price: Optional[float] = None,
    max_price: Optional[float] = None,
    min_volume: Optional[int] = None,
    max_volume: Optional[int] = None,
) -> Dict[str, Any]:
    row = get_consolidated_metrics(
        ticker, start_date, end_date, min_price, max_price, min_volume, max_volume
    )
    if row and any(row):
        return {
            "ticker": ticker.upper(),
            "avg_price": row[0],
            "max_close": row[1],
            "min_close": row[2],
            "avg_volume": row[3],
        }
    return {
        "error": f"Nenhum dado encontrado para {ticker.upper()} "
                 f"(filtros: start={start_date}, end={end_date}, "
                 f"min_price={min_price}, max_price={max_price}, "
                 f"min_volume={min_volume}, max_volume={max_volume})"
    }


# Nova métrica consolidada (
def service_consolidated_metrics(
    ticker: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
) -> Dict[str, list]:
    rows = get_consolidated_table(ticker, start_date, end_date)
    result = [
        {
            "ticker": r[0],
            "date": r[1],
            "avg_price": r[2],
            "variation_pct": r[3]
        }
        for r in rows
    ]
    return {"data": result}
