from fastapi import FastAPI, UploadFile, File
from fastapi.responses import StreamingResponse
from typing import List, Optional
from datetime import date
from app.services.services import (
    service_insert_csv,
    service_watchlist,
    service_ativos,
    service_highest_volume,
    service_lowest_closing,
    service_consolidated_summary,  
    service_consolidated_metrics, 
)
import pandas as pd
from io import BytesIO

app = FastAPI(title="Desafio Aboda API")


# Health Check
@app.get("/", summary="Health Check", description="Verifica se a API está funcionando.")
def home():
    return {"message": "API funcionando!"}


# Upload de um único CSV
@app.post("/upload_csv/", summary="Upload de um único CSV")
async def upload_csv(ticker: str, file: UploadFile = File(...)):
    try:
        service_insert_csv(ticker, file.file)
        return {"message": f"CSV do ativo {ticker.upper()} processado e salvo."}
    except ValueError as e:
        return {"error": str(e)}
    except Exception as e:
        return {"error": f"Erro inesperado: {e}"}


# Upload de múltiplos CSVs
@app.post("/upload_csvs_batch/", summary="Upload de múltiplos CSVs")
async def upload_csvs_batch(tickers: List[str], files: List[UploadFile] = File(...)):
    if len(tickers) != len(files):
        return {"error": "Número de tickers deve ser igual ao número de arquivos enviados."}

    results = []
    for ticker, file in zip(tickers, files):
        try:
            service_insert_csv(ticker, file.file)
            results.append({"ticker": ticker.upper(), "status": "ok"})
        except Exception as e:
            results.append({"ticker": ticker.upper(), "status": f"erro: {e}"})

    return {"message": f"{len(files)} CSVs processados.", "detalhes": results}


# Listar tickers já importados
@app.get("/watchlist", summary="Listar tickers importados")
def watchlist():
    return service_watchlist()


# Consultar registros de um ticker
@app.get("/ativos/{ticker}", summary="Consultar registros de um ativo")
def ativos(
    ticker: str,
    limit: int = 5,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    min_price: Optional[float] = None,
    max_price: Optional[float] = None,
    min_volume: Optional[int] = None,
    max_volume: Optional[int] = None,
):
    return service_ativos(ticker, limit, start_date, end_date, min_price, max_price, min_volume, max_volume)


# Maior volume de negociação
@app.get("/highest_volume/{ticker}", summary="Maior volume de negociação")
def highest_volume(ticker: str, start_date: Optional[date] = None, end_date: Optional[date] = None):
    return service_highest_volume(ticker, start_date, end_date)


# Menor preço de fechamento
@app.get("/lowest_closing_price/{ticker}", summary="Menor preço de fechamento")
def lowest_closing_price(ticker: str, start_date: Optional[date] = None, end_date: Optional[date] = None):
    return service_lowest_closing(ticker, start_date, end_date)


# Resumo de métricas (médias/máximos/mínimos)
@app.get("/consolidated_summary/{ticker}", summary="Resumo de métricas agregadas de um ativo")
def consolidated_summary(
    ticker: str,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    min_price: Optional[float] = None,
    max_price: Optional[float] = None,
    min_volume: Optional[int] = None,
    max_volume: Optional[int] = None,
):
    return service_consolidated_summary(
        ticker, start_date, end_date, min_price, max_price, min_volume, max_volume
    )


# Tabela consolidada em Excel 
@app.get("/consolidated_metrics", summary="Tabela consolidada de ativos (Excel)")
def consolidated_metrics(
    ticker: Optional[str] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
):
    data = service_consolidated_metrics(ticker, start_date, end_date)

    if not data.get("data"):
        return {"error": "Nenhum dado encontrado."}

    df = pd.DataFrame(data["data"])
    output = BytesIO()
    df.to_excel(output, index=False)
    output.seek(0)

    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={ticker or 'all'}_consolidated.xlsx"}
    )
