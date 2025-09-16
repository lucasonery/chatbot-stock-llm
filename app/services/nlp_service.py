import os
import json
import calendar
from datetime import datetime
from groq import Groq
from dotenv import load_dotenv

load_dotenv()
GROQ_API_KEY = os.getenv("GROQ_API_KEY")

client = Groq(api_key=GROQ_API_KEY)


def adjust_month_dates(start_date: str, end_date: str):
    """
    Se a pergunta mencionar um mês específico, corrige end_date
    para o último dia do mês.
    """
    try:
        dt = datetime.strptime(start_date, "%Y-%m-%d")
        last_day = calendar.monthrange(dt.year, dt.month)[1]
        return start_date, f"{dt.year}-{dt.month:02d}-{last_day}"
    except Exception:
        return start_date, end_date


def parse_with_groq(user_text: str) -> dict:
    """
    Usa o modelo Groq para interpretar uma pergunta em linguagem natural
    e extrair intent, ticker, start_date, end_date.
    - Se a pergunta mencionar um ano, retorna AAAA-01-01 até AAAA-12-31
    - Se mencionar mês + ano, ajusta end_date para último dia do mês
    """

    prompt = f"""
    Você é um parser de linguagem natural para consultas de ações.

    Responda SOMENTE em JSON válido, sem explicações extras.
    O JSON deve conter exatamente os campos: intent, ticker, start_date, end_date.
    
    Valores permitidos para "intent":
    - "highest_volume"
    - "lowest_closing_price"
    - "consolidated_metrics"

    Se algum campo não for informado, use null.

    Regras especiais:
    - Se a pergunta mencionar apenas um ano (ex: "em 2019"), defina:
        start_date = "AAAA-01-01"
        end_date   = "AAAA-12-31"
    - Se a pergunta mencionar um mês e ano (ex: "em março de 2020"), defina:
        start_date = "AAAA-MM-01"
        end_date   = "AAAA-MM-último_dia" (calcule corretamente o último dia do mês)

    Exemplo válido:
    {{
      "intent": "highest_volume",
      "ticker": "AAPL",
      "start_date": "2019-01-01",
      "end_date": "2019-12-31"
    }}

    Pergunta: "{user_text}"
    """

    try:
        response = client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[{"role": "user", "content": prompt}],
            temperature=0
        )

        content = response.choices[0].message.content.strip()

        # tenta converter em JSON
        parsed = json.loads(content)

        start_date = parsed.get("start_date")
        end_date = parsed.get("end_date")

        # ajusta mês se necessário
        if start_date and end_date:
            start_date, end_date = adjust_month_dates(start_date, end_date)

        return {
            "intent": parsed.get("intent", "unknown"),
            "ticker": parsed.get("ticker"),
            "start_date": start_date,
            "end_date": end_date,
            "raw": content  # para debug
        }

    except Exception as e:
        # fallback seguro
        return {"intent": "unknown", "error": str(e), "raw": user_text}
