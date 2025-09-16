.PHONY: api bot all

api:
	uvicorn main:app --reload

bot:
	python telegram_bot.py

all:
	uvicorn main:app --reload & python telegram_bot.py
