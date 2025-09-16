import os
from app.services.services import normalize_csv
from app.repository.repository import insert_data

CSV_FOLDER = "./stocks"
BATCH_SIZE = 500

def main():
    files = [f for f in os.listdir(CSV_FOLDER) if f.endswith(".csv")]
    total = len(files)
    print(f"Encontrados {total} arquivos CSV para importar.")

    for i in range(0, total, BATCH_SIZE):
        batch = files[i:i + BATCH_SIZE]
        print(f"\n🔹 Processando lote {i // BATCH_SIZE + 1} ({len(batch)} arquivos)...")

        for filename in batch:
            ticker = filename.replace(".csv", "").upper()
            file_path = os.path.join(CSV_FOLDER, filename)
            print(f"  - {ticker}")

            try:
                with open(file_path, "r") as f:
                    df = normalize_csv(f)
                    insert_data(ticker, df)
            except Exception as e:
                print(f"Erro ao processar {filename}: {e}")

        print(f"Lote {i // BATCH_SIZE + 1} gravado no banco.")

    print("\n Importação concluída!")

if __name__ == "__main__":
    main()
