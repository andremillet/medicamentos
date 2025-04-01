import pandas as pd

CSV_FILE = "medicamentos.csv"  # Ajuste o caminho se necessário

# Carregar o CSV
df = pd.read_csv(CSV_FILE, delimiter=";", encoding="latin1")

# Normalizar SITUACAO_REGISTRO para ignorar case e acentos
df["SITUACAO_NORMALIZED"] = df["SITUACAO_REGISTRO"].str.lower().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('ascii')
valid_df = df[df["SITUACAO_NORMALIZED"].isin(["valido", "ativo"])]

# Contar registros únicos por NUMERO_REGISTRO_PRODUTO
unique_count = valid_df['NUMERO_REGISTRO_PRODUTO'].nunique()
total_count = len(valid_df)

print(f"Total de registros válidos (antes de remover duplicatas): {total_count}")
print(f"Registros únicos por NUMERO_REGISTRO_PRODUTO: {unique_count}")

# Verificar duplicatas
duplicates = valid_df[valid_df.duplicated(subset=['NUMERO_REGISTRO_PRODUTO'], keep=False)]
print(f"Duplicatas encontradas: {len(duplicates)}")
if len(duplicates) > 0:
    print("Exemplos de duplicatas:")
    print(duplicates[['NUMERO_REGISTRO_PRODUTO', 'NOME_PRODUTO', 'SITUACAO_REGISTRO']].head().to_string())
