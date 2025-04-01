import pandas as pd
import sqlite3
import re

CMED_FILE = "lista_precos.csv"
ANVISA_FILE = "medicamentos.csv"

def clean_registro(registro):
    if pd.isna(registro):
        return None
    cleaned = re.sub(r'[^0-9]', '', str(registro))
    return cleaned[:10] if len(cleaned) >= 10 else cleaned

def process_apresentacao(apresentacao):
    if pd.isna(apresentacao):
        return None, None
    
    apresentacao = str(apresentacao).strip()
    
    dose_match = re.search(r'(\d+(?:,\d+)?\s*(?:MG|G|ML|%|mL|g|mg)(?:/\w+)?(?:\s*\+\s*\d+(?:,\d+)?\s*(?:MG|G|ML|%|mL|g|mg)(?:/\w+)?)*)', apresentacao, re.IGNORECASE)
    dose = dose_match.group(1) if dose_match else None
    
    forma = apresentacao
    if dose:
        forma = forma.replace(dose, "").strip()
    forma = re.sub(r'(?:CT|CX|FR|BG|VD|PLAS|AMB|TRANS|OPC|PEAD|DESCARTÁVEL)\s*.*$', '', forma, flags=re.IGNORECASE).strip()
    forma = re.sub(r'\s+', ' ', forma).strip()
    
    formas_validas = ['COMPRIMIDO', 'CÁPSULA', 'SOLUÇÃO', 'INJEÇÃO', 'SUSPENSÃO', 'XAROPE', 'CREME', 'POMADA', 'GEL', 'EMU']
    apresentacao_final = None
    for f in formas_validas:
        if f in forma.upper():
            apresentacao_final = forma
            break
    if not apresentacao_final:
        apresentacao_final = forma if forma else None
    
    return dose, apresentacao_final

def update_database():
    # Restaurar o banco a partir do CSV da ANVISA
    conn = sqlite3.connect("medicamentos.db")
    anvisa_df = pd.read_csv(ANVISA_FILE, delimiter=";", encoding="latin1")
    anvisa_df["SITUACAO_NORMALIZED"] = anvisa_df["SITUACAO_REGISTRO"].str.lower().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('ascii')
    valid_df = anvisa_df[anvisa_df["SITUACAO_NORMALIZED"].isin(["valido", "ativo"])]
    valid_df = valid_df[['NOME_PRODUTO', 'PRINCIPIO_ATIVO', 'EMPRESA_DETENTORA_REGISTRO', 'NUMERO_REGISTRO_PRODUTO']]
    valid_df = valid_df.drop_duplicates(subset=['NUMERO_REGISTRO_PRODUTO'], keep='first')
    valid_df['NUMERO_REGISTRO_PRODUTO'] = valid_df['NUMERO_REGISTRO_PRODUTO'].apply(clean_registro)
    print(f"Registros únicos carregados do CSV da ANVISA: {len(valid_df)}")
    
    # Processar dados da CMED
    cmed_df = pd.read_csv(CMED_FILE, low_memory=False)
    cmed_df = cmed_df.rename(columns={
        'REGISTRO': 'NUMERO_REGISTRO_PRODUTO',
        'APRESENTAÇÃO': 'apresentacao_raw'
    })
    print("Colunas disponíveis após renomeação (CMED):", cmed_df.columns.tolist())
    print(f"Processando {len(cmed_df)} registros da CMED")
    
    cmed_df['NUMERO_REGISTRO_PRODUTO'] = cmed_df['NUMERO_REGISTRO_PRODUTO'].apply(clean_registro)
    cmed_df = cmed_df.drop_duplicates(subset=['NUMERO_REGISTRO_PRODUTO'], keep='first')
    print(f"Após remover duplicatas: {len(cmed_df)} registros únicos na CMED")
    
    cmed_df[['dose', 'apresentacao']] = cmed_df['apresentacao_raw'].apply(
        lambda x: pd.Series(process_apresentacao(x))
    )
    cmed_df = cmed_df[['NUMERO_REGISTRO_PRODUTO', 'dose', 'apresentacao']]
    
    # Mesclar os dados
    merged_df = pd.merge(
        valid_df,
        cmed_df,
        on='NUMERO_REGISTRO_PRODUTO',
        how='left'
    )
    
    print("Colunas após merge:", merged_df.columns.tolist())
    print("Registros com correspondência:", merged_df['dose'].notna().sum())
    
    merged_df.to_sql('medicamentos', conn, if_exists='replace', index=False)
    conn.close()
    
    print(f"Banco atualizado com {len(merged_df)} registros.")
    print(f"Registros com dose preenchida: {merged_df['dose'].notna().sum()}")
    print(f"Registros com apresentação preenchida: {merged_df['apresentacao'].notna().sum()}")

if __name__ == "__main__":
    update_database()
