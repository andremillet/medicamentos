import requests
import pandas as pd
import sqlite3
import os
import sys

# URL base da API CKAN
CKAN_API_URL = "https://dados.gov.br/api/3/action/package_search"
DATASET_QUERY = "medicamentos registrados no brasil"
LOCAL_CSV_PATH = "medicamentos.csv"  # Arquivo local padrão

def get_csv_url_from_api():
    """Tenta obter a URL do CSV mais recente via API CKAN."""
    params = {"q": DATASET_QUERY}
    headers = {"User-Agent": "Mozilla/5.0 (compatible; MedicamentosAPI/1.0)"}
    
    try:
        response = requests.get(CKAN_API_URL, params=params, headers=headers, timeout=10)
        print(f"Status da resposta: {response.status_code}")
        print(f"Conteúdo da resposta (primeiros 200 caracteres): {response.text[:200]}...")
        
        if "text/html" in response.headers.get("Content-Type", "").lower():
            raise ValueError("Resposta recebida é HTML, não JSON. A API pode estar inacessível ou redirecionando.")
        
        if not response.text.strip():
            raise ValueError("Resposta vazia recebida da API.")
        
        data = response.json()
        print("Resposta da API parseada como JSON com sucesso.")
        
        for result in data["result"]["results"]:
            if "medicamentos registrados no brasil" in result["title"].lower():
                for resource in result["resources"]:
                    if resource["format"].lower() == "csv":
                        return resource["url"]
        raise Exception("CSV não encontrado no dataset.")
    
    except requests.exceptions.RequestException as e:
        print(f"Erro na requisição: {e}")
        raise
    except ValueError as e:
        print(f"Erro de conteúdo: {e}")
        raise
    except Exception as e:
        print(f"Erro inesperado: {e}")
        raise

def download_and_process_data(csv_url, output_file="medicamentos_temp.csv"):
    """Baixa ou lê o CSV e processa os dados, salvando apenas medicamentos válidos."""
    if csv_url.startswith("http"):
        response = requests.get(csv_url)
        response.raise_for_status()
        with open(output_file, "wb") as f:
            f.write(response.content)
        csv_path = output_file
    else:
        csv_path = csv_url
    
    try:
        df = pd.read_csv(csv_path, delimiter=";", encoding="latin1")
        print(f"Total de linhas no CSV: {len(df)}")
        print(f"Colunas disponíveis: {list(df.columns)}")
        
        # Normaliza e filtra por "VÁLIDO" ou "ATIVO"
        status_col = "SITUACAO_REGISTRO"
        if status_col not in df.columns:
            raise KeyError(f"Coluna '{status_col}' não encontrada no CSV.")
        
        df["SITUACAO_NORMALIZED"] = df[status_col].str.lower().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('ascii')
        df_valid = df[df["SITUACAO_NORMALIZED"].isin(["valido", "ativo"])]
        print(f"Linhas após filtro 'Válido' ou 'Ativo': {len(df_valid)}")
        
        # Colunas desejadas
        columns = [
            "NOME_PRODUTO",
            "PRINCIPIO_ATIVO",
            "EMPRESA_DETENTORA_REGISTRO",
            "NUMERO_REGISTRO_PRODUTO"
        ]       
        available_columns = [col for col in columns if col in df_valid.columns]
        if not available_columns:
            raise KeyError("Nenhuma coluna esperada encontrada no CSV. Colunas disponíveis: " + str(list(df.columns)))
        
        df_selected = df_valid[available_columns]
        if csv_url.startswith("http"):
            os.remove(output_file)
        return df_selected
    
    except pd.errors.EmptyDataError:
        raise ValueError("O arquivo CSV está vazio.")
    except Exception as e:
        raise Exception(f"Erro ao processar o CSV: {str(e)}")

def update_database(df, db_name="medicamentos.db"):
    """Atualiza o banco de dados SQLite com os dados processados."""
    conn = sqlite3.connect(db_name)
    df.to_sql("medicamentos", conn, if_exists="replace", index=False)
    conn.close()
    print(f"Banco de dados '{db_name}' atualizado com {len(df)} registros.")

def main():
    """Executa o script com suporte a comandos."""
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
    else:
        command = "local"
    
    try:
        if command == "update":
            print("Tentando atualizar via API...")
            csv_url = get_csv_url_from_api()
            print(f"URL encontrada: {csv_url}")
        else:
            if not os.path.exists(LOCAL_CSV_PATH):
                raise FileNotFoundError(f"Arquivo local '{LOCAL_CSV_PATH}' não encontrado. Baixe-o manualmente.")
            csv_url = LOCAL_CSV_PATH
            print(f"Usando arquivo local: {csv_url}")
        
        print("Processando dados...")
        df = download_and_process_data(csv_url)
        
        print("Atualizando banco de dados...")
        update_database(df)
        
    except Exception as e:
        print(f"Erro final: {str(e)}")

if __name__ == "__main__":
    main()
