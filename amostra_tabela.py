import sqlite3
import pandas as pd

# Conectar ao banco de dados SQLite
conn = sqlite3.connect('medicamentos.db')

# Obter a lista de tabelas disponíveis
cursor = conn.cursor()
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tabelas = cursor.fetchall()

# Verificar se há tabelas no banco de dados
if not tabelas:
    print("Nenhuma tabela encontrada no banco de dados!")
    conn.close()
    exit()

# Exibir as tabelas disponíveis
print("Tabelas disponíveis no banco de dados:")
for i, tabela in enumerate(tabelas, 1):
    print(f"{i}. {tabela[0]}")

# Solicitar ao usuário que escolha uma tabela
while True:
    try:
        escolha = int(input("Digite o número da tabela que deseja exportar: "))
        if 1 <= escolha <= len(tabelas):
            nome_tabela = tabelas[escolha - 1][0]
            break
        else:
            print(f"Por favor, escolha um número entre 1 e {len(tabelas)}.")
    except ValueError:
        print("Entrada inválida! Digite um número.")

# Solicitar o tamanho da amostra
while True:
    try:
        limite = int(input("Digite o número de linhas para a amostra (ex.: 100): "))
        if limite > 0:
            break
        else:
            print("Por favor, digite um número maior que 0.")
    except ValueError:
        print("Entrada inválida! Digite um número.")

# Query para selecionar uma amostra da tabela escolhida
# Usando RANDOM() para amostra aleatória
query = f"SELECT * FROM {nome_tabela} ORDER BY RANDOM() LIMIT {limite}"

# Carregar os dados em um DataFrame do pandas
df = pd.read_sql_query(query, conn)

# Fechar a conexão com o banco de dados
conn.close()

# Exportar o DataFrame para um arquivo CSV
nome_arquivo = f'amostra_{nome_tabela}.csv'
df.to_csv(nome_arquivo, index=False, encoding='utf-8')

print(f"Amostra de {limite} linhas da tabela '{nome_tabela}' exportada com sucesso para '{nome_arquivo}'!")
