import sqlite3

# Conectar ao banco de dados original
conn = sqlite3.connect('medicamentos.db')
cursor = conn.cursor()

# Assumindo que a tabela original se chama 'medicamentos' (ajuste se necessário)
tabela_original = 'medicamentos'

# Verificar se a tabela existe
cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{tabela_original}'")
if not cursor.fetchone():
    print(f"A tabela '{tabela_original}' não foi encontrada no banco de dados!")
    conn.close()
    exit()

# Criar as novas tabelas normalizadas com nomes em minúsculo
cursor.execute('''
    CREATE TABLE IF NOT EXISTS empresas (
        cnpj TEXT PRIMARY KEY,
        nome_empresa TEXT
    )
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS principios_ativos (
        id_principio INTEGER PRIMARY KEY AUTOINCREMENT,
        nome_principio TEXT UNIQUE
    )
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS produtos (
        numero_registro_produto TEXT PRIMARY KEY,
        nome_produto TEXT,
        cnpj_empresa TEXT,
        dose TEXT,
        apresentacao TEXT,
        FOREIGN KEY (cnpj_empresa) REFERENCES empresas(cnpj)
    )
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS produto_principios (
        numero_registro_produto TEXT,
        id_principio INTEGER,
        FOREIGN KEY (numero_registro_produto) REFERENCES produtos(numero_registro_produto),
        FOREIGN KEY (id_principio) REFERENCES principios_ativos(id_principio),
        PRIMARY KEY (numero_registro_produto, id_principio)
    )
''')

# Função para separar CNPJ e nome da empresa
def split_empresa(empresa_str):
    if not empresa_str or empresa_str == '':
        return None, None
    partes = empresa_str.split(' - ', 1)
    return partes[0], partes[1] if len(partes) > 1 else None

# Função para dividir princípios ativos
def split_principios(principio_str):
    if not principio_str or principio_str == '':
        return []
    return [p.strip() for p in principio_str.split('+')]

# 1. Preencher a tabela empresas
cursor.execute(f'SELECT DISTINCT EMPRESA_DETENTORA_REGISTRO FROM {tabela_original}')
empresas = set()
for row in cursor.fetchall():
    cnpj, nome = split_empresa(row[0])
    if cnpj and nome:
        empresas.add((cnpj, nome))
cursor.executemany('INSERT OR IGNORE INTO empresas (cnpj, nome_empresa) VALUES (?, ?)', empresas)

# 2. Preencher a tabela principios_ativos
cursor.execute(f'SELECT DISTINCT PRINCIPIO_ATIVO FROM {tabela_original}')
principios_unicos = set()
for row in cursor.fetchall():
    for principio in split_principios(row[0]):
        principios_unicos.add(principio)
cursor.executemany('INSERT OR IGNORE INTO principios_ativos (nome_principio) VALUES (?)', [(p,) for p in principios_unicos])

# 3. Preencher a tabela produtos
cursor.execute(f'''
    SELECT NUMERO_REGISTRO_PRODUTO, NOME_PRODUTO, EMPRESA_DETENTORA_REGISTRO, dose, apresentacao
    FROM {tabela_original}
''')
produtos = []
for row in cursor.fetchall():
    cnpj, _ = split_empresa(row[2])
    produtos.append((row[0], row[1], cnpj, row[3], row[4]))
cursor.executemany('INSERT OR IGNORE INTO produtos VALUES (?, ?, ?, ?, ?)', produtos)

# 4. Preencher a tabela produto_principios
cursor.execute(f'SELECT NUMERO_REGISTRO_PRODUTO, PRINCIPIO_ATIVO FROM {tabela_original}')
produto_principios = []
for row in cursor.fetchall():
    numero_registro = row[0]
    principios = split_principios(row[1])
    for principio in principios:
        cursor.execute('SELECT id_principio FROM principios_ativos WHERE nome_principio = ?', (principio,))
        id_principio = cursor.fetchone()
        if id_principio:
            produto_principios.append((numero_registro, id_principio[0]))
cursor.executemany('INSERT OR IGNORE INTO produto_principios VALUES (?, ?)', produto_principios)

# (Opcional) Remover a tabela original após a migração
# Descomente as linhas abaixo se quiser apagar a tabela original
# cursor.execute(f'DROP TABLE {tabela_original}')
# print(f"Tabela original '{tabela_original}' removida com sucesso!")

# Confirmar as alterações e fechar a conexão
conn.commit()
conn.close()

print("Tabela original normalizada com sucesso no banco 'medicamentos.db'!")
