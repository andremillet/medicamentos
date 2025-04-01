import pandas as pd
import sqlite3

conn = sqlite3.connect("medicamentos.db")
df = pd.read_sql_query("SELECT NUMERO_REGISTRO_PRODUTO, COUNT(*) as count FROM medicamentos GROUP BY NUMERO_REGISTRO_PRODUTO HAVING count > 1", conn)
print(f"Duplicatas encontradas: {len(df)}")
if len(df) > 0:
    print("Exemplos de duplicatas:", df.head().to_string())
total_unique = pd.read_sql_query("SELECT COUNT(DISTINCT NUMERO_REGISTRO_PRODUTO) FROM medicamentos", conn).iloc[0, 0]
print(f"Total de registros únicos no banco: {total_unique}")
conn.close()
