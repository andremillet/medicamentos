from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import sqlite3
from typing import List

app = FastAPI(title="API de Medicamentos ANVISA")

# Modelo ajustado para as colunas disponíveis
class Medication(BaseModel):
    NOME_PRODUTO: str
    PRINCIPIO_ATIVO: str | None = None
    EMPRESA_DETENTORA_REGISTRO: str | None = None

def get_db_connection(db_name="medicamentos.db"):
    """Conecta ao banco de dados SQLite."""
    conn = sqlite3.connect(db_name)
    conn.row_factory = sqlite3.Row  # Retorna resultados como dicionários
    return conn

@app.get("/medications", response_model=List[Medication])
async def get_all_medications():
    """Retorna todos os medicamentos do banco de dados."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM medicamentos")
    rows = cursor.fetchall()
    conn.close()
    
    if not rows:
        raise HTTPException(status_code=404, detail="Nenhum medicamento encontrado.")
    
    return [dict(row) for row in rows]

@app.get("/medications/search", response_model=List[Medication])
async def search_medications(name: str):
    """Busca medicamentos por nome (parcial)."""
    conn = get_db_connection()
    cursor = conn.cursor()
    query = "SELECT * FROM medicamentos WHERE NOME_PRODUTO LIKE ?"
    cursor.execute(query, (f"%{name}%",))
    rows = cursor.fetchall()
    conn.close()
    
    if not rows:
        raise HTTPException(status_code=404, detail="Nenhum medicamento encontrado com esse nome.")
    
    return [dict(row) for row in rows]

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000, reload=True)
