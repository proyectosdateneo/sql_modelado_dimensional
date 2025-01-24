import duckdb
import os

script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)
# Ruta de la base de datos
db_path = '../datos/reto_sql.duckdb'

# Ruta donde se encuentran los archivos .sql
sql_dir = '.'

# Orden de ejecución de los archivos .sql
sql_files = [
    "dim_suscripciones.sql",
    "dim_cuentas.sql",
    "dim_contenidos.sql",
    "dim_tiempo_dia.sql",
    "fact_creacion_contenido.sql",
    "fact_cuentas_suscripcion.sql"
]

# Conexión a la base de datos DuckDB
conn = duckdb.connect(db_path)

# Función para ejecutar un archivo .sql
def execute_sql_file(conn, file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            sql_script = f.read()
        conn.execute(sql_script)
        print(f"Ejecutado: {file_path}")
    except Exception as e:
        print(f"Error ejecutando {file_path}: {e}")

# Ejecutar los archivos .sql en orden
for sql_file in sql_files:
    file_path = os.path.join(sql_dir, sql_file)
    if os.path.exists(file_path):
        execute_sql_file(conn, file_path)
    else:
        print(f"Archivo no encontrado: {file_path}")

# Cerrar la conexión
conn.close()

print("Ejecución completa.")
