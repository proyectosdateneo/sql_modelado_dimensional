import duckdb
import os
import glob
from charset_normalizer import detect

# Asegurar que el script opera en su propio directorio
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

# Nombre de la base de datos de DuckDB
db_path = 'reto_sql.duckdb'

# Directorio donde están los archivos CSV
csv_dir = '.'

# Conexión a la base de datos DuckDB
conn = duckdb.connect(db_path)

# Función para convertir CSV a UTF-8
def convert_to_utf8(file_path):
    """Convierte un archivo CSV a UTF-8 detectando su codificación original."""
    try:
        # Detectar la codificación del archivo original
        with open(file_path, 'rb') as src_file:
            raw_data = src_file.read()
            detected = detect(raw_data)
            original_encoding = detected['encoding']
        
        print(f"Convirtiendo {file_path} desde {original_encoding} a UTF-8...")
        
        # Leer con la codificación detectada y reescribir en UTF-8
        with open(file_path, 'r', encoding=original_encoding, errors='replace') as src_file:
            content = src_file.read()
        
        # Escribir el archivo en UTF-8-sig (compatible con Excel y DuckDB)
        with open(file_path, 'w', encoding='utf-8-sig') as dest_file:
            dest_file.write(content)
        
        print(f"Archivo {file_path} convertido exitosamente.")
    except Exception as e:
        print(f"Error al convertir {file_path}: {e}")

# Función para crear una tabla desde un archivo CSV
def create_table_from_csv(conn, csv_file):
    table_name = os.path.splitext(os.path.basename(csv_file))[0]  # Nombre de la tabla basado en el archivo
    print(f'Creando tabla: {table_name}')
    print(f'Leyendo archivo CSV: {csv_file}')
    try:
        # Validar que el archivo se lea correctamente antes de insertarlo
        with open(csv_file, 'r', encoding='utf-8') as f:
            print('Primeras 5 líneas del archivo:')
            for i, line in enumerate(f):
                print(line.strip())
                if i == 4:
                    break
        
        # Crear tabla y cargar datos
        conn.execute(f"""
            CREATE TABLE {table_name} AS
            SELECT * FROM read_csv_auto('{csv_file}', HEADER=True)
        """)
        
        # Mostrar primeras filas de la tabla creada
        print(f'Primeras 5 filas de la tabla {table_name}:')
        result = conn.execute(f"SELECT * FROM {table_name} LIMIT 5").fetchall()
        for row in result:
            print(row)
    except Exception as e:
        print(f"Error al crear la tabla {table_name}: {e}")

# Buscar todos los archivos CSV en el directorio
csv_files = glob.glob(os.path.join(csv_dir, '*.csv'))

# Convertir todos los archivos CSV a UTF-8
for csv_file in csv_files:
    convert_to_utf8(csv_file)

# Crear una tabla para cada archivo CSV
for csv_file in csv_files:
    create_table_from_csv(conn, csv_file)

print("Tablas creadas exitosamente en la base reto_sql.")

# Cerrar conexión
conn.close()
