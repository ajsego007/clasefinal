import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from io import StringIO
import os
from dotenv import load_dotenv

# Configuración (usa .env para seguridad)
load_dotenv()

BASE_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', ''),
    'port': os.getenv('DB_PORT', '5432')
}

DB_NAME = os.getenv('DB_NAME', 'bdd_threats_cybersecurity')

def crear_basedatos(dbname):
    """Crea la base de datos si no existe"""
    try:
        # Conexión al servidor PostgreSQL (sin base de datos específica)
        conn = psycopg2.connect(**BASE_CONFIG)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        
        with conn.cursor() as cursor:
            # Verificar si la base de datos existe
            cursor.execute(sql.SQL("SELECT 1 FROM pg_database WHERE datname = %s"), [dbname])
            exists = cursor.fetchone()
            
            if not exists:
                cursor.execute(sql.SQL("CREATE DATABASE {}").format(
                    sql.Identifier(dbname)))
                print(f"✅ Base de datos '{dbname}' creada exitosamente")
            else:
                print(f"ℹ️ La base de datos '{dbname}' ya existe")
    
    except Exception as e:
        print(f"❌ Error al crear la base de datos: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

def guardar_registros(df, table_name, dbname=DB_NAME, n_registros=None):
    """
    Carga un DataFrame de pandas a una tabla de PostgreSQL.
    
    Args:
        df (pd.DataFrame): DataFrame con los datos a cargar
        table_name (str): Nombre de la tabla destino
        dbname (str): Nombre de la base de datos        
        n_registros (int): Número de registros a cargar (None para todos)
    """
    try:
        # 1. Crear la base de datos si no existe
        crear_basedatos(dbname)
        
        # 2. Configuración de conexión con la base de datos específica
        db_config = {**BASE_CONFIG, 'database': dbname}
        
        # 3. Filtrar registros si se especifica n_registros
        if n_registros is not None:
            df = df.head(n_registros)
            print(f"⚠️ Se cargarán solo los primeros {n_registros} registros")
        
        # Resto del código permanece igual...
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cursor:
                conn.autocommit = False
                
                # Crear tabla (si no existe)
                columns = []
                for col, dtype in df.dtypes.items():
                    col_clean = col.replace(' ', '_').lower()
                    if pd.api.types.is_integer_dtype(dtype):
                        pg_type = 'BIGINT'
                    elif pd.api.types.is_float_dtype(dtype):
                        pg_type = 'FLOAT'
                    elif pd.api.types.is_datetime64_any_dtype(dtype):
                        pg_type = 'TIMESTAMP'
                    else:
                        pg_type = 'TEXT'
                    columns.append(f'"{col_clean}" {pg_type}')
                
                create_query = sql.SQL("""
                    CREATE TABLE IF NOT EXISTS {table} (
                        {columns}
                    );
                """).format(
                    table=sql.Identifier(table_name),
                    columns=sql.SQL(',\n'.join(columns)))
                
                cursor.execute(create_query)
                
                # Cargar datos usando COPY
                buffer = StringIO()
                df.to_csv(buffer, index=False, header=False, sep='\t', na_rep='NULL')
                buffer.seek(0)
                
                cursor.copy_expert(
                    sql.SQL("COPY {} FROM STDIN WITH (FORMAT CSV, DELIMITER '\t', NULL 'NULL')")
                    .format(sql.Identifier(table_name)),
                    buffer)
                
                conn.commit()
                print(f"✅ Datos cargados exitosamente en {dbname}.{table_name}: {len(df)} registros")
    
    except Exception as e:
        print(f"❌ Error: {e}")
        if 'conn' in locals():
            conn.rollback()
        raise

# Ejecutar programa
if __name__ == "__main__":    
    # Ruta de acceso a los datos
    csv_path = 'Global_Cybersecurity_Threats_2015-2024.csv'

    # Especificamos el separador
    threats = pd.read_csv(csv_path, sep=',')
    
    # Cargar todos los registros
    #guardar_registros(threats, 'cyber_threats')

    # Cargar los registros especificados en n_registros
    guardar_registros(threats, 'cyber_threats', n_registros=10)