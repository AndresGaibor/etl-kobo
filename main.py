#!/usr/bin/env python3
"""
Migrate KoboToolbox data to PostgreSQL
"""

import os
import json
import re
from typing import Dict, Any, List
import psycopg2
from psycopg2 import sql, extras
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration from environment variables
KOBOTOOLBOX_TOKEN = os.getenv('KOBOTOOLBOX_TOKEN')
ASSET_UID = os.getenv('ASSET_UID')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_DATABASE = os.getenv('DB_DATABASE')


def infer_pg_type(value: Any) -> str:
    """Infer PostgreSQL type from Python value"""
    if value is None:
        return 'TEXT'

    if isinstance(value, bool):
        return 'BOOLEAN'
    elif isinstance(value, int):
        return 'INTEGER'
    elif isinstance(value, float):
        return 'NUMERIC'
    elif isinstance(value, str):
        # Detect timestamp (formato ISO 8601)
        if re.match(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}', value):
            return 'TIMESTAMP'
        # Detect geopunto (lat,long)
        if re.match(r'^-?\d+(\.\d+)?,-?\d+(\.\d+)?$', value):
            return 'POINT'
        return 'TEXT'
    elif isinstance(value, (dict, list)):
        # Para arrays o objetos anidados, JSONB es una excelente opción en PostgreSQL
        return 'JSONB'

    return 'TEXT'  # Por defecto


def create_table(connection, schema: Dict[str, str]) -> None:
    """Create table in PostgreSQL"""
    table_name = f"kobo_{ASSET_UID}"

    # Build column definitions
    column_definitions = []
    for col_name, col_type in schema.items():
        column_def = sql.SQL("{} {}").format(
            sql.Identifier(col_name),
            sql.SQL(col_type)
        )
        column_definitions.append(column_def)

    # Create table query
    create_table_query = sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(
        sql.Identifier(table_name),
        sql.SQL(', ').join(column_definitions)
    )

    with connection.cursor() as cursor:
        cursor.execute(create_table_query)
        connection.commit()
        print(f"Tabla '{table_name}' verificada/creada exitosamente.")


def insert_data(connection, submissions: List[Dict[str, Any]], schema: Dict[str, str]) -> None:
    """Insert data into PostgreSQL"""
    table_name = f"kobo_{ASSET_UID}"

    columns = list(schema.keys())

    for submission in submissions:
        # Prepare values for insertion
        values = []
        for col in columns:
            value = submission.get(col)
            # If the column type is JSONB, stringify the object
            if schema[col] == 'JSONB' and (isinstance(value, (dict, list))):
                value = json.dumps(value)
            values.append(value)

        # Build INSERT query
        insert_query = sql.SQL("""
            INSERT INTO {} ({}) VALUES ({})
            ON CONFLICT DO NOTHING
        """).format(
            sql.Identifier(table_name),
            sql.SQL(', ').join(map(sql.Identifier, columns)),
            sql.SQL(', ').join(sql.Placeholder() * len(columns))
        )

        try:
            with connection.cursor() as cursor:
                cursor.execute(insert_query, values)
                connection.commit()
        except Exception as error:
            print(f"Error inserting record: {error}")
            print(f"Problematic data: {json.dumps(submission, indent=2)}")
            raise error

    print(f"Se insertaron {len(submissions)} registros en la tabla '{table_name}'.")


def migrate_kobo_to_postgres():
    """Main migration function"""
    connection = None

    try:
        # 1. Obtener datos de la API de KoboToolbox
        print("Obteniendo datos de KoboToolbox...")
        api_url = f"https://kf.kobotoolbox.org/api/v2/assets/{ASSET_UID}/data/"
        headers = {
            "Authorization": f"Token {KOBOTOOLBOX_TOKEN}",
            "Accept": "application/json"
        }

        response = requests.get(api_url, headers=headers)
        response.raise_for_status()

        # Debug: Print response content if JSON parsing fails
        try:
            data = response.json()
        except json.JSONDecodeError as e:
            print(f"Error decodificando JSON: {e}")
            print(f"Status code: {response.status_code}")
            print(f"Response headers: {response.headers}")
            print(f"Response text (primeros 500 chars): {response.text[:500]}")
            raise

        submissions = data.get('results', [])

        if not submissions:
            print("No se encontraron envíos para migrar.")
            return

        print(f"Se encontraron {len(submissions)} envíos.")

        # 2. Analizar la estructura de los datos y crear esquema
        print("Analizando estructura de datos...")
        first_submission = submissions[0]

        # Esquema base con campos estándar de KoboToolbox
        schema = {
            '_id': 'TEXT',
            '_uuid': 'TEXT',
            '_submission_time': 'TIMESTAMP',
            '_submitted_by': 'TEXT',
            '_status': 'TEXT'
        }

        # Inferir tipos para los campos adicionales
        for key, value in first_submission.items():
            if key not in schema:
                schema[key] = infer_pg_type(value)

        print("Esquema inferido:", schema)

        # 3. Crear conexión a PostgreSQL
        print("Conectando a PostgreSQL...")
        connection = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_DATABASE,
            sslmode='require'  # Adjust as needed
        )

        # 4. Crear tabla en PostgreSQL
        print("Creando tabla en PostgreSQL...")
        create_table(connection, schema)

        # 5. Insertar datos
        print("Insertando datos en PostgreSQL...")
        insert_data(connection, submissions, schema)

        print("Migración completada exitosamente.")

    except Exception as error:
        print(f"Error durante la migración: {error}")
    finally:
        # 6. Cerrar la conexión
        if connection:
            connection.close()
            print("Conexión a PostgreSQL cerrada.")


if __name__ == "__main__":
    migrate_kobo_to_postgres()