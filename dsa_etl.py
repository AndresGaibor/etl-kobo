#!/usr/bin/env python3
"""
DSA ETL: Carga datos de KoboToolbox a PostgreSQL sin transformaciones
Los nombres de columnas se mantienen exactamente como vienen de la API de Kobo
"""

from typing import Dict, Any, List
import psycopg2
from psycopg2.extensions import connection as pg_connection
from psycopg2 import sql, extras

# Importar configuración, API y utilidades
from config import (
    KOBOTOOLBOX_TOKEN,
    ASSET_UID,
    DB_HOST,
    DB_PORT,
    DB_USER,
    DB_PASSWORD,
    DB_DATABASE
)
from api import get_survey_metadata, get_survey_submissions
from utils import clean_survey_name, inferir_tipo_pg


def crear_esquema(connection: pg_connection, nombre: str = "dsa") -> None:
    """Crea un esquema y verifica la conexión a PostgreSQL."""
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(nombre)))
            connection.commit()
            print(f"Esquema '{nombre}' verificado/creado exitosamente.")
    except Exception as error:
        print(f"Error creando/verificando el esquema '{nombre}': {error}")
        raise


def create_table(connection: pg_connection, nombre: str, schema: Dict[str, str]) -> None:
    """Create table in PostgreSQL (drop if exists to ensure clean structure)"""
    crear_esquema(connection, "dsa")

    table_name = f"dsa.{nombre}"

    with connection.cursor() as cursor:
        # Drop table if exists to ensure columns are recreated
        drop_table_query = sql.SQL("DROP TABLE IF EXISTS {}").format(
            sql.Identifier("dsa", nombre)
        )
        cursor.execute(drop_table_query)
        print(f"Tabla '{table_name}' eliminada (si existía).")

        # Build column definitions
        column_definitions = []
        for col_name, col_type in schema.items():
            column_def = sql.SQL("{} {}").format(
                sql.Identifier(col_name),
                sql.SQL(col_type)
            )
            column_definitions.append(column_def)

        # Create table query
        create_table_query = sql.SQL("CREATE TABLE {} ({})").format(
            sql.Identifier("dsa", nombre),
            sql.SQL(', ').join(column_definitions)
        )

        cursor.execute(create_table_query)
        connection.commit()
        print(f"Tabla '{table_name}' creada exitosamente.")


def insert_data(connection: pg_connection, nombre: str, submissions: List[Dict[str, Any]], schema: Dict[str, str]) -> None:
    """Insert data into PostgreSQL"""
    table_name = nombre

    columns = list(schema.keys())

    for submission in submissions:
        # Prepare values for insertion
        values = []
        for col in columns:
            value = submission.get(col)
            # If the column type is JSONB, adapt the object for psycopg2
            if schema[col] == 'JSONB' and isinstance(value, (dict, list)):
                value = extras.Json(value)
            values.append(value)

        # Build INSERT query
        insert_query = sql.SQL("""
            INSERT INTO {} ({}) VALUES ({})
            ON CONFLICT DO NOTHING
        """).format(
            sql.Identifier("dsa", table_name),
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

    print(f"Se insertaron {len(submissions)} registros en la tabla 'dsa.{table_name}'.")


def migrate_kobo_to_postgres():
    """Main migration function - carga datos raw sin transformaciones"""
    connection = None

    try:
        # 1. Obtener metadata del asset (nombre de la encuesta)
        asset_data = get_survey_metadata(ASSET_UID, KOBOTOOLBOX_TOKEN)
        survey_name = asset_data.get('name', ASSET_UID)
        clean_name = clean_survey_name(survey_name)

        print(f"Encuesta: '{survey_name}' -> Tabla: 'dsa.{clean_name}'")

        # 2. Obtener submissions de la API de KoboToolbox
        submissions = get_survey_submissions(ASSET_UID, KOBOTOOLBOX_TOKEN)

        if not submissions:
            print("No se encontraron envíos para migrar.")
            return

        print(f"Se encontraron {len(submissions)} envíos.")

        # 2. Analizar la estructura de los datos y crear esquema RAW
        print("Analizando estructura de datos (raw)...")
        first_submission = submissions[0]

        # Esquema base con campos estándar de KoboToolbox
        schema = {}

        # Inferir tipos para TODOS los campos sin transformación
        for key, value in first_submission.items():
            # Mantener nombres exactamente como vienen de Kobo
            schema[key] = inferir_tipo_pg(value)

        print("Esquema inferido (raw):")
        for key, value in schema.items():
            print(f"  {key}: {value}")

        # 3. Crear esquema en PostgreSQL
        

        # 4. Crear conexión a PostgreSQL
        print("Conectando a PostgreSQL...")
        connection = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_DATABASE,
            sslmode='require'
        )

        # 5. Crear tabla en PostgreSQL
        print("Creando tabla en PostgreSQL...")
        create_table(connection, clean_name, schema)

        # 6. Insertar datos
        print("Insertando datos en PostgreSQL...")
        insert_data(connection, clean_name, submissions, schema)

        print("Migración raw completada exitosamente.")

    except Exception as error:
        print(f"Error durante la migración: {error}")
        raise
    finally:
        # 7. Cerrar la conexión
        if connection:
            connection.close()
            print("Conexión a PostgreSQL cerrada.")


if __name__ == "__main__":
    migrate_kobo_to_postgres()
