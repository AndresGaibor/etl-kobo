#!/usr/bin/env python3
"""
Limpieza ETL: Limpia datos de la tabla raw eliminando columnas vacias
Elimina columnas que estan completamente vacias, con {} o NULL
"""

from typing import Set
import psycopg2
from psycopg2.extensions import connection as pg_connection
from psycopg2 import sql

from config import (
    DB_HOST,
    DB_PORT,
    DB_USER,
    DB_PASSWORD,
    DB_DATABASE,
    KOBOTOOLBOX_TOKEN,
    ASSET_UID
)
from api import get_survey_metadata
from utils import clean_survey_name


def get_redundant_columns() -> Set[str]:
    """
    Retorna un set de columnas redundantes que siempre deben eliminarse
    """
    return {
        'meta/instanceID',      # Redundante con _uuid
        'meta/rootUuid',        # Redundante con _uuid
        'formhub/uuid',         # Deployment UUID (constante)
        '_xform_id_string'      # ASSET_UID (constante),
        '_uuid',
        '_status',
        '_uuid',
        '_submission_time',
        '__version__',
        '_id'
    }


def get_empty_columns(connection: pg_connection, table_name: str) -> Set[str]:
    """
    Identifica columnas que estan completamente vacias, con {} o NULL
    Retorna un set con los nombres de las columnas a eliminar
    """
    print(f"Analizando columnas vacias en '{table_name}'...")

    with connection.cursor() as cursor:
        # Obtener todas las columnas de la tabla
        cursor.execute(sql.SQL("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'dsa' AND table_name = %s
        """), [table_name.replace('dsa.', '')])

        columns = cursor.fetchall()
        empty_columns = set()

        for col_name, data_type in columns:
            # Construir query para verificar si la columna está completamente vacía
            if data_type == 'jsonb':
                # Para JSONB, verificar si es NULL, {}, [], o arrays con solo nulls como [null, null]
                check_query = sql.SQL("""
                    SELECT COUNT(*)
                    FROM {}
                    WHERE {} IS NOT NULL
                      AND {} != '{{}}'::jsonb
                      AND {} != '[]'::jsonb
                      AND NOT (
                        jsonb_typeof({}) = 'array'
                        AND jsonb_array_length({}) > 0
                        AND (
                          SELECT bool_and(value = 'null'::jsonb)
                          FROM jsonb_array_elements({})
                        )
                      )
                """).format(
                    sql.Identifier("dsa", table_name.replace('dsa.', '')),
                    sql.Identifier(col_name),
                    sql.Identifier(col_name),
                    sql.Identifier(col_name),
                    sql.Identifier(col_name),
                    sql.Identifier(col_name),
                    sql.Identifier(col_name)
                )
            else:
                # Para otros tipos, solo verificar NULL o string vac�o
                check_query = sql.SQL("""
                    SELECT COUNT(*)
                    FROM {}
                    WHERE {} IS NOT NULL
                      AND CAST({} AS TEXT) != ''
                """).format(
                    sql.Identifier("dsa", table_name.replace('dsa.', '')),
                    sql.Identifier(col_name),
                    sql.Identifier(col_name)
                )

            cursor.execute(check_query)
            non_empty_count = cursor.fetchone()[0]

            if non_empty_count == 0:
                empty_columns.add(col_name)
                print(f"   Columna vacia detectada: '{col_name}' ({data_type})")

    return empty_columns


def drop_columns(connection: pg_connection, table_name: str, columns_to_drop: Set[str]) -> None:
    """Elimina las columnas especificadas de la tabla"""
    if not columns_to_drop:
        print("No hay columnas para eliminar.")
        return

    with connection.cursor() as cursor:
        for col_name in columns_to_drop:
            drop_query = sql.SQL("ALTER TABLE {} DROP COLUMN {}").format(
                sql.Identifier("dsa", table_name.replace('dsa.', '')),
                sql.Identifier(col_name)
            )
            cursor.execute(drop_query)
            print(f"  ✓ '{col_name}'")

        connection.commit()

    print(f"\n✓ Total: {len(columns_to_drop)} columnas eliminadas exitosamente.")


def limpiar_tabla():
    """Funci�n principal de limpieza"""
    connection = None

    try:
        # 1. Obtener nombre de la encuesta
        asset_data = get_survey_metadata(ASSET_UID, KOBOTOOLBOX_TOKEN)
        survey_name = asset_data.get('name', ASSET_UID)
        clean_name = clean_survey_name(survey_name)
        table_name = f"dsa.{clean_name}"

        print(f"=== Limpieza de tabla '{table_name}' ===\n")

        # 2. Conectar a PostgreSQL
        print("Conectando a PostgreSQL...")
        connection = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_DATABASE,
            sslmode='require'
        )

        # 3. Obtener columnas redundantes predefinidas
        print("\n[1] Identificando columnas redundantes predefinidas...")
        redundant_columns = get_redundant_columns()

        # Verificar cuáles existen en la tabla
        with connection.cursor() as cursor:
            cursor.execute(sql.SQL("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'dsa' AND table_name = %s
            """), [table_name.replace('dsa.', '')])
            existing_columns = {row[0] for row in cursor.fetchall()}

        redundant_to_drop = redundant_columns & existing_columns

        if redundant_to_drop:
            print(f"Columnas redundantes encontradas: {len(redundant_to_drop)}")
            for col in redundant_to_drop:
                print(f"  → {col}")
        else:
            print("No se encontraron columnas redundantes.")

        # 4. Identificar columnas vacías
        print("\n[2] Identificando columnas vacías...")
        empty_columns = get_empty_columns(connection, table_name)

        # 5. Combinar todas las columnas a eliminar
        all_columns_to_drop = redundant_to_drop | empty_columns

        # 6. Eliminar columnas
        if all_columns_to_drop:
            print(f"\n[3] Eliminando {len(all_columns_to_drop)} columnas en total...")
            drop_columns(connection, table_name, all_columns_to_drop)
        else:
            print("\nNo se encontraron columnas para eliminar. La tabla ya esta limpia.")

        print("\n=== Limpieza completada ===")

    except Exception as error:
        print(f"Error durante la limpieza: {error}")
        raise
    finally:
        if connection:
            connection.close()
            print("Conexi�n a PostgreSQL cerrada.")


if __name__ == "__main__":
    limpiar_tabla()
