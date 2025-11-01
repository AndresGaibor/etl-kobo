#!/usr/bin/env python3
"""
Normalización ETL Interactiva con Questionary
1. Limpia columnas redundantes y vacías en DSA
2. Detecta grupos anidados (tabla/columna)
3. Pregunta interactivamente con questionary
4. Crea dimensiones en EDW con prefijo d_
5. IDs autoincrementales y fecha_carga_etl en tablas con timestamps
"""

import re
from typing import Dict, List, Tuple, Optional
from collections import defaultdict
import psycopg2
from psycopg2.extensions import connection as pg_connection
from psycopg2 import sql
import questionary
from questionary import Style

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
from limpieza import get_redundant_columns, get_empty_columns, drop_columns


# Estilo personalizado para questionary
custom_style = Style([
    ('qmark', 'fg:#673ab7 bold'),
    ('question', 'bold'),
    ('answer', 'fg:#f44336 bold'),
    ('pointer', 'fg:#673ab7 bold'),
    ('highlighted', 'fg:#673ab7 bold'),
    ('selected', 'fg:#cc5454'),
    ('separator', 'fg:#cc5454'),
    ('instruction', ''),
    ('text', ''),
])


def to_snake_case(name: str) -> str:
    """Convierte cualquier string a snake_case"""
    name = re.sub(r'[^\w/]', '', name)
    name = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', name)
    return name.lower()


def limpiar_tabla_dsa(connection: pg_connection, table_name: str):
    """Ejecuta limpieza de columnas redundantes y vacías en DSA"""
    print(f"\n{'='*80}")
    print("FASE 1: LIMPIEZA DSA")
    print(f"{'='*80}\n")

    redundant = get_redundant_columns()

    with connection.cursor() as cursor:
        cursor.execute(sql.SQL("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'dsa' AND table_name = %s
        """), [table_name.replace('dsa.', '')])
        existing = {row[0] for row in cursor.fetchall()}

    redundant_to_drop = redundant & existing
    if redundant_to_drop:
        print(f"[1] Columnas redundantes: {', '.join(redundant_to_drop)}")
    else:
        print("[1] No hay columnas redundantes")

    print("\n[2] Identificando columnas vacías...")
    empty = get_empty_columns(connection, table_name)

    all_to_drop = redundant_to_drop | empty
    if all_to_drop:
        print(f"\n[3] Eliminando {len(all_to_drop)} columnas...")
        drop_columns(connection, table_name, all_to_drop)
    else:
        print("\n✓ Tabla DSA ya está limpia")


def detectar_estructura(connection: pg_connection, table_name: str):
    """Detecta grupos anidados y columnas principales"""
    with connection.cursor() as cursor:
        cursor.execute(sql.SQL("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'dsa' AND table_name = %s
            ORDER BY ordinal_position
        """), [table_name.replace('dsa.', '')])
        columns = cursor.fetchall()

    nested_groups = defaultdict(list)
    main_columns = []

    for col_name, col_type in columns:
        if '/' in col_name:
            prefix, field = col_name.split('/', 1)
            group_name = to_snake_case(prefix)
            field_name = to_snake_case(field)
            nested_groups[group_name].append((field_name, col_type, col_name))
        else:
            main_columns.append((to_snake_case(col_name), col_type, col_name))

    return dict(nested_groups), main_columns


def tiene_columnas_timestamp(columns: List[Tuple[str, str, str]]) -> bool:
    """Verifica si hay columnas de tipo TIMESTAMP"""
    return any(col_type == 'timestamp without time zone' or col_type.startswith('timestamp')
               for _, col_type, _ in columns)


def preguntar_nombre_tabla(nombre_default: str) -> str:
    """Pregunta el nombre de la tabla con questionary"""
    return questionary.text(
        f"Nombre de la tabla:",
        default=f"d_{nombre_default}",
        style=custom_style
    ).ask()


def preguntar_nombre_columna(columna_actual: str) -> str:
    """Pregunta si quiere cambiar el nombre de una columna"""
    cambiar = questionary.confirm(
        f"  ¿Cambiar nombre de '{columna_actual}'?",
        default=False,
        style=custom_style
    ).ask()

    if cambiar:
        return questionary.text(
            f"    Nuevo nombre:",
            default=columna_actual,
            style=custom_style
        ).ask()
    return columna_actual


def configurar_tabla_principal(main_columns: List[Tuple[str, str, str]]):
    """Configura tabla principal interactivamente"""
    print(f"\n{'='*80}")
    print("CONFIGURACIÓN TABLA PRINCIPAL")
    print(f"{'='*80}\n")

    # Nombre de tabla
    nombre = preguntar_nombre_tabla("encuesta")

    # Mostrar columnas
    print(f"\nColumnas disponibles ({len(main_columns)}):")
    for i, (snake, col_type, orig) in enumerate(main_columns, 1):
        print(f"  {i}. {snake:30} ({col_type})")

    # Seleccionar columnas
    mantener_todas = questionary.confirm(
        "\n¿Mantener todas las columnas?",
        default=True,
        style=custom_style
    ).ask()

    if mantener_todas:
        columnas_seleccionadas = main_columns
    else:
        indices_str = questionary.text(
            "Índices de columnas a mantener (separados por coma):",
            style=custom_style
        ).ask()

        try:
            indices = [int(x.strip()) - 1 for x in indices_str.split(',')]
            columnas_seleccionadas = [main_columns[i] for i in indices if 0 <= i < len(main_columns)]
        except (ValueError, IndexError):
            print("  ⚠️  Entrada inválida, manteniendo todas")
            columnas_seleccionadas = main_columns

    # Renombrar columnas
    renombrar = questionary.confirm(
        "\n¿Renombrar alguna columna?",
        default=False,
        style=custom_style
    ).ask()

    if renombrar:
        columnas_finales = []
        for snake, col_type, orig in columnas_seleccionadas:
            nuevo_nombre = preguntar_nombre_columna(snake)
            columnas_finales.append((nuevo_nombre, col_type, orig))
        return nombre, columnas_finales
    else:
        return nombre, columnas_seleccionadas


def crear_dimension_edw(
    connection: pg_connection,
    dsa_table: str,
    table_name: str,
    columns: List[Tuple[str, str, str]],
    fk_tabla: Optional[str] = None,
    fk_columna: Optional[str] = None,
    unique_columns: Optional[List[str]] = None,
    fk_match_column: Optional[str] = None,
    fk_parent_match_column: Optional[str] = None
):
    """
    Crea dimensión en EDW con ID autoincremental y opcionalmente UNIQUE constraint.

    Args:
        fk_match_column: Columna en DSA para buscar valor de match (ej: 'nombreEstudiante')
        fk_parent_match_column: Columna en tabla padre EDW para hacer match (ej: 'nombre_estudiante')
    """
    with connection.cursor() as cursor:
        # Crear esquema EDW si no existe
        cursor.execute("CREATE SCHEMA IF NOT EXISTS edw")

        # Drop tabla si existe
        cursor.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(
            sql.Identifier("edw", table_name)
        ))

        # Construir columnas
        column_defs = [sql.SQL("id SERIAL PRIMARY KEY")]

        # FK si existe
        if fk_tabla and fk_columna:
            column_defs.append(sql.SQL("{} INTEGER").format(sql.Identifier(fk_columna)))

        # Columnas del grupo (filtrar solo si el nombre ORIGINAL es 'id')
        for snake_name, col_type, orig in columns:
            # Solo filtrar si la columna original se llama 'id', no si fue renombrada a 'id'
            if orig.lower() != 'id':
                column_defs.append(sql.SQL("{} {}").format(
                    sql.Identifier(snake_name),
                    sql.SQL(col_type)
                ))

        # Agregar fecha_carga_etl si hay timestamps
        if tiene_columnas_timestamp(columns):
            column_defs.append(sql.SQL("fecha_carga_etl TIMESTAMP DEFAULT NOW()"))

        # Crear tabla
        create_query = sql.SQL("CREATE TABLE {} ({})").format(
            sql.Identifier("edw", table_name),
            sql.SQL(', ').join(column_defs)
        )
        cursor.execute(create_query)

        # Agregar UNIQUE constraint si hay identificadores naturales
        if unique_columns:
            unique_cols = [sql.Identifier(col) for col in unique_columns]
            unique_query = sql.SQL("""
                ALTER TABLE {}
                ADD CONSTRAINT unique_{}_{}
                UNIQUE ({})
            """).format(
                sql.Identifier("edw", table_name),
                sql.Identifier(table_name),
                sql.Identifier('_'.join(unique_columns)),
                sql.SQL(', ').join(unique_cols)
            )
            cursor.execute(unique_query)

        # FK constraint si existe
        if fk_tabla and fk_columna:
            fk_query = sql.SQL("""
                ALTER TABLE {}
                ADD CONSTRAINT fk_{}_{}
                FOREIGN KEY ({}) REFERENCES {} (id) ON DELETE CASCADE
            """).format(
                sql.Identifier("edw", table_name),
                sql.Identifier(table_name),
                sql.Identifier(fk_tabla),
                sql.Identifier(fk_columna),
                sql.Identifier("edw", fk_tabla)
            )
            cursor.execute(fk_query)

        # Migrar datos desde DSA
        # Filtrar solo si la columna ORIGINAL se llama 'id'
        filtered_columns = [(snake, col_type, orig) for snake, col_type, orig in columns if orig.lower() != 'id']
        source_cols = [sql.Identifier(orig) for _, _, orig in filtered_columns]
        target_cols = [sql.Identifier(snake) for snake, _, _ in filtered_columns]

        if not fk_tabla:
            # Tabla independiente - migración directa
            if unique_columns:
                unique_conflict = [sql.Identifier(col) for col in unique_columns]
                insert_query = sql.SQL("""
                    INSERT INTO {} ({})
                    SELECT DISTINCT ON ({}) {}
                    FROM {}
                    ORDER BY {}
                    ON CONFLICT ({}) DO NOTHING
                """).format(
                    sql.Identifier("edw", table_name),
                    sql.SQL(', ').join(target_cols),
                    sql.SQL(', ').join(unique_conflict),
                    sql.SQL(', ').join(source_cols),
                    sql.Identifier("dsa", dsa_table),
                    sql.SQL(', ').join(unique_conflict),
                    sql.SQL(', ').join(unique_conflict)
                )
            else:
                insert_query = sql.SQL("""
                    INSERT INTO {} ({})
                    SELECT {}
                    FROM {}
                """).format(
                    sql.Identifier("edw", table_name),
                    sql.SQL(', ').join(target_cols),
                    sql.SQL(', ').join(source_cols),
                    sql.Identifier("dsa", dsa_table)
                )

            cursor.execute(insert_query)
        else:
            # Tabla con FK - mapear datos desde DSA a IDs de tabla padre
            if not fk_match_column or not fk_parent_match_column:
                print(f"  ⚠️  Advertencia: FK especificado sin columnas de match. FK quedará NULL.")
                return

            # Agregar FK column a target_cols
            all_target_cols = [sql.Identifier(fk_columna)] + target_cols

            # Construir query con JOIN para obtener FK
            if unique_columns:
                unique_conflict = [sql.Identifier(col) for col in unique_columns]
                insert_query = sql.SQL("""
                    INSERT INTO {} ({})
                    SELECT DISTINCT ON ({})
                        parent.id,
                        {}
                    FROM {} dsa
                    LEFT JOIN {} parent ON dsa.{} = parent.{}
                    ORDER BY {}
                    ON CONFLICT ({}) DO NOTHING
                """).format(
                    sql.Identifier("edw", table_name),
                    sql.SQL(', ').join(all_target_cols),
                    sql.SQL(', ').join(unique_conflict),
                    sql.SQL(', ').join(source_cols),
                    sql.Identifier("dsa", dsa_table),
                    sql.Identifier("edw", fk_tabla),
                    sql.Identifier(fk_match_column),
                    sql.Identifier(fk_parent_match_column),
                    sql.SQL(', ').join(unique_conflict),
                    sql.SQL(', ').join(unique_conflict)
                )
            else:
                insert_query = sql.SQL("""
                    INSERT INTO {} ({})
                    SELECT
                        parent.id,
                        {}
                    FROM {} dsa
                    LEFT JOIN {} parent ON dsa.{} = parent.{}
                """).format(
                    sql.Identifier("edw", table_name),
                    sql.SQL(', ').join(all_target_cols),
                    sql.SQL(', ').join(source_cols),
                    sql.Identifier("dsa", dsa_table),
                    sql.Identifier("edw", fk_tabla),
                    sql.Identifier(fk_match_column),
                    sql.Identifier(fk_parent_match_column)
                )

            cursor.execute(insert_query)

        connection.commit()

        # Mostrar info
        tiene_fecha = tiene_columnas_timestamp(columns)
        # Contar columnas reales (excluyendo 'id' ORIGINAL)
        cols_count = len([orig for _, _, orig in columns if orig.lower() != 'id'])
        unique_info = f" | UNIQUE({', '.join(unique_columns)})" if unique_columns else ""
        fecha_info = " | fecha_carga_etl" if tiene_fecha else ""
        print(f"  ✓ edw.{table_name} creada ({cols_count} cols{unique_info}{fecha_info})")


def normalizar_interactivo():
    """Función principal interactiva"""
    connection = None

    try:
        # 1. Obtener tabla DSA
        asset_data = get_survey_metadata(ASSET_UID, KOBOTOOLBOX_TOKEN)
        survey_name = asset_data.get('name', ASSET_UID)
        clean_name = clean_survey_name(survey_name)
        dsa_table = clean_name

        print(f"\n{'='*80}")
        print(f"NORMALIZACIÓN INTERACTIVA EDW: {survey_name}")
        print(f"{'='*80}")

        # 2. Conectar
        connection = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_DATABASE,
            sslmode='require'
        )

        # 3. Limpieza DSA
        limpiar_tabla_dsa(connection, f"dsa.{dsa_table}")

        # 4. Detectar estructura
        print(f"\n{'='*80}")
        print("FASE 2: DETECCIÓN DE ESTRUCTURA")
        print(f"{'='*80}\n")

        nested_groups, main_columns = detectar_estructura(connection, f"dsa.{dsa_table}")

        print(f"Detectados:")
        print(f"  - {len(main_columns)} columnas principales")
        print(f"  - {len(nested_groups)} grupos anidados")
        for group, fields in nested_groups.items():
            print(f"    • {group}: {len(fields)} columnas")

        # 5. Crear tablas desde columnas principales
        print(f"\n{'='*80}")
        print("FASE 3: CREACIÓN DE DIMENSIONES EDW DESDE COLUMNAS PRINCIPALES")
        print(f"{'='*80}\n")

        tablas_creadas = {}
        columnas_disponibles = list(main_columns)  # Copia de columnas disponibles
        tabla_counter = 1

        while columnas_disponibles:
            print(f"\n{'─'*80}")
            print(f"TABLA #{tabla_counter}")
            print(f"{'─'*80}\n")

            # Mostrar columnas disponibles
            print(f"Columnas disponibles ({len(columnas_disponibles)}):")
            for i, (snake, col_type, orig) in enumerate(columnas_disponibles, 1):
                print(f"  {i}. {snake:30} ({col_type})")

            # Preguntar nombre de tabla
            nombre_default = "encuesta" if tabla_counter == 1 else f"dimension_{tabla_counter}"
            nombre_tabla = preguntar_nombre_tabla(nombre_default)

            # Seleccionar columnas
            mantener_todas = questionary.confirm(
                "\n¿Usar todas las columnas disponibles?",
                default=False,
                style=custom_style
            ).ask()

            if mantener_todas:
                columnas_seleccionadas = columnas_disponibles
            else:
                indices_str = questionary.text(
                    "Índices de columnas a usar (separados por coma):",
                    style=custom_style
                ).ask()

                try:
                    indices = [int(x.strip()) - 1 for x in indices_str.split(',')]
                    columnas_seleccionadas = [columnas_disponibles[i] for i in indices if 0 <= i < len(columnas_disponibles)]
                except (ValueError, IndexError):
                    print("  ⚠️  Entrada inválida, usando primera columna")
                    columnas_seleccionadas = [columnas_disponibles[0]]

            # Renombrar columnas
            renombrar = questionary.confirm(
                "\n¿Renombrar alguna columna?",
                default=False,
                style=custom_style
            ).ask()

            if renombrar:
                columnas_finales = []
                for snake, col_type, orig in columnas_seleccionadas:
                    nuevo_nombre = preguntar_nombre_columna(snake)
                    columnas_finales.append((nuevo_nombre, col_type, orig))
                columnas_seleccionadas = columnas_finales

            # Preguntar si usar identificador único natural
            usar_unique = questionary.confirm(
                "\n¿Usar identificador único natural (evitar duplicados)?",
                default=False,
                style=custom_style
            ).ask()

            unique_cols = None
            if usar_unique:
                columnas_nombres = [snake for snake, _, _ in columnas_seleccionadas]
                unique_cols = questionary.checkbox(
                    "Selecciona columna(s) que identifican únicamente:",
                    choices=columnas_nombres,
                    style=custom_style
                ).ask()

                if not unique_cols:
                    print("  ⚠️  No se seleccionaron columnas, no se usará UNIQUE constraint")

            # Crear tabla
            print(f"\nCreando dimensión '{nombre_tabla}'...")
            crear_dimension_edw(connection, dsa_table, nombre_tabla, columnas_seleccionadas, unique_columns=unique_cols)
            tablas_creadas[nombre_tabla] = None

            # Remover columnas usadas
            columnas_disponibles = [col for col in columnas_disponibles if col not in columnas_seleccionadas]

            # Preguntar si crear otra tabla
            if columnas_disponibles:
                crear_otra = questionary.confirm(
                    f"\n¿Crear otra tabla con las {len(columnas_disponibles)} columna(s) restante(s)?",
                    default=False,
                    style=custom_style
                ).ask()

                if not crear_otra:
                    print(f"\n  → {len(columnas_disponibles)} columna(s) no asignada(s) a ninguna tabla")
                    break
                tabla_counter += 1
            else:
                print("\n  ✓ Todas las columnas principales han sido asignadas")
                break

        # 6. Procesar grupos anidados
        print(f"\n{'='*80}")
        print("FASE 4: PROCESAMIENTO DE GRUPOS ANIDADOS")
        print(f"{'='*80}\n")

        for group_name, columns in nested_groups.items():
            print(f"\n{'─'*80}")

            # Preguntar si crear
            crear = questionary.confirm(
                f"¿Crear dimensión '{group_name}' ({len(columns)} columnas)?",
                default=True,
                style=custom_style
            ).ask()

            if not crear:
                print(f"  → Omitiendo '{group_name}'")
                continue

            # Preguntar nombre
            nombre_dim = preguntar_nombre_tabla(group_name)

            # Renombrar columnas si quiere
            renombrar = questionary.confirm(
                "  ¿Renombrar columnas?",
                default=False,
                style=custom_style
            ).ask()

            if renombrar:
                columnas_finales = []
                for snake, col_type, orig in columns:
                    nuevo = preguntar_nombre_columna(snake)
                    columnas_finales.append((nuevo, col_type, orig))
                columns = columnas_finales

            # Preguntar si es independiente
            independiente = questionary.confirm(
                "  ¿Es dimensión independiente (sin FK)?",
                default=False,
                style=custom_style
            ).ask()

            if independiente:
                # Preguntar si usar identificador único natural
                usar_unique = questionary.confirm(
                    "  ¿Usar identificador único natural (evitar duplicados)?",
                    default=False,
                    style=custom_style
                ).ask()

                unique_cols = None
                if usar_unique:
                    # Mostrar columnas disponibles
                    columnas_nombres = [snake for snake, _, _ in columns]
                    unique_cols = questionary.checkbox(
                        "  Selecciona columna(s) que identifican únicamente:",
                        choices=columnas_nombres,
                        style=custom_style
                    ).ask()

                    if not unique_cols:
                        print("  ⚠️  No se seleccionaron columnas, no se usará UNIQUE constraint")

                crear_dimension_edw(connection, dsa_table, nombre_dim, columns, unique_columns=unique_cols)
                tablas_creadas[nombre_dim] = None
            else:
                # Preguntar con qué tabla se relaciona
                tabla_padre = questionary.select(
                    "  ¿Con qué dimensión se relaciona?",
                    choices=list(tablas_creadas.keys()) + ['Ninguna'],
                    style=custom_style
                ).ask()

                if tabla_padre and tabla_padre != 'Ninguna':
                    fk_col = f"{tabla_padre}_id"

                    # Preguntar columnas para hacer match
                    print(f"\n  Configurando relación con '{tabla_padre}'...")

                    # Obtener columnas de la tabla padre
                    with connection.cursor() as cursor:
                        cursor.execute(sql.SQL("""
                            SELECT column_name
                            FROM information_schema.columns
                            WHERE table_schema = 'edw' AND table_name = %s
                            AND column_name NOT IN ('id', 'fecha_carga_etl')
                            ORDER BY ordinal_position
                        """), [tabla_padre])
                        columnas_padre = [row[0] for row in cursor.fetchall()]

                    # Obtener columnas de DSA (todas, no solo las del grupo)
                    with connection.cursor() as cursor:
                        cursor.execute(sql.SQL("""
                            SELECT column_name
                            FROM information_schema.columns
                            WHERE table_schema = 'dsa' AND table_name = %s
                            ORDER BY ordinal_position
                        """), [dsa_table])
                        columnas_dsa = [row[0] for row in cursor.fetchall()]

                    # Preguntar columna de match en tabla padre
                    col_padre_match = questionary.select(
                        f"  Columna en '{tabla_padre}' para hacer match:",
                        choices=columnas_padre,
                        style=custom_style
                    ).ask()

                    # Preguntar columna de match en DSA
                    col_dsa_match = questionary.select(
                        f"  Columna en DSA que contiene el valor de '{col_padre_match}':",
                        choices=columnas_dsa,
                        style=custom_style
                    ).ask()

                    print(f"  → Match: dsa.{col_dsa_match} = edw.{tabla_padre}.{col_padre_match}")

                    crear_dimension_edw(
                        connection, dsa_table, nombre_dim, columns,
                        fk_tabla=tabla_padre,
                        fk_columna=fk_col,
                        fk_match_column=col_dsa_match,
                        fk_parent_match_column=col_padre_match
                    )
                    tablas_creadas[nombre_dim] = tabla_padre
                else:
                    crear_dimension_edw(connection, dsa_table, nombre_dim, columns)
                    tablas_creadas[nombre_dim] = None

        # 8. Resumen
        print(f"\n{'='*80}")
        print("✓ NORMALIZACIÓN EDW COMPLETADA")
        print(f"{'='*80}\n")
        print("Dimensiones creadas:")
        for tabla, padre in tablas_creadas.items():
            if padre:
                print(f"  • edw.{tabla} (FK → {padre})")
            else:
                print(f"  • edw.{tabla} (independiente)")

    except Exception as error:
        print(f"\n❌ Error: {error}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        if connection:
            connection.close()


if __name__ == "__main__":
    normalizar_interactivo()
