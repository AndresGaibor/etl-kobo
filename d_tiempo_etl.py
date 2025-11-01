#!/usr/bin/env python3
"""
ETL Dimensión Tiempo
Genera calendario del año actual con estructura:
- id_tiempo (YYYYMMDD)
- anio
- mes_num
- mes_nombre
- sem_num (1=PRIMER SEM, 2=SEGUNDO SEM)
- sem_nombre
- dia
"""

from datetime import date, timedelta
import psycopg2
from psycopg2 import sql

from config import DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_DATABASE


def obtener_nombre_mes(mes: int) -> str:
    """Retorna nombre del mes en español"""
    meses = {
        1: 'Enero', 2: 'Febrero', 3: 'Marzo', 4: 'Abril',
        5: 'Mayo', 6: 'Junio', 7: 'Julio', 8: 'Agosto',
        9: 'Septiembre', 10: 'Octubre', 11: 'Noviembre', 12: 'Diciembre'
    }
    return meses[mes]


def obtener_semestre(mes: int) -> tuple:
    """Retorna (número, nombre) del semestre"""
    if mes <= 6:
        return (1, 'PRIMER SEM')
    else:
        return (2, 'SEGUNDO SEM')


def generar_calendario(anio: int):
    """Genera todos los días del año"""
    inicio = date(anio, 1, 1)
    fin = date(anio, 12, 31)

    calendario = []
    dia_actual = inicio

    while dia_actual <= fin:
        id_tiempo = int(dia_actual.strftime('%Y%m%d'))
        mes_num = dia_actual.month
        sem_num, sem_nombre = obtener_semestre(mes_num)

        registro = {
            'id_tiempo': id_tiempo,
            'anio': dia_actual.year,
            'mes_num': mes_num,
            'mes_nombre': obtener_nombre_mes(mes_num),
            'sem_num': sem_num,
            'sem_nombre': sem_nombre,
            'dia': dia_actual
        }

        calendario.append(registro)
        dia_actual += timedelta(days=1)

    return calendario


def crear_dimension_tiempo():
    """Crea y puebla la dimensión d_tiempo en EDW"""
    connection = None

    try:
        # Año actual
        anio_actual = date.today().year

        print(f"{'='*80}")
        print(f"GENERANDO DIMENSIÓN d_tiempo PARA EL AÑO {anio_actual}")
        print(f"{'='*80}\n")

        # Conectar a PostgreSQL
        print("Conectando a PostgreSQL...")
        connection = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_DATABASE,
            sslmode='require'
        )

        with connection.cursor() as cursor:
            # Crear esquema edw si no existe
            print("Creando esquema 'edw' si no existe...")
            cursor.execute("CREATE SCHEMA IF NOT EXISTS edw")
            connection.commit()
            print("  ✓ Esquema 'edw' verificado")

            # Drop tabla si existe
            print("\nEliminando tabla anterior si existe...")
            cursor.execute("DROP TABLE IF EXISTS edw.d_tiempo CASCADE")
            connection.commit()
            print("  ✓ Tabla anterior eliminada")

            # Crear tabla
            print("\nCreando tabla edw.d_tiempo...")
            create_query = """
                CREATE TABLE edw.d_tiempo (
                    id_tiempo INTEGER PRIMARY KEY,
                    anio INTEGER NOT NULL,
                    mes_num INTEGER NOT NULL,
                    mes_nombre VARCHAR(10) NOT NULL,
                    sem_num INTEGER NOT NULL,
                    sem_nombre VARCHAR(12) NOT NULL,
                    dia DATE NOT NULL
                )
            """
            cursor.execute(create_query)
            connection.commit()
            print("  ✓ Tabla creada")

            # Generar calendario
            print(f"\nGenerando calendario del año {anio_actual}...")
            calendario = generar_calendario(anio_actual)
            print(f"  ✓ {len(calendario)} días generados")

            # Insertar datos
            print("\nInsertando datos...")
            insert_query = """
                INSERT INTO edw.d_tiempo
                (id_tiempo, anio, mes_num, mes_nombre, sem_num, sem_nombre, dia)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """

            for registro in calendario:
                cursor.execute(insert_query, (
                    registro['id_tiempo'],
                    registro['anio'],
                    registro['mes_num'],
                    registro['mes_nombre'],
                    registro['sem_num'],
                    registro['sem_nombre'],
                    registro['dia']
                ))

            connection.commit()
            print(f"  ✓ {len(calendario)} registros insertados")

            # Verificar
            cursor.execute("SELECT COUNT(*) FROM edw.d_tiempo")
            total = cursor.fetchone()[0]

            cursor.execute("SELECT MIN(dia), MAX(dia) FROM edw.d_tiempo")
            min_dia, max_dia = cursor.fetchone()

            print(f"\n{'='*80}")
            print("✓ DIMENSIÓN d_tiempo CREADA EXITOSAMENTE")
            print(f"{'='*80}")
            print(f"\nEstadísticas:")
            print(f"  • Total registros: {total}")
            print(f"  • Rango: {min_dia} a {max_dia}")
            print(f"  • Semestres: 2 (PRIMER SEM, SEGUNDO SEM)")

            # Mostrar primeros 5 registros
            print(f"\nPrimeros 5 registros:")
            cursor.execute("""
                SELECT id_tiempo, anio, mes_nombre, sem_nombre, dia
                FROM edw.d_tiempo
                ORDER BY id_tiempo
                LIMIT 5
            """)

            print(f"\n  {'id_tiempo':<12} {'Año':<6} {'Mes':<12} {'Semestre':<14} {'Día'}")
            print(f"  {'-'*12} {'-'*6} {'-'*12} {'-'*14} {'-'*10}")
            for row in cursor.fetchall():
                print(f"  {row[0]:<12} {row[1]:<6} {row[2]:<12} {row[3]:<14} {row[4]}")

    except Exception as error:
        print(f"\n❌ Error: {error}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        if connection:
            connection.close()
            print("\nConexión a PostgreSQL cerrada.")


if __name__ == "__main__":
    crear_dimension_tiempo()
