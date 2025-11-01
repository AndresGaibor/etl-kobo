#!/usr/bin/env python3
"""
ETL KoboToolbox a PostgreSQL
Punto de entrada principal del sistema ETL
"""

import dsa_etl


def main():
    """
    Ejecuta el proceso ETL completo
    Actualmente ejecuta raw_etl para cargar datos sin transformaciones
    """
    print("=== Iniciando ETL de KoboToolbox a PostgreSQL ===\n")

    # Ejecutar carga DSA de datos
    dsa_etl.migrate_kobo_to_postgres()

    print("\n=== ETL completado ===")


if __name__ == "__main__":
    main()