#!/usr/bin/env python3
"""
Utilidades compartidas para el ETL de KoboToolbox
"""

import re
from typing import Any


def clean_survey_name(survey_name: str) -> str:
    """Limpia el nombre de la encuesta para usarlo como nombre de tabla"""
    # Convertir a minúsculas y reemplazar espacios/caracteres especiales con guiones bajos
    clean_name = re.sub(r'[^\w\s-]', '', survey_name.lower())
    clean_name = re.sub(r'[-\s]+', '_', clean_name)
    return clean_name


def inferir_tipo_pg(value: Any) -> str:
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
