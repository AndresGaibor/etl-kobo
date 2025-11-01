#!/usr/bin/env python3
"""
Script de diagnóstico para verificar la respuesta de la API y validar cómo se guardan los datos
"""

import json
from config import KOBOTOOLBOX_TOKEN, ASSET_UID
from api import get_survey_metadata, get_survey_submissions


def diagnosticar_api():
    """Diagnóstico completo de la API y estructura de datos"""

    print("="*80)
    print("DIAGNÓSTICO DE API DE KOBOTOOLBOX")
    print("="*80)

    # 1. Obtener metadata
    print("\n[1] METADATA DE LA ENCUESTA")
    print("-"*80)
    try:
        metadata = get_survey_metadata(ASSET_UID, KOBOTOOLBOX_TOKEN)
        print(f"Nombre: {metadata.get('name')}")
        print(f"UID: {metadata.get('uid')}")
        print(f"Fecha creación: {metadata.get('date_created')}")
        print(f"Fecha modificación: {metadata.get('date_modified')}")
        print(f"\nMetadata completa:")
        print(json.dumps(metadata, indent=2, ensure_ascii=False))
    except Exception as e:
        print(f"ERROR obteniendo metadata: {e}")
        return

    # 2. Obtener submissions
    print("\n[2] SUBMISSIONS")
    print("-"*80)
    try:
        submissions = get_survey_submissions(ASSET_UID, KOBOTOOLBOX_TOKEN)
        print(f"Total de submissions: {len(submissions)}")

        if not submissions:
            print("No hay submissions disponibles")
            return

        # 3. Analizar primera submission
        print("\n[3] ANÁLISIS DE LA PRIMERA SUBMISSION")
        print("-"*80)
        first = submissions[0]
        print(f"Total de campos: {len(first)}")
        print("\nCampos y tipos de datos:")

        for key, value in first.items():
            tipo = type(value).__name__

            # Análisis detallado por tipo
            if value is None:
                detalle = "NULL"
            elif isinstance(value, dict):
                if len(value) == 0:
                    detalle = "{} (objeto vacío)"
                else:
                    detalle = f"{{}} con {len(value)} keys: {list(value.keys())[:3]}"
            elif isinstance(value, list):
                if len(value) == 0:
                    detalle = "[] (array vacío)"
                else:
                    # Verificar si todos son null
                    all_null = all(item is None for item in value)
                    if all_null:
                        detalle = f"[null × {len(value)}] (array solo con nulls)"
                    else:
                        detalle = f"[...] con {len(value)} elementos"
            elif isinstance(value, str):
                if len(value) == 0:
                    detalle = "'' (string vacío)"
                else:
                    detalle = f"'{value[:50]}...'" if len(value) > 50 else f"'{value}'"
            else:
                detalle = str(value)

            print(f"  {key:40} | {tipo:10} | {detalle}")

        # 4. Primera submission completa
        print("\n[4] PRIMERA SUBMISSION COMPLETA (JSON)")
        print("-"*80)
        print(json.dumps(first, indent=2, ensure_ascii=False))

        # 5. Análisis de campos problemáticos
        print("\n[5] ANÁLISIS DE CAMPOS POTENCIALMENTE PROBLEMÁTICOS")
        print("-"*80)

        campos_vacios = []
        campos_null_arrays = []
        campos_empty_objects = []
        campos_empty_arrays = []

        for key, value in first.items():
            if value is None:
                campos_vacios.append(key)
            elif isinstance(value, dict) and len(value) == 0:
                campos_empty_objects.append(key)
            elif isinstance(value, list):
                if len(value) == 0:
                    campos_empty_arrays.append(key)
                elif all(item is None for item in value):
                    campos_null_arrays.append(key)

        print(f"\nCampos NULL: {len(campos_vacios)}")
        for c in campos_vacios:
            print(f"  - {c}")

        print(f"\nCampos con {{}} (objetos vacíos): {len(campos_empty_objects)}")
        for c in campos_empty_objects:
            print(f"  - {c}")

        print(f"\nCampos con [] (arrays vacíos): {len(campos_empty_arrays)}")
        for c in campos_empty_arrays:
            print(f"  - {c}")

        print(f"\nCampos con [null, null, ...] (arrays solo nulls): {len(campos_null_arrays)}")
        for c in campos_null_arrays:
            print(f"  - {c}")

        # 6. Verificar consistencia en todas las submissions
        if len(submissions) > 1:
            print("\n[6] VERIFICACIÓN DE CONSISTENCIA (todas las submissions)")
            print("-"*80)
            print(f"Analizando {len(submissions)} submissions...")

            # Campos que SIEMPRE están vacíos en todas las submissions
            siempre_vacios = set(campos_vacios)

            for submission in submissions[1:]:
                campos_vacios_actual = {k for k, v in submission.items() if v is None}
                siempre_vacios &= campos_vacios_actual

            print(f"\nCampos que están NULL en TODAS las submissions: {len(siempre_vacios)}")
            for c in sorted(siempre_vacios):
                print(f"  - {c}")

    except Exception as e:
        print(f"ERROR obteniendo submissions: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    diagnosticar_api()
