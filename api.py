#!/usr/bin/env python3
"""
KoboToolbox API - Funciones para interactuar con la API de KoboToolbox
"""

from typing import Dict, Any, List
import requests


def get_survey_metadata(asset_uid: str, token: str) -> Dict[str, Any]:
    """Obtiene metadata de la encuesta desde la API de KoboToolbox"""
    print("Obteniendo información de la encuesta...")
    asset_url = f"https://kf.kobotoolbox.org/api/v2/assets/{asset_uid}/"
    headers = {
        "Authorization": f"Token {token}",
        "Accept": "application/json"
    }

    response = requests.get(asset_url, headers=headers)
    response.raise_for_status()

    return response.json()


def get_survey_submissions(asset_uid: str, token: str) -> List[Dict[str, Any]]:
    """Obtiene todas las submissions de la encuesta desde la API de KoboToolbox"""
    print("Obteniendo datos de KoboToolbox...")
    api_url = f"https://kf.kobotoolbox.org/api/v2/assets/{asset_uid}/data/"
    headers = {
        "Authorization": f"Token {token}",
        "Accept": "application/json"
    }

    response = requests.get(api_url, headers=headers)
    response.raise_for_status()

    try:
        data = response.json()
    except requests.exceptions.JSONDecodeError as e:
        print(f"Error decodificando JSON: {e}")
        print(f"Status code: {response.status_code}")
        print(f"Response headers: {response.headers}")
        print(f"Response text (primeros 500 chars): {response.text[:500]}")
        raise

    return data.get('results', [])
