#!/usr/bin/env python3
"""
Configuración del ETL - Variables de entorno
"""

import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# KoboToolbox Configuration
KOBOTOOLBOX_TOKEN = os.getenv('KOBOTOOLBOX_TOKEN')
ASSET_UID = os.getenv('ASSET_UID')

# PostgreSQL Database Configuration
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_DATABASE = os.getenv('DB_DATABASE')
