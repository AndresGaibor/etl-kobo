# ETL KoboToolbox a PostgreSQL (Python)

Script para migrar datos desde KoboToolbox a una base de datos PostgreSQL.

## Requisitos

- Python 3.7+
- Dependencias listadas en `requirements.txt`

## Instalación

1. Crear entorno virtual:
```bash
python -m venv venv
source venv/bin/activate  # En Windows: venv\Scripts\activate
```

2. Instalar dependencias:
```bash
pip install -r requirements.txt
```

## Configuración

1. Copiar el archivo de ejemplo:
```bash
cp .env.example .env
```

2. Editar `.env` con tus credenciales:
```
# KoboToolbox API Configuration
KOBOTOOLBOX_TOKEN=your_token_here
ASSET_UID=your_asset_uid_here

# PostgreSQL Database Configuration
DB_HOST=your_host
DB_PORT=5432
DB_USER=your_user
DB_PASSWORD=your_password
DB_DATABASE=your_database
```

## Uso

Ejecutar el script:
```bash
python main.py
```

El script:
1. Obtiene datos de la API de KoboToolbox
2. Analiza la estructura de los datos
3. Crea automáticamente una tabla PostgreSQL con el esquema inferido
4. Inserta todos los registros

## Características

- **Inferencia automática de tipos**: Detecta automáticamente el tipo de dato apropiado para cada campo
- **Manejo de columnas con caracteres especiales**: Soporta nombres de columna con `/` y otros caracteres especiales
- **Manejo de datos JSON**: Convierte automáticamente objetos y arrays a formato JSONB
- **Evita duplicados**: Usa `ON CONFLICT DO NOTHING` para no insertar duplicados
- **Manejo de errores**: Muestra información detallada si ocurre un error

## Estructura de la tabla creada

La tabla se nombrará como `kobo_{ASSET_UID}` y contendrá:
- Campos estándar de KoboToolbox (`_id`, `_uuid`, `_submission_time`, etc.)
- Todos los campos adicionales del formulario con tipos inferidos automáticamente
- Campos especiales como `_attachments`, `_geolocation`, `_tags`, `_notes`, `_validation_status` como JSONB