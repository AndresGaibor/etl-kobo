# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Python ETL system that migrates survey data from KoboToolbox to PostgreSQL. The project is structured with:
- **`main.py`**: Entry point that orchestrates the ETL process
- **`raw_etl.py`**: Loads raw data from Kobo to PostgreSQL without transformations

## Environment Setup

Create and activate virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
```

Install dependencies:
```bash
pip install -r requirements.txt
```

Configuration requires a `.env` file (copy from `.env.example`):
- `KOBOTOOLBOX_TOKEN`: API token from KoboToolbox account security page
- `ASSET_UID`: Survey identifier extracted from KoboToolbox form URL
- `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `DB_DATABASE`: PostgreSQL connection credentials

## Running the ETL

Execute the migration:
```bash
python main.py
```

The script connects with SSL mode (`sslmode='require'`) which is configured for cloud PostgreSQL providers like Neon.

## Code Architecture

### Main Modules

**`main.py`**
- Entry point that calls `raw_etl.migrate_kobo_to_postgres()`
- Designed to be extended with additional ETL stages in the future

**`raw_etl.py`**
- Loads data **completely raw** from KoboToolbox API
- Column names are preserved exactly as they come from Kobo (no transformations)
- Process flow:
  1. Fetch submissions from KoboToolbox API
  2. Infer PostgreSQL schema from data types
  3. Create schema `dsa` if not exists
  4. Create table `dsa.kobo_{ASSET_UID}`
  5. Insert all records with `ON CONFLICT DO NOTHING`

### Type Inference System (`inferir_tipo_pg`)
Automatically maps Python types to PostgreSQL types:
- ISO 8601 timestamps → `TIMESTAMP` (pattern: `^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}`)
- Geographic coordinates (lat,long) → `POINT` (pattern: `^-?\d+(\.\d+)?,-?\d+(\.\d+)?$`)
- Nested objects/arrays → `JSONB`
- Booleans → `BOOLEAN`
- Integers → `INTEGER`
- Floats → `NUMERIC`
- Everything else → `TEXT`

### Data Insertion Strategy
- Uses `ON CONFLICT DO NOTHING` for idempotent operations
- JSONB values are wrapped with `psycopg2.extras.Json()` for proper serialization
- Individual error handling per submission with detailed logging
- All records committed individually (could be optimized with batch commits in the future)

## Database Schema Location

All tables are created in the `dsa` schema (created if not exists). Table names follow the pattern `dsa.kobo_{ASSET_UID}`.

## Design Notes

The raw ETL preserves original column names from Kobo without any transformations (no snake_case conversion, no prefix removal). This allows for future transformation stages to be added in separate modules if needed.
