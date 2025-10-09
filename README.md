# ETL KoboToolbox a PostgreSQL (Python)

Script para migrar datos desde KoboToolbox a una base de datos PostgreSQL.

## Obtencion de la API Key
Una vez iniciada sesion de Kobo, ir a la siguiente pagina: https://kf.kobotoolbox.org/#/account/security

![Pagina de seguridad de Kobo](https://i.imgur.com/A9ByplD.png)

En la pagina de datos del formulario obtenemos **ASSET ID** de la URL, el asset id se ve mas o menos asi: **b8XkLmjprAFcdCiYMz81Tj**
![Datos de la encuesta](https://i.imgur.com/Mi2HdPO.png)

Damos clic en Display y copiamos la API Key

## Neon (PostgreSQL Online)
Crea una cuenta en https://neon.com/

1. Creamos un nuevo proyecto
![Consola de Neon Tech](https://i.imgur.com/ryxVYKu.png)

2. Ponemos un nombre al proyecto y lo creamos dando clic en "Create"
![Formulario de creacion de proyecto](https://i.imgur.com/DdBCt6G.png)

3. Pagina del proyecto
![Dashboard del proyecto](https://i.imgur.com/h28LDoa.png)

4. Obtenemos las credenciales de la base de datos dando clic en "Connect" 
![Credenciales de postgres](https://i.imgur.com/vPTeSWn.png)

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

Las credenciales de Postgres las obtenemos de la [seccion de Neon](##Neon (PostgreSQL Online))
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

## Revisar los datos
Para revisar los datos de la encuesta en **Tables** y ahi esta la tabla generada por el script, kobo_(ASSET_ID)
![Datos de la encuesta](https://i.imgur.com/sXBvZtr.png)

Podemos revisar la base de datos en otro gestor o visualizador de base de datos para postgres.

## Ejemplo en DBBeaver
1. Creamos una nueva conexion PostgreSQL
![Nueva conexion](https://i.imgur.com/f82XzcG.png)

De los datos que obtuvimos en la [seccion anterior de Neon](##Neon (PostgreSQL Online)) ponemos las credenciales y damos a Finish
![Configuracion de la conexion](https://i.imgur.com/6JuiPGz.png)
2. Visualizacion de los datos
![Visualizacion de los datos](https://i.imgur.com/IWFXKKn.png)