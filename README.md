# Spotify's "New Releases" : ETL Project

## Descripción
-------------------
Este proyecto consiste en un ETL utilizando lenguaje Python, la API de Spotify y AWS Redshift para recopilar datos de los álbums, que se actualiza periódicamente según los últimos 50 lanzamientos subidos en dicha plataforma en todo el mundo.

Los componentes principales de este proyecto son las dos clases contructoras DataManager y Dataconn situadas en el Módulo __ETL_manager.py__, cada una de las cuales ejecuta a través de funciones los pasos de extracción, transformación, conexión con base de datos y carga de datos en la misma, respectivamente. 
Todo esto orquestado desde un archivo __main.py__.

### La función Extract (data_extract)
------------------------------
La forma en que interactuamos con la API de Spotify es mediante el uso de la biblioteca __SPOTIPY__ y ayuda principalmente con la autenticación de la API.Una vez que se han extraído los datos, se almacenan en un diccionario de python.
Se puede conseguir las credenciales de la API de Spotyfy en el siguiente link
Generate your Spotify API access keys here: https://developer.spotify.com

### La función Transform (data_transform)
--------------------------------
La función instancia y estructura en un Dataframe de __PANDAS__ los datos obtenidos en el proceso de Extracción; separa la totalidad de los datos de Spotify en conjuntos de datos lógicos para los datos de Album y Artista y se realiza un limpieza y trasformación de los mismos, según el caso.

### La función Load  (upload_data)
Después de que los datos del álbum, artista se hayan transformado  estructurados en nuestro Dataframe, a travez de la libreria __SQLAlchemy__ realizamos la conexion con el motor de base de datos, y se carga la informacion en la tabla correspondiente en el Datawerehouse de __AWS-Redshift__.

#### Dependencias y librerías 
Todas las librerías y dependencias necesiarias que utilizamos en el proyecto estan en el archivo __requirements.txt__