# Spotify's "New Releases" Playlist: ETL Project

## Descripcion
-------------------
Este proyecto consiste en un ETL utilizando lenguaje Python, la API de Spotify y AWS Redshift para recopilar datos de la lista de reproducción "Realese-radar" que se actualiza periodicamente segun los ultimos 50 lanzamientos de albums subidos en dicha plataforma en todo el mundo.

Los componentes principales de este proyecto son las dos clases contructoras DataManager y Dataconn situadas en el MODULO ETL_manager.py, cada una de las cuales ejecuta a través de funciones los pasos de extracción, transformación, conexion con base de datos y carga de datos en la misma, respectivamente. 
Todo esto orquestado desde un archivo main.py.

### La función Extract (data_extract)
------------------------------
La forma en que interactuamos con la API de Spotify es mediante el uso de la biblioteca SPOTIPY y ayuda principalmente con la autenticación de la API. Una vez que se han extraído los datos, se almacenan en un Diccionario de python.
Se puede ocnseguir las credenciales de la API de Spotyfy en el siguiente link
Generate your Spotify API access keys here: https://developer.spotify.com

### La función Transform (data_transform)
--------------------------------
La función instancia y estructura en un Dataframe de  PANDAS los datos obtenidos en la Extraccion ,separa la totalidad de los datos JSON de Spotify en conjuntos de datos lógicos para los datos de Álbum y Artista y se realiza un limpoeza y trasformacion para los conjunto de datos necesarios segun el caso.

### La función Load  (upload_data)
 Después de que los datos del álbum, artista se hayan transformado ...

#### Dependencias y librerias 
Todas las librerias y dependencias necesiarias estan en el archivo __requirements.txt__