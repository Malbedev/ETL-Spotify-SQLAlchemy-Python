
import os
from modules import DataManager, DataConn
from dotenv import load_dotenv

### El archivo __main__ ejecutara todo nuestro código desarrollado en el archivo ETL_manager,
### asignando los valores necesarios para la ejecución de cada método.

#Llamar al método de la libreria dotenv para obtener nustras variables de entorno 
load_dotenv()

#Agregar a un diccionario nuetras credenciales 
user_credentials = {
    "REDSHIFT_USERNAME" : os.getenv('REDSHIFT_USERNAME'),
    "REDSHIFT_PASSWORD" : os.getenv('REDSHIFT_PASSWORD'),
    "REDSHIFT_HOST" : os.getenv('REDSHIFT_HOST'),
    "REDSHIFT_PORT" : os.getenv('REDSHIFT_PORT', '5439'),
    "REDSHIFT_DBNAME" : os.getenv('REDSHIFT_DBNAME')
}

#Asignamos la veriables table y schema, con el nombre de la tabla y esquema correpondiente
table='stage_spotify_new_releases_table'
schema = "mauroalberelli_coderhouse"

#Intanciar las Clases
data_conn = DataConn(user_credentials, schema)
SpotifyApi = DataManager()

#Ejecutar métodos respetando el orden de prioridades
try:
    data_conn.get_conn()
    data_conn.create_table(table)
    data_conn.create_all_tables()
    data=SpotifyApi.data_transform()
    data_conn.upload_data(data,table)
finally:
    data_conn.close_conn()