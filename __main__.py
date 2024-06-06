
import os
from modules import DataManager, DataConn
from dotenv import load_dotenv

load_dotenv()

user_credentials = {
    "REDSHIFT_USERNAME" : os.getenv('REDSHIFT_USERNAME'),
    "REDSHIFT_PASSWORD" : os.getenv('REDSHIFT_PASSWORD'),
    "REDSHIFT_HOST" : os.getenv('REDSHIFT_HOST'),
    "REDSHIFT_PORT" : os.getenv('REDSHIFT_PORT', '5439'),
    "REDSHIFT_DBNAME" : os.getenv('REDSHIFT_DBNAME')
}

schema = "mauroalberelli_coderhouse"
data_conn = DataConn(user_credentials, schema)
SpotifyApi = DataManager()

try:
    data_conn.get_conn()
    data=SpotifyApi.data_transform()
    data_conn.upload_data(data,'stage_spotify_new_releases_table')
finally:
    data_conn.close_conn()