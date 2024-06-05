
import os
import pandas as pd
import logging
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import datetime
from sqlalchemy import create_engine
from dotenv import load_dotenv
load_dotenv()

CLIENT_ID =os.getenv('CLIENT_ID')
CLIENT_SECRET =os.getenv('CLIENT_SECRET')

logging.basicConfig(
    filename='app.log',
    filemode='a',
    format='%(asctime)s: %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO)
    
class DataConn:
    def __init__(self, config: dict,schema: str):
        self.config = config
        self.schema = schema
        self.db_engine = None


    def get_conn(self):
        username = self.config.get('REDSHIFT_USERNAME')
        password = self.config.get('REDSHIFT_PASSWORD')
        host = self.config.get('REDSHIFT_HOST')
        port = self.config.get('REDSHIFT_PORT', '5439')
        dbname = self.config.get('REDSHIFT_DBNAME')

        # Construct the connection URL
        connection_url = f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{dbname}"
        self.db_engine = create_engine(connection_url)

        try:
            with self.db_engine.connect() as connection:
                result = connection.execute('SELECT 1;')
            if result:
                logging.info("Connection created")
                return
        except Exception as e:
            logging.error(f"Failed to create connection: {e}")
            raise

    def upload_data(self, data: pd.DataFrame, table: str):
        try:
            data.to_sql(
                table,
                con=self.db_engine,
                schema=self.schema,
                if_exists='append',
                index=False
            )

            logging.info(f"Data from the DataFrame has been uploaded to the {self.schema}.{table} table in Redshift.")
        except Exception as e:
            logging.error(f"Failed to upload data to {self.schema}.{table}: {e}")
            raise

    def close_conn(self):
        if self.db_engine:
            self.db_engine.dispose()
            logging.info("Connection to Redshift closed.")
        else:
            logging.warning("No active connection to close.")

class DataManager:

    def __init__(self):
        self.data = None

    def data_extract(self):
       
        try:
            logging.info("Extracting data from Api ...")
            client_id =CLIENT_ID
            client_secret =CLIENT_SECRET
            client_credentials_manager = SpotifyClientCredentials(client_id, client_secret)
            sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
            results = sp.new_releases(limit=50)

            data = {'Id': [],'Album_type': [], 'Album_name': [],'Artis_name': [],'Total_tracks': [],'Album_genre':[], 'Realese_date': [], 'Album_img': [],'Album_link':[],'Artist_link':[],'Load_date': []}
            for album in results['albums']['items']:
                id = album['id']
                album_type = album['album_type']
                album_name = album['name']
                artist_name = album['artists'][0]['name']
                total_tracks = album['total_tracks']
                artist_id = album['artists'][0]['id']
                album_genre = sp.artist(artist_id)['genres']
                realese_date = album['release_date']
                album_img = album['images'][0]['url'] #imagen de album
                album_link = album['external_urls']['spotify']
                artist_link = album['artists'][0]['external_urls']['spotify']

                #Quitar las comillas 
                album_name = album_name.replace("'", "")
                #Separar el género por coma
                album_genre = ', '.join(album_genre)
                #Agregamos una fecha de modificacion 
                now = datetime.date.today().strftime('%Y-%m-%d')

                data['Id'].append(id)
                data['Album_type'].append(album_type)
                data['Album_name'].append(album_name)
                data['Artis_name'].append(artist_name)
                data['Total_tracks'].append(total_tracks)
                data['Album_genre'].append(album_genre)
                data['Realese_date'].append(realese_date)
                data['Album_img'].append(album_img)
                data['Album_link'].append(album_link)
                data['Artist_link'].append(artist_link)
                data['Load_date'].append(now)

            return data
            
        except Exception as e:
            logging.error(e)
        finally:
            logging.warn("Check the data format")

    def data_transform(self):
        try:
            logging.info('Data to Pandas Dataframe')
            data = self.data_extract()
            df=pd.DataFrame(data)
           
            
            return df
        
        except Exception as e:
            logging.error(e)

