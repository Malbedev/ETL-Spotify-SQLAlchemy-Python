
import os
from modules.Utilities import  hashing_data
import pandas as pd
import logging
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import datetime
from sqlalchemy import create_engine,Column,NVARCHAR,Date,Integer
from sqlalchemy.ext.declarative import declarative_base
from dotenv import load_dotenv

# Llamar al método de la libreria dotenv para obtener nuestras variables de entorno 
load_dotenv()



# Intanciar las claves de Spotify con nuestras variables de entorno
CLIENT_ID =os.getenv('CLIENT_ID')
CLIENT_SECRET =os.getenv('CLIENT_SECRET')

# Generer un logger para manejo de erroes e info de proccesos
logging.basicConfig(
    filename='app.log',
    filemode='a',
    format='%(asctime)s: %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO)

## Gererar la clase constructura para la conexión a la base de datos
# que admite como parametros un diccionario con las credenciales y un STring con el nombnre del schema ## 

class DataConn:
    def __init__(self, config: dict,schema: str):
        self.config = config
        self.schema = schema
        self.db_engine = None
        # Método de la Libreria SqlAlchemy que nos permite generar tablas, ver documentacion de la libreria
        self.Base= declarative_base()


    def get_conn(self):
        # Obtener las credenciales alojadas en el archivo .env
        username = self.config.get('REDSHIFT_USERNAME')
        password = self.config.get('REDSHIFT_PASSWORD')
        host = self.config.get('REDSHIFT_HOST')
        port = self.config.get('REDSHIFT_PORT', '5439')
        dbname = self.config.get('REDSHIFT_DBNAME')

        # Construir la url para luego conectar la base da datos con el módulo de SQlAlquemy y su método crate_emgine
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

    # Crear una tabla con SqlAlquemy según documentación de la librería, recibe un string con el nombre de la tabla
    def create_table(self,table:str):
        try: 
            logging.info("Creating table....")
            schema=self.schema
            class MyTable(self.Base):
                __tablename__= str(table)
                __table_args__ = {'schema':str(schema)}

                # Determinar la columnas y los tipos de datos correspondientes; y Asignar una Primary key.
                Id = Column(NVARCHAR(50),primary_key=True)
                Album_type = Column(NVARCHAR(50))
                Album_name= Column(NVARCHAR(50))
                Artist_name = Column(NVARCHAR(50))
                Total_tracks= Column(Integer)
                Album_genre  = Column(NVARCHAR(500))
                Realese_date = Column(Date)
                Album_img = Column(NVARCHAR(200))
                Album_link = Column(NVARCHAR(200))
                Artist_link = Column(NVARCHAR(200))
                Load_date = Column(Date)

        except Exception as e:
            logging.warn(e)

    def create_all_tables(self):
        self.Base.metadata.create_all(self.db_engine)
        logging.info(f" Table created! ")

    # Cargar la data en nuestra tabla creada anteriormente
    ### Función que recibe com parámetros un Dataframe de pandas y un string con el nombre de la tabla ###
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

    # Cerrar la conexion
    def close_conn(self):
        if self.db_engine:
            self.db_engine.dispose()
            logging.info("Connection to Redshift closed.")
        else:
            logging.warning("No active connection to close.")


## Crear una clase para el manejo de los datos ## 
class DataManager:

    def __init__(self):
        self.data = None

    # Extraer la data de la Api

    ### Esta función nos devuelve los datos obtenidos de la api  
    # en una lista de diccionarios,con los valores requeridos ###
    def data_extract(self):
       
        try:
            logging.info("Extracting data from Api ...")
            client_id =CLIENT_ID
            client_secret =CLIENT_SECRET
            client_credentials_manager = SpotifyClientCredentials(client_id, client_secret)
            # Extraer los datos con la libreria Spotipy
            sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
            results = sp.new_releases(limit=50)

            # Crear la lista destino donde se alojaran los datos con las columnas que necesitamos
            data = {'Id': [],'Album_type': [], 'Album_name': [],'Artist_name': [],'Total_tracks': [],'Album_genre':[], 'Realese_date': [], 'Album_img': [],'Album_link':[],'Artist_link':[],'Load_date': []}
            # Iterar los elementos necesarios e instanciamos cada uno de ellos
            for album in results['albums']['items']:
                id = album['id']
                album_type = album['album_type']
                album_name = album['name']
                artist_name = album['artists'][0]['name']
                total_tracks = album['total_tracks']
                artist_id = album['artists'][0]['id']
                album_genre = sp.artist(artist_id)['genres']
                realese_date = album['release_date']
                album_img = album['images'][0]['url'] 
                album_link = album['external_urls']['spotify']
                artist_link = album['artists'][0]['external_urls']['spotify']

                # Quitar las comillas 
                album_name = album_name.replace("'", "")
                # Separar el género por coma
                album_genre = ', '.join(album_genre)
                # Agregamos una fecha de carga de datos 
                now = datetime.date.today().strftime('%Y-%m-%d')

                # Agregar los valores obtenidos a la lista creada anteriormente, según corresponda
                data['Id'].append(id)
                data['Album_type'].append(album_type)
                data['Album_name'].append(album_name)
                data['Artist_name'].append(artist_name)
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

    
    # Trasformar la data con pandas

    ### Esta función nos devuelve un Dataframe de pandas con las trasformaciones correpondientes###
    def data_transform(self):
        try:
            # Llamar a nuestra función de extracción e instanciar en una variable
            data = self.data_extract()
            logging.info('Data to Pandas Dataframe')
            # Instaciar una nueva variable con los datos obtenidos en un data frame de Pandas
            df = pd.DataFrame(data)

            # Limpiar los datos

            # Evitar que haya duplicados
            df.drop_duplicates(subset=['Album_name', 'Artist_name'], keep='first', inplace=True)
            # Reemplazar valores nulos o vacios en el campo Género por Desconocido
            df['Album_genre'].fillna('Desconocido', inplace=True)
            df.loc[df['Album_genre'] == '', 'Album_genre'] = 'Desconocido'
            # Verificar que la fecha se muestre en formato fecha 
            df['Realese_date'] = pd.to_datetime(df['Realese_date'], format='%Y-%m-%d')
            df['Realese_date'] = df['Realese_date'].dt.strftime('%Y-%m-%d')

            # Hashear información utilizando nuestra función hashing_data del módulo Utilities
            # Como ejemplo lo hacemos con el Id del albúm
            df['Id'] = df['Id'].apply(hashing_data)


            return(df)
            
        
        except Exception as e:
            logging.error(e)

