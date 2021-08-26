import pandas as pd
import time
from pathlib import Path
import sshtunnel
import pymongo
from pymongo import MongoClient
client = MongoClient()

class DataBaseConnection():
    """
    Conexión a la Mongo (Local o Remoto).
    """
    def __init__(self, server_name='localhost'):
        # Por default me conecto a localhost
        print(f"\n --- Conectando a {server_name} --- ")
        # Estaría bueno aplicar variables de entorno.
        if server_name == 'misato':
            ip = '168.61.73.89'
            port = 31415
            user = 'bb'
            password = 'KLM2012a'
            bind=('127.0.0.1', 27017)
        # Estaría bueno aplicar variables de entorno.
        if server_name == 'kaji':
            ip = '104.42.112.134'
            port = 31415
            user = 'bb'
            password = 'KLM2012a'
            bind=('127.0.0.1', 27017)

        if server_name == 'localhost':
            hostMongo  = 'mongodb://localhost:27017/'
            self.db = MongoClient(hostMongo)             
        else:
            base_path = Path(__file__).parent.parent.parent
            file_path = (base_path / server_name)
            __file = str(file_path)

            sshtunnel.SSH_TIMEOUT = sshtunnel.TUNNEL_TIMEOUT = 10.0

            server = sshtunnel.open_tunnel(
                (ip, port),
                ssh_username=user,
                ssh_pkey=__file,
                ssh_private_key_password=password,
                remote_bind_address=bind
            )
            server.start()
            time.sleep(11)
            self.db = pymongo.MongoClient('127.0.0.1', server.local_bind_port)

    def connection(self):
        return self.db

class ConsultsDB(): #MongoConnection
    """
    Realizar consultas una vez conectado a Mongo.
    """
    def __init__(self, server='localhost'):
        self.server = DataBaseConnection(server).connection()
        self.db = None
        self.db_name = None
        self.collection = None

        # BBDD y colecciones por default.
        if server == 'misato' or server == 'kaji':
            self.db_name = 'business'
        else:
            self.db_name = 'titan'
        if server == 'localhost' or server == 'kaji':
            self.collection = 'titanScraping'
        else:
            self.collection = 'apiPresence'          

        self.db = self.server[self.db_name]
        self.collection_names = self.db.collection_names()

        print(f" --- BBDD -> {self.db_name}.")

    def find_mongo(self, query, collection=None):
        """Método para hacer la consulta a mongo.
        Estaría bueno a futuro mejorar la validación.
        """
        if collection:
            self.collection = collection
            if not collection in self.collection_names:
                print(f'--- ERROR: No existe la collection \"{collection}\" ')
                return None
            print(f" --- Collection -> {self.collection}.")
        else:
            print(f" --- Collection -> {self.collection}. (Por default)")
        db_collection = self.db[self.collection]
    
        df = pd.DataFrame(list(db_collection.find(query)))

        self.server.close()

        return df 

class GetDataFrame():
    """
    Clase que permite importar DataFrames de localhost, kaji y misato.
    """
    @staticmethod
    def local(query, collection=None):
        """Devuelve un df de localhost según la query que se ingrese.
        La query es un diccionario, es decir, una consulta a Mongo.

        Args:
            query (dict): La consulta
            collection (str, optional): Por default usa la collection
            "titanScraping". Defaults to None.

        Returns:
            DataFrame: Devuelve un dataframe.
        """
        return ConsultsDB().find_mongo(query, collection)        

    @staticmethod
    def kaji(query, collection=None):
        """Devuelve un df de kaji según la query que se ingrese.
        La query es un diccionario, es decir, una consulta a Mongo.

        Args:
            query (dict): La consulta
            collection (str, optional): Por default usa la collection
            "titanScraping". Defaults to None.

        Returns:
            DataFrame: Devuelve un dataframe.
        """
        return ConsultsDB('kaji').find_mongo(query, collection)        

    @staticmethod
    def misato(query, collection=None):
        """Devuelve un df de misato según la query que se ingrese.
        La query es un diccionario, es decir, una consulta a Mongo.

        Args:
            query (dict): La consulta
            collection (str, optional): Por default usa la collection
            "apiPresence". Defaults to None.

        Returns:
            DataFrame: Devuelve un dataframe.
        """
        return ConsultsDB('misato').find_mongo(query, collection)

class Misato():
    """
    Devuelve un DataFrame de las colecciones más importantes de Misato.
    """
    @staticmethod
    def last_update(query):
        return GetDataFrame.misato(query, collection='last_update')        
    @staticmethod
    def updateStats(query):
        return GetDataFrame.misato(query, collection='updateStats')
    @staticmethod
    def apiPlatforms(query):
        return GetDataFrame.misato(query, collection='apiPlatforms')
    @staticmethod
    def updateDups(query):
        return GetDataFrame.misato(query, collection='updateDups')
    @staticmethod
    def titanLog(query):
        return GetDataFrame.misato(query, collection='titanLog')
    @staticmethod
    def updateLog(query):
        return GetDataFrame.misato(query, collection='updateLog')
    @staticmethod
    def apiPresence(query):
        return GetDataFrame.misato(query, collection='apiPresence')
    @staticmethod
    def apiPresenceEpisodes(query):
        df = GetDataFrame.misato(query, collection='apiPresence')
        series = df['Type'] == 'serie'
        df_series = df[series]

        series_dict = df_series['Episodes'].to_dict()

        # Lista de indices donde los contenidos son series.
        list_index = list(series_dict.keys())

        # Creo df vacío a concatenar.
        import pandas as pd
        df_final = pd.DataFrame()
        for index in list_index:
            df_nuevo = pd.DataFrame(series_dict[index]).set_index(['ParentId'])
            df_final = pd.concat([df_final, df_nuevo])
        
        return df_final