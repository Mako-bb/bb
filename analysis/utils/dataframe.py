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
        if server_name == 'localhost':
            hostMongo  = 'mongodb://localhost:27017/'
            self.db = MongoClient(hostMongo)

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