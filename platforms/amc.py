import time
import requests
import pymongo
import re
#import json
from handle.replace import _replace
from common import config 
from datetime import datetime
from handle.mongo import mongo
from handle.datamanager import Datamanager
from updates.upload import Upload

class Amc():

    """
    Amc es una ott de Estados Unidos.

    DATOS IMPORTANTES:
    - Versión Final: Si.
    - VPN: No.
    - ¿Usa Selenium?: No.
    - ¿Tiene API?: Si.
    - ¿Usa BS4?: No.
    - ¿Se relaciona con scripts TP? No.
    - ¿Instanacia otro archivo de la carpeta "platforms"?: No.
    - ¿Cuanto demoró la ultima vez? 1 min
    - ¿Cuanto contenidos trajo la ultima vez? TS:163 TSE: 960 07/10/21

    OTROS COMENTARIOS:
        Contenia series sin episodios, se modificó el script para excluirlas.

    """
    def __init__(self, ott_site_uid, ott_site_country, type):
        self._config = config()['ott_sites'][ott_site_uid]
        self._platform_code = self._config['countries'][ott_site_country]
        # self._start_url             = self._config['start_url']
        self._created_at = time.strftime("%Y-%m-%d")
        self.mongo = mongo()
        self.titanPreScraping = config()['mongo']['collections']['prescraping']
        self.titanScraping = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodios = config()['mongo']['collections']['episode']
        self.skippedEpis = 0
        self.skippedTitles = 0
        ################# URLS  #################
        self._movies_url = self._config['movie_url']
        self._show_url = self._config['show_url']
        #Url para encontrar la información de los contenidos por separado
        self._format_url = self._config['format_url'] 
        self._episode_url = self._config['episode_url']
        self.testing = False
        self.sesion = requests.session()
        self.headers = {"Accept": "application/json",
                        "Content-Type": "application/json; charset=utf-8"}

        if type == 'return':
            '''
            Retorna a la Ultima Fecha
            '''
            params = {"PlatformCode": self._platform_code}
            lastItem = self.mongo.lastCretedAt(self.titanScraping, params)
            if lastItem.count() > 0:
                for lastContent in lastItem:
                    self._created_at = lastContent['CreatedAt']

            self._scraping()

        if type == 'scraping':
            self._scraping()

        if type == 'testing':
            self.testing = True
            self._scraping()

    def _scraping(self, testing=False):
        payloads = []
        payloads_series = []
        list_db_episodes = Datamanager._getListDB(self, self.titanScrapingEpisodios)
       # Definimos los links de las apis y con el Datamanager usamos la función _getJson
        episode_data = Datamanager._getJSON(self, self._episode_url)
        serie_data = Datamanager._getJSON(self,self._show_url )
        movie_data = Datamanager._getJSON(self, self._movies_url)

        self.get_payload_movies(movie_data)
        self.get_payload_series(serie_data)

    def get_payload_movies(self,content):
        payloads = []
        list_db = Datamanager._getListDB(self, self.titanScraping)
        data = content['data']['children']

        for item in data:
            if item['properties'].get('title'):
                if 'Movies' in item['properties']['title']:
                    movie_data = item
                    break
    
        for movie in movie_data['children']:
            self.get_title(movie)
            payload_movies = {
                "PlatformCode":  self._platform_code,
                "Title":         self.get_title(movie),
                "CleanTitle":    _replace(self.get_title(movie)),
                "OriginalTitle": None,
                "Type":          "Movies",
                "Year":          None,
                "Duration":      None,

                "Id":            self.get_id(movie),
                "Deeplinks": {

                    "Web":       self.get_deeplink(movie),
                    "Android":   None,
                    "iOS":       None,
                },
                "Synopsis":      self.get_synopsis(movie),
                "Image":         None,
                "Rating":        None,  # Important!
                "Provider":      None,
                "Genres":        self.get_genre(movie),  # Important!
                "Cast":          None,
                "Directors":     None,  # Important!
                "Availability":  None,  # Important!
                "Download":      None,
                "IsOriginal":    None,  # Important!
                "IsAdult":       None,  # Important!
                "IsBranded":     None,  # Important!
                # Obligatorio
                "Packages":      [{'Type': 'tv-everywhere'}],
                "Country":       None,
                "Timestamp":     datetime.now().isoformat(),  # Obligatorio
                "CreatedAt":     self._created_at,  # Obligatorio
        }
            Datamanager._checkDBandAppend(self, payload_movies, list_db, payloads)
            Datamanager._insertIntoDB(self, payloads, self.titanScraping)



    def get_title(self, data):
        return data['properties']['cardData']['text']['title']

    def get_synopsis(self, data):
        return data['properties']['cardData']['text']['description']

    def get_id(self, data):
        return data['properties']['cardData']['meta']['nid']

    def get_deeplink(self, data):
        return (self._format_url).format(data['properties']['cardData']['meta']['permalink'])

    def get_genre(self, data):
        return data['properties']['cardData']['meta']['genre']



    def get_payload_series(self,content):
        payloads = []
        list_db_series = Datamanager._getListDB(self, self.titanScraping)
        data = content['data']['children']
        for item in data:
            if item['properties'].get('title'):
                if 'Shows' in item['properties']['title']:
                    serie_data = item
                    break
        for serie in serie_data['children']:
            print('Series')
            self.get_title(serie)
            payload_series = {
                "PlatformCode":  self._platform_code,
                "Id":            self.get_id(serie),
                'Title':         self.get_title(serie),
                "Type":          "serie",
                'OriginalTitle': None,
                'Year':          None,
                'Duration':      None,
                'Deeplinks': {
                    'Web':       self.get_deeplink(serie),
                    'Android':   None,
                    'iOS':       None,
                },
                'Playback':      None,
                "CleanTitle":    _replace(self.get_title(serie)),
                'Synopsis':      self.get_synopsis(serie),
                'Image':         None,
                'Rating':        None,
                'Provider':      None,
                'Genres':        None,
                'Cast':          None,
                'Directors':     None,
                'Availability':  None,
                'Download':      None,
                'IsOriginal':    None,
                'IsAdult':       None,
                'Packages':
                [{'Type': 'tv-everywhere'}],
                    'Country':       None,
                    'Timestamp':     datetime.now().isoformat(),
                    'CreatedAt':     self._created_at
            }
            
            Datamanager._checkDBandAppend(self, payload_series, list_db_series, payloads)
            Datamanager._insertIntoDB(self, payloads, self.titanScraping)
     