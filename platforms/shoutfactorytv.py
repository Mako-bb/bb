import time
import requests
from handle.replace import _replace
from common import config
from datetime import datetime
from bs4 import BeautifulSoup as BS
from handle.mongo import mongo
from handle.datamanager import Datamanager
from updates.upload import Upload 
class Shoutfactorytv():

    def __init__(self, ott_site_uid, ott_site_country, type):
        self.url="https://www.shoutfactorytv.com/"
        response= requests.get(self.url)
        self.soup= BS(response.text, 'parser.html')
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
        pelis_genres=self.soup.find_all("div",{"dropdown","has-drop-down-a"}).find
        ################# URLS  #################
        self._movies_url = self.url +
        self._show_url = self._config['show_url']
        
        
        #Url para encontrar la información de los contenidos por separado
        self._format_url = self._config['format_url'] 
        self._episode_url = self._config['episode_url']
        self.testing = False
        self.sesion = requests.session()
        self.headers = {"Accept": "application/json",
                        "Content-Type": "application/json; charset=utf-8"}
        self.list_db  = Datamanager._getListDB(self, self.titanScraping)
        self.list_db_epi=Datamanager._getListDB(self, self.titanScrapingEpisodios)

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
        payloads_series = []
        # Definimos los links de las apis y con el Datamanager usamos la función _getJson
        episode_data = Datamanager._getJSON(self, self._episode_url)
        serie_data = Datamanager._getJSON(self,self._show_url )
        movie_data = Datamanager._getJSON(self, self._movies_url)

        self.get_payload_movies(movie_data)
        self.get_payload_serie(serie_data)
        self.get_payload_episodes(episode_data)
        Upload(self._platform_code, self._created_at, testing = self.testing)

    def get_payload_movies(movie_data):

