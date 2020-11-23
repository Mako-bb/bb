import json
import time
import requests
import hashlib   
from common                 import config
from bs4                    import BeautifulSoup
from datetime               import datetime
from handle.mongo           import mongo
from updates.upload         import Upload
from handle.datamanager  import Datamanager
from handle.replace         import _replace

class CwSeed_Tati():
    def __init__(self, ott_site_uid, ott_site_country, type):
        self._config                = config()['ott_sites'][ott_site_uid]
        self._created_at            = time.strftime("%Y-%m-%d")
        self.mongo                  = mongo()
        self._platform_code         = self._config['countries'][ott_site_country]   
        self.titanScraping          = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodios = config()['mongo']['collections']['episode']
        self.sesion                 = requests.session()
        self.skippedEpis            = 0
        self.skippedTitles          = 0
        
        if type == 'scraping':
            self._scraping()

        if type == 'testing':
            self._scraping(testing = True)  
            
    def _scraping(self, testing = False):
        URL = "https://www.cwseed.com/shows/genre/shows-a-z/"
        soup = Datamanager._getSoup(self, URL)

        # Trae lista de Data Base & declara lista payload episodios
        listDBEpi = Datamanager._getListDB(self,self.titanScrapingEpisodios)
        listPayloadEpi = []

        all_titles = soup.find("div", {"id": "show-hub"})
        
        for item in all_titles.findAll("li",{"class":"showitem"}):
            deeplink = item.find("a").get("href")       #No es link, falta rellener con el ID de cada episodio
            titulo = item.find("p").text                #Title
            
            id_link = item.find("a").get("data-slug")
            print(id_link)

            arma_link = 'www.cwseed.com/shows/{}'.format(id_link)

            # Armar un soup del armalink y buscar