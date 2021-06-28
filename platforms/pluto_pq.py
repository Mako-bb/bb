import threading
import time
import requests
from handle.replace         import _replace
from common                 import config
from handle.mongo           import mongo
from updates.upload         import Upload
from handle.payload         import Payload
from handle.datamanager     import Datamanager
# from time import sleep
# import re


class PlutoPQ():
    """
    """
    def __init__(self, ott_site_uid, ott_site_country, type):
        self.ott_site_uid = ott_site_uid
        self.ott_site_country = ott_site_country
        self._config = config()['ott_sites'][ott_site_uid]
        self._platform_code = self._config['countries'][ott_site_country]
        # self._start_url             = self._config['start_url']
        self._created_at = time.strftime("%Y-%m-%d")
        self.mongo = mongo()
        self.titanPreScraping = config()['mongo']['collections']['prescraping']
        self.titanScraping = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodios = config()['mongo']['collections']['episode']

        self.all_titles_url = self._config['all_titles_url']
        self.all_episodes_url = self._config['all_episodes_url']

        self.session = requests.session()

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
            self._scraping(testing=True)
    def query_field(self, collection, field=None):
        """Método que devuelve una lista de una columna específica
        de la bbdd.

        Args:
            collection (str): Indica la colección de la bbdd.
            field (str, optional): Indica la columna, por ejemplo puede ser
            'Id' o 'CleanTitle. Defaults to None.

        Returns:
            list: Lista de los field encontrados.
        """
        find_filter = {
            'PlatformCode': self._platform_code,
            'CreatedAt': self._created_at
        }

        find_projection = {'_id': 0, field: 1, } if field else None

        query = self.mongo.db[collection].find(
            filter=find_filter,
            projection=find_projection,
            no_cursor_timeout=False
        )

        if field:
            query = [item[field] for item in query if item.get(field)]
        else:
            query = list(query)

        return query
    
    
    def _scraping(self, testing=False):
        url_episodes = self.all_episodes_url
        url_titles = self.all_titles_url    
        all_titles = {}
        all_episodes = {}
        response = self.session.get(url_titles)
        res = response.json()
        #executer = ThreadPoolExecutor(max_workers=20)
        for cat in res["categories"]:
            for item in cat["items"]:         
                
                if not all_titles.get(item["_id"]):
                    all_titles.setdefault(item["slug"], True)
                    payload = {
                        "Id": item["_id"],
                        "Title": item["name"],
                        "Rating": item["rating"],
                        "PlatformCode": self._platform_code
                    }
                    self.mongo.insert("titanScraping", payload)
                    if item["type"] == "series":
                        
                        response_episodes = self.session.get(url_episodes+item["slug"])
                        res_episodes = response_episodes.json()
                        #executer.submit(self.get_episodes, res_episodes, all_episodes, item["_id"], item["slug"])
                        
                        self.get_episodes(res_episodes, all_episodes, item["_id"], item["slug"])


    def get_episodes(self, res_episodes, all_episodes, id_title, slug):
        try:
            for season in res_episodes["seasons"]:
                for episode in season["episodes"]:
                    if not all_episodes.get(episode["_id"]):
                        all_episodes.setdefault(episode["_id"], True)
                        payload_series = {
                            "Id": episode["_id"],
                            "Id_title": id_title,
                            "Title": episode["name"],
                            "Description": episode["description"],
                            "Season": episode["season"],
                            "Episode": episode["number"]
                        }
                        self.mongo.insert("titanScrapingEpisodes", payload_series)
        except:
            print(slug)  


#19,694 episodes
#con None hilos 152.44045281410217
#con 10 hilos 163.54908108711243
#con 20 hilos 145.26501035690308
#sin hilos 154.00749516487122


        
        