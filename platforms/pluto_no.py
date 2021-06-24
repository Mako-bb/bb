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

class PlutoNO():
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

        # self.api_url = self._config['api_url']

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

        
        url = 'https://service-vod.clusters.pluto.tv/v3/vod/categories?includeItems=true&includeCategoryFields=imageFeatured%2CiconPng&itemOffset=10000&advertisingId=&appName=web&appVersion=5.17.1-be7b5e79fc7cad022e22627cbb64a390ca9429c7&app_name=web&clientDeviceType=0&clientID=5ba90432-9a1d-46d1-8f93-b54afe54cd1e&clientModelNumber=na&country=AR&deviceDNT=false&deviceId=5ba90432-9a1d-46d1-8f93-b54afe54cd1e&deviceLat=-34.5106&deviceLon=-58.7536&deviceMake=Microsoft%2BEdge&deviceModel=web&deviceType=web&deviceVersion=91.0.864.54&marketingRegion=VE&serverSideAds=true'
        
        response = self.session.get(url)

        contents_metadata = response.json()        
        
        categories = contents_metadata["categories"]

        slug = []

        
        
        for categorie in categories:
            contents = categorie.get("items")
            for content in contents:
                id = content.get("_id")
                title = content.get("name")
                type = content.get("type")
                synopsis = content.get("summary")
                duration = content.get("duration")
                rating = content.get("rating")
                genres = content.get("genre")
                covers = content["covers"]
                
                
                slug = content["slug"]
                url_episodios = f'https://service-vod.clusters.pluto.tv/v3/vod/slugs/{slug}?advertisingId=&appName=web&appVersion=5.17.1-be7b5e79fc7cad022e22627cbb64a390ca9429c7&app_name=web&clientDeviceType=0&clientID=95b00792-ce58-4e87-b310-caaf6c8d8de4&clientModelNumber=na&country=AR&deviceDNT=false&deviceId=95b00792-ce58-4e87-b310-caaf6c8d8de4&deviceLat=-34.6022&deviceLon=-58.3845&deviceMake=Firefox&deviceModel=web&deviceType=web&deviceVersion=89.0&marketingRegion=VE&serverSideAds=true&sessionID=4987c7e3-d482-11eb-bee6-0242ac110002&sid=4987c7e3-d482-11eb-bee6-0242ac110002&userId=&attributeV4=foo'
                response_episodios = self.session.get(url_episodios)
                contents_metadata_episodios = response_episodios.json()
                temporadas = contents_metadata_episodios["seasons"]
                

                for temporada in temporadas:
                    episodes = temporada.get("episodes")
                    for episode in episodes:
                        id_episode = episode.get("_id")
                        ParentId_episode = temporada.get("_id")
                        ParentTitle_episode = temporada.get("name")
                        Episode_episode = episode.get("number")
                        Season_episode = episode.get("season")
                        Title_episode = episode.get("name")
                        Duration_episode = episode.get("duration")
                        Genres_episode = episode.get("genre")
                        Cover_episode = episode["covers"]
                        for content_ep in Cover_episode:
                            Image_episode = content_ep.get("url")


                        payload_episodes = {
                            "PlatformCode": "ar.pultotv",
                            "Id": id_episode,
                            "ParentId": ParentId_episode,
                            "ParentTitle": ParentTitle_episode,
                            "Episode": Episode_episode,
                            "Season": Season_episode,
                            "Title": Title_episode,
                            "CleanTitle": _replace(Title_episode),
                            "Duration": Duration_episode, #no pude ponerlo en segundos
                            "Image": Image_episode,
                            "Genres": Genres_episode,
                            
                        }
                        print(payload_episodes)       


                for content in covers:
                    image = content.get("url")

 
                payload_contenidos = {
                    "PlatformCode": "ar.pultotv",
                    "Id": id,
                    "Title": title,
                    "CleanTitle": _replace(title),
                    "Type": type,
                    "Duration": duration, #no pude ponerlo en segundos
                    "Synopsis" : synopsis,
                    "Image": image,
                    "Rating": rating,
                    "Genres": genres,
                    
                }
                print(payload_contenidos)                 

                
        
                
                
                    

                                  
                    
        

