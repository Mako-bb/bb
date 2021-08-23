# -*- coding: utf-8 -*-
import requests # Si el script usa requests/api o requests/bs4
import time
from bs4                import BeautifulSoup # Si el script usa bs4
from selenium           import webdriver # Si el script usa selenium
from handle.datamanager import Datamanager # Opcional si el script usa Datamanager
from common             import config
from handle.mongo       import mongo
from updates.upload     import Upload
from handle.replace     import _replace
from handle.payload     import Payload

class Pluto():
    
    """
    - Status: EN PROCESO
    - VPN: NO
    - Método: API
    - Runtime: 8 MINUTOS
    """

    def __init__(self, ott_platforms, ott_site_country, ott_operation):
        self.test = ott_operation in ("testing", "return") #
        config_ = config()['ott_sites'][ott_platforms] # Obligatorio
        self.country = ott_site_country # Opcional, puede ser útil dependiendo de la lógica del script.
        self._created_at = time.strftime('%Y-%m-%d')
        self._platform_code = config_['countries'][ott_site_country]
        self.mongo = mongo()
        self.sesion                 = requests.session() # Requerido si se va a usar Datamanager
        self.titanPreScraping       = config()['mongo']['collections']['prescraping'] # Opcional
        self.titanScraping          = config()['mongo']['collections']['scraping'] # Obligatorio. También lo usa Datamanager
        self.titanScrapingEpisodios = config()['mongo']['collections']['episode'] # Obligatorio. También lo usa Datamanager
        self.skippedTitles          = 0 # Requerido si se va a usar Datamanager
        self.skippedEpis            = 0 # Requerido si se va a usar Datamanager
        self._url                   = config_['url']
        self._headers               = {
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:78.0) Gecko/20100101 Firefox/78.0',
                'Accept': '*/*',
                'Accept-Language': 'en-US,en;q=0.5',
                'Referer': 'https://pluto.tv/en/on-demand',
                'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsImtpZCI6IjJjNTE3NWIzLTJkYzEtNDcwMy1iM2NiLTM2YjA0Y2VmMDNhNyIsInR5cCI6IkpXVCJ9.eyJzZXNzaW9uSUQiOiIyNTg1YWM2OC0wNDA3LTExZWMtOGIwMS0wMjQyYWMxMTAwMDMiLCJjbGllbnRJUCI6IjI4MDA6ODEwOjU1NDpiZDQ6YjE1Zjo1ZWI1OjQ5MzI6YjJiYSIsImNpdHkiOiJMb21hcyBkZSBaYW1vcmEiLCJwb3N0YWxDb2RlIjoiMTgzMiIsImNvdW50cnkiOiJBUiIsImRtYSI6MCwiYWN0aXZlUmVnaW9uIjoiVkUiLCJkZXZpY2VMYXQiOi0zNC43NjYxLCJkZXZpY2VMb24iOi01OC4zOTU3LCJwcmVmZXJyZWRMYW5ndWFnZSI6ImVzIiwiZGV2aWNlVHlwZSI6IndlYiIsImRldmljZVZlcnNpb24iOiI5Mi4wLjkwMiIsImRldmljZU1ha2UiOiJlZGdlLWNocm9taXVtIiwiZGV2aWNlTW9kZWwiOiJ3ZWIiLCJhcHBOYW1lIjoid2ViIiwiYXBwVmVyc2lvbiI6IjUuMTAyLjAtZmIxZmVkYmQ2NDE5M2Y5ZGIwMDBmOTdjNDRjZTI5ZGU2YjBiYjlkNSIsImNsaWVudElEIjoiZDk3MDkxODYtNzJkNi00OGRkLWFkOWItMDRhYjczMTAzYWM1IiwiY21BdWRpZW5jZUlEIjoiIiwiaXNDbGllbnRETlQiOmZhbHNlLCJ1c2VySUQiOiIiLCJsb2dMZXZlbCI6IkRFRkFVTFQiLCJ0aW1lWm9uZSI6IkFtZXJpY2EvQXJnZW50aW5hL0J1ZW5vc19BaXJlcyIsInNlcnZlclNpZGVBZHMiOnRydWUsImUyZUJlYWNvbnMiOmZhbHNlLCJmZWF0dXJlcyI6e30sImF1ZCI6IioucGx1dG8udHYiLCJleHAiOjE2Mjk3NDc3MjgsImp0aSI6IjU5MjUzYmI4LTA5ZjUtNGFhYy04NzQ2LWYxZjM1NzFmZmJlNiIsImlhdCI6MTYyOTcxODkyOCwiaXNzIjoiYm9vdC5wbHV0by50diIsInN1YiI6InByaTp2MTpwbHV0bzpkZXZpY2VzOlZFOlpEazNNRGt4T0RZdE56SmtOaTAwT0dSa0xXRmtPV0l0TURSaFlqY3pNVEF6WVdNMSJ9.uNeKgJrO0YLXViACSwMHQBgfQ-9RAhIBaYF_Vpur_wk',
                'Origin': 'https://pluto.tv',
                'Connection': 'keep-alive',
                'TE': 'Trailers'
                }
        self.payloads = []
        self.payloads_episodes = []
        self.ids_scrapeados = Datamanager._getListDB(self,self.titanScraping)
        self.ids_episcrap = Datamanager._getListDB(self,self.titanScrapingEpisodios)

        #### OBLIGATORIO si se usa Selenium para que pueda correr en los servers
        try:
            if platform.system() == 'Linux':
                Display(visible=0, size=(1366, 768)).start()
        except Exception:
            pass
        ####

        """
        La operación 'return' la usamos en caso que se nos corte el script a mitad de camino cuando
        testeamos, sea por un error de conexión u otra cosa. Nos crea una lista de ids ya insertados en
        nuestro Mongo local, la cual podemos usar para saltar los contenidos scrapeados y volver rápidamente
        a donde había cortado el script.
        """
        if ott_operation == 'return':
            return_params = {'PlatformCode' : self._platform_code}
            last_item = self.mongo.lastCretedAt('titanPreScraping', return_params)
            if last_item.count() > 0:
                for last_content in last_item:
                    self._created_at = last_content['CreatedAt']
            self.prescraped_ids = [pay["Id"] for pay in Datamanager._getListDB(self, self.titanPreScraping)]

            self.scraping()
        else:
            self.prescraped_ids = list()

        if ott_operation in ('testing', 'scraping'):
            self.scraping()

    def get_categories(self):
        res = requests.get(self._url + "categories", headers=self._headers)
        cat_ = res.json()
        return cat_['categories']

    def get_movies(self, categories):
        cat_ = categories
        for category in cat_:
            res = requests.get(self._url + "categories/" + category['_id'] + '/items', headers=self._headers)
            res = res.json()
            for movie in res['items']:
                if movie['type'] == "movie":
                    payload = self.get_payload(movie)
                    payload_movie = payload.payload_movie()
                    Datamanager._checkDBandAppend(self,payload_movie,self.ids_scrapeados,self.payloads)

    def get_series(self, categories):
        cat_ = categories
        for category in cat_:
            res = requests.get(self._url + "categories/" + category['_id'] + '/items', headers=self._headers)
            res = res.json()
            for serie in res['items']:
                if serie['type'] == "series":
                    payload = self.get_payload(serie)
                    payload_serie = payload.payload_serie()
                    Datamanager._checkDBandAppend(self,payload_serie,self.ids_scrapeados,self.payloads)



    def get_payload(self,content_metadata, parent_id=None, parent_name=None,is_episode=None):
            """Método para crear el payload. Se reutiliza tanto para
            titanScraping, como para titanScrapingEpisodes.
            Args:
                content_metadata (dict): Indica la metadata del contenido.
                is_episode (bool, optional): Indica si hay que crear un payload
                que es un episodio. Defaults to False.
            Returns:
                Payload: Retorna el payload.
            """
            payload = Payload()
            # Indica si el payload a completar es un episodio:
            if is_episode:
                self.is_episode = True
            else:
                self.is_episode = False
            payload.platform_code = self._platform_code
            payload.id = self.get_id(content_metadata)
            payload.title = self.get_title(content_metadata)
            payload.original_title = self.get_original_title(content_metadata)
            payload.clean_title = self.get_clean_title(content_metadata)
            payload.deeplink_web = self.get_deeplinks(content_metadata)
            if self.is_episode:
                payload.parent_title = parent_name
                payload.parent_id = parent_id
                payload.season = self.get_season(content_metadata)
                payload.episode = self.get_episode(content_metadata)
            if content_metadata['type'] == "series":
                payload.seasons = self.get_seasons(content_metadata)

            payload.year = self.get_year(content_metadata)
            payload.duration = self.get_duration(content_metadata)
            payload.synopsis = self.get_synopsis(content_metadata)
            payload.image = self.get_images(content_metadata)
            payload.rating = self.get_ratings(content_metadata)
            payload.genres = self.get_genres(content_metadata)
            payload.cast = self.get_cast(content_metadata)
            payload.directors = self.get_directors(content_metadata)
            payload.availability = self.get_availability(content_metadata)
            payload.packages = self.get_packages(content_metadata)
            payload.country = self.get_country(content_metadata)
            payload.createdAt = self._created_at
            return payload

    def get_id(self, content_metadata):
        id = content_metadata['_id']
        return id
    
    def get_title(self, content_metadata):
        title = content_metadata['name']
        return title
    
    def get_clean_title(self, content_metadata):
        title = content_metadata['name']
        clean_title = _replace(title)
        return clean_title
    def get_original_title(self, content_metadata):
        pass
    
    def get_year(self, content_metadata):
        pass
    def get_duration(self, content_metadata):
        if content_metadata['type'] == "series":
            duration = None
        else:
            duration = content_metadata['duration'] / 1000
            duration = duration / 60
        return duration
    def get_deeplinks(self, content_metadata):
       deeplink = {
           "Web": None,
           "Android": None,
           "iOS": None
           }
       if content_metadata['type'] == "movie":
           deeplink["Web"] = "https://pluto.tv/es/on-demand/movies/" + content_metadata["slug"]
       else:
           deeplink["Web"] = "https://pluto.tv/es/on-demand/series/" + content_metadata["slug"]
       return deeplink
    
    def get_synopsis(self, content_metadata):
        synopsis = content_metadata['description']
        return synopsis
    def get_images(self, content_metadata):
        image = content_metadata['covers']
        return image
    def get_ratings(self, content_metadata):
        rating = content_metadata['rating']
    def get_genres(self, content_metadata):
        genres = content_metadata['genre'].replace(" & ", ",").split(",")
        return genres
    def get_cast(self, content_metadata):
        pass
    def get_directors(self, content_metadata):
        pass
    def get_availability(self, content_metadata):
        pass
    def get_packages(self, content_metadata):
        return [{'Type':'free-vod'}]
    def get_country(self, content_metadata):
        pass
    def get_episode(self, content_metadata):
        episode = content_metadata['number']
        return episode
    
    
    def get_episodes(self, content_metadata):
        parent_name = content_metadata['name']
        parent_id = content_metadata['_id']
        for i in content_metadata['seasons']:
            for episode in i['episodes']:
                payload = self.get_payload(episode,parent_id=parent_id, parent_name=parent_name, is_episode=True)
                payload_episode = payload.payload_episode()
                Datamanager._checkDBandAppend(self,payload_episode,self.ids_episcrap,self.payloads,isEpi=True)

    def get_season(self, content_metadata):
        season = content_metadata['season']
        return season
    def get_seasons(self, content_metadata):
        seasons = []
        payload_season = {
            "Number": None,
            "Episodes": None,
            "Deeplink": None
            }
        res = requests.get(self._url + "series/" + content_metadata['_id'] + "/seasons", headers=self._headers)
        res = res.json()
        slug = res['slug']
        self.get_episodes(res)
        
        for season in res['seasons']:
            payload_season["Number"] = season['number']
            payload_season["Episodes"] = len(season['episodes'])
            payload_season["Deeplink"] = self._url + "series/" + slug + "/details" + "/seasons" + str(season['number'])
            seasons.append(payload_season)
        return seasons

    def scraping(self):
        cat = self.get_categories()
        self.get_movies(cat)
        self.get_series(cat)
        Datamanager._insertIntoDB(self,self.payloads_episodes,self.titanScrapingEpisodios)
        Datamanager._insertIntoDB(self,self.payloads,self.titanScraping)
        