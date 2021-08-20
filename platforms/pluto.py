# -*- coding: utf-8 -*-
import requests # Si el script usa requests/api o requests/bs4
import time
import json
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

    DATOS IMPORTANTES:
    - ESTADO: EN PROCESO
    - VPN: No
    - ¿Usa Selenium?: No
    - ¿Tiene API?: Si
    - ¿Usa BS4?: No
    - ¿Cuanto demoró la ultima vez?
    - ¿Cuanto contenidos trajo la ultima vez?

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
                'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsImtpZCI6IjI3Y2Y3MWM4LWJlOWQtNGQ3Mi1hNmVmLWUzZGE3ZmRmZTljYyIsInR5cCI6IkpXVCJ9.eyJzZXNzaW9uSUQiOiI1NTQ0Y2NjYS0wMTllLTExZWMtODE4Yi0wMjQyYWMxMTAwMDMiLCJjbGllbnRJUCI6IjE4Ni4yMi4yMzguMTEiLCJjaXR5IjoiTG9tYXMgZGUgWmFtb3JhIiwicG9zdGFsQ29kZSI6IjE4MzIiLCJjb3VudHJ5IjoiQVIiLCJkbWEiOjAsImFjdGl2ZVJlZ2lvbiI6IlZFIiwiZGV2aWNlTGF0IjotMzQuNzY2MSwiZGV2aWNlTG9uIjotNTguMzk1NywicHJlZmVycmVkTGFuZ3VhZ2UiOiJlbiIsImRldmljZVR5cGUiOiJ3ZWIiLCJkZXZpY2VWZXJzaW9uIjoiNzguMC4wIiwiZGV2aWNlTWFrZSI6ImZpcmVmb3giLCJkZXZpY2VNb2RlbCI6IndlYiIsImFwcE5hbWUiOiJ3ZWIiLCJhcHBWZXJzaW9uIjoiNS4xMDIuMC1mYjFmZWRiZDY0MTkzZjlkYjAwMGY5N2M0NGNlMjlkZTZiMGJiOWQ1IiwiY2xpZW50SUQiOiJjY2Q5Zjc5Ni1kNzAzLTQwNTUtODEzNy0yODAzMWM2OGQ4YTIiLCJjbUF1ZGllbmNlSUQiOiIiLCJpc0NsaWVudEROVCI6ZmFsc2UsInVzZXJJRCI6IiIsImxvZ0xldmVsIjoiREVGQVVMVCIsInRpbWVab25lIjoiQW1lcmljYS9BcmdlbnRpbmEvQnVlbm9zX0FpcmVzIiwic2VydmVyU2lkZUFkcyI6dHJ1ZSwiZTJlQmVhY29ucyI6ZmFsc2UsImZlYXR1cmVzIjp7fSwiYXVkIjoiKi5wbHV0by50diIsImV4cCI6MTYyOTQ4MjgwOCwianRpIjoiZmFkNzNlOWYtNTYxZC00NmE0LWE1NTctMTI0ZjhhM2U5YzcxIiwiaWF0IjoxNjI5NDU0MDA4LCJpc3MiOiJib290LnBsdXRvLnR2Iiwic3ViIjoicHJpOnYxOnBsdXRvOmRldmljZXM6VkU6WTJOa09XWTNPVFl0WkRjd015MDBNRFUxTFRneE16Y3RNamd3TXpGak5qaGtPR0V5In0.y6NReTcbh5RVCgRJvBidW0awtYmffVdRdzEYDN35QCw',
                'Origin': 'https://pluto.tv',
                'Connection': 'keep-alive',
                'TE': 'Trailers'
                }

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
        """Obtiene todas las categorias

        Returns:
            JSON: Categorias
        """
        res = requests.get(self._url + "categories", headers=self._headers)
        cat_ = res.json()
        return cat_['categories']





    def get_series(self, categories):
        """[summary]

        Args:
            categories ([type]): [description]

        Returns:
            [type]: [description]
        """
        series = []
        for category in categories:
            res = requests.get(self._url + "categories/" + category['_id'] + '/items', headers=self._headers)
            res = res.json()
            for serie in res['items']:
                if serie['type'] == "series":
                    series.append(serie)
        return series




    def get_movies(self, categories):
        """[summary]

        Args:
            categories ([type]): [description]

        Returns:
            [type]: [description]
        """
        movies = []
        for category in categories:
            res = requests.get(self._url + "categories/" + category['_id'] + '/items', headers=self._headers)
            res = res.json()
            for movie in res['items']:
                if movie['type'] == "movie":
                    movies.append(movie)
        return movies

    #def get_payload(self,content_metadata,is_episode=None):
    #        """Método para crear el payload. Se reutiliza tanto para
    #        titanScraping, como para titanScrapingEpisodes.
    #        Args:
    #            content_metadata (dict): Indica la metadata del contenido.
    #            is_episode (bool, optional): Indica si hay que crear un payload
    #            que es un episodio. Defaults to False.
    #        Returns:
    #            Payload: Retorna el payload.
    #        """
    #        payload = Payload()
    #        # Indica si el payload a completar es un episodio:
    #        if is_episode:
    #            self.is_episode = True
    #        else:
    #            self.is_episode = False
    #        payload.platform_code = self._platform_code
    #        payload.id = self.get_id(content_metadata)
    #        payload.title = self.get_title(content_metadata)
    #        payload.original_title = self.get_original_title(content_metadata)
    #        payload.clean_title = ""# self.get_clean_title(content_metadata)
    #        payload.deeplink_web = self.get_deeplinks(content_metadata)
    #        # Si no es un episodio, los datos pasan a scrapearse del html.
    #        if self.is_episode:
    #            payload.parent_title = self.get_parent_title(content_metadata)
    #            payload.parent_id = self.get_parent_id(content_metadata)
    #            payload.season = self.get_season(content_metadata)
    #            payload.episode = self.get_episode(content_metadata)
#
    #        payload.year = self.get_year(content_metadata)
    #        payload.duration = self.get_duration(content_metadata)
    #        payload.synopsis = self.get_synopsis(content_metadata)
    #        payload.image = self.get_images(content_metadata)
    #        payload.rating = self.get_ratings(content_metadata)
    #        payload.genres = self.get_genres(content_metadata)
    #        payload.cast = self.get_cast(content_metadata)
    #        payload.directors = self.get_directors(content_metadata)
    #        payload.availability = self.get_availability(content_metadata)
    #        payload.packages = self.get_packages(content_metadata)
    #        payload.country = self.get_country(content_metadata)
    #        payload.createdAt = self._created_at
    #        return payload

    def get_id(self, content_metadata):
        """
        Este metodo se encarga de Obtener la ID 
        """

        pass
    def get_title(self, content_metadata):
        pass

    def scraping(self):
        cat_ = self.get_categories()
        #self.get_movies(cat_)
        #print(self.get_movies(cat_))