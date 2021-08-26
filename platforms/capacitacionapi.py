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


class CapacitacionApi():

    """
    - Status: (Si aún se está trabajando en la plataforma o si ya se terminó)
    - VPN: (La plataforma requiere o no VPN)
    - Método: (Si la plataforma se scrapea con Requests, BS4, Selenium o alguna mezcla)
    - Runtime: (Tiempo de corrida aproximado del script)
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
        ###############################################
        self.urls = config_['urls']
        self.payloads = []
        self.payloads_episodes = []
        self.ids_scrapeados = Datamanager._getListDB(self,self.titanScraping)

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

        if ott_operation == 'scraping':
            self.scraping()
        if ott_operation == "testing":
            self.scraping(testing=True)

    def get_movies(self, movies):

        pass

    def get_series(self):
        pass

    def get_episodes(self):
        pass
    def test_request(self):
        """
        Con el módulo Requests vamos a obtener las respuestas de la API.
        """
        headers = {
            "authority": 'content-delivery-gw.svc.ds.amcn.com',
            "method": 'GET',
            "path": '/api/v2/content/amcn/amc/url/movies?cacheHash=6fbe285914ba1b125a543cb2a78a8e5d2b8bf1962737c01b1fd874e87f8dbdb9&device=web',
            "scheme": 'https',
            "accept": '*/*',
            "accept-encoding": 'gzip, deflate, br',
            "accept-language": 'es-419,es;q=0.9,es-ES;q=0.8,en;q=0.7,en-GB;q=0.6,en-US;q=0.5',
            "amcn-cache-hash": '6fbe285914ba1b125a543cb2a78a8e5d2b8bf1962737c01b1fd874e87f8dbdb9',
            "amcn-user-state": '{"entitlements":["amc_unauth"],"device":"web"}',
            "cache-control": 'no-cache',
            "origin": 'https://www.amc.com',
            "pragma": 'no-cache',
            "referer": 'https://www.amc.com/',
            "sec-ch-ua": '"Chromium";v="92", " Not A;Brand";v="99", "Microsoft Edge";v="92"',
            "sec-ch-ua-mobile": '?0',
            "sec-fetch-dest": 'empty',
            "sec-fetch-mode": 'cors',
            "sec-fetch-site": 'cross-site',
            "user-agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36 Edg/92.0.902.73',
        }
        rq_ = requests.get(
        "https://content-delivery-gw.svc.ds.amcn.com/api/v2/content/amcn/amc/url/movies?cacheHash=6fbe285914ba1b125a543cb2a78a8e5d2b8bf1962737c01b1fd874e87f8dbdb9&device=web",
        headers=headers)
        json_ = rq_.json()
        #print(json_)
        """
        Luego de obtener el JSON (JavaScript Object Notation) vamos a
        acceder a como accederíamos a un diccionario, analizando los campos
        para llegar a los datos requeridos.
        """
        movies = json_['data']['children'][4]['children']
        print(movies)
        for movie in movies:
            payload = self.get_payload(movie)
            payload_movie = payload.payload_movie()
            payload_episode = payload.payload_episode()
            payload_serie = payload.payload_serie()
            Datamanager._checkDBandAppend(self,payload_movie,self.ids_scrapeados,self.payloads)
        """
        Cuando ya tenemos una lista con todas las peliculas de la plataforma,
        la recorremos para obtener todos los datos.
        """
        pass
    def scraping(self,testing=True):
        """
        Data manager nos simplifica la manera de interactuar entre las listas
        y la base de datos.
        """
        self.test_request()

        # self.scraped = query_field(Collecion, Campo, )
        # if payload not in self.scraped:
            # self.payloads.append(payload)
        # self.test_request()
        # payload = self.get_payload()
        # movie = payload.payload_movie()
        # self.payloads.append(movie)
        # Datamanager._checkDBandAppend(self,movie,ids,self.payloads)

        Datamanager._insertIntoDB(self,self.payloads,self.titanScraping)
        # Datamanager._insertIntoDB(self,self.payloads_episodes,self.titanScrapingEpisodios)
        """
        Hace una Query para ver lo que scrapeamos.
        Chequea los payloads para ver que esten correctos.
        Si estamos en testing no intenta subir a misato,
        pero realiza las validaciones.
        """
        Upload(self._platform_code,self._created_at,testing=testing)
        #print(movie)
        pass



    def get_payload(self,content_metadata,is_episode=None):
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
            # Si no es un episodio, los datos pasan a scrapearse del html.
            if self.is_episode:
                payload.parent_title = self.get_parent_title(content_metadata)
                payload.parent_id = self.get_parent_id(content_metadata)
                payload.season = self.get_season(content_metadata)
                payload.episode = self.get_episode(content_metadata)

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
        """
        Este metodo se encarga de Obtener la ID 
        """
        id = content_metadata['properties']['cardData']['meta']['nid']
        return id
        pass
    def get_title(self, content_metadata):
        title = content_metadata['properties']['cardData']['text']['title']
        return title
        pass
    def get_clean_title(self, content_metadata):
        title = content_metadata['properties']['cardData']['text']['title']
        clean_title = _replace(title)
        return clean_title
        pass
    def get_original_title(self, content_metadata):
        pass
    def get_year(self, content_metadata):
        pass
    def get_duration(self, content_metadata):
        pass
    def get_deeplinks(self, content_metadata):
        pass
    def get_synopsis(self, content_metadata):
        pass
    def get_images(self, content_metadata):
        pass
    def get_ratings(self, content_metadata):
        pass
    def get_genres(self, content_metadata):
        pass
    def get_cast(self, content_metadata):
        pass
    def get_directors(self, content_metadata):
        pass
    def get_availability(self, content_metadata):
        pass
    def get_packages(self, content_metadata):
        return [{'Type':'subscription-vod'}]
        pass
    def get_country(self, content_metadata):
        pass
    def get_parent_title(self, content_metadata):
        pass
    def get_parent_id(self, content_metadata):
        pass
    def get_episode(self, content_metadata):
        pass
    def get_season(self, content_metadata):
        pass