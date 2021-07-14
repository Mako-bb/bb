from os import replace
import time
from pymongo.common import CONNECT_TIMEOUT
import requests
from yaml.tokens import FlowMappingStartToken
from handle.replace import _replace
from common import config
from handle.mongo import mongo
from updates.upload import Upload
from handle.payload import Payload
from handle.datamanager import Datamanager
from bs4 import BeautifulSoup
import datetime
# from time import sleep
import re
start_time = time.time()


class Iviru():
    def __init__(self, ott_site_uid, ott_site_country, type):
        """
        Iviru es una ott de Rusia.

        DATOS IMPORTANTES:
        - VPN: No
        - ¿Usa Selenium?: No.
        - ¿Tiene API?: Si.
        - ¿Usa BS4?: No.
        - ¿Cuanto demoró la ultima vez?. NA
        - ¿Cuanto contenidos trajo la ultima vez? NA.

        OTROS COMENTARIOS:
        """
        self.ott_site_uid = ott_site_uid
        self.ott_site_country = ott_site_country
        self._config = config()['ott_sites'][ott_site_uid]
        self._platform_code = self._config['countries'][ott_site_country]
        # self._start_url             = self._config['start_url']
        self._created_at = time.strftime("%Y-%m-%d")
        self.mongo = mongo()
        self.titanPreScraping = config()['mongo']['collections']['prescraping']
        self.titanScraping = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodes = config(
        )['mongo']['collections']['episode']

        self.api_url = self._config['api_collections_url']
        self.url = self._config['url']

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

        self.scraped = self.query_field(self.titanScraping, field='Id')
        self.scraped_episodes = self.query_field(
            self.titanScrapingEpisodes, field='Id')
        self.payloads = []
        self.episodes_payloads = []
        self.collections_ids = []
        self.contents_ids = []
        self.movies = []
        self.series = []
        self.episodes = []
        self.movies_series_ids = []
        self.episodes_ids = []

        self.get_collections()
        self.get_contents()
        #self.get_episodes(episode_id)
        print(len(self.movies_series_ids))
        print(len(self.movies))
        print(len(self.series))
 
        # self.insert_payloads_close(self.payloads,self.episodes_payloads)
        print("--- %s seconds ---" % (time.time() - start_time))

    def get_collections(self):
        '''
            Obtenemos todos los ids correspondientes a cada categoria de contenidos, como esta dividida la pagina
        '''
        collections_api = self.api_url
        response = self.session.get(collections_api)
        json_data = response.json()
        json_data = json_data['result']
        for content in json_data:
            self.collections_ids.append(content['id'])

    def get_contents(self):
        '''
            Obtenemos los ids correspondientes a los contenidos por categoria
        '''
        for collection in self.collections_ids:
            collection_api = 'https://api.ivi.ru/mobileapi/collection/catalog/v5/?id={}&app_version=870'.format(
                str(collection))
            response = self.session.get(collection_api)
            json_data = response.json()
            json_data = json_data['result']
            for content in json_data:
                if not self.isDuplicate(self.movies_series_ids, content['id']):
                    if content['object_type'] == 'compilation':
                        self.series.append(content)
                    elif content['object_type'] == 'video':
                        if content['duration_minutes'] > 4:
                            self.movies.append(content)
                    self.movies_series_ids.append(content['id'])      

    def get_episodes(self, content_id):
        episode_api = 'https://api.ivi.ru/mobileapi/videoinfo/v6/?id={}'.format(str(content_id))
        response = self.session.get(episode_api)
        json_data = response.json()
        if 'error' not in json_data :
            episode = json_data['result']
            if not self.isDuplicate(self.episodes_ids, episode['id']):
                self.episodes.append(episode)
                self.episodes_ids.append(episode['id'])

    def isDuplicate(self, scraped_list, key_search):
        '''
            Metodo para validar elementos duplicados segun valor(key) pasado por parametro en una lista de scrapeados.
        '''
        isDup = False
        if key_search in scraped_list:
            isDup = True
        return isDup

    def insert_payloads_close(self, payloads, epi_payloads):
        '''
            El metodo checkea que las listas contengan elementos para ser subidos y corre el Upload en testing.
        '''
        if payloads:
            self.mongo.insertMany(self.titanScraping, payloads)
        if epi_payloads:
            self.mongo.insertMany(self.titanScrapingEpisodes, epi_payloads)
        self.session.close()
        Upload(self._platform_code, self._created_at, testing=True)

    def get_payload(self):
        '''
        '''
        if self.movies:
            for movie in self.movies:
                if not self.isDuplicate(self.scraped,movie['id']):
                    payload = self.generic_payload(movie,'movie')
                    self.payloads.append(payload)
                    self.scraped.append(movie['id'])
        
        if self.series:
            for serie in self.series:
                if not self.isDuplicate(self.scraped,serie['id']):
                    payload = self.generic_payload(serie,'serie')
                    self.payloads.append(payload)
                    self.scraped.append(serie['id'])

        if self.episodes:
            for episode in self.episodes:
                if not self.isDuplicate(self.scraped_episodes,episode['id']):
                    payload = self.episode_payload(episode)
                    self.episodes_payloads.append(payload)
                    self.scraped_episodes.append(episode['id'])
    
    def generic_payload(self,content,content_type):
        '''
            Aca voy a validar el argumento content_type si es serie o movie, dependiendo del type
            va a completar los campos segun corresponda en el payload. Ej. if content_type == serie, agregar
            el par key-value 'Seasons':content['seasons'],... etc.

        '''
        if content_type == 'serie':
            pass
        else:
            pass
        
        payload = {
            'PlatformCode': self._platform_code,
            'Id': self.get_id(content),
            'Crew': self.get_crew(content),
            'Title': self.get_title(content),
            'OriginalTitle': None,
            'CleanTitle': self.get_clean_title(content),
            'Type': content_type,
            'Year': self.get_year(content),
            'Duration': self.get_duration(content),
            'Deeplinks': self.get_Deeplinks(content),
            'Synopsis': self.get_synopsis(content),
            'Image': self.get_image(content),
            'Rating': self.get_rating(content),
            'Provider': self.get_provider(content),
            'ExternalIds': self.get_external_ids(content),
            'Genres': self.get_genres(content),
            'Cast': self.get_cast(content),
            'Directors': self.get_directors(content),
            'Availability': self.get_availability(content),
            'Download': self.get_download(content),
            'IsOriginal': self.get_isOriginal(content),
            'IsBranded':self.get_isBranded(content),
            'IsAdult': self.get_isAdult(content),
            "Packages": self.get_package(content),
            'Country': [self.ott_site_country],
            'Timestamp': datetime.datetime.now().isoformat(),
            'CreatedAt': self._created_at,
        }
        return payload


    def get_Deeplinks(self, content):
        Deeplinks = {
            "Web": None,
            "Android": None,
            "Ios": None,
        }      
        if content["share_link"]:
            Deeplinks["Web"] = content["share_link"]
        else:
            available = content['available_in_countries']
            if available:
                Deeplinks["Web"] = 'https://www.ivi.tv/watch/{}'.format(content["id"]),
            else:
                pass
        return Deeplinks

    def get_id(self, content):
        try:
            id = int(content["id"])
            return id
        except:
            pass

    def get_title(self, content):
        try:
            title = content["title"]
            return title
        except:
            pass

    def get_clean_title(self, content):
        """
        Metodo para traer los titulos sin caracteres innecesarios.
        Seguramente se va a tener que mejorar una vez hecho el analisis.
        """
        try:
            clean_title = _replace(content["title"])
            return clean_title
        except:
            pass


    def get_year(self, content):
        try:
            year = int(content["year"])
            if year < 2022:
                return year
            else:
                pass
        except:
            pass

    def get_duration(self, content):
        try:
            duration = int(content["duration_minutes"])
            return duration
        except:
            pass

    def get_external_ids(self, content):
        """
        Metodo para mostrar las ids externas que entrega.
        En cuanto a ID solo trae la de 'kp', después, de otras páginas
        trae el rating o la fecha de salida.
        """
        try:
            external_ids = {
                "Provider": "Kp",
                "Id": content["kp_id"]

            }
            return external_ids
        except:
            pass

    def get_synopsis(self, content):
        try:
            synopsis = content["synopsis"]
            return synopsis
        except:
            pass

    def get_image(self, content):
        """
        Metodo para conseguir las imagenes.
        Como vimos que habian posters, miniaturas e imagenes de promo, intentamos traerlas 
        con un for que deberia funcionar.
        """
        try:
            image = {
                "ImagenesPromocionales": [content["promo_images"]["url"] for content["promo_images"]["url"] in content["promo_images"] if content["promo_images"]],
                "Posters": [content["poster_originals"]["path"] for content["poster_originals"]["path"] in content["poster_originals"] if content["poster_originals"]],
                "Miniaturas": [content["thumbnails"]["path"] for content["thumbnails"]["path"] in content["thumbnails"] if content["thumbnails"]],
                "MiniaturasOriginales": [content["thumb_originals"]["path"] for content["thumb_originals"]["path"] in content["thumb_originals"] if content["thumb_originals"]],
            }   
            return image
        except:
            pass

    def get_rating(self, content):
        """
        Metodo para traer los generos.        
        """
        try:
            genres = content["restrict"]
            return genres
        except:
            pass

    def get_provider(self):
        """
        Metodo para los provider.
        Por parte de la pagina no hay algo relacionado a lo pedido.
        """
        return None

    def get_genres(self, content):
        """
        La página trae los generos en un formato de código interno que hace referencia a las palabras (generos).
        Por el momento, no encontramos la forma de hacerlo por api.

        Posibles resoluciones: 1- BS4 o Selenium | 2- Hardcodeo.

        """

        pass
    
    def get_cast(self, content):
        """
        Por api no parece darlo, pero en el link de cada contenido, hay un apartado de cast "url + /person"
        así que seguramente se va a poder sacar por bs4.
        
        """
        deeplink = self.get_Deeplinks + "/" + "person"

        request = self.sesion.get(deeplink)

        soup = BeautifulSoup(request.text, 'html.parser')
              
        actores_contenidos = soup.find('div', {'class':'gallery movieDetails__gallery', 'data-test':"actors_actors_block"})
       
        actores = []

        for item in actores_contenidos:
            nombre = actores_contenidos.find('div', {'class':"slimPosterBlock__title"})
            apellido = actores_contenidos.find('div', {'class':"slimPosterBlock__secondTitle"})
            actores.append(nombre, apellido)
            print(actores)
        
        return actores

    def get_directors(self, content):
        deeplink = self.get_Deeplinks + "/" + "person"

        request = self.sesion.get(deeplink)

        soup = BeautifulSoup(request.text, 'html.parser')

        directores_contenidos = soup.find('div', {'class':'gallery movieDetails__gallery', 'data-test':"actors_directors_block"})

        directores = []

        for item in directores_contenidos:
            nombre = directores_contenidos.find('div', {'class':"slimPosterBlock__title"})
            apellido = directores_contenidos.find('div', {'class':"slimPosterBlock__secondTitle"})
            directores.append(nombre, apellido)
            print(directores)

        return directores

    def get_availability(self, content):
        """
        Metodo para chequear la disponibilidad.    
        Devuelve una lista con los paises en los cual está disponible el contenido.    
        """

        try:
            availability = content["available_in_countries"]
            return availability
        except:
            pass


    def get_download(self, content):
        """
        Metodo para ver si se puede descargar.
        Devuelve un booleano        
        """

        try:
            download = content["allow_download"]
            return download
        except:
            pass    

    def get_isOriginal(self, content):
        """
        Metodo para ver si es original de la página.
        
        Devuelve un booleano        
        """

        try:
            original = None #aca estaba repetido el metodo download
            return original
        except:
            pass         

    def get_isBranded(self,content):
        pass

    def get_isAdult(self,content):
        pass

    def get_package(self,content):
        pass

    def get_crew(self,content):
        pass