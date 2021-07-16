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
        - ¿Usa BS4?: Si.
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
        self.other = []

        self.get_collections()
        self.get_contents()
        self.get_content_data()


        print('movies:', len(self.movies))
        print('series:', len(self.series)) #Rari, pasa todo por episodios
        print('episodes:', len(self.episodes))
        
        self.check_other()
        self.check_trailers(self.movies)
        self.check_trailers(self.episodes)

        #log para ver que trae episodios, mas rari, episodios sueltos, seasons incompletas.
        for serie in self.series:
            print(serie['id'])
            try:
                print('Episode: ',serie['episodes'])
            except:
                pass   
            try:
                print('Season: ',serie['seasons'])
            except:
                pass
            try:
                print('Serie: ',serie['title'])
            except:
                pass

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
                self.contents_ids.append(content['id'])

    def get_content_data(self):
        '''
            Obtenemos la data de cada contenido por su id. Cada contenido tiene una lista de categories, es decir puede tener mas de un valor.
            Segun la key categories si es distinta de 1 (este contenido corresponde a canciones OST), el valor 14 corresponde a peliculas y si es 15 a series 
            (mas especificamente a episodios, por como organiza el contenido la pagina. No hay un contenido padre de serie sino un contenido por cada episodio). 
        '''
        self.contents_ids.sort()
        self.contents_ids = list(set(self.contents_ids))
        for id in self.contents_ids:
            content_api = 'https://api.ivi.ru/mobileapi/videoinfo/v6/?id={}'.format(
                str(id))
            response = self.session.get(content_api)
            json_data = response.json()
            if 'error' not in json_data :
                content = json_data['result']
                self.generic_payload(content)
                self.get_type(content)
            else:
                pass
        

               

    def check_other(self):
        '''
        '''
        if self.other:
            for other in self.other:
                print(other['id'])
        else:
            print('lista other vacia')

    def check_trailers(self, content_list):
        '''
            Pareciera no haber trailers, pero este metodo busca contenido de corta duracion por las dudas y por ahora solo imprime el id y la duracion.
        '''
        for content in content_list:
            if 'duration_minutes' in content:
                if content['duration_minutes'] < 5:
                    print('---DURACION SOSPECHOSA---')
                    print(content['id'])
                    print(content['duration_minutes'])
                else:
                    pass
            else:
                print('----CONTENIDO SIN DURACION----')
                print(content['id'])

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
    
    def generic_payload(self,content):
        '''
            Aca voy a validar el argumento content_type si es serie o movie, dependiendo del type
            va a completar los campos segun corresponda en el payload. Ej. if content_type == serie, agregar
            el par key-value 'Seasons':content['seasons'],... etc.

        # '''
        # if content_type == 'serie':
        #     pass
        # else:
        #     pass
        
        payload = {
            'PlatformCode': self._platform_code,
            'Id': self.get_id(content),
            'Crew': self.get_crew(content),
            'Title': self.get_title(content),
            'OriginalTitle': None,
            'CleanTitle': self.get_clean_title(content),
            # 'Type': content_type,
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

    def get_type(self, content):
        """
        """
        content_type = content['categories']
        if content_type:
            if 1 in content_type:
                pass
            else:
                if 14 in content_type:
                    self.movies.append(content)
                elif 15 in content_type:
                    if content['duration_minutes']:
                        self.episodes.append(content)
                    elif content['duration_minutes'] not in content:
                         self.series.append(content)
                    else:
                        pass
        else:
            pass

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

        Image = {
        "ImagenesPromocionales": None,
        "Posters": None,
        "Miniaturas": None,
        "MiniaturasOriginales": None,
        }   


        if content["promo_images"]:
            Image["ImagenesPromocionales"] = [content["url"] for content in content["promo_images"]]
        if content["poster_originals"]:
            Image["Posters"] = [content["path"] for content["path"] in content["poster_originals"]]
        if content["thumbnails"]:
            Image["Miniaturas"] = [content["path"] for content["path"] in content["thumbnails"]]
        if content["thumb_originals"]:
            Image["MinitaurasOriginales"] = [content["path"] for content["path"] in content["thumb_originals"]]
        else:
            pass
        return Image

    def get_rating(self, content):
        """
        Metodo para traer los generos.        
        """
        try:
            genres = content["restrict"]
            return genres
        except:
            pass

    def get_provider(self, content):
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
        deeplink = self.get_Deeplinks(content)

        deeplink = deeplink["web"] + "/" + "person"

        request = self.sesion.get(deeplink)

        soup = BeautifulSoup(request.text, 'html.parser')
              
        actores_contenidos = soup.find('div', {'class':'gallery movieDetails__gallery', 'data-test':"actors_actors_block"})
       
        actores = []

        for item in actores_contenidos:
            nombre = actores_contenidos.find('div', {'class':"slimPosterBlock__title"})
            apellido = actores_contenidos.find('div', {'class':"slimPosterBlock__secondTitle"})
            actor = nombre.join(apellido)
            actores.append(actor)
            print(actores)
        
        return actores

    def get_directors(self, content):
        deeplink = self.get_Deeplinks(content)

        deeplink = deeplink["web"] + "/" + "person"

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
from selenium import webdriver
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
        self.driver = webdriver.Firefox()

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
        self.episodes_payloads = None
        self.collections_ids = []
        self.contents_ids = []
        self.movies = []
        self.series = []
        self.episodes = []
        self.movies_series_ids = []
        self.episodes_ids = []

        self.get_collections()
        self.get_contents()
        self.get_payload()
        print(len(self.payloads))
        print(len(self.movies_series_ids))
 
        #self.insert_payloads_close(self.payloads, self.episodes_payloads)
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
        '''
        if self.episodes:
            for episode in self.episodes:
                if not self.isDuplicate(self.scraped_episodes,episode['id']):
                    payload = self.episode_payload(episode)
                    self.episodes_payloads.append(payload)
                    self.scraped_episodes.append(episode['id'])
        '''   
    def generic_payload(self,content,content_type):
        '''
            Aca voy a validar el argumento content_type si es serie o movie, dependiendo del type
            va a completar los campos segun corresponda en el payload.
        '''
        payload = {
            'PlatformCode': self._platform_code,
            'Id': self.get_id(content),
            'Crew': self.get_crew(content),
            'Title': self.get_title(content),
            'OriginalTitle': None,
            'CleanTitle': self.get_clean_title(content),
            'Type': content_type,
            'Year': self.get_year(content),
            'Year':None,
            'Duration': None,
            'Deeplinks': self.get_Deeplinks(content),
            'Synopsis': self.get_synopsis(content),
            'Image': self.get_image(content),
            'Rating': self.get_rating(content),
            'Provider': self.get_provider(content),
            'ExternalIds': self.get_external_ids(content),
            #'Genres': self.get_genres(content),
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
        if content_type == 'serie':
            seasons = self.season_payload(content)
            payload['Seasons'] = seasons
            payload['Playback'] = None
        else:
            payload['Duration'] = self.get_duration(content)
        return payload

    def get_episode(self,content_id):
        episode_api = 'https://api.ivi.ru/mobileapi/videoinfo/v6/?id={}'.format(str(content_id))
        response = self.session.get(episode_api)
        json_data = response.json()
        if 'error' not in json_data :
            episode = json_data['result']
            if not self.isDuplicate(self.episodes_ids, episode['id']):
                self.episodes.append(episode)
                self.episodes_ids.append(episode['id'])
    
    def episode_payload(self,content):
        episode = {
            'PlatformCode':self._platform_code,
            'ParentId': None,
            'ParentTitle': None,
            'Id': None,
            'Title':None ,
            'Episode':None,
            'Season': None,
            'Year': None,
            'Image':None ,
            'Duration':None,
            'Deeplinks':{
                'Web':None,
                'Android': None,
                'iOS':None ,
            },
            'Synopsis':None,
            'Rating':None,
            'Provider':None,
            'ExternalIds': None,
            'Genres': None,
            'Cast':None,
            'Directors':None,
            'Availability':None,
            'Download': None,
            'IsOriginal': None,
            'IsAdult': None,
            'Country': [self.ott_site_country],
            'Packages': None,
            'Timestamp': datetime.datetime.now().isoformat(),
            'CreatedAt': self._created_at,
        }
        return episode

    def season_payload(self,content):
        seasons_list=[]
        for key, season in content['seasons_extra_info'].items():
            season_num = int(key) + 1
            season_str = str(season_num)
            s = {
                "Id":season['season_id'], 
                "Synopsis": self.get_seasons_synopsis(content,str(season_str)), 
                "Title": self.get_title(season),
                #"Deeplink": self.get_Deeplinks(content), #ver como hacer deeplink para seasons
                "Deeplink": {
                    "Web": None,
                    'Android': None,
                    'iOS': None,
                },
                "Number": season_num, 
                "Year": None, 
                "Image": None, 
                "Directors": None, 
                "Cast": None, 
                "Episodes": season['max_episode'], 
                "IsOriginal": None 
            }
            seasons_list.append(s)
        return seasons_list

    def get_seasons_synopsis(self,content,season_num):
        synopsis = None
        try:
            descriptions = content['seasons_description']
            if season_num in descriptions:
                synopsis = descriptions[season_num]
        except:
            pass
        return synopsis

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
        '''
            Paso el id a str porque asi lo pide el payload para hacer el upload
        '''
        try:
            id = str(content["id"])
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

        Image = {
        "ImagenesPromocionales": None,
        "Posters": None,
        }   


        if content["promo_images"]:
            Image["ImagenesPromocionales"] = [content["url"] for content in content["promo_images"]]
        if content["poster_originals"]:
            Image["Posters"] = [content["path"] for content["path"] in content["poster_originals"]]
        else:
            pass
        return Image

    def get_rating(self, content):
        """
        Metodo para traer los generos.        
        """
        try:
            genres = content["restrict"]
            return genres
        except:
            pass

    def get_provider(self, content):
        """
        Metodo para los provider.
        Por parte de la pagina no hay algo relacionado a lo pedido.
        """
        return None

    def get_genres(self, content):
        genres = None
        content_genres=[]
        content_categories=[]
        
        for key,val in content['genres'].items():    
            content_genres.append(val)
        for key, val in content['categories'].items():
            content_categories.append(val)
 
        if content_genres and content_categories:
            response = self.session.get(self.categories_api)
            json_data = response.json()
            categories_list = json_data['result']
            for categorie in categories_list:
                if categorie['id'] in content_categories:
                    for genre in categorie['genres']:
                        for key,val in genre.items():
                            if val['id'] in content_genres:
                                genres.append(val['hru'])
                            else: pass
                else: pass
        else: pass
        return genres
    
    def get_cast(self, content):
        """
        Se consigue mediante BS4 el contenido del cast.
        Hay tantos "find()" xq no traía el contenido de la página de una.
        Al final realiza un for e indexa todo a una lista
        
        """
        deeplink = self.get_Deeplinks(content)
        deeplink = deeplink["Web"] + "/" + "person"
        request = self.session.get(deeplink)
        soup = BeautifulSoup(request.text, 'html.parser')        
        general_content = soup.find('div', {'class':'page-wrapper'}) #Contenido general del sector que necesitamos usar.
        section_content = general_content.find('div', {'class':'pageSection__container-inner'}) #Seccionamos la data (ya que no trae el html entero).
        button = section_content.find('div', {'class':'content_creators__showAllCreators'})
        actors_block = section_content.find('div', attrs={'class':'gallery movieDetails__gallery', 'data-test':"actors_actors_block"}) #Traemos el bloque de pagina que hace referencia a los actores.
        actors_content = actors_block.find('ul', {'class':'gallery__list gallery__list_slimPosterBlock gallery__list_type_person'})
        actores = []

        if button is None:
            for item in actors_content:
                nombre = item.find('div', {'class':"slimPosterBlock__title"}).contents[0]
                try:
                    apellido = item.find('div', {'class':"slimPosterBlock__secondTitle"}).contents[0]
                except: 
                    apellido = " " #En lo casos donde no hay apellidos, enviamos un espacio.
                actor = nombre + " " + apellido
                actores.append(actor)
        else:  
            
            self.driver.get(deeplink) 
            content_button = self.driver.find_element_by_class_name('content_creators__showAllCreators')
            try:
                other_button = self.driver.find_element_by_class_name('lowest-teaser__close')
                other_button.click()
            except:
                pass
            content_button.click()
            html = self.driver.page_source
            soup = BeautifulSoup(html, 'html.parser')        
            general_content = soup.find('div', {'class':'page-wrapper'}) #Contenido general del sector que necesitamos usar.
            section_content = general_content.find('div', {'class':'pageSection__container-inner'}) #Seccionamos la data (ya que no trae el html entero).
            actors_block = section_content.find('div', attrs={'class':'gallery movieDetails__gallery', 'data-test':"actors_actors_block"}) #Traemos el bloque de pagina que hace referencia a los actores.
            actors_content = actors_block.find('ul', {'class':'gallery__list gallery__list_slimPosterBlock gallery__list_type_person'})
            actores = []
            for item in actors_content:
                nombre = item.find('div', {'class':"slimPosterBlock__title"}).contents[0]
                try:
                    apellido = item.find('div', {'class':"slimPosterBlock__secondTitle"}).contents[0]
                except: " " #En lo casos donde no hay apellidos, enviamos un espacio.
                actor = nombre + " " + apellido
                actores.append(actor)

        
        return actores

    def get_directors(self, content):
        """
        Script parecido al del cast.
        Traemos los directores y los indexamos a una lista.
        
        """
        deeplink = self.get_Deeplinks(content)
        deeplink = deeplink["Web"] + "/" + "person"
        request = self.session.get(deeplink)
        soup = BeautifulSoup(request.text, 'html.parser')        
        general_content = soup.find('div', {'class':'page-wrapper'}) #Contenido general del sector que necesitamos usar.
        section_content = general_content.find('div', {'class':'pageSection__container-inner'}) #Seccionamos la data (ya que no trae el html entero).
        directors_block = section_content.find('div', attrs={'class':'gallery movieDetails__gallery', 'data-test':"actors_directors_block"}) #Traemos el bloque de pagina que hace referencia a los directores.
        directors_content = directors_block.find('ul', {'class':'gallery__list gallery__list_slimPosterBlock gallery__list_type_person'})
        directores = []

        for item in directors_content:
            nombre = item.find('div', {'class':"slimPosterBlock__title"}).contents[0]
            try:
                apellido = item.find('div', {'class':"slimPosterBlock__secondTitle"}).contents[0]
            except: 
                apellido = " " #En lo casos donde no hay apellidos, enviamos un espacio.
            director = nombre + " " + apellido
            directores.append(director)

        
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
        """
        Hay que chequear bien, pero parece no haber algo así en la página.
        """

    def get_isAdult(self,content):
        """
        Metodo para ver si es contenido para adultos.
        Devuelve un booleano        
        """
        try:
            isAdult = content["is_erotic"]
            return isAdult
        except:
            pass    
    def get_package(self,content):
        """
        Metodo para el package.      
        """
        try:
            packages = content["content_paid_type"]
            return packages
        except:
            pass  
    def get_crew(self,content):
        """
        Script parecido al del cast.
        Se trae el cast según su apartado, productores, operadores, etc.
        Se hace un if para evitar que rompa al no encontrar alguno de los apartados.
        
        """
        deeplink = self.get_Deeplinks(content)
        deeplink = deeplink["Web"] + "/" + "person"
        request = self.session.get(deeplink)
        soup = BeautifulSoup(request.text, 'html.parser')        
        general_content = soup.find('div', {'class':'page-wrapper'}) #Contenido general del sector que necesitamos usar.
        section_content = general_content.find('div', {'class':'pageSection__container-inner'}) #Seccionamos la data (ya que no trae el html entero).
        crew_option = ['producers', 'operators', 'painter', 'editor', 'screenwriters', 'montage', 'composer']
        crew = []
        for option in crew_option:
            crew_block = section_content.find('div', attrs={'class':'gallery movieDetails__gallery', 'data-test':"actors_{}_block".format(option)})
            if crew_block == None:
                pass
            else:
                crew_content = crew_block.find('ul', {'class':'gallery__list gallery__list_slimPosterBlock gallery__list_type_person'})
                for item in crew_content:
                    rol = crew_block.find('span', {'class':'gallery__headerLink'}).contents[0]
                    nombre = item.find('div', {'class':"slimPosterBlock__title"}).contents[0]
                    try:
                        apellido = item.find('div', {'class':"slimPosterBlock__secondTitle"}).contents[0]
                    except: 
                        apellido = " " #En lo casos donde no hay apellidos, enviamos un espacio.
                    persona = nombre + " " + apellido
                    crew_dict = {
                        "Role": rol,
                        "Name": persona
                    }

                    crew.append(crew_dict)

        return crew
