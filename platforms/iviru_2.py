from os import replace
import time
import requests
from yaml.tokens import FlowMappingStartToken
from handle.replace import _replace
from common import config
from handle.mongo import mongo
from updates.upload import Upload
from handle.payload import Payload
from handle.datamanager import Datamanager
import datetime
# from time import sleep
import re
start_time = time.time()

class Iviru():
    def __init__(self, ott_site_uid, ott_site_country, type):
        """
        iviru es una ott de Rusia que opera en todo el mundo.

        DATOS IMPORTANTES:
        - VPN: No
        - ¿Usa Selenium?: No.
        - ¿Tiene API?: Si.
        - ¿Usa BS4?: No.
        - ¿Cuanto demoró la ultima vez?. 0.7531681060791016 seconds
        - ¿Cuanto contenidos trajo la ultima vez? titanScraping: 184, titanScrapingEpisodes: 970, CreatedAt: 2021-07-05 .

        OTROS COMENTARIOS:
        ---
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
        self.titanScrapingEpisodes = config()['mongo']['collections']['episode']

        self.api_url = self._config['api_collections_url']
        self.url=self._config['url']

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

    def _scraping(self, testing = False):

        self.scraped = self.query_field(self.titanScraping, field='Id')
        self.scraped_episodes = self.query_field(self.titanScrapingEpisodes, field='Id')
        self.payloads = []
        self.episodes_payloads = []
        self.collections_ids=[]
        self.contents_ids = []
        
        self.get_collections()
        self.get_contents()
        self.get_content_data()

        self.insert_payloads_close(self.payloads,self.episodes_payloads)
        print("--- %s seconds ---" % (time.time() - start_time))
    
    def get_collections(self):
        '''
            Obtenemos todos los ids correspondientes a cada categoria de contenidos, como esta dividida la pagina
        '''
        collections_api = self.api_url
        response = self.session.get(collections_api)
        json_data=response.json()
        json_data = json_data["result"]
        for content in json_data:

            self.collections_ids.append(content['id'])

    def get_contents(self):
        '''
            Obtenemos los ids correspondientes a los contenidos por categoria
        '''
        for collection in self.collections_ids:
            collection_api = 'https://api.ivi.ru/mobileapi/collection/catalog/v5/?id={}&app_version=870'.format(str(collection))
            response = self.session.get(collection_api)
            json_data=response.json()
            json_data = json_data["result"]
            for content in json_data:
                self.contents_ids.append(content['id'])

    def get_content_data(self):
        '''
            Obtenemos la data de cada contenido por su id y armamos el payload
        '''
        for id in self.contents_ids:
            content_api = 'https://api.ivi.ru/mobileapi/videoinfo/v6/?id={}'.format(str(id))
            response = self.session.get(content_api)
            json_data=response.json()
            json_data = json_data["result"]
            for content in json_data:

                self.get_image(content)
    
    def isDuplicate(self, scraped_list, key_search):
        '''
            Metodo para validar elementos duplicados segun valor(key) pasado por parametro en una lista de scrapeados.
        '''
        isDup=False
        if key_search in scraped_list:
            isDup = True
        return isDup
    
    def insert_payloads_close(self,payloads,epi_payloads):
        '''
            El metodo checkea que las listas contengan elementos para ser subidos y corre el Upload en testing.
        '''     
        if payloads:
            self.mongo.insertMany(self.titanScraping, payloads)
        if epi_payloads:
            self.mongo.insertMany(self.titanScrapingEpisodes, epi_payloads)
        self.session.close()
        Upload(self._platform_code, self._created_at, testing=True)

    

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

    def get_type(self, content):
        """
        Metodo para definir el tipo de contenido.
        Como no hay un apartado especifico en la api que traiga el dato,
        vamos a utilizar la existencia del content["episode"] para compro-
        bar el tipo.

        Se podría verificar que el 'padre' del archivo con el cual estemos
        trabajando tenga el content["episode"] para chequear si es un episodio
        o una pelicula.
        """
        try:
            if content["episode"] == True:
                type = "serie"
                return type
            if content["episode"] == False:
                type = "movie"
                return type
        except:
            pass

    def get_year(self, content):
        try:
            year = int(content["year"])
            return year
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

    def get_Deeplinks(self, content):
        try:
            Deeplinks = {
                "Web": content["share_link"],
                "Android": None,
                "Ios": None,
            },
            return Deeplinks
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
                "ImagenesPromocionales": [content["promo_images"]["url"] for content["promo_images"]["url"] in content["promo_images"]],
                "Posters": [content["poster_originals"]["path"] for content["poster_originals"]["path"] in content["poster_originals"]],
                "Miniaturas": [content["thumbnails"]["path"] for content["thumbnails"]["path"] in content["thumbnails"]],
            }
            return image
        except:
            pass        
    
    def get_rating(self):
        """
        Metodo para los ratings.
        Por parte de la pagina no hay un sistema de calificacion por edad.
        """
        return None
    
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

    