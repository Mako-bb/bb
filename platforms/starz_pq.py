from logging import getLogRecordFactory
import threading
import time
import requests
from handle.replace         import _replace
from common                 import config
from handle.mongo           import mongo
from updates.upload         import Upload
from handle.payload         import Payload
from handle.datamanager     import Datamanager
from datetime               import datetime
# from time import sleep
# import re


class StarzPQ():
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
        self.titanScrapingEpisodes = config()['mongo']['collections']['episode']

        self.all_titles_url = self._config['all_titles_url']

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
        
        self.scraped = [] #self.query_field(self.titanScraping, field='Id')
        self.scraped_episodes = [] #self.query_field(self.titanScrapingEpisodes, field='Id')

        payloads = self.get_content()

    
    def get_content(self):
        
        payloads = []
        res_all_titles = self.request(self.all_titles_url) #me traigo el json con todos los datos
        print(len(res_all_titles["playContentArray"]["playContents"])) #consultar cantidad total
        for content in res_all_titles["playContentArray"]["playContents"]:
            
            if not content["contentId"] in self.scraped:
                self.scraped.append(content["contentId"])
                payload = self.get_payload(content)
                #print(payload)
                payloads.append(payload)
            else:
                print(content["contentId"])
               
    
    def get_payload(self, content_dict):
        """Método para crear el payload. Para titanScraping.

        Args:
            content_metadata (dict): Indica la metadata del contenido.

        Returns:
            dict: Retorna el payload.
        """
        payload = {}
        
        # Indica si el payload a completar es un episodio:        
        payload['PlatformCode'] = self._platform_code
        payload['Id'] = content_dict["contentId"]
        payload['Title'] = content_dict["title"]
        payload['OriginalTitle'] = content_dict["titleSort"]
        payload['CleanTitle'] = _replace(content_dict["title"])
        payload['Duration'] = self.get_duration(content_dict)
        payload['Type'] = self.get_type(content_dict["contentType"]) 
        payload['Year'] = self.get_year(content_dict)
        payload['Deeplinks'] = self.get_deeplinks(content_dict)
        payload['Playback'] = None #consultar
        payload['Synopsis'] = content_dict["logLine"]
        payload['Image'] = self.get_image(content_dict["contentId"]) #consultar si no hay a mano
        payload['Rating'] = content_dict["ratingCode"]
        payload['Provider'] = None
        payload['Genres'] = self.get_genres(content_dict["genres"])
        payload['Cast'] = None
        payload['Directors'] = None
        payload['Availability'] = None
        payload['Download'] = None
        payload['IsOriginal'] = None
        payload['Seasons'] = None
        payload['IsBranded'] = None
        payload['IsAdult'] = None
        payload['Packages'] = [{"Type":"free-vod"}]
        payload['Country'] = None
        payload['Crew'] = None        
        payload['Timestamp'] = datetime.now().isoformat()
        payload['CreatedAt'] = self._created_at
        return payload

    ##############################FUNCIONES PARA OBTENER CADA PAYLOAD#######################################
    def get_duration(self, content):
        if content["contentType"] == "Movie":
            return content["runtime"]//60
        else:
            return None
    
    def get_type(self, type_):
        if type_ == "Movie":
            return "movie"
        else:
            return "series"

    def get_deeplinks(self, content): #armo la url con el titulo en minusculas y con - en cada espacio. Luego le agrego el id
        title = (content["titleSort"].replace(" ","-")).lower()
        url = title+"-"+str(content["contentId"])
        return url
    
    def get_year(self, content):
        if content["contentType"] == "Movie":
            return content["releaseYear"]
        else:
            return content["minReleaseYear"]
    
    def get_image(self, id): #las imagenes de la season y las imagenes de los episodios son los mismos
        return "https://stz1.imgix.net/Web_AR/contentId/"+str(id)+"/type/KEY/dimension/1536x2048/lang/es-419"
        #Esta es para una segunda img pero hay que comprobar si para cada season o episode cambia
        #https://stz1.imgix.net/Web_AR/contentId/PONER-ID/type/STUDIO/dimension/2560x1440/lang/es-419
    
    def get_genres(self, genres):
        res = []
        for genre in genres:
            res.append(genre["description"])
        return res
    ########################################################################################################
    
    def request(self, url):
            '''
            Método para hacer una petición
            '''
            requestsTimeout = 5
            while True:
                try:
                    response = self.session.get(url, timeout=requestsTimeout)
                    return response.json()
                except requests.exceptions.ConnectionError:
                    print("Connection Error, Retrying")
                    time.sleep(requestsTimeout)
                    continue
                except requests.exceptions.RequestException:
                    print('Waiting...')
                    time.sleep(requestsTimeout)
                    continue
