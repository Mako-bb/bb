from pprint                 import pp
import time
from pymongo.common import clean_node
import requests
from requests.models        import Response
from handle.replace         import _replace
from common                 import config
from handle.mongo           import mongo
from updates.upload         import Upload
from handle.datamanager     import Datamanager
from datetime               import datetime
# from time import sleep
# import re
 
class Starz_mv():
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

        self.api_url =      self._config['api_url']
        self.session =      requests.session()

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
        # Listas de contentenido scrapeado:
        self.scraped = self.query_field(self.titanScraping, field='Id')                     #APRENDER
        self.scraped_episodes = self.query_field(self.titanScrapingEpisodios, field='Id')

        self.payloads = []
        self.episodes_payloads = []

        
        contents_list = self.get_contents()                         #GET_CONTENTS(self)
        for n, content in contents_list:
            print(f"\n----- Progreso ({n}/{len(contents_list)}) -----\n")     
            self.content_scraping(content)                          #FALTA CONTENT_SCRAPPING
            
            # Almaceno la lista de payloads en mongo:
        if self.payloads:
            self.mongo.insertMany(self.titanScraping, self.payloads)
        if self.episodes_payloads:
            self.mongo.insertMany(self.titanScrapingEpisodios, self.episodes_payloads)

        self.session.close()
    
        # Validar tipo de datos de mongo:
        Upload(self._platform_code, self._created_at, testing=True)

        print("Fin")

    
    def get_contents(self):                        #Con este metodo solo saco la informacion que vamos a utilizar
        """Método que trae los contenidos en forma de diccionario.

        Returns:
            list: Lista de diccionarios
        """
        content_list = []
        uri = self.api_url
        response = self.request(uri)                #FALTA REQUEST()
        dict_contents = response.json()
        list_byid = dict_contents['blocks']['playContentsById']
        for content in list_byid:
            content_list += content
        return content_list

    def request(self, uri):                         #Request de la apiURL

        print(uri)
        response = self.session.get(uri)
        print(response)
        if (response.status_code == 200): 
            return response

    def content_scraping(self, content):
        content_id = content['contentId']

        if not content_id in self.scraped:
            payload = self.get_payload(content)    #GET PAYLOAD
            if payload['contentType'] == 'Series with Season':
                self.get_payloadEpi(content)        #FALTA GET PAYLAOAD EPI (series)
            if payload:                              
                # 1) Almaceno el dict en la lista.
                self.payloads.append(payload)
                # 2) Almaceno el id (str) en la lista.
                self.scraped.append(content_id)
    
    def get_payload(self,content):
        elements_list = content['contentType']
        for element in elements_list:
            if element['contentType'] == 'Movie':
                self.movie_payload(element)
            elif element['contentType'] == 'Series with Season':
                self.serie_payload(element)
    
    def movie_payload(self, dict_metadata):
        duration = str(int(dict_metadata['runtime'])/60)
        clean_text= self.clean_text(dict_metadata['title'])
        deeplink = self.movie_deeplink(clean_text, dict_metadata['contentId'])
        image = "FALTA LA FOTO CHABOON"
        payloads = []
        payload = { 
            "PlatformCode": self._platform_code,        #Obligatorio 
            "Id": dict_metadata['contentId'],           #Obligatorio
            "Title": dict_metadata['title'],            #Obligatorio 
            "CleanTitle": clean_text, #Obligatorio 
            "OriginalTitle": dict_metadata['title'], 
            "Type": dict_metadata['type'], #Obligatorio 
            "Year": dict_metadata['releaseYear'], #Important! 
            "Duration": duration,                                   
            "ExternalIds": dict_metadata['mediaId'],  
            "Deeplinks": { 
            "Web": deeplink, #Obligatorio                           
            "Android": None, 
            "iOS": None, 
            }, 
            "Synopsis": dict_metadata['logLine'],                   
            "Image": [image],                                       #SACAR IMAGE
            "Rating": dict_metadata['ratingCode'], #Important! 
            "Provider": None,                                       ##IMDB?
            "Genres": [dict_metadata['genres']], #Important!        #SACAR METODO GENRES
            "Cast": None,                                           #SACAR CAST
            "Directors": None, #Important!                          #SACAR DIRECTORS 
            "Availability": None, #Important!           
            "Download": None, 
            "IsOriginal": None, #Important! 
            "IsAdult": None, #Important! 
            "IsBranded": None, #Important! (ver link explicativo)
            "Packages": "subscripcion-vod",
            "Country": None, 
            "Timestamp": datetime.now().isoformat(), #Obligatorio 
            "CreatedAt": self._created_at, #Obligatorio
            }
        payloads.append(payload)


    def clean_text(self,title):
        cleanText = title.lower()
        cleanText = _replace(cleanText)         #verificar el _replace funcione bien.
        cleanText = cleanText.replace(':', '')
        cleanText = cleanText.replace(' ', '-') 
        cleanText = cleanText.replace('¡', '') 
        cleanText = cleanText.replace('!', '')
        cleanText = cleanText.replace('ó', 'o')
        cleanText = cleanText.replace('á', 'a')
        cleanText = cleanText.replace('é', 'e')
        cleanText = cleanText.replace('í', 'i')
        cleanText = cleanText.replace('ú', 'u')      
        cleanText = cleanText.replace("'", '') 
        return cleanText

    #Deeplinks:
    def movie_deeplink(self, title, id ):
        deeplink = "https://www.starz.com/ar/es/movies/{}-{}".format(title, str(id))
        return deeplink
    def serie_deeplink(self, title, id):
        deeplink = "https://www.starz.com/ar/es/series/{}}/{}".format(title,str(id))
        return deeplink
    def seasson_deeplink(self, title,seasson, id):                                              #HACER XD
        deeplink = "https://www.starz.com/ar/es/series/{}/{}/{}".format(title,seasson,str(id))
        return deeplink
    def episode_deeplink(self,title,seasson,episode,id):
        deeplink = "https://www.starz.com/ar/es/series/{}/{}/{}/{}".format(title,seasson,episode,str(id))
        return deeplink

        