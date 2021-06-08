import requests
import time 
from requests import api 
from handle.replace import _replace
from common import config
from handle.mongo import mongo
from pprint import pprint 
from bs4 import BeautifulSoup
import json
from updates.upload         import Upload


class Starz():

    def __init__(self, ott_site_uid, ott_site_country, type):

        self._config = config()['ott_sites'][ott_site_uid]
        self._platform_code = self._config['countries'][ott_site_country]
        #self._start_url = self._config['start_url']        
        self._created_at = time.strftime("%Y-%m-%d")
        self.mongo = mongo()
        self.titanPreScraping = config()['mongo']['collections']['prescraping']
        self.titanScraping = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodios = config()['mongo']['collections']['episode']
        self.api_url = self._config['api_url']
        self.sesion = requests.session()
        if type == 'return':
            '''            Retorna a la Ultima Fecha            '''            
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
    
    def _scraping(self, testing = False ):

        items = self._request_parser(self.api_url)
        payload_list = self._payloads(items)
        

        self.scraped = self.query_field(self.titanScraping, field='Id')
        # self.scraped_episodes = self.query_field(self.titanScrapingEpisodios, field='Id')

        # TODO: Aprender Datamanager
        # scraped = Datamanager._getListDB(self,self.titanScraping)
        # scraped_episodes = Datamanager._getListDB(self,self.titanScrapingEpisodios)

        # Listas con contenidos y episodios dentro (DICT):
        self.payloads = []
        self.episodes_payloads = []

        if payload_list:
            self.mongo.insertMany(self.titanScraping, payload_list)
        #if self.episodes_payloads:
        #     self.mongo.insertMany(self.titanScrapingEpisodios, self.episodes_payloads)


        # Validar tipo de datos de mongo:
        Upload(self._platform_code, self._created_at, testing=True)

        print("Fin")
    
        

        #self.mongo.insertMany(self.titanScraping, self.payloads)

        
    def _request_parser(self, uri):

        response = self.sesion.get(uri)
        if (response.status_code == 200): 
            dict_content = response.json()
        else:
            print('SERVER ERROR' + response.status_code)
        return dict_content
    
    def _payloads(self, items):
        
        items_list = items['playContentArray']['playContents']
        payload_list = []
        for item_list in items_list:
            payload = {}
            payload['Title'] = item_list['title']
            payload['ID'] = item_list['contentId']
            payload['Type'] = item_list['contentType']
            payload['Credits'] = self._get_credits(item_list['credits'])
            payload['Log Line'] = item_list['logLine']

            if payload['Type'] == "Series with Season" :
                payload['isSerie'] = self._get_data_serie(item_list['childContent'])
                payload['isSerie'] = item_list['episodeCount']
            
            try:
                payload['Actors'] = self._get_cast(item_list['actors'])
            except: 
                KeyError
            payload_list.append(payload)
        return payload_list
         
            
    def _get_cast (self, items):

        for item in items:
            payload = {
            'Name' : item['fullName'],
            'ID' : item['id']
            }
        return payload 

    def _get_credits(self, items):

        for item in items:
            payload = {}
            try :
                payload['Name'] = item['name']
                payload['Rol'] = item['keyedRoles'][0]['name']
                payload['ID'] = item['id']

            except:
                KeyError
            
        return payload 

    def _get_data_serie(self, items):

        for item in items:
            payload = {
                'Season Title' : item['childContent'][0]['title'],
                'ID' : item['childContent'][0]['contentId'],
                'Log Line' : item['logLine'],
                'Episodes' :self._get_episodes(item['childContent']),
                'Credits' : self._get_credits(item),
                'Genres' : item['childContent'][0]['genres'],
            }
        return payload 
    
    def _get_episodes(self, items):

        for item in items:
            payload = {
                "Title" : item['title'],
                'ID' : item['contentId'],
                'Logline' : item['logLine'],
                'isHD' : item['HD'],
                #'Audio Type' : item['audioType'],
                #'Downloadable' : item['downloadable']
            }
        return payload

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



        
        #     "PlatformCode":  "str", #Obligatorio
        #     "Id":
        #     "ParentId":
        #     "ParentTitle":
        #     "Episode":
        #     "Season":
        #     "Seasons":
        #     "str", #Obligatorio
        #     "str", #Obligatorio #Unicamente en Episodios
        #     "str", #Unicamente en Episodios
        #     "int", #Obligatorio #Unicamente en Episodios
        #     "int", #Obligatorio #Unicamente en Episodios
        #     [ #Unicamente para series
        #     {
        #     "Id": "str",
        #     "Synopsis": "str",
        #     "Title": "str",
        #     "Deeplink":  "str",    #Importante
        #     "Number": "int",
        #     "Year": "int",
        #     "Image": "list",
        #     "Directors": "list",
        #     "Cast": "list",
        #     "Episodes": "int"
        #     #Importante
        #     #Importante
        #     #Importante
        #     #Importante
        #     #Importante
        #     #Importante
        #     #Importante
        #     #Importante, E.J. The Wallking Dead: Season 1
        #     ],
        #     "Title":
        #     "CleanTitle":
        #     "OriginalTitle": "str",
        #     "Type":          "str",
        #     "Year":          "int",
        #     "Duration":      "int",
        #     "ExternalIds":   "list", *
        #     "Deeplinks": {
        #     "Web":       "str",
        #     "Android":   "str",
        #     "iOS": },
        #     "Synopsis":
        #     "Image":
        #     "Rating":
        #     "Provider":
        #     "Genres":
        #     "str",
        #     "IsOriginal": "bool"
        #     },
        #     ...
        #     "str", #Obligatorio
        #     "_replace(str)", #Obligatorio
        #     #Obligatorio
        #     #Important!
        #     #Obligatorio
        #     "str",
        #     "list",
        #     "str",     #Important!
        #     "list",
        #     "list",    #Important!
        #     }
        #     "Cast":          "list",
        #     "Directors":     "list",    #Important!
        #     "Availability":  "str",     #Important!
        #     "Download":      "bool",
        #     "IsOriginal":    "bool",    #Important!
        #     "IsAdult":
        #     "IsBranded":
        #     "Packages":
        #     "Country":
        #     "Timestamp":
        #     "CreatedAt":
        #     "bool",    #Important!
        #     "bool",    #Important!
        #     "list",    #Obligatorio
        #     "list",
        #     "str", #Obligatorio
        #     "str", #Obligatorio
        #     (ver link explicativo)
        #     }
        


