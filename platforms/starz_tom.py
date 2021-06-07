import requests
import time 
from requests import api 
from handle.replace import _replace
from common import config
from handle.mongo import mongo
from pprint import pprint 
from bs4 import BeautifulSoup
import json


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
        self._payloads(items)  

        
    def _request_parser(self, uri):

        response = self.sesion.get(uri)
        if (response.status_code == 200): 
            dict_content = response.json()
        else:
            print('SERVER ERROR' + response.status_code)
        return dict_content
    
    def _payloads(self, items):
        
        items_list = items['playContentArray']['playContents']
        payload = {}
        for item_list in items_list:
            payload['Title'] = item_list['title']
            payload['ID'] = item_list['contentId']
            payload['Type'] = item_list['contentType']
            payload['Cast'] = self._get_cast(item_list['actors'])
            print(payload['Cast'])



    def _get_cast (self, items):

        for item in items:
            payload = {}
            payload['Name'] = item['fullName']
            payload['ID'] = item['id']
        return payload 


        
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
        


