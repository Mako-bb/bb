from pprint import pp
import time
import requests
from requests.models import Response
#from handle.replace import _replace
from common import config
from handle.mongo import mongo
from updates.upload         import Upload

# from time import sleep
# import re
 
class Pluto_mv():
    """
    """
    def __init__(self, ott_site_uid, ott_site_country, type):
        self._config =                  config()['ott_sites'][ott_site_uid]
        self._platform_code =           self._config['countries'][ott_site_country]
        # self._start_url =             self._config['start_url']
        self._created_at =              time.strftime("%Y-%m-%d")
        self.mongo =                    mongo()
        self.titanPreScraping =         config()['mongo']['collections']['prescraping']
        self.titanScraping =            config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodios =   config()['mongo']['collections']['episode']
 
        self.api_url =      self._config['api_url']
        self.api_serie =    self._config['api_serie']
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
   
    #####

    def _scraping(self, testing=False):
        payloads = []
        episodes = []
        
        contents_list = self.get_contents()
        for content in contents_list:
            payload = self.get_payload(content)
            payloads.append(payload)
               

    def request(self, url):
        response = self.session.get(url)
        if response.status_code == 200:
            return response

    def get_contents(self):
        uri = self.api_url
        response = self.request(uri)
        dict_contents = response.json()
        list_categories = dict_contents['categories']
        for categories in list_categories:
            for items in categories['items']:
                try:
                    if items['type'] == 'movie':            #MOVIE
                        payloads = self.get_payload(items)
                        self.mongo.insertMany(self.titanScraping, payloads)
                         
                    else:                                   #SERIE
                        uri_epi = self.api_serie + items['slug']
                        response_epi = self.request(uri_epi)
                        list_epi = response_epi.json()
                        
                        episodes = self.get_payload(list_epi)
                        self.mongo.insertMany(self.titanScrapingEpisodios, episodes)
       
                except:
                    print("UPS")
        
        #self.mongo.insertMany(self.titanScraping, payloads)        
        #self.mongo.insertMany(self.titanScrapingEpisodios, episodes)
    
    def get_payload(self, dict_metadata):
        payload = {}
        payload['Id'] =         dict_metadata['_id']
        payload['Title'] =      dict_metadata['name']
        payload['Type'] =       dict_metadata['type']
        payload['Duration'] =   self.get_duration(dict_metadata)
        payload['Deeplinks'] =  self.api_serie + dict_metadata['slug']
        payload['Synopsis'] =   dict_metadata.get('description')
        payload['Image'] =      dict_metadata.get(['covers'][1]['url'])  ###probar
        payload['Rating'] =     dict_metadata['rating']
        payload['Genre'] =      dict_metadata['genre']
        payload['Packages'] =   "free-vod"
        
        return payload

    def get_duration(self, dict_metadata):
        return int(dict_metadata['allotment'] // 60) or int(dict_metadata['duration'] // 60000)
