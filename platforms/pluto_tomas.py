import time 
import requests
from requests import api 
from handle.replace import _replace
from common import config
from handle.mongo import mongo
from pprint import pprint 
from bs4 import BeautifulSoup

class Pluto():
    """    """    
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

    def _scraping(self, testing=False):
        uri = self.api_url        
        response = self.sesion.get(uri)
        if response.status_code == 200:
            dict_contents = response.json()
            payloads = self._parse_response(dict_contents)
            #pprint(data_response)
            self.mongo.insertMany(self.titanScraping, payloads)

    def _parse_response(self, response_json):
        data_response = []

        list_categories = response_json['categories']
        for category in list_categories:
            titles_categories = category['name']
            items = category['items']
            for item in items:
                payload = self._get_payload(item)
  
                data_response.append(payload)
        return data_response

    def _get_payload(self, item):
        """[summary]

        Args:
            item ([type]): [description]

        Returns:
            [type]: [description]
        """

        title = item['name']
        description = item['description']
        slug = item['slug']
        _type = item['type']
        duration = item.get('duration')
        genre = item['genre']
        id = item['_id']

        deep_link = self._get_deep_link(_type, slug)

        payload ={
                'Title': title,
                'Description' : description,
                'Duration': duration,
                'ID': id,
                'Genre': genre,
                'Deep Link': deep_link,
                'Type': _type, 
                #'Seasons': seasonsNumbers,
            }
        return payload

    def _get_deep_link(self, _type, slug ):

        if _type == 'series':
            deep_link = "https://pluto.tv/on-demand/{}/{}".format(_type, slug)
            #seasonsNumbers = categories.get('seasonsNumbers')
        else:
            deep_link = "https://pluto.tv/on-demand/{}s/{}".format(_type, slug)

        return deep_link
                

