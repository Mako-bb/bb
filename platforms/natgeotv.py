import time
from pymongo.message import insert
import requests
from bs4 import BeautifulSoup
import ast
from handle.replace import _replace
from common import config
from handle.mongo import mongo
from updates.upload         import Upload
from datetime import datetime
# from time import sleep
# import re

class Natgeotv():
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
        self.api_url = self._config['api_url']

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
        
    def _scraping(self, testing=False):
        self.serie_payloads = []
        self.episode_payloads = []
        metadata = self.get_contents(self.api_url)
        for element in metadata:
            for content in [element]:
                soup = self.bs4request(("https://www.nationalgeographic.com" + content["link"]["urlValue"]))
                isSerie = self.season_request(soup)
                if isSerie:                   # SI TIENE SEASONS, ES PORQUE ES UNA SERIE. SINO, ES UN EPISODIO
                    self.serie_payload(content, isSerie)
                else:
                    print('es un episodio')
                
                
    def serie_payload(self, content, seasons):
        image = self.get_image(content)
        payload = payload = {
            "PlatformCode": "us.national-geographic",
            "Id": content["show"]["id"],
            "Seasons": len(seasons),
            "Title": content["show"]["title"],
            "CleanTitle": _replace(content["show"]["title"]),#Obligatorio 
            "OriginalTitle": content["show"]["title"],  
            "Type": 'Show with Seasons/Episodes',
            "Year": 'ver si con BS4',
            "ExternalIds": None, 
            "ExternalIds": None,
            "Deeplinks": { 
                "Web": "https://www.nationalgeographic.com" + content["link"]["urlValue"], 
                "Android": None, 
                "iOS": None,
                }, 
            "Synopsis": content['show']['aboutTheShowSummary'],
            "Image": image,
            "Rating": 'ver si con BS4',
            "Provider": None, 
            "Genres": content['show']['genre'],
            "Directors": None, #Important! 
            "Availability": None, #Important! 
            "Download": None, 
            "IsOriginal": None, #Important! 
            "IsAdult": None, #Important! 
            "IsBranded": None, #Important! (ver link explicativo)
            "Packages": [{'Type':'subscription-vod'}],
            "Country": None, 
            "Timestamp": datetime.now().isoformat(), #Obligatorio 
            "CreatedAt": self._created_at, #Obligatorio
        }        
        #print(payload)
        
    def seasons_data(self, content):
        print(self.allSeasons)    
        
    def season_request(self, soup):
        allSeasons = soup.find_all('span', class_='titletext')
        seasons = []
        for season in allSeasons:
            season = season.text.strip()
            if season != 'You May Also Like':
                seasons.append(season)
            else:
                pass
        return seasons
        
    def get_image(self, content):
        images_list = content['images']
        image = None
        for all_images in images_list:
            for images in [all_images]:
                if 'showimages' in images['value']:
                    image = images['value']
        return image
                    
        
            

    def bs4request(self, uri):
        page = requests.get(uri)
        soup = BeautifulSoup(page.content, 'html.parser')
        return soup


    def request(self, uri):
        response = self.session.get(uri)
        contents = response.json()
        return contents

    def get_contents(self, uri):
        data_dict = self.request(uri) 
        content_list = data_dict["tiles"]
        return content_list
