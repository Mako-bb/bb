from os import replace
import time
import requests
import hashlib   
import pymongo 
import re
import json
import platform
import itertools
from handle.replace         import _replace
from common                 import config
from datetime               import datetime
from handle.mongo           import mongo
from slugify                import slugify
from bs4                    import BeautifulSoup
from selenium               import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from handle.datamanager  import Datamanager
from updates.upload         import Upload
from handle.payload_testing import Payload

class WWENetwork():
    def __init__(self, ott_site_uid, ott_site_country, type):
        self._config                = config()['ott_sites'][ott_site_uid]
        self._platform_code         = self._config['countries'][ott_site_country]
        #self._start_url             = self._config['start_url']
        self._created_at            = time.strftime("%Y-%m-%d")
        self.mongo                  = mongo()
        self.titanPreScraping       = config()['mongo']['collections']['prescraping']
        self.titanScraping          = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodes  = config()['mongo']['collections']['episode']
        self.titanScrapingEpisodios  = config()['mongo']['collections']['episode']
        self.skippedTitles = 0
        self.skippedEpis = 0
        self.start_url = self._config['urls']['start_url']
        self.payloadsShows = []
        self.payloadsEpisodes = []

        self.sesion = requests.session()
        self.headers  = {"Accept":"application/json",
                         "Content-Type":"application/json; charset=utf-8"}

        if type == 'return':
            '''
            Retorna a la Ultima Fecha
            '''
            params = {"PlatformCode" : self._platform_code}
            lastItem = self.mongo.lastCretedAt(self.titanScraping, params)
            if lastItem.count() > 0:
                for lastContent in lastItem:
                    self._created_at = lastContent['CreatedAt']
                    
            self._scraping()
        
        if type == 'scraping':
            self._scraping()

        if type == 'testing':
            self._scraping(testing = True)


    def _scraping(self, testing = False):
        saved_show_ids = Datamanager._getListDB(self,self.titanScraping)
        saved_episode_ids = Datamanager._getListDB(self,self.titanScrapingEpisodios)

        soup = Datamanager._getSoup(self,self.start_url + '/shows/',parser='lxml')

        page_data = soup.find('div',id = 'page')

        # Tengo que hacer esto porque el soup contiene muchos '\n' en el medio
        # y solo necesito 3 cosas de este html.
        show_list_html = page_data.contents[slice(4 , 11 , 3)] 

        for each in show_list_html:
            contents = each.find_all('div',class_ = 'wwe-shows__section')
            shows = contents[0].contents[1::2]

            #La pagina esta divida en Featured shows y Show plates
            if len(shows) < 3:
                show_plates = []
                for content in contents:
                    show_plates.append(content.contents[1::2][1].contents[1::2])
                shows = list(itertools.chain(*show_plates))

            for show in shows:
                if show.name != 'h2':
                    show.contents[1]
