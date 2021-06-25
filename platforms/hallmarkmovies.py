 # -*- coding: utf-8 -*-
import json
import time
import requests
import hashlib   
from common                 import config
from bs4                    import BeautifulSoup
from selenium               import webdriver
from datetime               import datetime
from handle.mongo           import mongo
from updates.upload         import Upload
from handle.datamanager     import Datamanager
from handle.replace         import _replace

class HallMark():
    def __init__(self, ott_site_uid, ott_site_country, type):
        self._config                = config()['ott_sites'][ott_site_uid]
        self._created_at            = time.strftime("%Y-%m-%d")
        self.mongo                  = mongo()
        self._platform_code         = self._config['countries'][ott_site_country]   
        self.titanScraping          = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodios = config()['mongo']['collections']['episode']
        self.sesion                 = requests.session()
        self.skippedEpis            = 0
        self.skippedTitles          = 0

        
        if type == 'scraping':
            self._scraping()

        if type == 'testing':
            self._scraping(testing = True)
        
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
            
    def _scraping(self, testing = False):       
        
        browser = webdriver.Firefox()

        browser.get('https://www.hmnow.com/')

        time.sleep(5)

        # browser.find_element_by_link_text('Sign in').click()

        browser.find_element_by_xpath('//button[@data-cy="Header_menuUtils_button"]').click()


        user = browser.find_element_by_xpath('//input[@id="signInEmailAddress"]')
        password = browser.find_element_by_xpath('//input[@id="currentPassword"]')

        user.send_keys('nv@bb.vision')
        password.send_keys('Asdasd123$')

        time.sleep(2)

        browser.find_element_by_xpath('//button[@class="sc-bdVaJa hwAwMP sc-hMqMXs dkZZip"]').click()


        # browser = webdriver.Firefox()

        # browser.get('https://www.hbo.com/movies/catalog')

        # # soup = BeautifulSoup(browser.page_source, 'html.parser')

        # contenedor = browser.find_element_by_xpath('//div[@class="components/MovieGrid--container"]')

        # peliculas = contenedor.find_elements_by_xpath('.//div[@class="modules/cards/CatalogCard--container modules/cards/MovieCatalogCard--container modules/cards/CatalogCard--notIE modules/cards/CatalogCard--desktop"]')

        # for peli in peliculas:

        #     title = peli.find_element_by_xpath('.//p[@class="modules/cards/CatalogCard--title"]').text

        #     print(title)
        #     print(peli.find_element_by_xpath('.//p[@class="modules/cards/CatalogCard--details"]').get_attribute('class'))

        #     x = input()
        
        
        