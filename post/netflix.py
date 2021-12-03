# -*- coding: utf-8 -*-
import pymongo
import re
import requests
import json
from bs4 import BeautifulSoup
from common import config
from selenium              import webdriver
from pyvirtualdisplay      import Display
from handle.datamanager import Datamanager
from datetime               import datetime

class PostScraping():
    def __init__(self, platform_code, created_at):
        self._platform_code         = platform_code
        self._created_at            = created_at
        self.mongo                  = config()['mongo']['host']
        self._titanScraping         = config()['mongo']['collections']['scraping']
        self._titanScrapingEpisodes = config()['mongo']['collections']['episode']
        self._titanScrapingOriginal = config()['mongo']['collections']['original']
        self.sesion                 = requests.session()

    def run(self):
        connection = pymongo.MongoClient(self.mongo, connect=False, maxPoolSize=None)
        db = connection.titan

        # cursor = db[self._titanScraping].find(
        #     filter={
        #         'PlatformCode': self._platform_code,
        #         'CreatedAt': self._created_at,
        #     },
        #     projection={
        #         '_id': 0,
        #         'Id': 1,
        #         'Title': 1,
        #         'Deeplinks': 1
        #     }
        # )
        # cursor = list(cursor)
        # listPayload = []

        # ultimoIdencontrado = False
        # for item in cursor:
        #     if item['Id'] == "80089199":
        #         ultimoIdencontrado = True
            
        #     if ultimoIdencontrado == True:
        #         soup = Datamanager._getSoup(self,item['Deeplinks']['Web'])

        #         script = soup.findAll('script')

        #         for scriptItem in script:
        #             if str(scriptItem.text).startswith("window.netflix = window.netflix ||"):
        #                 print(str(scriptItem.text).find('"isOriginal":true'))
        #                 if str(scriptItem.text).find('"isOriginal":true') != -1:
        #                     print(item['Title'])

        #                     payload = {
        #                         'PlatformCode': self._platform_code,
        #                         'Id'          : item['Id'],
        #                         'Title'       : item['Title'],
        #                         'Deeplink'    : item['Deeplinks']['Web'],
        #                         'isOriginal'  : True,
        #                         'Timestamp'   : datetime.now().isoformat(),
        #                         'CreatedAt'   : self._created_at
        #                     }
        #                     db[self._titanScrapingOriginal].insert_one(payload)

        # soup = Datamanager._getSoup(self,"https://www.netflix.com/us-es/browse/genre/839338")
        # listPayload = []
        # for item in soup.findAll('li',{'class':'nm-content-horizontal-row-item'}):
        #     link = item.find('a',{'class':"nm-collections-title nm-collections-link"}).get('href')
        #     Id = str(link.replace("https://www.netflix.com/us-es/title/",""))
        #     payload = {
        #         'PlatformCode': self._platform_code,
        #         'Id'          : Id,
        #         'Title'       : item.find('span',{'class':"nm-collections-title-name"}).text,
        #         'Deeplink'    : link,
        #         'isOriginal'  : True,
        #         'Timestamp'   : datetime.now().isoformat(),
        #         'CreatedAt'   : self._created_at
        #     }
        #     db[self._titanScrapingOriginal].insert_one(payload)

        # self._platform_code = "hulu"
        # soup = Datamanager._getSoup(self,"https://www.hulu.com/originals")
        # listPayload = []
        # sliders = soup.findAll('div',{'class':'jsx-1977775403 NonSubSimpleCollection cu-non-sub-simple-collection'})
        # sliderAZ = sliders[len(sliders)-1]
        # for item in sliderAZ.findAll('div',{'class':'Slider__item'}):
        #     link = "https://www.hulu.com" + item.find('a',{'class':"Tile__thumbnail Tile__thumbnail--with-hover"}).get('href')
        #     Id = str(link.split("/")[4])
        #     payload = {
        #         'PlatformCode': self._platform_code,
        #         'Id'          : Id,
        #         'Title'       : item.find('div',{'style':"-webkit-box-orient:vertical;-webkit-line-clamp:2;overflow:hidden;line-height:1.18em;max-height:2.36em"}).text,
        #         'Deeplink'    : link,
        #         'isOriginal'  : True,
        #         'Timestamp'   : datetime.now().isoformat(),
        #         'CreatedAt'   : self._created_at
        #     }
        #     db[self._titanScrapingOriginal].insert_one(payload)

        # self._platform_code = "us.disneyplus"
        # self.driver = webdriver.Firefox()
        # soup = Datamanager._getSoupSelenium(self,"https://disneyplusoriginals.disney.com/")
        # listPayload = []
        # #print(soup)
        # sliders = soup.findAll('li',{'class':'slider-page'})
        # for slider in sliders:
        #     for item in slider.findAll('li',{'class':re.compile(r'col item*')}):
        #         link = item.find('a',{'class':"entity-link outer-link"}).get('href')
        #         Id = item.find('a',{'class':"entity-link outer-link"}).get('data-core-id')
        #         title = item.find('img',{'class':"thumb"})
        #         payload = {
        #             'PlatformCode': self._platform_code,
        #             'Id'          : Id,
        #             'Title'       : title.get('alt').split("-")[0].strip(),
        #             'Deeplink'    : link,
        #             'isOriginal'  : True,
        #             'Timestamp'   : datetime.now().isoformat(),
        #             'CreatedAt'   : self._created_at
        #         }
        #         print(payload)
        #         db[self._titanScrapingOriginal].insert_one(payload)

        self._platform_code = "us.amazonprimevideo"
        soup = Datamanager._getSoup(self,"https://www.amazon.com/-/es/gp/video/storefront/ref=sv_atv_0?language=es_US&merchId=originals1&ie=UTF8")
        listPayload = []
        #print(soup)
        sliders = soup.findAll('div',{'class':'_1gQKv6 u-collection tst-collection'})
        
        for slider in sliders:
            for item in slider.findAll('li',{'class':"_2KEnNZ tst-card-wrapper dv-universal-hover-enabled"}):
                link = "https://www.amazon.com" + item.find('a').get('href')
                Id = item.get('data-asin')
                title = item.find('a').get('aria-label')
                payload = {
                    'PlatformCode': self._platform_code,
                    'Id'          : Id,
                    'Title'       : title.split("-")[0].split("Season")[0].strip(),
                    'Deeplink'    : link,
                    'isOriginal'  : True,
                    'Timestamp'   : datetime.now().isoformat(),
                    'CreatedAt'   : self._created_at
                }
                print(payload)
                
                if not any(payload['Id'] == d['Id'] for d in listPayload):
                    listPayload.append(payload)
                    db[self._titanScrapingOriginal].insert_one(payload)





        