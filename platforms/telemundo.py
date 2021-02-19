# -*- coding: utf-8 -*-
import time
import requests
import hashlib   
import pymongo 
import re
import json
import platform
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from handle.replace         import _replace
from common                 import config
from datetime               import datetime
from handle.mongo           import mongo
from slugify                import slugify
from handle.datamanager     import Datamanager
from updates.upload         import Upload
from bs4                    import BeautifulSoup
from selenium.webdriver     import ActionChains
from handle.payload_testing import Payload
import sys

class Telemundo():
    def __init__(self, ott_site_uid, ott_site_country, type):
        self._config                = config()['ott_sites'][ott_site_uid]
        self._platform_code         = self._config['countries'][ott_site_country]
        #self._start_url             = self._config['start_url']
        self._created_at            = time.strftime("%Y-%m-%d")
        self.mongo                  = mongo()
        self.titanPreScraping       = config()['mongo']['collections']['prescraping']
        self.titanScraping          = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodios  = config()['mongo']['collections']['episode']
        # self.driver                 = webdriver.Firefox()
        self.sesion = requests.session()
        self.skippedTitles=0
        self.skippedEpis = 0
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

    def __query_field(self, collection, field, extra_filter=None):
        if not extra_filter:
            extra_filter = {}

        find_filter = {
            'PlatformCode': self._platform_code,
            'CreatedAt': self._created_at,
        }

        find_filter.update(extra_filter)

        query = self.mongo.db[collection].find(
            filter=find_filter,
            projection={
                '_id': 0,
                field: 1,
            },
            no_cursor_timeout=False
        )

        query = {item[field] for item in query}

        return query

    def _scraping(self, testing = False):
       
        scraped = Datamanager._getListDB(self,self.titanScraping)
        scrapedEpisodes = Datamanager._getListDB(self,self.titanScrapingEpisodios)
        payloads = []
        payloadsEpisodios = []
        packages = [
                        {
                            'Type': 'tv-everywhere'
                        }
                    ]
        
        urlNbc = 'https://www.nbc.com/'  # la pagina de telemundo te rederige a esta pagina para poder ver los capitulos
        """
        Aprovechando que la pagina de nbc tiene una api con toda la informacion voy a usar esa api para extraer lo que quiero
        """
        api = 'https://friendship.nbc.co/v2/graphql?variables=%7B%22name%22:%22paginatedAllShows%22,%22type%22:%22PAGE%22,%22userId%22:%223681070535274955148%22,%22platform%22:%22web%22,%22device%22:%22web%22,%22timeZone%22:%22America%2FNew_York%22,%22ld%22:true,%22profile%22:[%2200000%22],%22oneApp%22:true,%22app%22:%22nbc%22,%22language%22:%22en%22,%22authorized%22:false,%22brand%22:%22telemundo%22,%22appVersion%22:1180009%7D&extensions=%7B%22persistedQuery%22:%7B%22version%22:1,%22sha256Hash%22:%22778d8ab0f222484583c39a3bcbe74b85c9e74847a3d58579f714b6eca13ac6d9%22%7D%7D'
        json=Datamanager._getJSON(self,api)
        

        datas = json['data']['brandTitleCategories']['data']['items'] #es una lista, por lo que ha

        _platform_code = self._platform_code
        for data in datas:
            """
            Ahora  data es la primer componente, esta componente presenta un diccionario llamado 
            data y lo importante es que dentro de este diccionario hay una lista de diccionarios 
            llamada items. Esta lista, a su vez tiene  diccionarios con los datos que nos interesa.
            """
            items = data['data']['items']
            tittleAppend = [] #lista para controlar que no se repitan los titulos, ya que la api puede tener series repetidas
            for item in items:
                genre=[]
                img=[]
                title = item['data']['title']
                #controlo si esta el titulo:
                if title in tittleAppend:
                    continue
                else:
                    _id = item['data']['instanceID']
                    img.append(item['data']['image'])
                    apiSerie = 'https://friendship.nbc.co/v2/graphql?variables=%7B%22app%22:%22nbc%22,%22userId%22:%223681070535274955148%22,%22device%22:%22web%22,%22platform%22:%22web%22,%22language%22:%22en%22,%22oneApp%22:true,%22name%22:%22{}%22,%22type%22:%22TITLE%22,%22timeZone%22:%22America%2FNew_York%22,%22authorized%22:false,%22ld%22:true,%22profile%22:[%2200000%22]%7D&extensions=%7B%22persistedQuery%22:%7B%22version%22:1,%22sha256Hash%22:%22e323415cb0b53d1e95d743d9d79abdad22dbcb7129e35f92b96ffc5e3708d7cc%22%7D%7D'.format(item['data']['urlAlias'])
                    urlShow = urlNbc + item['data']['urlAlias']
                    genre.append(item['analytics']['genre'])
                    if item['component'] == 'SeriesTile':
                        _type = "serie"
                    else:
                        _type = "movie"

                    jsonSerie=Datamanager._getJSON(self, apiSerie)

                    dataEpisodio = jsonSerie['data']['bonanzaPage']['data']['sections'] #lista que contiene 5 parametros, los que nos interesa es el tercero (indice 2 porque python empieza en 0) y el ultimo para sacar los datos.
                    dataShow = dataEpisodio[-1]['data'] #ultimo
                    description = dataShow['description']
                    img.append(dataShow['image'])

                    dataSeasons = dataEpisodio[2]['data']['items'] #tercero, es un diccionario y me quedo con la lista de items

                    #### SERIE NOMBRE PAYLOADS####
                    payload=Payload(packages = packages, type = _type, image = img, id = _id, synopsis = description, cleanTitle = _replace(title), genres = genre, title = title, platformCode = _platform_code,
                                    deeplinksWeb=urlShow, timestamp=datetime.now().isoformat(), createdAt=self._created_at)
                    
                    tittleAppend.append(title)
                    Datamanager._checkDBandAppend(
                        self, payload.payloadJson(), scraped, payloads)
                    Datamanager._insertIntoDB(self, payloads, self.titanScraping)

                    #### EPISODIOS #### 
                    nameShow=title
                    parrentId=_id
                    for datoSeason in dataSeasons:
                        try:
                            datoEpisodios = datoSeason['data']['items']
                        except:
                            continue

                        for datoEpisodio in datoEpisodios:                       
                            genreEps = []
                            imgEps = []
                            if datoEpisodio['data']['secondaryTitle']:
                                year = datoEpisodio['data']['title'].split("|")[-1]
                                title = datoEpisodio['data']['secondaryTitle']
                            else:
                                title = datoEpisodio['data']['title']
                                year = datoEpisodio['data']['airDate']
                            season = int(datoEpisodio['data']['seasonNumber'])
                            episode = int(datoEpisodio['data']['episodeNumber'])
                            duration = int(datoEpisodio['data']['duration'])/60 +1
                            
                            rating = datoEpisodio['data']['rating']
                            _id = datoEpisodio['data']['instanceID']
                            img.append(datoEpisodio['data']['image'])
                            genreEps.append(datoEpisodio['analytics']['genre'])
                            description = datoEpisodio['data']['description']
                            urlEps = urlShow + datoEpisodio['data']['permalink']

                            payload = Payload(packages=packages,genres=genreEps, id=_id,image=imgEps, cleanTitle=_replace(title), parentId=parrentId, parentTitle=nameShow, title=title,
                                            platformCode=_platform_code, deeplinksWeb=urlEps, synopsis=description, timestamp=datetime.now().isoformat(), createdAt=self._created_at)
                            Datamanager._checkDBandAppend(self, payload.payloadEpisodeJson(), scrapedEpisodes, payloadsEpisodios, isEpi=True)
                            Datamanager._insertIntoDB(self, payloadsEpisodios, self.titanScrapingEpisodios)

        


        self.sesion.close()

        if not testing:
            Upload(self._platform_code, self._created_at, testing=True)


    
