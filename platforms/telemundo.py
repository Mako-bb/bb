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
        """
        ¿VPN? SI
        ¿API,HTML o SELENIUM? API

        Telemundo por si sola no tiene api pero cuandop queres ver un contenido de telemundo la misma pagina te manda
        a NBC que presenta una api con todo el contenido de telemundo, por lo que hacer un scraping de telemundo o 
        hacerlo a NBC filtrando el contenindo a telemundo es casi lo mismo. Por lo que realizo con la api de NBC.
        """
        start_time = time.time()
        scraped = Datamanager._getListDB(self,self.titanScraping)
        scrapedEpisodes = Datamanager._getListDB(self,self.titanScrapingEpisodios)
        payloads = []
        payloadsEpisodios = []
        packages = [
                        {
                            'Type': 'subscription-vod'
                        }
                    ]
        
        urlNbc = self._config['urls']['nbc_url']  # la pagina de telemundo te rederige a esta pagina para poder ver los capitulos
        """
        Aprovechando que la pagina de nbc tiene una api con toda la informacion 
        voy a usar esa api para extraer lo que quiero. Aparte la API esta organizada por dicionarios y lista,
        por lo que voy tomando los diccionarios para quedarme con las listas que presenta los datos,
        a veces me quedo con una componente de la lista en particular porque presenta todos los datos que 
        necesito. Luego, las listas adentro presentan diccionarios para acceder a los datos.
        """
        api = self._config['apis']['nbc_api']
        json=Datamanager._getJSON(self,api,showURL=False)
        

        items = json['data']['brandTitleCategories']['data']['items'][1]['data']['items'] #el 1 viene porque necesito la segunda componente de la lista, ahi estan todos los datos 

        _platform_code = self._platform_code
        
        for item in items:
            genre=[]
            img=[]
            title = item['data']['title']
        
            _id = hashlib.md5((urlNbc + item['data']['urlAlias']).encode('utf-8')).hexdigest()
            img.append(item['data']['image'])
            apiSerie = self._config['apis']['series_api'].format(item['data']['urlAlias'])
            urlShow = urlNbc + item['data']['urlAlias']
            genre.append(item['analytics']['genre'])
            if item['component'] == 'SeriesTile':
                _type = "serie"
            else:
                _type = "movie"



            jsonSerie=Datamanager._getJSON(self, apiSerie,showURL=False)

            dataEpisodio = jsonSerie['data']['bonanzaPage']['data']['sections'] #lista que contiene 5 parametros, los que nos interesa es el tercero (indice 2 porque python empieza en 0) y el ultimo para sacar los datos.
            dataShow = dataEpisodio[-1]['data'] #ultimo
            description = dataShow['description']
            img.append(dataShow['image'])

            dataSeasons = dataEpisodio[2]['data']['items'] #tercero, es un diccionario y me quedo con la lista de items

            #### SERIE NOMBRE PAYLOADS####
            payload=Payload(packages = packages, type = _type, image = img, id = _id, synopsis = description, cleanTitle = _replace(title), genres = genre, title = title, platformCode = _platform_code,
                            deeplinksWeb=urlShow, timestamp=datetime.now().isoformat(), createdAt=self._created_at)
            
            Datamanager._checkDBandAppend(
                self, payload.payloadJson(), scraped, payloads)
            Datamanager._insertIntoDB(self, payloads, self.titanScraping)

            #### EPISODIOS #### 
            nameShow=title
            parrentId=_id
            for datoSeason in dataSeasons:
                datoEpisodios=[]
                try:
                    #si todo sale bien, en la api se encuentra la informacion de los capitulos y lo extraigo normalmente
                    datoEpisodios = datoSeason['data']['items']
                except:
                    pass
                #Ahora, como no tenia la informacion tengo que ir a la Api de la temporada y extraerlo normalmente pero en este punto existen dos posibilidades, que tenga temporada con un numero o que tengo programtype con un string
                if not dataEpisodio:
                    try:
                        seasonNumber = datoSeason['meta']['seasonNumber']
                        apiSeason = self._config['apis']['season_api_format_one'].format(item['data']['urlAlias'],item['data']['urlAlias'],seasonNumber)
                        jsonSerie=Datamanager._getJSON(self, apiSeason,showURL=False)
                        datoEpisodios = jsonSerie['data']['videosSection']['data']['items']
                    except:
                        programmingType = datoSeason['meta']['programmingType']
                        apiSeason = self._config['apis']['season_api_format_two'].format(item['data']['urlAlias'],item['data']['urlAlias'],programmingType)
                        jsonSerie=Datamanager._getJSON(self, apiSeason,showURL=False)
                        datoEpisodios = jsonSerie['data']['videosSection']['data']['items']
                        # continue

                #Aca recoro la apiu de cada episodio por lo que voy a conseguir la informacion de los
                #episodios
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
                    
                    try:
                        rating = datoEpisodio['data']['rating']
                    except:
                        rating = None
                    _id = datoEpisodio['data']['instanceID']
                    imgEps.append(datoEpisodio['data']['image'])
                    genreEps.append(datoEpisodio['analytics']['genre'])
                    description = datoEpisodio['data']['description']
                    urlEps = urlShow + datoEpisodio['data']['permalink']

                    payload = Payload(packages=packages,genres=genreEps, id=_id,image=imgEps, cleanTitle=_replace(title), parentId=parrentId, parentTitle=nameShow, title=title, duration=int(duration/60),
                                    platformCode=_platform_code, season=season,episode=episode, deeplinksWeb=urlEps, synopsis=description, timestamp=datetime.now().isoformat(), createdAt=self._created_at)
                    Datamanager._checkDBandAppend(self, payload.payloadEpisodeJson(), scrapedEpisodes, payloadsEpisodios, isEpi=True)
                    Datamanager._insertIntoDB(self, payloadsEpisodios, self.titanScrapingEpisodios)

        


        self.sesion.close()

        Upload(self._platform_code, self._created_at, testing=testing)

        finish_time = time.time()
        print("Tiempo de ejecucion: "+str((finish_time-start_time)/60)+" min")
    
