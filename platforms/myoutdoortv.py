# -*- coding: utf-8 -*-
import time
import requests
import hashlib   
import pymongo 
import re
import json
import platform
from handle.replace         import _replace
from common                 import config
from datetime               import datetime
from handle.mongo           import mongo
from slugify                import slugify
from bs4                    import BeautifulSoup
from selenium               import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from handle.datamanager     import Datamanager
from updates.upload         import Upload
'''
MyOutdoorTV es una plataforma que requiere suscripcion paga para la mayoria de su contenido aunque cuenta con algun contenido free-vod. 
Es posible acceder sin necesidad de VPN. La plataforma cuenta con todas series y en algunos casos el mismo epidosio forma parte de 2 series diferentes. 
Este es un detalle a tener en cuenta, por lo que se optó por generar un ID de episodio compuesto por ID Serie + ID episodio con hashlib, de esta forma
evitamos los IDs repetidos en aquellos episodios iguales que forman parte de 2 series a la vez. El scraping comienza haciendo una requests a traves de 
una API a las categorias mostradas en el sitio (path), dentro de las cuales encuentra las series y luego se realiza un request mas a cada serie donde
se obtienen los episodios de las mismas. Si bien el scraping lo realizamos a traves de API la misma no aporta mucha informacion para el payload. 
Pensando a futuro, el codigo tiene comentado (linea 248 a 252) una parte que genera un archivo json con aquellos episodios repetidos para tenerlos 
identificados. 
'''
class MyOutdoorTV():
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

    def _scraping(self, testing = False):
        listDBMovie = Datamanager._getListDB(self,self.titanScraping)
        listDBEpi = Datamanager._getListDB(self,self.titanScrapingEpisodios)

        listPayload = []
        listPayloadEpi = []

        ### {ID : {TITLE,DEEPLINK}} OF SERIES ###
        series_list = {}
        ### CHECK REPEAT EPISODES ###
        check_episodes = []  
        repeat_episodes = {}  ### TO IDENTIFY REPEAT EPISODES
        ### CATEGORIES WEBSITE
        path = ['/category/hunting',
            '/category/eu-hunting',
            '/category/fishing',
            '/category/adventure',
            '/category/subtitles',
            '/category/watchlists',
            '/category/global-watchlists',
            '/category/shooting',
            '/category/hunters-video-channel',
            '/category/recipes',
            '/category/tips']
        ### ROOT WEBLINK ###
        weblink = 'https://app.myoutdoortv.com'
        ### GET SERIES LIST OF CATEGORIES ###
        for category in path:
            offset = 0
            URL = ('https://prod-api-cached-2.viewlift.com/content/pages?site=myoutdoortv&path='+category+'&includeContent=true&offset='+str(offset)+'&languageCode=en&countryCode=AR')
            reqjson = Datamanager._getJSON(self,URL)
            content_list = reqjson['modules'][1]['contentData']
            while content_list != []:
                for content in content_list:
                    content_id = content['gist']['id']
                    content_title = content['gist']['title']
                    deeplink = content['gist']['permalink']
                    if not content_id in series_list:
                        series_list[content_id] = {'title':content_title,'deeplink':deeplink,'category':category}
                        
                offset += 20
                URL = ('https://prod-api-cached-2.viewlift.com/content/pages?site=myoutdoortv&path='+category+'&includeContent=true&offset='+str(offset)+'&languageCode=en&countryCode=AR')
                reqjson = Datamanager._getJSON(self,URL)
                content_list = reqjson['modules'][1]['contentData']
        ### GET SERIES & EPISODES INFO
        for serie in series_list:
            ### ID ###
            serie_id = serie
            ### TITLE ### 
            serie_title = series_list[serie]['title']
            ### DEEPLINK ###
            serie_deeplink = series_list[serie]['deeplink']           
            URL = 'https://prod-api-cached-2.viewlift.com/content/pages?path='+serie_deeplink+'&site=myoutdoortv&includeContent=true&moduleOffset=0&moduleLimit=6&languageCode=en&countryCode=AR'
            request = Datamanager._getJSON(self,URL)
            ### TYPE ### 
            typeOf = 'serie' 
            ### SYNOPSIS ###
            serie_synopsis = request['metadataMap']['description']
            ### IMAGE ###
            serie_image = request['metadataMap']['image'].split(',')
            ### PACKAGE ### 
            packages = [
                            {
                            'Type': 'subscription-vod'
                            }
                        ]
            ### SERIE PAYLOAD ###
            payload = {
                    'PlatformCode'      : self._platform_code,
                    'Id'                : serie_id,
                    'Type'              : typeOf,
                    'Title'             : serie_title,
                    'CleanTitle'        : _replace(serie_title),
                    'OriginalTitle'     : None,
                    'Year'              : None,
                    'Duration'          : None,
                    'Deeplinks'         : {
                                        'Web': weblink + serie_deeplink,
                                        'Android': None,
                                        'iOS': None
                    },
                    'Synopsis'          : serie_synopsis,
                    'Image'             : serie_image,
                    'Rating'            : None,
                    'Provider'          : None,
                    'Genres'            : None,
                    'Cast'              : None,
                    'Directors'         : None,
                    'Availability'      : None,
                    'Download'          : None,
                    'IsOriginal'        : None,
                    'IsAdult'           : None,
                    'Packages'          : packages,
                    'Country'           : None,
                    'Timestamp'         : datetime.now().isoformat(),
                    'CreatedAt'         : self._created_at
                }
            Datamanager._checkDBandAppend(self,payload,listDBMovie,listPayload)
            
            if request['modules'][1]['contentData'][0].get('seasons') :
                seasons = request['modules'][1]['contentData'][0]['seasons']
                for season in seasons:
                    if 'Season' in season['title']:
                        try:
                            season_number = int(season['title'].strip('Season '))
                        except ValueError:
                            season_number = None
                    else:
                        season_number = 0
                    ### PARENT ID ###
                    parent_id = serie_id
                    episodes = season['episodes']
                    ### MANUAL EPISODES NUMBER ###
                    episodes_number = None
                    for episode in episodes:
                        #if 'watchlist' in serie_title.lower():
                        #    continue
                        ### EPISODE ID (HASHID) CREATED BY SERIEID + EPISODE ID###
                        episode_id = episode['id']
                        if episode_id in check_episodes:
                            episode_hash_id = hashlib.md5((parent_id+episode_id).encode("UTF-8")).hexdigest()
                            repeat_episodes[episode_hash_id] = {'Id':episode_id,'title':episode['title'],'deeplink':episode['gist']['permalink'],'serieTitle':serie_title,'serieId':parent_id}
                            episode_id = episode_hash_id
                        else:
                            check_episodes.append(episode_id)
                            episode_hash_id = hashlib.md5((parent_id+episode_id).encode("UTF-8")).hexdigest()
                            episode_id = episode_hash_id
                        ### PARENT TITLE ###
                        parent_title = serie_title
                        ### EPISODE TITLE ###
                        episode_title = episode['title']
                        ### EPISODE TYPE ###
                        episode_type = typeOf
                        ### EPISODE DURATION ###
                        episode_duration = episode['gist']['runtime']//60
                        ### EPISODE DEELINK ###
                        episode_deeplink = episode['gist']['permalink']
                        ### EPISODE SYNOPSIS ###
                        episode_synopsis = episode['gist']['description']
                        ### EPISODE LIST OF IMAGES ###
                        if episode['gist'].get('videoImageUrl'):
                            episode_image = episode['gist']['videoImageUrl'].split(',')
                        else:
                            episode_image = None
                        ### EPISODE RATING ###
                        episode_rating = episode['parentalRating']
                        ### EPISODE PACKAGE ###
                        episode_package = episode['pricing']['type']
                        if episode_package == 'AVOD' or episode_package == 'FREE':
                            episode_package = 'free-vod'
                        else:
                            episode_package = 'subscription-vod'
                        packages = [
                            {
                            'Type': episode_package
                            }
                        ]
                        ### EPISODE PAYLOAD ###
                        payloadEpi = {
                        'PlatformCode'  : self._platform_code,
                        'ParentId'      : parent_id,
                        'ParentTitle'   : parent_title,
                        'Id'            : episode_id,
                        'Title'         : episode_title,
                        'Episode'       : episodes_number,
                        'Season'        : season_number,
                        'Year'          : None,
                        'Duration'      : episode_duration,
                        'Deeplinks'     : {
                            'Web': weblink + episode_deeplink,
                            'Android': None,
                            'iOS': None
                        },
                        'Synopsis'      : episode_synopsis,
                        'Image'         : episode_image,
                        'Rating'        : episode_rating,
                        'Provider'      : None,
                        'Genres'        : None,
                        'Cast'          : None,
                        'Directors'     : None,
                        'Availability'  : None,
                        'Download'      : None,
                        'IsOriginal'    : None,
                        'IsAdult'       : None,
                        'Country'       : None,
                        'Packages'      : packages,
                        'Timestamp'     : datetime.now().isoformat(),
                        'CreatedAt'     : self._created_at
                        }
                        
                        Datamanager._checkDBandAppend(self,payloadEpi,listDBEpi,listPayloadEpi,isEpi=True)


            
        Datamanager._insertIntoDB(self,listPayload, self.titanScraping)
        Datamanager._insertIntoDB(self,listPayloadEpi,self.titanScrapingEpisodios)

        self.sesion.close()
        Upload(self._platform_code, self._created_at, testing=True)

        ### CREATE JSON FILE WITH REPEAT EPISODES ###
        #file = open('repeatEpi.json','w+')
        #jsonfile = json.dumps(repeat_episodes)
        #file.write(jsonfile)
        #file.close()
        ### python main.py MyOutdoorTV --c US --o scraping ###
