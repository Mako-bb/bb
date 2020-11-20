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
from bs4                    import BeautifulSoup
from selenium               import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from handle.datamanager  import Datamanager
from updates.upload         import Upload

class OptimumTati():
    def __init__(self, ott_site_uid, ott_site_country, type):
        self._config                = config()['ott_sites'][ott_site_uid]
        self._platform_code         = self._config['countries'][ott_site_country]
        #self._start_url             = self._config['start_url']
        self._created_at            = time.strftime("%Y-%m-%d")
        self.mongo                  = mongo()
        self.titanPreScraping       = config()['mongo']['collections']['prescraping']
        self.titanScraping          = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodes  = config()['mongo']['collections']['episode']

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

    def _scraping(self):
        #Declaramos lista DB
        listDBMovie: Datamanager._getListDB(self,self.titanScraping)
        
        #Declaramos payloads
        listPayload = []

        #Establecemos las páginas de A-E; F-J; K-O; P-T; U-Z; OTROS.
        categories = [
            "48265007", #a-e
            "48266007", #f-j
            "48270007", #k-o
            "48268007", #p-t
            "48269007", #u-z
            "48267007"  #otros
        ]

        
        for categorie in categories:
            
            index = 0
            
            while True:
                """
                req = self.currentSession.get(f"https://optimum.net/api/vod-webapp/services/v1/onyx/getTitlesForPagination/{categorie}/20/{index}?sort=1&filter=0")
                print(req.status_code, req.url)
                getjson = req.json()
                """

                url = f"https://optimum.net/api/vod-webapp/services/v1/onyx/getTitlesForPagination/{categorie}/20/{index}?sort=1&filter=0"
                getjson = Datamanager._getJSON(self,url)

                all_info = getjson["data"]["result"] ["titles"]


                for title in all_info:
                    if title.get("tms_title"):          #Titulo
                        titulo = title["tms_title"] 
                    else:
                        titulo = title["title"]

                    id_ = title["title_id"]             #ID en int

                    if title.get("actors"):             #Actors
                        actors = title["actors"]
                    else:
                        actors = None

                    if title.get("directors"):          #Directors
                        directors = title["directors"]
                    else:
                        directors = None

                    year = title["release_year"]        #Year en int
                    
                    rent_price = title["price"]
                    
                    packs = [                           #Packages
                        {
                            "Type": "transaction-vod",
                            "RentPrice": rent_price
                        }
                    ]

                    duration = title["stream_runtime"] // 60      #Duration

                    genero = title["genres"]                    #Genero

                    descript = title["long_desc"]               #Descripcion
                    
                    
                    #### COSAS QUE FALTAN: ####
                    ######## CHEQUEAR SI FALTA GENERO EN ALGUNA PELICULA PONER UN IF #######
                    ######## CHEQUEAR SI FALTA DESCRIPCION EN ALGUNA PELICULA PONER UN IF #######
                    ######## CAMBIAR LAS REQ A DATAMANAGER ########
                    ######## AGREGAR PAYLOAD A LA BASE DE DATOS CON DATAMANAGER ########
                    ######## UPLOAD ########



                    payload = {
                        'PlatformCode':  self._platform_code,
                        'Id':            str(id_),
                        'Title':         titulo,
                        'OriginalTitle': None,
                        'CleanTitle':    _replace(titulo),
                        'Type':          "movie",
                        'Year':          year,
                        'Duration':      duration,
                        'Deeplinks': {
                            'Web':       "https://www.optimum.net/tv/asset/#/movie/{}".format(id_),
                            'Android':   None,
                            'iOS':       None,
                        },
                        'Playback':      None,
                        'Synopsis':      descript,
                        'Image':         None,
                        'Rating':        None,
                        'Provider':      None,
                        'Genres':        genero,
                        'Cast':          actors,
                        'Directors':     directors,
                        'Availability':  None,
                        'Download':      None,
                        'IsOriginal':    None,
                        'IsAdult':       None,
                        'Packages':      packs,
                        'Country':       None,
                        'Timestamp':     datetime.now().isoformat(),
                        'CreatedAt':     self._created_at
                    }
                    Datamanager._checkDBandAppend(self,payload,listDBMovie,listPayload)
                
            #Avanzamos a siguiente pag        
                if getjson["data"]["result"]["next"] == "0":
                    break
                else:
                    index += 20

        Datamanager._insertIntoDB(self,listPayload,self.titanScraping)
                        
        
        