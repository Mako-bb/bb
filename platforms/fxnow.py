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
from handle.datamanager     import Datamanager
from updates.upload         import Upload
import sys
class Fxnow():
    def __init__(self, ott_site_uid, ott_site_country, type):
        self._config                = config()['ott_sites'][ott_site_uid]
        self._platform_code         = self._config['countries'][ott_site_country]
        #self._start_url             = self._config['start_url']
        self._created_at            = time.strftime("%Y-%m-%d")
        self.mongo                  = mongo()
        self.titanPreScraping       = config()['mongo']['collections']['prescraping']
        self.titanScraping          = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodes  = config()['mongo']['collections']['episode']

        self.currentSession = requests.session()
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

        -FxNow es una plataforma de estados unidos que se necesita un vpn para conectar. La pagina presenta una API para poder sacar los dartos
        navegando en la pagina podes encontrar dos APIS para sacar las peliculas y las series. Todo el contenido es original. 
        Cada APi presenta el listado del contenido, y cada uno es por series y peliculas.

        -Las API son diccionarios con todos los datos que buscamos, tanto para la peliculas como para las series. La API va tomando 24 contenidos
        y corta cuando no encuentra la siguiente pagina.

        """
       
        print("Peliculas")
        self.getMovies(testing)
        print("series")
        self.getSeries(testing)

        
        Upload(self._platform_code, self._created_at, testing=testing)

    def getMovies(self,testing):
        """
        -Este metodo me trae los datos de las peliculas, la peliculas se encuentran en una api que va cargando de a 24 contenidos, por lo que voy
        variando una variable para tomar 24 contenidos hasta el final.
        -Cuando llega al final, y pregunta si hay peliculas devuelve un error y si ocurre corto el ciclo while true.
        """
        payloads=[]
        packages = [
                        {
                            'Type': 'tv-everywhere'
                        }
                    ]

        ids_guardados = self.__query_field('titanScraping', 'Id')
        offset=0 #variable para cambiar la API para poder tomar todos los contonidos.
        while True:
            
            request = self.currentSession.get('https://prod.gatekeeper.us-abc.symphony.edgedatg.com/api/ws/pluto/v1/module/tilegroup/2430495?start={}&size=24&authlevel=0&brand=025&device=001'.format(offset))
            print(offset)
            print(request.status_code, request.url)
            data = request.json()
            
            # A partir de aca preguntos por los datos, el primer try sirve para romper el while True, porque si no encuentra un  titulo
            # significa que no hay mas contenido.
            try:
                titulos = data['tiles']
            except:
                break 

            # Recorro los titulos para poder sacar los datos.
            for titulo in titulos:
                genre=[]
                img=[]
                try:
                    images = titulo['images']
                    for image in images:
                        img.append(image['value'])
                except:
                    img=None
                try: 
                    genre.append(titulo['show']['genre']) 
                except: 
                    genre=None

                title = titulo['title']
                #year = titulo['release_year']

                id_ = str(titulo['show']['id'])

                # id_ = hashlib.md5(title.encode('utf-8')).hexdigest()

                # Lo guardo en el payload.
                payload = {
                    'PlatformCode':  self._platform_code,
                    'Id':            id_,
                    'Title':         title,
                    'OriginalTitle': None,
                    'CleanTitle':    _replace(title),
                    'Type':          'movie',
                    'Year':          None,
                    'Duration':      None,
                    'Deeplinks': {
                        'Web':       'https://fxnow.fxnetworks.com/{}'.format(titulo['link']['urlValue']),
                        'Android':   None,
                        'iOS':       None,
                    },
                    'Playback':      None,
                    'Synopsis':      titulo['show']['aboutTheShowSummary'],
                    'Image':         img,
                    'Rating':        None,
                    'Provider':      None,
                    'Genres':        genre,
                    'Cast':          None,
                    'Directors':     None,
                    'Availability':  None,
                    'Download':      None,
                    'IsOriginal':    True,
                    'IsAdult':       None,
                    'Packages':      packages,
                    'Country':       None,
                    'Timestamp':     datetime.now().isoformat(),
                    'CreatedAt':     self._created_at
                    }

                    # print(payload)
                if payload['Id'] not in ids_guardados:
                    payloads.append(payload)
                    ids_guardados.add(payload['Id'])
                    print('Insertado titulo {}'.format(payload['Title']))
                else:
                    print('Id ya guardado {}'.format(payload['Id']))
                """
                if data['data']['result']['next'] == '0':
                    break
                """ 
            offset +=24

        if payloads:
            self.mongo.insertMany(self.titanScraping, payloads)
            print('Insertados {} en {}'.format(len(payloads), self.titanScraping))

        self.currentSession.close()

    def getSeries(self,testing):
        """
        Este dato saco las series, es parecido al metodo getMovies para obtener, sigue casi los mismos pasos.
        """


        payloads=[]
        packages = [
                        {
                            'Type': 'tv-everywhere'
                        }
                    ]

        ids_guardados = self.__query_field('titanScraping', 'Id')
        offset=0
        while True:
            
            request = self.currentSession.get('https://prod.gatekeeper.us-abc.symphony.edgedatg.com/api/ws/pluto/v1/module/tilegroup/2430493?start={}&size=24&authlevel=0&brand=025&device=001'.format(offset))
            print(offset)
            print(request.status_code, request.url)
            data = request.json()
            try:
                titulos = data['tiles']
            except:
                break 
            for titulo in titulos:

                genre=[]
                img=[]
                try:
                    images = titulo['images']
                    for image in images:
                        img.append(image['value'])
                except:
                    img=None
                try: 
                    genre.append(titulo['show']['genre']) 
                except: 
                    genre=None

                title = titulo['title']
                #year = titulo['release_year']

                id_ = str(titulo['show']['id'])

                # id_ = hashlib.md5(title.encode('utf-8')).hexdigest()

                payload = {
                    'PlatformCode':  self._platform_code,
                    'Id':            id_,
                    'Title':         title,
                    'OriginalTitle': None,
                    'CleanTitle':    _replace(title),
                    'Type':          'serie',
                    'Year':          None,
                    'Duration':      None,
                    'Deeplinks': {
                        'Web':       'https://fxnow.fxnetworks.com/{}'.format(titulo['link']['urlValue']),
                        'Android':   None,
                        'iOS':       None,
                    },
                    'Playback':      None,
                    'Synopsis':      titulo['show']['aboutTheShowSummary'],
                    'Image':         None,
                    'Rating':        None,
                    'Provider':      None,
                    'Genres':        genre,
                    'Cast':          None,
                    'Directors':     None,
                    'Availability':  None,
                    'Download':      None,
                    'IsOriginal':    True,
                    'IsAdult':       None,
                    'Packages':      packages,
                    'Country':       None,
                    'Timestamp':     datetime.now().isoformat(),
                    'CreatedAt':     self._created_at
                    }

                    # print(payload)
                if payload['Id'] not in ids_guardados:
                    payloads.append(payload)
                    ids_guardados.add(payload['Id'])
                    print('Insertado titulo {}'.format(payload['Title']))
                else:
                    print('Id ya guardado {}'.format(payload['Id']))
                """
                if data['data']['result']['next'] == '0':
                    break
                """ 
            offset +=24

        if payloads:
            self.mongo.insertMany(self.titanScraping, payloads)
            print('Insertados {} en {}'.format(len(payloads), self.titanScraping))

        self.currentSession.close()



