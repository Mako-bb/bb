from os import replace
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
from handle.datamanager  import Datamanager
from updates.upload         import Upload
from platforms.payload_testing import Payload

class DiscoveryLifeTest():
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


    def extractToken(self, cookie):
        tokenStr = str(cookie.get('eosAn')).split('%2552')[0]
        pattern = re.search(r"ey.*\%",tokenStr).group(0).split('%')[0]
        print(pattern)
        return pattern

    def episode_scraping(self,episodeLinks,parentTitle,headers,ePayloads,ids_guardados_episodes):
        #request = self.sesion.get(episodeLinks, headers = headers)
        #episodes = request.json()

        episodes = Datamanager._getJSON(self,episodeLinks,headers=headers)
        for episode in episodes:
            #Ver file: episode_response.json que tiene todos los episodes de una serie ejemplo y la info de cada cosa
            parentId = episode['show']['id']
            id = episode['id']
            title = episode['name']
            rating = episode['parental']['rating']
            #type = episode['type']
            genres = self.getGenres(episode)
            duration = episode['duration']
            synopsis = episode['description']['standard']
            seasonNumber = episode['season']['number']
            episodeNumber = episode['episodeNumber']
            deepLinkWeb = episode['socialUrl']
            year = episode['networks'][0]['airDate'].split(':')[0].split('-')[0]

            payload = Payload(platformCode=self._platform_code,id = id,title=title,year=year,rating=rating,cleanTitle=_replace(title),genres=genres,duration=duration,synopsis=synopsis,episode=episodeNumber,season=seasonNumber,parentId=parentId,parentTitle=parentTitle,deeplinksWeb=deepLinkWeb,packages=[{'Type' : 'tv-everywhere'}],timestamp=datetime.now().isoformat(),createdAt=self._created_at)
            payloadJson = payload.payloadEpisodeJson()
            Datamanager._checkDBandAppend(self,payloadJson,ids_guardados_episodes,ePayloads,isEpi=True)
            #payloads.append(payload.payloadEpisodeJson())

        #return payloads

    def getGenres(self, episode):
        genreDict = episode['genres']
        genreList = []
        for genre in genreDict:
            genreList.append(genre['name'])
        ''' genres = ""
        if genreList:
            for each in genreList:
                if genres:
                    genres =  genres + ', ' + each
                else:
                    genres = each
        else:
            genres = None
        print(genreList) '''
        return genreList

    def savePayloads(self, payloads, ids_guardados, payload):

        if payload['Id'] not in ids_guardados:
            payloads.append(payload)
            ids_guardados.add(payload['Id'])
            print('Insertado titulo {}'.format(payload['Title']))
        else:
            print('Id ya guardado {}'.format(payload['Id']))
        
        return payloads

    def _scraping(self, testing = False):
        episodePayloads = []
        payloads = []
        ids_guardados_shows = Datamanager._getListDB(self,self.titanScraping)
        ids_guardados_episodes = Datamanager._getListDB(self,self.titanScrapingEpisodios)


        cookie = self.sesion.get('https://www.discoverylife.com/tv-shows/').cookies.get_dict()
        authToken = self.extractToken(cookie)
        
        #No cambiar el limite, es el maximo permitido por la API
        limit = 24
        #Se cambia el offset para acceder a todas las series
        offset = 0

        while True:
            headers = {'authorization' : 'Bearer {}'.format(authToken)}
            data = Datamanager._getJSON(self,'https://api.discovery.com/v1/content/shows?limit={}&networks_code=&offset={}&platform=desktop&products_code=dlf&sort=-video.airDate.type(episode|limited|event|stunt|extra)'.format(limit,offset),headers = headers)
            
            if not data:
                break

            for show in data:
                title = show['name']
                _id = show['id']
                #type = show['type'] 
                # --No se usa este campo ya que hay 'specials' y no se toman en cuenta
                type = 'serie'
                deeplinkWeb = show['socialUrl']
                synopsis = show['description']
                genres = self.getGenres(show)
                
                newPayload = Payload(platformCode=self._platform_code,title=title,id=_id,type=type,cleanTitle=_replace(title),packages=[{'Type' : 'tv-everywhere'}],deeplinksWeb=deeplinkWeb,synopsis=synopsis,genres=genres,timestamp=datetime.now().isoformat(),createdAt=self._created_at)
                showPayload = newPayload.payloadJson()

                episodeLinks = None
                for linkDict in show['links']:
                    if linkDict['rel'] == 'episodes':
                        episodeLinks = linkDict['href']
                
                Datamanager._checkDBandAppend(self,showPayload,ids_guardados_shows,payloads)
                self.episode_scraping(episodeLinks,title,headers,episodePayloads,ids_guardados_episodes)

            offset += limit

        Datamanager._insertIntoDB(self,payloads,self.titanScraping)
        Datamanager._insertIntoDB(self,episodePayloads,self.titanScrapingEpisodes)
        
        self.sesion.close()

        if not testing:
            Upload(self._platform_code, self._created_at, testing=True)

