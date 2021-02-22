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
from handle.payload_testing import Payload

class DiscoveryLife():
    """ Datos importantes:
                Necesita VPN: NO Al correr el script en Argentina o USA, trae el mismo contenido.
                Metodo de extraccion: Api.
                Tiempo de ejecucion: Depende del internet ya que son muchas requests. Aprox: 2 Mins.
                Excepciones: El script lanza una excepcion si el reintento del token supera el 20 .
    """
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

    def extractToken(self):
        """ Funcion para extraer el token de autorizacion para utilizar 
            la API, tiene una politica de reintento por si no puede 
            recibirlo a la primera.

            Returns:
                String
        """
        contador = 0
        
        while True:
            cookie = self.sesion.get('https://www.discoverylife.com/tv-shows/').cookies.get_dict()
            tokenStr = str(cookie.get('eosAn')).split('%2552')[0]
            try:
                token = re.search(r"ey.*\%",tokenStr).group(0).split('%')[0]
                print(token)
                break
            except:
                if(contador > 20):
                    raise Exception("No se pudo conseguir el token, re-intentar en unos minutos")
                time.sleep(3)
                self.sesion.close()
                self.sesion = requests.session()
                contador = contador + 1
            
        return token

    def episode_scraping(self,episodeLinks,parentTitle,headers,ePayloads,ids_guardados_episodes):

        episodes = Datamanager._getJSON(self,episodeLinks,headers=headers)
        for episode in episodes:
            parentId = episode['show']['id']
            id = episode['id']
            title = episode['name']
            
            imgList = []
            for image in episode['image'].get('links'):
                if 'height' in image.get('href'):
                    continue
                imgList.append(image.get('href').format(width = 500))
            image = imgList
            
            rating = episode['parental']['rating']
            genres = self.getGenres(episode)
            duration = str(episode['duration'])
            
            if len(duration) > 4:
                duration = int(duration[1:3]) + int(duration[0])*60
            else:
                if len(duration) <= 2:
                    duration = 1
                else:
                    duration = int(duration[0:2])

            synopsis = episode['description']['standard']
            seasonNumber = episode['season']['number']
            episodeNumber = episode['episodeNumber']
            deepLinkWeb = episode['socialUrl']
            year = episode['networks'][0]['airDate'].split(':')[0].split('-')[0]

            payload = Payload(platformCode=self._platform_code,id = id,title=title,year=year,
                              rating=rating,cleanTitle=_replace(title),genres=genres,image=image,
                              duration=duration,synopsis=synopsis,episode=episodeNumber,
                              season=seasonNumber,parentId=parentId,parentTitle=parentTitle,
                              deeplinksWeb=deepLinkWeb,packages=[{'Type' : 'tv-everywhere'}],
                              timestamp=datetime.now().isoformat(),createdAt=self._created_at)

            payloadJson = payload.payloadEpisodeJson()

            Datamanager._checkDBandAppend(self,payloadJson,ids_guardados_episodes,ePayloads,isEpi=True)

    def getGenres(self, episode):
        """ Funcion para obtener el genero de cada episodio

            Returns:
                List or None
        """
        genreDict = episode['genres']
        genreList = []
        for genre in genreDict:
            genreList.append(genre['name'])
        if not genreList:
            genreList = None
        return genreList

    def _scraping(self, testing = False):
        episodePayloads = []
        payloads = []
        ids_guardados_shows = Datamanager._getListDB(self,self.titanScraping)
        ids_guardados_episodes = Datamanager._getListDB(self,self.titanScrapingEpisodios)

        authToken = self.extractToken()
        
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
                deeplinkWeb = 'https://www.discoverylife.com' + '/tv-shows/' + show['socialUrl'].split('/')[3] + '/'
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

        Upload(self._platform_code, self._created_at, testing=testing)

