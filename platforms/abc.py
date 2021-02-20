from os import replace
import time
import requests
import hashlib   
import pymongo 
import re
import json
import platform
import calendar
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

class Abc():
    """ Datos importantes:
                Necesita VPN: NO Al correr el script en Argentina o USA, trae el mismo contenido.
                Metodo de extraccion: Soup.
                Tiempo de ejecucion: Depende del internet ya que son muchas requests. Aprox: 20Mins.
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
        self.start_url = self._config['urls']['start_url']
        self.api_url = self._config['urls']['api_url']

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
    
    def monthToNum(self,month):
        """ Devuelve el numero de mes segun una string con el mes.

            Returns:
                Int(1-12)
        """
        return {
                'JANUARY' : 1,
                'FEBRUARY' : 2,
                'MARCH' : 3,
                'APRIL' : 4,
                'MAY' : 5,
                'JUNE' : 6,
                'JULY' : 7,
                'AUGUST' : 8,
                'SEPTEMBER' : 9, 
                'OCTOBER' : 10,
                'NOVEMBER' : 11,
                'DECEMBER' : 12
        }[month]
    
    def get_all_seasons(self, seasons):
        """ Se obtienen todas las temporadas de cada serie con soup y retorna
            una lista con todas ellas.

            Returns:
                List(Str())
        """

        splitSeasons = []
        if not seasons:
            return
        if seasons[0][0] == 's':
            for season in seasons:
                splitSeasons.append(season.replace(' ','-'))
        else:
            for season in seasons:
                splitMonth = season.split(' ')
                month = str(self.monthToNum(splitMonth[0].upper()))
                if len(month) < 2:
                    month = '0' + month
                splitSeasons.append(splitMonth[1] + '-' + month)
        print(splitSeasons)

        return splitSeasons

    def get_episode_payload(self, all_tile_details,episode_details, parentId, parentTitle):
        """ Funcion para obtener todos los datos de un 
            episodio del soup y retorna la payload llena.

            Returns: 
                Payload()
        """
        payload = Payload()

        # No se puede extraer la imagen ya que carga con javascript.
        # Ninguno de los 4 links esta fijo en el html.
        #
        # payload.image = all_tile_details.find_all('img',{"data-mptype" : "image", "title" : False})[0].get('src')
        
        payload.genres = [episode_details.get('data-track-video_genre')]
        payload.title = episode_details.get('data-track-link_name_custom').split(':')[-1].strip()
        if len(payload.title) > 30:
            try:
                payload.title = all_tile_details.contents[1].contents[0].text.split('-')[1].strip()
            except:
                payload.title = all_tile_details.contents[1].contents[0].contents[0].contents[0].text.strip()

        if ':' in payload.title:
            payload.title.split(':')[1]
        
        duration_str = episode_details.text.split(':')
        if(len(duration_str) < 3):
            payload.duration = int(duration_str[0].replace('NEW',''))

            #Si la duracion es menor a 1 minuto le pongo 1 minuto.
            if not payload.duration:
                payload.duration = 1
        else:
            payload.duration = int(duration_str[1].replace('NEW','')) + int(duration_str[0].replace('NEW',''))*60
        
        tile_data = all_tile_details.contents[1]

        payload.synopsis = tile_data.contents[1].text
        payload.season = tile_data.contents[0].text.split('-')[0].split(' ')[0].lower().replace('s','')
        payload.episode = tile_data.contents[0].text.split('-')[0].split(' ')[1].lower().replace('e','')
        payload.deeplinksWeb = self.start_url + all_tile_details.get('href')
        
        if(len(payload.season) > 5):
        # Las temporadas y episodios sin numero se pueden sacar del URL del episodio, en lugar 
        # de asignar None pero tiene un costo ya que tengo que hacer aun mas requests.
            soup = Datamanager._getSoup(self,payload.deeplinksWeb)

            video_data = soup.find('div',class_='Video__Head')

            try:
                payload.season = int(video_data.contents[0].text.replace('S',''))
                payload.episode = int(video_data.contents[1].text.replace('E',''))
            except: 
                payload.season = None
                payload.episode = None
 
        else:
            payload.season = int(payload.season)

        if payload.episode and isinstance(payload.episode,str):
            if len(payload.episode) < 2:
                payload.episode = int('0' + payload.episode)
            else:
                payload.episode = int(payload.episode)

        payload.year = re.search(r'\d\d\d\d',episode_details.get('data-track-video_air_date'))[0]
        payload.id = episode_details.get('data-track-video_id_code')

        payload.cleanTitle=_replace(payload.title)
        payload.packages=[{'Type': 'subscription-vod'}]
        payload.timestamp=datetime.now().isoformat()
        payload.createdAt=self._created_at
        payload.parentId = parentId
        payload.parentTitle = parentTitle
        payload.platformCode = self._platform_code

        return payload

    def extract_episodes(self,showPayload,payloadsEpisodes,ids_guardados):
        parentTitle = showPayload.title
        parentId = showPayload.id

        soup = Datamanager._getSoup(self,showPayload.deeplinksWeb)

        matches = soup.find_all('span', class_='titletext')
        seasons = list(map(lambda a: a.text,matches))
        seasons = list(filter(lambda a: bool(
            re.search(r'season|January|February|March|April|May|June|July|August|September|October|November|December',a)),seasons))

        splitSeasons = self.get_all_seasons(seasons)

        #Por cada "temporada", encuentro los datos de los episodios y si no tiene temporadas retorna.
        if seasons:
            for season in splitSeasons:
                soup = Datamanager._getSoup(self,showPayload.deeplinksWeb + '/episode-guide/' + season)

                all_episodes = soup.find('div',class_='tilegroup')
                if all_episodes:

                    for episode_details in all_episodes.contents:

                        payload = self.get_episode_payload(episode_details,episode_details.find('div',class_='fitt-tracker'),parentId,parentTitle)

                        Datamanager._checkDBandAppend(self,payload.payloadEpisodeJson(),ids_guardados,payloadsEpisodes,isEpi=True)

    def _scraping(self, testing = False):
        payloadsShows = []
        payloadsEpisodes = []
        ids_guardados_shows = Datamanager._getListDB(self,self.titanScraping)
        ids_guardados_episodes = Datamanager._getListDB(self,self.titanScrapingEpisodios)
        
        
        # Con esto se obtienen la cantidad completa de series y se almacena en size.
        #-------------------------------------------------------------------------#
        start = 0
        size = 24
        url = self.api_url.format(start,size)
        size = int(Datamanager._getJSON(self,url)['total'])
        #-------------------------------------------------------------------------#

        url = self.api_url.format(start,size)
        data = Datamanager._getJSON(self,url)

        for show in data['tiles']:
            try:
                title = show['show']['title']
            except:
                continue

            __id = show['show']['id']
            genres = [show['show']['genre']]
            try:
                synopsis = show['show']['aboutTheShowSummary']
            except:
                synopsis = None
            
            try:
                #Se evalua si tiene link, y luego tiene una llave porque esta mal.

                urlValue = show['link']['urlValue']
                if '{' not in urlValue:
                    deepLink = self.start_url + urlValue.replace('/index','').replace(':','')
                else:
                    raise Exception('La url contiene una llave y no esta permitido')

            except:
                #Hay distintos tipos de links por lo que tengo que hacerlo por distintos metodos.

                if '\'' in title:
                    deepLink = self.start_url + '/shows/' + show['show']['trackTitle'].replace(' ','-').lower().replace(':','')
                else:
                    deepLink = self.start_url + '/shows/' + title.replace(' ','-').lower().replace(':','')
            
            payload = Payload(id=__id,type='serie',cleanTitle=_replace(title),platformCode=self._platform_code,
                              synopsis=synopsis,createdAt=self._created_at,timestamp=datetime.now().isoformat(),
                              title=title,deeplinksWeb=deepLink,packages=[{'Type' : 'subscription-vod'}],genres=genres)
            
            Datamanager._checkDBandAppend(self,payload.payloadJson(),ids_guardados_shows,payloadsShows)

            self.extract_episodes(payload,payloadsEpisodes,ids_guardados_episodes)
      
        Datamanager._insertIntoDB(self,payloadsShows,self.titanScraping)
        Datamanager._insertIntoDB(self,payloadsEpisodes,self.titanScrapingEpisodios)
        
        self.sesion.close()

        Upload(self._platform_code, self._created_at, testing=testing)
