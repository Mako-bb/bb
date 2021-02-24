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

class BravoTv():
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
        IMPORTANTE -----------------> TARDA DEMASIADO EN EJECUTAR (como unas 2 o 3 horas)
        ¿VPN? NO
        ¿API,HTML o SELENIUM? HTML con bs4

        BravoTv es una plataforma de estados unidos que presenta una pagina con todo el contenido. Para hacer el scraping, saco
        primero toda la informacion de los shows que tiene (principalmente el nombre y la url del show).

        IMPORTANTE:
        -Los episodios de las series estan en la url que es el urlPagina/NombreSerie/"episode-guide". Pero
        hoy dos tipos de series, lo que tienen episode-guide y los que no, lo que no tienen los episodios estan
        en la url de la serie
        -Las descripciones de los episodios se encuentran en la url de la forma urlPagina/NombreSerie/"about".
        """
       
        scraped = Datamanager._getListDB(self,self.titanScraping)
        scrapedEpisodes = Datamanager._getListDB(self,self.titanScrapingEpisodios)
        payloads = []
        payloadsEpisodios = []
        packages = [
                        {
                            'Type': 'subscription-vod'
                        }
                    ]
        url = 'https://www.bravotv.com/shows'
        urlWithoutShow = 'https://www.bravotv.com'
        episode_guide = '/episode-guide'
        about = '/about'


        soup = Datamanager._getSoup(self,url)

        allShows = soup.find('div',{"class":"container view-shows container"})
        allShows = allShows.findAll('a',{"class":"teaser__wrapper-link"})


        """
        Lo primer que voy a hacer es sacar el nombre y la url de cada show. Despues en la pagina para acceder a los episodios 
        de los shows, se hace con el siguiente link: https://www.bravotv.com/<NombreDelShow>/episode-guide y lo mismo para la
        informacion del show pero reemplazando episode-guide por about
        """
        nameShows = []
        urlShows = []
        urlEpisodeShows = []
        urlAboutShows = []
        imgShows=[]
        for allShow in allShows:
            nameShows.append(allShow.find('div',{"class":"teaser__meta-wrapper"}).text.split('\n')[4][12::])
            urlShows.append(urlWithoutShow+allShow['href'])
            urlEpisodeShows.append(urlWithoutShow+allShow['href']+episode_guide)
            urlAboutShows.append(urlWithoutShow+allShow['href']+about)
            imgShows.append(allShow.find('div',{"class":"teaser__image-wrapper"}).figure.img['src'])


        """
        Recorando la lista de urlAboutShows puedo terminar de sacar la informacion de cada show 
        """
        descriptionsShows = []
        castShows =[]
        for urlAboutShow in urlAboutShows:
            cast = []
            soup = Datamanager._getSoup(self,urlAboutShow)
            descriptionsShows.append(soup.find('div',class_="show-about__body").text)
            castSoups = soup.findAll('div',class_="teaser__meta")
            for castSoup in castSoups:
                cast.append(castSoup.text.split('\n')[2])
            castShows.append(cast)
       
        """
        Ahora tengo las url para poder sacar los episodios en una lista llamda urlEpisodeShows, lo que hago ahora es recorrer la
        la lista y sacar la url de cada episodio
        """
        urlEpisodes = []
        imgEpisodes = []
        for i in range(0,len(urlEpisodeShows)):
            urlEpisode = []
            imgEpisode = []
            soup = Datamanager._getSoup(self,urlEpisodeShows[i])
            """
            El siguiente if es para revisar si tienen boton de temporadas, si lo tiene las temporadas se ponen en un link especial
            que es ocultado por javascript por lo que si lo quieres ver tenes que desabilitar el javascript del navegador usado.
            """
            if not soup.find('select',{"id":"edit-field-tv-shows-season","class":"form-select"}):
                episodesSoup = soup.findAll('article',class_=re.compile("teaser teaser--episode-guide-teaser"))
                """
                Como algunos shows no tenian el link de episode-guide, sino que los episodios estan en la pagina principal del
                show, realizo un if para ver si encuentra el id y sino realizo un request a la url del nombre del show
                """
                if not episodesSoup:
                    soup = Datamanager._getSoup(self,urlShows[i],showURL=False)
                    episodesSoup = soup.findAll('article',class_=re.compile("teaser teaser--playlist-teaser video"))
                for episodeSoup in episodesSoup:
                    urlEpisode.append(urlWithoutShow+episodeSoup.a['href'])
                    # algunos episodios no tienen imagen
                    try:
                        imgEpisode.append(urlWithoutShow+episodeSoup.find('div',{"class":"teaser__image-wrapper"}).figure.picture.img['src'])
                    except:
                        imgEpisode.append(None)
            else:
                seasons =soup.find('select',{"id":"edit-field-tv-shows-season","class":"form-select"})
                seasons = seasons.findAll('option')
                """
                Si estoy aca, significa que tengo temporadas y recoro cada una de ellas para sacar la informacion necesaria de los
                episodios.
                """
                for season in seasons:
                    url = urlEpisodeShows[i]+'?field_tv_shows_season='+season['value']

                    soup = Datamanager._getSoup(self,url,showURL=False)
                    """
                    Por ultimo, hay series con un boton de "Load More" pero al igual que las temporadas, se puede cargar a travez de
                    un link que oculta javascript, por lo que navego a ese link para acceder a todo el contenido de episodios.
                    """
                    if not soup.find('li',class_="pager__item"):
                        episodesSoup = soup.findAll('article',class_=re.compile("teaser teaser--episode-guide-teaser"))
                        for episodeSoup in episodesSoup:
                            urlEpisode.append(urlWithoutShow+episodeSoup.a['href'])
                    else:
                        pages = soup.find('li',class_="pager__item").a['href'].split('&')[-1]
                        url = url+'&'+pages
                        episodesSoup = soup.findAll('article',class_=re.compile("teaser teaser--episode-guide-teaser"))
                        for episodeSoup in episodesSoup:
                            urlEpisode.append(urlWithoutShow+episodeSoup.a['href'])

            urlEpisodes.append(urlEpisode)
            imgEpisodes.append(imgEpisode)

        """
        Por ultimo, tengo que recorrer cada url de los episodios para sacar la informacion, toda la informacion es guardada en
        lista, puede ser que algunas listas tengan listas adentro.
        """

        episodes_seasons=[]
        titles_episodes=[]
        description_episodes=[]
        air_dates=[]
        rating_episodes=[]
        for urlEpisode in urlEpisodes:
            episode_season=[]
            title_episode=[]
            description_episode=[]
            air_date=[]
            rating_episode=[]
            for url in urlEpisode:
                
                soup = Datamanager._getSoup(self,url)
                
                infoEpisode = soup.find('details',{"class":"video__meta"})

                summary = infoEpisode.find('summary')
                episodeAndSeason = summary.div.text
                title = summary.h1.text.split(':')[-1]
                try:
                    description = infoEpisode.find('div',class_="video__description").text
                except:
                    description = None
                try:
                    airDate = infoEpisode.find('div',class_="video__air-date").text
                except:
                    airDate = None
                try:
                    rating = infoEpisode.find('div',class_="video__rating_expiration").text
                except:
                    rating = None

                episode_season.append(episodeAndSeason)
                title_episode.append(title)
                description_episode.append(description)
                air_date.append(airDate)
                rating_episode.append(rating)

            episodes_seasons.append(episode_season)
            titles_episodes.append(title_episode)
            description_episodes.append(description_episode)
            air_dates.append(air_date)
            rating_episodes.append(rating_episode)

        _platform_code = self._platform_code
        for i in range(0,len(nameShows)):
            img=[]
            title = nameShows[i].strip()
            _id = hashlib.md5(title.encode('utf-8')).hexdigest()
            _type = 'serie'
            URLContenido = urlShows[i]
            img.append(imgShows[i])
            description = descriptionsShows[i].strip()
            cast = castShows[i]
            payload = Payload(packages=packages,id=_id,title=title,image = img,cleanTitle= _replace(title),platformCode=_platform_code,type=_type,deeplinksWeb = URLContenido,synopsis = description,cast = cast,timestamp=datetime.now().isoformat(),createdAt=self._created_at)
            Datamanager._checkDBandAppend(self, payload.payloadJson(),scraped,payloads)
            Datamanager._insertIntoDB(self,payloads,self.titanScraping)

        for i in range(0,len(titles_episodes)):
            
            #Saco la informacion de las series que necesito
            nameShow = nameShows[i].strip()
            parrentId = hashlib.md5(nameShow.encode('utf-8')).hexdigest()
            
            for j in range(0,len(titles_episodes[i])):
                img = []
                title = titles_episodes[i][j].strip()
                try:
                    img.append(imgEpisodes[i][j])
                except:
                    img.append(None)
                _id = hashlib.md5(title.encode('utf-8')+nameShow.encode('utf-8')).hexdigest()
                # seasons = int(episodesSeason[i][j][0][1::])
                # episode =  int(episodesSeason[i][j][1][2::])
                URLContenido = urlEpisodes[i][j]
                description = description_episodes[i][j].strip()
                payload = Payload(packages=packages,id=_id,cleanTitle= _replace(title),image = img,parentId = parrentId,parentTitle=nameShow,title=title,platformCode=_platform_code,deeplinksWeb = URLContenido,synopsis = description,timestamp=datetime.now().isoformat(),createdAt=self._created_at)
                Datamanager._checkDBandAppend(self, payload.payloadEpisodeJson(),scrapedEpisodes,payloadsEpisodios,isEpi=True)
                Datamanager._insertIntoDB(self,payloadsEpisodios,self.titanScrapingEpisodios)


        #     soup = Datamanager._getSoup(self,urlShow)
        #     nameShows.append(soup.find('div',class_="nav__title__wrapper").text.split('\n')[2])
       
        self.sesion.close()


        
        Upload(self._platform_code, self._created_at, testing=testing)


    
