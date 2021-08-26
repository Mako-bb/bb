# -*- coding: utf-8 -*-
import json
from typing import Counter
import requests # Si el script usa requests/api o requests/bs4
import time
from bs4                import BeautifulSoup # Si el script usa bs4
from selenium           import webdriver # Si el script usa selenium
from handle.datamanager import Datamanager # Opcional si el script usa Datamanager
from common             import config
from handle.mongo       import mongo
from updates.upload     import Upload
from handle.replace     import _replace
from handle.payload     import Payload

class StarzPlay():

    """
    - Status: EN PROGRESO
    - VPN: NO
    - Método: API
    - Runtime: < 30sec

    """

    def __init__(self, ott_platforms, ott_site_country, ott_operation):
        self.test = ott_operation in ("testing", "return") #
        config_ = config()['ott_sites'][ott_platforms] # Obligatorio
        self.country = ott_site_country # Opcional, puede ser útil dependiendo de la lógica del script.
        self._created_at = time.strftime('%Y-%m-%d')
        self._platform_code = config_['countries'][ott_site_country]
        self.mongo = mongo()
        self.sesion                 = requests.session() # Requerido si se va a usar Datamanager
        self.titanPreScraping       = config()['mongo']['collections']['prescraping'] # Opcional
        self.titanScraping          = config()['mongo']['collections']['scraping'] # Obligatorio. También lo usa Datamanager
        self.titanScrapingEpisodios = config()['mongo']['collections']['episode'] # Obligatorio. También lo usa Datamanager
        self.skippedTitles          = 0 # Requerido si se va a usar Datamanager
        self.skippedEpis            = 0 # Requerido si se va a usar Datamanager
        self.url                    = config_['url']
        self.url_api_ids            = config_['url_api_ids']
        self.url_api                = config_['url_api']
        self.payloads               = []
        self.payloads_episodes      = []
        self.ids_scrapeados         = Datamanager._getListDB(self,self.titanScraping)
        self.ids_scrapeados_episodios = Datamanager._getListDB(self,self.titanScrapingEpisodios)
        """
        La operación 'return' la usamos en caso que se nos corte el script a mitad de camino cuando
        testeamos, sea por un error de conexión u otra cosa. Nos crea una lista de ids ya insertados en
        nuestro Mongo local, la cual podemos usar para saltar los contenidos scrapeados y volver rápidamente
        a donde había cortado el script.
        """
        if ott_operation == 'return':
            return_params = {'PlatformCode' : self._platform_code}
            last_item = self.mongo.lastCretedAt('titanPreScraping', return_params)
            if last_item.count() > 0:
                for last_content in last_item:
                    self._created_at = last_content['CreatedAt']
            self.prescraped_ids = [pay["Id"] for pay in Datamanager._getListDB(self, self.titanPreScraping)]

            self.scraping()
        else:
            self.prescraped_ids = list()

        if ott_operation in ('testing', 'scraping'):
            self.scraping()
    

    def scraping(self,testing=True):
        ids_movies = self.get_ids()
        #IDS MOVIES CONTIENE ID DE PELICULAS Y SERIES
        for n, id in enumerate(ids_movies):
            print(f"\nProgress {n}/{len(ids_movies)}\n")

            content = self.get_content(id, ids_movies)
            type_ = self.get_type(content)

            #SI EL TIPO NO ES MOVIE NI EPISODE ES UNA SERIE
            if type_ != "Movie" and type_ != 'Episode':
                self.get_series(content) 
                #AGARRO LAS SEASON DE LAS SERIES PARA LUEGO RECORRER LOS EPISODIOS
                seasons = content['childContent']
                for season in seasons:
                    episodes = season['childContent']
                    self.get_episodes(episodes)    

            else:
                self.get_movies(content)

        Datamanager._insertIntoDB(self,self.payloads,self.titanScraping)
        Datamanager._insertIntoDB(self,self.payloads_episodes,self.titanScrapingEpisodios)
        
        Upload(self._platform_code,self._created_at,testing=testing)


    def get_movies(self, content):
        payload = self.get_payload(content)
        payload_movie = payload.payload_movie()
        Datamanager._checkDBandAppend(self,payload_movie,self.ids_scrapeados,self.payloads)
        

    def get_series(self,content):
        payload = self.get_payload(content)
        payload_serie = payload.payload_serie()
        if content['episodeCount']>1:
            Datamanager._checkDBandAppend(self,payload_serie,self.ids_scrapeados,self.payloads)

        pass


    def get_episodes(self, episodes):
        for episode in episodes:
            if episode['runtime']/60 > 5 and not 'Actualizaciones de la Temporada' in episode['title']:
                payload_season = self.get_payload(episode, True)
                payload_episode = payload_season.payload_episode()
                Datamanager._checkDBandAppend(self,payload_episode,self.ids_scrapeados_episodios,self.payloads_episodes, isEpi=True)
        
    
    def get_type(self,content):
        return content['contentType']

    
    def get_ids(self):
        
        """
        Summary:
            [Este metodo le pega a la api que contiene el id
            de todas las peliculas y series y lo retorna]

        Returns:
            [int]: [ids_movies]
        """
        rq_ = requests.get(self.url_api_ids)
        json_ = rq_.json()
        ids_movies = json_['blocks'][7]['playContentsById']
        return ids_movies


    def get_content(self,id, ids_movies):
        url_ = (f'{self.url_api}{ids_movies[id]["contentId"]}')
        rq__ = requests.get(url_)
        json__ = rq__.json()
        return rq__.json()['playContentArray']['playContents'][0]
    

    def get_payload(self,content_metadata,is_episode=None):

            payload = Payload()
            # Indica si el payload a completar es un episodio:
            if is_episode:
                self.is_episode = True
            else:
                self.is_episode = False 
            payload.platform_code = self._platform_code
            payload.id = self.get_id(content_metadata)
            payload.title = self.get_title(content_metadata)
            payload.original_title = self.get_original_title(content_metadata)
            payload.clean_title = self.get_clean_title(content_metadata)
            payload.deeplink_web = self.get_deeplinks(content_metadata)
            #Si no es un episodio, los datos pasan a scrapearse del html.
            if self.is_episode:
                payload.parent_title = self.get_parent_title(content_metadata)
                payload.parent_id = self.get_parent_id(content_metadata)
                payload.season = self.get_season(content_metadata)

            payload.year = self.get_year(content_metadata)
            payload.duration = self.get_duration(content_metadata)
            payload.synopsis = self.get_synopsis(content_metadata)
            payload.rating = self.get_ratings(content_metadata)
            payload.genres = self.get_genres(content_metadata)
            payload.cast = self.get_cast(content_metadata)
            payload.directors = self.get_directors(content_metadata)
            payload.availability = self.get_availability(content_metadata)
            payload.packages = self.get_packages(content_metadata)
            #Agregados
            payload.download = self.get_download(content_metadata)
            payload.is_original = self.get_is_original(content_metadata)
            payload.is_adult = self.get_is_adult(content_metadata)
            payload.crew = self.get_crew(content_metadata)
            payload.createdAt = self._created_at

            return payload
    

    def get_crew(self, content_metadata):
        crew = []
        cast = self.get_cast(content_metadata)
        directors = self.get_directors(content_metadata)

        if ('credits' in content_metadata):
            credits = content_metadata['credits']
            for credit in credits:
                if not credit['name'] in cast and  not credit['name'] in directors:
                    crew.append({
                        "name": credit['name'],
                        "rol": credit['keyedRoles'][0]['name']
                    })
                
        return crew
        
    
    def get_is_adult(self, content_metadata):
       return content_metadata.get('ratingRank','')>=18 
        
    
    def get_is_original(self, content_metadata):
        return content_metadata.get('original','')


    def get_download(self, content_metadata):
        return content_metadata.get('downloadable','') or None


    def get_id(self, content_metadata):
        return str(content_metadata.get('contentId','')) or None
        
    
    def get_title(self, content_metadata): 
        return content_metadata.get('title','') or None

    
    def get_clean_title(self, content_metadata):
        return _replace(content_metadata.get('title','')) or None
      
        
    def get_original_title(self, content_metadata):
        return content_metadata.get('properCaseTitle','') or None
    
    
    def get_year(self, content_metadata):
        return int(content_metadata.get('releaseYear',0)) or int(content_metadata.get('minReleaseYear',0)) or None
    

    def get_duration(self, content_metadata):
        return round(content_metadata.get('runtime',0)/60) or None


    def get_deeplinks(self, content_metadata):
        title = content_metadata['title']
        id = content_metadata['contentId']
        type_ = content_metadata['contentType']
        new_title = title.replace(':','').replace(' ','-')
        #return (f'{self.url}movies/{new_title}-{id}').lower() if type_=='Movie' else (f'{self.url}series/{new_title}/{id}').lower()
        if type_ == 'Movie':
            return (f'{self.url}movies/{new_title}-{id}').lower()

        return (f'{self.url}series/{new_title}/{id}').lower()
         
    
    def get_synopsis(self, content_metadata):
        return content_metadata.get('logLine','') or None
    
    
    def get_ratings(self, content_metadata):
        return content_metadata.get('ratingName','') or content_metadata.get('playerRating','') or None
        

    def get_genres(self, content_metadata): 
        arr_genres = content_metadata['genres']
        list_genres = []
        for genres in arr_genres:
            list_genres.append(genres.get('description'))
        
        return list_genres
         
    
    def get_cast(self, content_metadata):
        cast = []
        if ('actors' in content_metadata):
            for actors in content_metadata['actors']:
                cast.append(actors['fullName'])
        return cast
    
    
    def get_directors(self, content_metadata):
        directors = []
        if ('directors' in content_metadata):
            for dir in content_metadata['directors']:
                directors.append(dir['fullName'])
        return directors
    
    
    def get_availability(self, content_metadata):
        return content_metadata.get('endDate','') or None
    
    
    def get_packages(self, content_metadata):
        """  Se hardcodeo el package porque no se encontró el dato. """
        return [{"Type":"subscription-vod"}]
         
    
    def get_parent_title(self, content_metadata):
        return content_metadata.get('seriesName','') or None
        
     
    def get_parent_id(self, content_metadata):
        return str(content_metadata.get('topContentId','')) or None
        
    
    def get_season(self, content_metadata):
        return content_metadata['seasonNumber']
