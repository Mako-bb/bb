# -*- coding: utf-8 -*-
from os import replace
from typing import Counter
import requests # Si el script usa requests/api o requests/bs4
import time

from requests.api import request
from handle.payload     import Payload
from bs4                import BeautifulSoup # Si el script usa bs4
from handle.datamanager import Datamanager # Opcional si el script usa Datamanager
from common             import config
from handle.mongo       import mongo
from updates.upload     import Upload
from handle.replace     import _replace
from handle.payload     import Payload
import json
import re
import hashlib
class HBOFacu():

    """
    - Status: En desarrollo
    - VPN: No
    - Método: BS4
    - Runtime: 
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

        self.URL                    = config_["url"]
        self.payloads               = []
        self.payloads_episodes      = []
        self.ids_scrapeados         = Datamanager._getListDB(self,self.titanScraping)
        self.ids_scrapeados_episodios = Datamanager._getListDB(self,self.titanScrapingEpisodios)
        
        
        if ott_operation == 'return':
            return_params = {'PlatformCode' : self._platform_code}
            last_item = self.mongo.lastCretedAt('titanPreScraping', return_params)
            if last_item.count() > 0:
                for last_content in last_item:
                    self._created_at = last_content['CreatedAt']
            self.prescraped_section_ids = [pay["Id"] for pay in Datamanager._getListDB(self, self.titanPreScraping)]

            self.scraping()
        else:
            self.prescraped_section_ids = list()

        if ott_operation in ('testing', 'scraping'):
            self.scraping()
    
    
    def scraping(self):
        all_movies = self.get_all_movies()
        all_series = self.get_all_series()

        for n,movie in enumerate(all_movies):
            print(f"\nProgress {n}/{len(all_movies)}\n")
            is_available = self.is_available_movie(movie)
            if is_available == True:
                self.get_movies(movie)
                

        for n,series in enumerate(all_series):
            print(f"\nProgress {n}/{len(all_series)}\n")
            is_available = self.is_available_serie(series)
            if is_available  ==True:
                credits = self.get_credits(series)
                payload = self.get_payload(series,credits,is_serie=True)
                payload_serie = payload.payload_serie()
                Datamanager._checkDBandAppend(self,payload_serie,self.ids_scrapeados,self.payloads)
            
                
        Datamanager._insertIntoDB(self, self.payloads,self.titanScraping)
        Datamanager._insertIntoDB(self,self.payloads_episodes,self.titanScrapingEpisodios)

        Upload(self._platform_code, self._created_at, testing=True, has_episodes=bool(self.payloads_episodes))
    
    
    def is_available_movie(self, movie):
        """
        confirmo si la pelicula sigue disponible 
        corroborando estado y que sea diferente 
        de la url home ya que en algunos casos redirecciona
        automaticamente
        """
        try:
            if movie.get('moviePageUrl','') !='':
                url = self.URL + movie['moviePageUrl']
                res = requests.get(url)
                if res.status_code == 200 and url != self.URL:
                    return True
        except Exception:
            print('Se produjo un error durante el intento de conexión ya que la parte conectada no respondió adecuadamente tras un periodo de tiempo')
        print(f'{movie["title"]} : NO DISPONIBLE')
        return False
        

    def is_available_serie(self, serie):
        """
        confirmo si la serie sigue disponible 
        corroborando estado y que sea diferente 
        de la url home ya que en algunos casos redirecciona
        automaticamente
        """
        if serie.get('catalogPath','') !='':
            url = serie['catalogPath'].split('.')
            url = f'{self.URL}/{url[1]}'
            try:
                res = requests.get(url)
                if res.status_code == 200 and url != self.URL:
                    return True
            except Exception:
                print('Se produjo un error durante el intento de conexión ya que la parte conectada no respondió adecuadamente tras un periodo de tiempo')
        
        return False


    def get_credits(self, serie):
        """
        Al haber un link donde se encuentra cast&crew todo mezclado
        y tener que hacer un request a ese apartado
        cree un diccionario para luego insertarlos en el payload por separado
        sin tener que llamarlo muchas veces mas adelante
        """
        credits = {
            'cast' : [],
            'crew' : []
        }

        deeplink = self.get_deeplinks(serie)
        cast_a_crew_url = (f'{deeplink}/cast-and-crew')
        try:
            res = requests.get(cast_a_crew_url)
            data = self.get_noscript(res)
            for band in data['bands']:
                if band['band'] == 'Cast':
                    credits['cast']+=self.get_cast(band)
                if band['band'] == 'Crew':
                    credits['crew']+=self.get_crew(band)
                    return credits
        except Exception:
            pass
       
        return credits

    
    def  get_crew(self, band):
        crew =[]
        for crews in band['data']['groups'][0]['categories'][0]['members']:
            crew.append({
                "name" : crews['name'],
                "rol" : crews['role']
            })
        
        return crew
        

    def get_cast(self, band):
        casts =[]
        for cast in band['data']['groups'][0]['members']:
            casts.append(cast['name'])
        
        return casts
        
    
    def get_movies(self, movie):
        payload = self.get_payload(movie)
        payload_movie = payload.payload_movie()
        Datamanager._checkDBandAppend(self,payload_movie,self.ids_scrapeados,self.payloads)


    def get_noscript(self, res):
        """
        Retorna en formato json el noscript del html
        el cual contiene la metadata
        """
        soup = BeautifulSoup(res.text, features="html.parser")
        noscript = soup.find('noscript',{'id':'react-data'})
        return json.loads(noscript['data-state'])


    def get_episodes(self, episodes_url, parent_id, parent_title):
        for episode_url in episodes_url:
            res = requests.get(episode_url)
            data = self.get_noscript(res)
            payload_episode = self.build_payload_episode(data, parent_id, parent_title,episode_url)
            Datamanager._checkDBandAppend(self,payload_episode,self.ids_scrapeados_episodios,self.payloads_episodes, isEpi=True)
        pass  


    def get_episodes_url(self, season):
        episodes = []
        try:
            res = requests.get(season)
            data = self.get_noscript(res)
            for bands in data['bands']:
                if bands['band'] == 'SerialEpisode':               
                    try:
                        if bands.get('data',{}).get('episode',{}).get('cta',{}).get('href',{}):
                            url_episode = bands['data']['episode']['cta']['href']
                            episodes.append((f'{self.URL}{url_episode}'))
                    except Exception:
                        pass
        except Exception:
            #Si existe algun problema de conexion
            #Espero 10 segundos y vuelvo a ejecutar la funcion
            print('HUBO UN ERROR DE CONEXION')
            time.sleep(10)
            self.get_episodes_url(season)

        return episodes
    

    def get_seasons_url(self,deeplink):
        seasons_url =[]
        num_season = 1
        while True:
            #Armo 2 url de season por que algunas tienen un 0 adelante del numero de la season
            URL_SEASON = (f'{deeplink}/season-{num_season}')
            URL_SEASON2 = (f'{deeplink}/season-0{num_season}')
            
            try:
                res = requests.get(URL_SEASON)
                res2 = requests.get(URL_SEASON2)
                
                if res.status_code == 200 and self.URL != URL_SEASON:
                    seasons_url.append(URL_SEASON)
                    num_season+=1

                if res2.status_code == 200 and self.URL != URL_SEASON2:
                    seasons_url.append(URL_SEASON2)
                    num_season+=1
                else:
                    return seasons_url
                if num_season > 20:
                    return []
                
            except Exception:
                print(f'Fallo request a {URL_SEASON}')    


    def get_all_movies(self):
        URL_MOVIES = (f'{self.URL}/movies/catalog')
        res = requests.get(URL_MOVIES)
        data = self.get_noscript(res)
        return data['bands'][1]['data']['entries']


    def get_all_series(self):
        URL_SERIES = (f'{self.URL}/series/all-series')
        res = requests.get(URL_SERIES)
        data = self.get_noscript(res)
        return data['bands'][1]['data']['entries']


    def get_payload(self,content_metadata, credits={},is_serie=False):
            
        payload = Payload()

        payload.platform_code = self._platform_code
        payload.id = self.get_id(content_metadata)
        payload.title = self.get_title(content_metadata)
        payload.clean_title = _replace(payload.title)
        payload.deeplink_web = self.get_deeplinks(content_metadata)
        if credits != {}:
            payload.cast = credits['cast']
            payload.crew = credits['crew']
        payload.year = self.get_year(content_metadata)
        payload.duration = self.get_duration(content_metadata)
        payload.synopsis = self.get_synopsis(content_metadata)
        payload.image = self.get_images(content_metadata)
        payload.rating = self.get_ratings(content_metadata)
        payload.genres = self.get_genres(content_metadata)
        payload.availability = self.get_availability(content_metadata)
        payload.packages = self.get_packages()
        payload.is_branded = self.get_is_branded(content_metadata)
        payload.createdAt = self._created_at

        if is_serie == True:
            seasons = []
            seasons_url = self.get_seasons_url(payload.deeplink_web)
            for season in seasons_url:
                seasons.append(self.build_payload_season(season, payload.title))
                episodes_url = self.get_episodes_url(season)
                self.get_episodes(episodes_url, payload.id, payload.title)
            payload.seasons = seasons
            
        return payload
        
    
    def get_is_branded(self, content_metadata):
        if content_metadata.get('slotData',{}).get('logoType',{}):
            if content_metadata['slotData']['logoType'] == 'HBOFilm':
                return True
        return None
        

    def get_id(self, content_metadata):
        if content_metadata.get('streamingId',{}).get('id',{}):
            return str(content_metadata.get('streamingId',{}).get('id',''))
        else:
            print('se seteo el id con haslib')
            return hashlib.md5(self.get_deeplinks(content_metadata).encode('utf-8')).hexdigest()
    
    
    def get_title(self, content_metadata): 
        return content_metadata.get('title','') or None
  
    
    def get_year(self, content_metadata):
        if content_metadata.get('releaseDate',''):
            year = content_metadata['releaseDate'].split('-')
            return int(year[0])
        return None
    

    def get_duration(self, content_metadata):
        if content_metadata.get('duration','') == '':
                return None
        else:
            horas = ''
            minutos = ''
            if 'HR' in content_metadata['duration']:
                horas = int(content_metadata['duration'].split(' ')[0])
            if 'MIN' in content_metadata['duration']:
                if 'HR' in content_metadata['duration']:
                    minutos = int(content_metadata['duration'].split(' ')[2])
                else:
                    minutos = int(content_metadata['duration'].split(' ')[0])
            if horas == '':
                horas = 0
            if minutos == '':
                minutos = 0
            return horas * 60 + minutos


    def get_deeplinks(self, content_metadata):
        if content_metadata.get('moviePageUrl','') !='':
            deeplink = (f'{self.URL}{content_metadata["moviePageUrl"]}')
            return deeplink
        if content_metadata.get('catalogPath','') !='':
            path = content_metadata["catalogPath"].replace('series/all-series.','')
            deeplink = (f'{self.URL}{path}')
            return deeplink
        return None
    
    
    def get_synopsis(self, content_metadata):
        patron = re.compile('^Short film.')
        synopsis = content_metadata.get('synopsis','')
        if patron.match(synopsis):
            return None
        return synopsis or None
        

    def get_images(self, content_metadata):
        images = []
        for image in content_metadata['thumbnail']['images']:
            if 'https' in image['src']:
                images.append(image['src'])
            else:
                images.append(self.URL + image['src'])
        return images
    
    
    def get_ratings(self, content_metadata):
        return content_metadata.get('rating','') or None
        

    def get_genres(self, content_metadata): 
        if content_metadata.get('genres','') != '':
            arr_genres = content_metadata['genres']
            list_genres = []
            for genres in arr_genres:
                if "horror-sci-fi" == genres.lower():
                    list_genres.append('horror')
                    list_genres.append('science fiction')
                else:
                        list_genres.append(genres)
            return list_genres
        return None
         
    
    def get_availability(self, content_metadata):
        if content_metadata.get('availability',''):
            for availability in content_metadata['availability']:
                return availability['end']
        return None
    
    
    def get_packages(self):
        return [{"Type":"subscription-vod"}]
                 
    
    def set_image(self, image):
            if 'https' in image['src']:
                return image['src']
            else:
                return self.URL + image['src']

    
    def build_payload_episode(self, content_metadata, parent_id, parent_title, deeplink ):
        
        def get_id():
            return str(content_metadata['bands'][1]['data']['infoSlice']['streamingId']['id'])
        
        def get_title():
            if content_metadata.get('title',None) != None:
                title = content_metadata['title']
                if '-' in title:
                    new_title = title.split('-')
                    return new_title[1]
                if ':' in title:
                    new_title = title.split(':')
                    return new_title[1]    
                else:
                    return title
            elif content_metadata.get('socialShareTitle', None) != None:
                title = content_metadata['socialShareTitle']
                if '-' in title:
                    new_title = title.split('-')
                    return new_title[1]
            else: 
                return ''
        
        def get_season_number():
            patron = re.compile('\A0\d')
            if content_metadata.get('dataLayer').get('pageInfo').get('seriesSeasonNumber') != None:
                return content_metadata['dataLayer']['pageInfo']['seriesSeasonNumber']
            else:
                #Si el noscript no me trae el num de season lo saco de la url
                num_season = deeplink.split('/')
                for n in num_season:
                    if 'season' in n:
                        season = n.split('-')
                        season = season[1]
                        if patron.match(season):
                            return int(season.replace('0',''))
                        else:
                            return int(season)

        def get_episode_number():
            patron = re.compile('\A0\d')
            episode = content_metadata['dataLayer']['pageInfo']['seriesEpisodeNumber']
            episode = str(episode)
            try:
                if patron.match(episode):
                    return int(episode.replace('0',''))
                else:
                    return int(episode)
            except Exception:
                pass        
        
        def get_images():
            images = []
            path = content_metadata['bands'][1]['data']

            if path.get('image'):
                for image in content_metadata['bands'][1]['data']['image']['images']:
                    images.append(self.set_image(image))
                    
            elif path.get('video').get('image'):
                for image in content_metadata['bands'][1]['data']['video']['image']['images']:
                    images.append(self.set_image(image))
                    print(images)
            
            return images or None
        
        def get_synopsis():
            if content_metadata['bands'][2].get('data',{}).get('summary',''):
                synopsis = content_metadata['bands'][2]['data']['summary']
                synopsis = text_plain(synopsis)
                return synopsis

        def text_plain(text):
            trash_text = ['<p>','</p>','<b>','</b>','<br>','</br>','\r\n','<u>','</u>',
                         '&nbsp;','·','<sup>','</sup>','<i>','</i>']
            clean_text = text            
            for trash in trash_text:
                clean_text = clean_text.replace(trash,'')
            return clean_text

            
        payload = Payload()
        payload.platform_code = self._platform_code
        payload.parent_id = parent_id
        payload.parent_title = parent_title
        payload.id = get_id() 
        payload.title = get_title()
        payload.season = get_season_number() 
        payload.episode = get_episode_number() 
        payload.deeplink_web = deeplink
        payload.synopsis = get_synopsis() 
        payload.image = get_images()
        payload.packages = [{"Type":"subscription-vod"}]
        payload.createdAt = self._created_at
        
        return payload.payload_episode()


    def build_payload_season(self, season, title_serie):
        res = requests.get(season)
        data = self.get_noscript(res)
    
        def get_id():
            id = data['bands'][1] 
            return  str(id.get('data',{}).get('infoSlice',{}).get('streamingId',{}).get('id',{}))

        def get_number_season():
            #sea es el num de season, le pongo ese nombre para que
            #no se choque la variable que paso por parametro
            patron = re.compile('\A0\d')
            num_season = season.split('/')
            for n in num_season:
                if 'season' in n:
                    sea = n.split('-')
                    sea = sea[1]
                    if patron.match(sea):
                        return int(sea.replace('0',''))
                    else:
                        return int(sea)

        def get_images():
            images = []
            path = data['bands'][1]['data']

            if path.get('image'):
                for image in data['bands'][1]['data']['image']['images']:
                    images.append(self.set_image(image))
                    
            elif path.get('video').get('image'):
                for image in data['bands'][1]['data']['video']['image']['images']:
                    images.append(self.set_image(image))
                    print(images)
            
            return images or None
            
        def get_cant_episodes():
            count = 0
            for bands in data['bands']:
                if bands['band'] == 'SerialEpisode':
                    count+=1
            return count    
            
        
        payload = Payload()
        
        payload.id = get_id()
        payload.deeplink_web = season
        payload.number = get_number_season()
        payload.episodes = get_cant_episodes()
        payload.title = (f'{title_serie} - Season {payload.number}')
        payload.image = get_images()

        return payload.payload_season()