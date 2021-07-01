import time
import requests
from yaml.tokens import FlowMappingStartToken
from handle.replace import _replace
from common import config
from handle.mongo import mongo
from updates.upload import Upload
from handle.payload import Payload
from handle.datamanager import Datamanager
import datetime
# from time import sleep
import re


class StarzMI():
    def __init__(self, ott_site_uid, ott_site_country, type):
        self.ott_site_uid = ott_site_uid
        self.ott_site_country = ott_site_country
        self._config = config()['ott_sites'][ott_site_uid]
        self._platform_code = self._config['countries'][ott_site_country]
        # self._start_url             = self._config['start_url']
        self._created_at = time.strftime("%Y-%m-%d")
        self.mongo = mongo()
        self.titanPreScraping = config()['mongo']['collections']['prescraping']
        self.titanScraping = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodes = config()['mongo']['collections']['episode']

        self.api_url = self._config['api_url']
        self.url=self._config['url']

        self.session = requests.session()

        if type == 'return':
            '''
            Retorna a la Ultima Fecha
            '''
            params = {"PlatformCode": self._platform_code}
            lastItem = self.mongo.lastCretedAt(self.titanScraping, params)
            if lastItem.count() > 0:
                for lastContent in lastItem:
                    self._created_at = lastContent['CreatedAt']

            self._scraping()

        if type == 'scraping':
            self._scraping()

        if type == 'testing':
            self._scraping(testing=True)

    def query_field(self, collection, field=None):
        """Método que devuelve una lista de una columna específica
        de la bbdd.

        Args:
            collection (str): Indica la colección de la bbdd.
            field (str, optional): Indica la columna, por ejemplo puede ser
            'Id' o 'CleanTitle. Defaults to None.

        Returns:
            list: Lista de los field encontrados.
        """
        find_filter = {
            'PlatformCode': self._platform_code,
            'CreatedAt': self._created_at
        }

        find_projection = {'_id': 0, field: 1, } if field else None

        query = self.mongo.db[collection].find(
            filter=find_filter,
            projection=find_projection,
            no_cursor_timeout=False
        )

        if field:
            query = [item[field] for item in query if item.get(field)]
        else:
            query = list(query)

        return query

    def _scraping(self, testing = False):
        self.scraped = self.query_field(self.titanScraping, field='Id')
        self.scraped_episodes = self.query_field(self.titanScrapingEpisodes, field='Id')
        self.payloads = []
        self.episodes_payloads = []
        contents=self.get_contents()
        key_movies_series='contentId' #es la key del id como viene del dictionario del contenido
        key_episodes='Id' #es la key del id del payload del episodio
        for content in contents:
            isSeries=False
            if self.isDuplicate(self.scraped,content[key_movies_series])==False:
                if content['contentType'] == 'Series with Season':
                    isSeries=True
                    self.epis_payload(content)
                self.scraped.append(content[key_movies_series])    
                new_payload = self.get_payload(content, isSeries)
                self.payloads.append(new_payload)
            else:
                pass

        self.insert_payloads_close(self.payloads,self.episodes_payloads)
    
    def get_contents(self):
        url_api = self.api_url
        contents=[]
        response = self.session.get(url_api)
        json_data=response.json()
        for content in json_data['playContentArray']['playContents']:
            contents.append(content)
        return contents
    
    def isDuplicate(self, scraped_list, key_search):
        isDup=False
        if key_search in scraped_list:
            isDup = True
        return isDup
    
    def insert_payloads_close(self,payloads,epi_payloads):    
        if payloads:
            self.mongo.insertMany(self.titanScraping, payloads)
        if epi_payloads:
            self.mongo.insertMany(self.titanScrapingEpisodes, epi_payloads)
        self.session.close()
        Upload(self._platform_code, self._created_at, testing=True)
    
    def get_payload(self, content, seriesBool):
        payload = self.generic_payload(content)
        if seriesBool:
            payload['Year'] = self.get_year_int(content['minReleaseYear'])
            payload['Seasons'] = len(content['childContent'])
            payload['Playback'] = None
        else:
            payload['Year'] =  self.get_year_int(content['releaseYear'])
            payload['Duration'] = self.get_duration(content)
            payload['Download'] = content['downloadable']
        return payload
    
    def generic_payload(self,content):
        payload = {
            'PlatformCode': self._platform_code,
            'Id': self.get_id_str(content),
            'Title': content['title'],
            'OriginalTitle': content['titleSort'],
            'CleanTitle': _replace(content['title']),
            'Type': self.get_type(content),
            'Year': None,
            'Duration': None,
            'Deeplinks': {
                "Web": self.get_deepLinks(content,None,None),
                'Android': None,
                'iOS': None,
            },
            'Synopsis': content['logLine'],
            'Image': None,
            'Rating': self.get_rating(content),
            'Provider': [content['studio']],
            'ExternalIds': None,
            'Genres': self.get_genres(content),
            'Cast': self.get_crew(content)[0],
            'Directors': self.get_crew(content)[1],
            'Availability': None,
            'Download': None,
            'IsOriginal': content['original'],
            'IsBranded': None,
            'IsAdult': None,
            "Packages": [{'Type':'subscription-vod'}],
            'Country': [self.ott_site_country],
            'Timestamp': datetime.datetime.now().isoformat(),
            'CreatedAt': self._created_at,
        }
        return payload

    def epis_payload(self,content):
        for seasonValue in content['childContent']:
            for epValue in seasonValue['childContent']:
                if self.isTrailer(epValue['order']):
                    if not self.isDuplicate(self.scraped_episodes,epValue['contentId']):
                        episode_num = self.get_episode_num(seasonValue['order'],epValue['order'])
                        episode = {
                            'PlatformCode':self._platform_code,
                            'ParentId': self.get_str_parent_id(epValue),
                            'ParentTitle': epValue['seriesName'],
                            'Id': self.get_id_str(epValue),
                            'Title':epValue['title'] ,
                            'Episode':episode_num,
                            'Season': seasonValue['order'],
                            'Year': self.get_year_int(epValue['releaseYear']),
                            'Image':None ,
                            'Duration': self.get_duration(epValue),
                            'Deeplinks':{
                                'Web':self.get_deepLinks(epValue,epValue['seriesName'],episode_num),
                                'Android': None,
                                'iOS':None ,
                            },
                            'Synopsis':epValue['logLine'],
                            'Rating':self.get_rating(epValue) ,
                            'Provider':[epValue['studio']],
                            'ExternalIds': None,
                            'Genres': self.get_genres(epValue),
                            'Cast':None,
                            'Directors':None,
                            'Availability':None,
                            'Download': None,
                            'IsOriginal': epValue['original'],
                            'IsAdult': None,
                            'Country': [self.ott_site_country],
                            'Packages': [{'Type':'subscription-vod'}],
                            'Timestamp': datetime.datetime.now().isoformat(),
                            'CreatedAt': self._created_at,
                        }
                        self.episodes_payloads.append(episode)
                        self.scraped_episodes.append(episode['Id'])
                    else:pass
                else: pass

    def get_rating(self,content):
        ratingCode=content['ratingCode']
        ratingSys=content['ratingSystem']
        rating=ratingSys.join(ratingCode)
        return rating

    def get_genres(self,content):
        genres=[]
        split_genres=[]
        search_for='&-'
        for genre in content['genres']:
            genres.append(genre['description'])
        for char in search_for:
            for genre in genres:
                if char in genre:
                    split_genres+= genre.split(char)
                else:
                    split_genres=genres

        return split_genres

    def get_type(self,content):
        if content['contentType']=='Movie':
            return'movie'
        else:
            return'serie'
    
    def get_id_str(self,content):
        return str(content['contentId'])

    def get_year_int(self,year):
        return int(year)

    def get_crew(self,content):
        crew=[]
        cast=[]
        directors=[]
        for credit in content['credits']:
            for rols in credit['keyedRoles']:
                if rols['key'] == 'D':
                    directors.append(credit['name']) 
                elif rols['key'] == 'C':
                        cast.append(credit['name'])
                else:
                    pass                  
        crew.append(cast)
        crew.append(directors)
        return crew
            
    def get_duration(self,content):
        seconds=content['runtime']
        minutes=seconds/60
        duration= int(minutes)
        return duration

    def get_episode_num(self,season,episode):
        season_mult=season*100
        episode_clean = episode-season_mult
        return episode_clean

    def isTrailer(self,num):
        return bool(num)
    
    def get_str_parent_id(self,content):
        parent_id=str(content['topContentId'])
        return parent_id

    def depurate_title(self, title):
        chars=' *,./|&¬!"£$%^()_+{@:<>?[]}`=;¿'
        title=title.lower()#paso el titulo original a minusculas
        if '-' in title:#primero elimino los guiones que vengan con el titulo original
            title=title.replace('-'," ")
        for c in chars:#luego elimino el resto de los caracteres especiales
            title=title.replace(c,'-')
        if "'" in title:#elimino los apostrofes simples que quedan fuera de la lista de caracteres especiales, este paso quizas se pueda evitar de otro modo.
            title=title.replace("'","")
        return title
    
    def get_deepLinks(self,content,parent,episode_num):
        content_title=_replace(content['properCaseTitle'])
        clean_title= self.depurate_title(content_title)
        if content['contentType']=='Episode':
            content_title=_replace(parent)
            clean_title= self.depurate_title(content_title)
            deeplink=self.url+'{}/{}/{}-{}/{}-{}/{}'.format('series',clean_title,'season',str(content['seasonNumber']),'episode',str(episode_num),content['contentId'])
        elif content['contentType']=='Movie':
            deeplink=self.url+'{}/{}-{}'.format('movies',clean_title,content['contentId'])
        elif content['contentType']=='Series with Season':
            deeplink=self.url+'{}/{}/{}'.format('series',clean_title,content['contentId'])
        else:
            deeplink=self.url
        return deeplink