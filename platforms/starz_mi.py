from os import replace
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
start_time = time.time()

class StarzMI():
    def __init__(self, ott_site_uid, ott_site_country, type):
        """
        Starz es una ott de Estados Unidos que opera en todo el mundo.

        DATOS IMPORTANTES:
        - VPN: No
        - ¿Usa Selenium?: No.
        - ¿Tiene API?: Si.
        - ¿Usa BS4?: No.
        - ¿Cuanto demoró la ultima vez?. 0.7531681060791016 seconds
        - ¿Cuanto contenidos trajo la ultima vez? titanScraping: 184, titanScrapingEpisodes: 970, CreatedAt: 2021-07-05 .

        OTROS COMENTARIOS:
        ---
        """
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
        # Pensando algoritmo:
        # 1) Método request (request)-> Validar todo.
        # 2) Método payload (get_payload)-> Para reutilizarlo.
        # 3) Método para traer los contenidos (get_contents)

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
        print("--- %s seconds ---" % (time.time() - start_time))
    
    def get_contents(self):
        url_api = self.api_url
        contents=[]
        response = self.session.get(url_api)
        json_data=response.json()
        for content in json_data['playContentArray']['playContents']:
            contents.append(content)
        return contents
    
    def isDuplicate(self, scraped_list, key_search):
        '''
            Metodo para validar elementos duplicados segun valor(key) pasado por parametro en una lista de scrapeados.
        '''
        isDup=False
        if key_search in scraped_list:
            isDup = True
        return isDup
    
    def insert_payloads_close(self,payloads,epi_payloads):
        '''
            El metodo checkea que las listas contengan elementos para ser subidos y corre el Upload en testing.
        '''     
        if payloads:
            self.mongo.insertMany(self.titanScraping, payloads)
        if epi_payloads:
            self.mongo.insertMany(self.titanScrapingEpisodes, epi_payloads)
        self.session.close()
        Upload(self._platform_code, self._created_at, testing=True)
    
    def get_payload(self, content, seriesBool):
        '''
            Valida el tipo de contenido y modifica los campos del diccionario general recibido por
            self.generic_payload segun sea necesario.
        '''
        payload = self.generic_payload(content)
        if seriesBool:
            payload['Year'] = self.get_year_int(content['minReleaseYear'])
            payload['Seasons'] = self.season_payload(content)
            payload['Playback'] = None
        else:
            payload['Year'] =  self.get_year_int(content['releaseYear'])
            payload['Duration'] = self.get_duration(content)
            payload['Download'] = content['downloadable']
        return payload
    
    def generic_payload(self,content):
        '''
            El metodo genera un payload general que reutiliza los campos que se pueden
            para peliculas o series.
        '''
        payload = {
            'PlatformCode': self._platform_code,
            'Id': self.get_id_str(content),
            'Crew':self.get_crew(content)[2],
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
        '''
            El metodo primero valida que el episodio no este cargado ya en lista de scrapeados segun id,
            luego valida que el episodio no sea de valor 0 para eliminar trailers, finalmente carga un
            payload especifico para episodios.

        '''
        for seasonValue in content['childContent']:
            for epValue in seasonValue['childContent']:
                episode_duration = self.get_duration(epValue)
                episode_num = self.get_episode_num(seasonValue['order'],epValue['order'])
                if self.isDuplicate(self.scraped_episodes,epValue['contentId'])==False and self.isNotTrailer(episode_duration)==False:
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
                        'Duration':episode_duration,
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

    def season_payload(self,content):
        seasons=content['childContent']
        seasons_list=[]
        for season in seasons:
            s={
                "Id": season['contentId'], 
                "Synopsis": season['logLine'], 
                "Title": season['title'],
                "Deeplink": None, 
                "Number": season['order'], 
                "Year": season['minReleaseYear'], 
                "Image": None, 
                "Directors": self.get_crew(season)[1], 
                "Cast": self.get_crew(season)[0], 
                "Episodes": season['episodeCount'], 
                "IsOriginal": season['original'] 
            }
            seasons_list.append(s)
        return seasons_list

    
    def get_rating(self,content):
        '''
            El rating en esta plataforma viene dividido por codigo y sistema,
            se hace la union de ambos valores y se devuelve como un solo dato.
        '''
        ratingCode=content['ratingCode']
        ratingSys=content['ratingSystem']
        rating=ratingSys.join(ratingCode)
        return rating

    def get_genres(self,content):
        '''
            Limpia los caracteres especiales o palabras de mas que pueda tener el genero como se recibe el dato.
            Primero lo pasa a minusculas,luego lo limpia y devuelve una lista.
        '''
        genres=[]
        search_for='&/-_|'
        for item in content['genres']:
            genre=item['description'].lower()
            genres.append(genre)     
        for genre in genres:
            for char in search_for:
                if char in genre:
                    genres.remove(genre)
                    genres+=genre.split(char)
                else:
                    pass
        for genre in genres:
            checkDe=genre.split(' ')
            if checkDe[0]=='de':
                genres.remove(genre)
                genres.append(checkDe[1])
        return genres

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
        directors=[]
        cast=[]
        crew=[]
        all=[]
        #En algun contenido de seasons rompe al tratar de acceder a credits, probablemente el dato falta, por eso el try catch.
        try:
            for credit in content['credits']:
                for rols in credit['keyedRoles']:
                    if rols['key'] == 'D':
                        directors.append(credit['name']) 
                    elif rols['key'] == 'C':
                            cast.append(credit['name'])
                    else:
                        other={}
                        other['Role'] = rols['name']
                        other['Name'] = credit['name']
                        crew.append(other)
        except:
            pass
        all.append(cast)
        all.append(directors)
        all.append(crew)
        return all
            
    def get_duration(self,content):
        '''
            El dato de la duracion viene expresado en milisegundos y se pasa a minutos.
        '''
        seconds=content['runtime']
        minutes=seconds/60
        duration= int(minutes)
        return duration

    def get_episode_num(self,season,episode):
        '''
            El dato de numero de episodio viene compuesto por la temporada expresada en centenas,
            unido del numero de episodio. El metodo devuelve el numero de episodio en unidades.
        '''
        season_mult=season*100
        episode_clean = episode-season_mult
        return episode_clean

    def isNotTrailer(self,num):
        '''
            Si el numero de episodio es 0 devuelve false y filtra los trailers.
        '''
        isTrailer=False
        if num < 1:
            isTrailer=True
        return isTrailer
    
    def get_str_parent_id(self,content):
        parent_id=str(content['topContentId'])
        return parent_id

    def depurate_title(self, title):
        '''
            Limpia el titulo pasandolo a minusculas y eliminando caracteres
            especiales que pudiera tener.
        '''
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
        '''
            Armado de deeplinks particulares segun el tipo de contenido. Los valores parent y episode_num
            solo seran usados para generar el deeplink de episodios, de otro caso se pasan como None.
        '''
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
