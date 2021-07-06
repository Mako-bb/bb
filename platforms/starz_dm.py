import time
import requests
from handle.replace         import _replace
from common                 import config
from handle.mongo           import mongo
from updates.upload         import Upload
from handle.payload         import Payload
from handle.datamanager     import Datamanager
import datetime
# import re

class StarzDM():
    """
    Starz es una ott de Estados Unidos que opera en todo el mundo.

    DATOS IMPORTANTES:
    - VPN: Si/No (Recomendación: Usar ExpressVPN).
    - ¿Usa Selenium?: No.
    - ¿Tiene API?: Si. Tiene 2, en una se obtienen los contenidos, y en otra las imágenes de los mismos.
    - ¿Usa BS4?: No.
    - La última vez demoró: 0.811063289642334 segundos(tieniendo la DB vacía), el 5/7/2021.
    - La ultima vez trajo:
        201 peliculas/series y 1023 episodios.

    OTROS COMENTARIOS:
    ...
    """

    def __init__(self, ott_site_uid, ott_site_country, type):

        self.initial_time = time.time()

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

        self.url = self._config['url']
        self.api_url = self._config['api_url']
        self.images_url = self._config['images_url']

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


    def _scraping(self, testing=False):
        self.scraped = self.query_field(self.titanScraping, field='Id')
        self.scraped_episodes = self.query_field(self.titanScrapingEpisodes, field='Id')

        self.payloads_list = []
        self.episodes_payloads = []
        all_items = self.get_content(self.api_url)
        no_insert = 'Tráiler'

        for item in all_items:
            if str(item["contentId"]) in self.scraped:
                print("Ya ingresado")
            else:
                self.scraped.append(item['contentId'])
                self.payloads_list.append(self.payload(item))

                if item['contentType'] == 'Series with Season':

                    for season in item['childContent']:
                        contador_episodes = 0

                        for episode in season['childContent']:

                            if int(episode['contentId']) in self.scraped_episodes or episode['order'] == 0 or no_insert in episode['title']:
                                print('capitulo ya ingresado, o es un trailer')
                            else:
                                contador_episodes += 1
                                self.scraped_episodes.append(episode['contentId'])
                                self.episodes_payloads.append(self.payload_episodes(item, episode, contador_episodes))

                
        if self.payloads_list:
            self.mongo.insertMany(self.titanScraping, self.payloads_list)
        if self.episodes_payloads:#Devuelve booleano, si no se insertó nada en la lista devuelve False
            self.mongo.insertMany(self.titanScrapingEpisodes, self.episodes_payloads)
        
        self.session.close()
        Upload(self._platform_code, self._created_at, testing=True)

        end_time = time.time()
        time_execute = end_time - self.initial_time
        print('el tiempo de ejecución es de: '+ str(time_execute) + ' segundos.')


    def payload(self,item_, is_season=False, is_episode=False):
        payload = {
            "PlatformCode": str(self._platform_code),
            "Id": str(item_['contentId']),
            "Seasons": self.get_seasons(item_),
            "Crew": self.get_crew(item_),
            "Title": str(item_['title']),
            "CleanTitle": _replace(str(item_['title'])),
            "OriginalTitle": None,
            "Type": str(self.get_type(item_['contentType'])),
            "Year": self.get_release_year(item_),
            "Duration": self.get_duration(item_),
            "ExternalIds": None,
            "Deeplinks": {
            "Web": self.get_deeplinks(item_),
            "Android": None,
            "iOS": None
            },
            "Synopsis": item_['logLine'],
            "Image": self.get_image(item_),
            "Rating": str(item_['ratingCode']),
            "Provider": None,
            "Genres": self.get_genres(item_),
            "Cast": self.get_cast(item_),
            "Directors": self.get_directors(item_),
            "Availability": None,
            "Download": self.get_download(item_),
            "IsOriginal": item_['original'],
            "IsAdult": None,
            "IsBranded": None,
            "Packages": [{'Type':'subscription-vod'}],#self.get_packages(),
            "Country": None,
            "Timestamp": str(datetime.datetime.now().isoformat()),
            "CreatedAt": str(self._created_at),
            }
        return payload
    
    def get_episodes(self,item):
        episodes = []
        for season in item['childContent']:
            for episode in season['childContent']:
                episodes.append(episode)
        
        return episodes



    def payload_episodes(self, item, episode, contador):
        payload_episode_ = {
    
        "PlatformCode": str(self._platform_code),
        "Id": str(episode['contentId']),
        "ParentId": str(item['contentId']),
        "ParentTitle": str(item['title']),
        "Episode": int(contador),
        "Season": episode['seasonNumber'],
        "Crew": self.get_crew(item, episode),
        "Title": episode['properCaseTitle'],
        "OriginalTitle": None, 
        "Year": int(episode['releaseYear']),
        "Duration": self.get_duration(item, is_episode=episode), 
        "ExternalIds": None,
        "Deeplinks": { 
        "Web": str(self.get_deeplinks(item, is_episode=episode, contador=contador)), 
        "Android": None,
        "iOS": None,
        },
        "Synopsis": episode['logLine'], 
        "Image": None, 
        "Rating": str(episode['ratingCode']),
        "Provider": None, 
        "Genres": self.get_genres(item, is_episode=episode),
        "Cast": self.get_cast(item, is_episode=episode),
        "Directors": self.get_directors(item, is_episode=episode), 
        "Availability": None,
        "Download": self.get_download(item, is_episode=episode),
        "IsOriginal": bool(episode['original']),
        "IsAdult": None,
        "IsBranded": None,
        "Packages": self.get_packages(),
        "Country": None,
        "Timestamp": str(datetime.datetime.now().isoformat()),
        "CreatedAt": str(self._created_at),
        }
        return payload_episode_

    def get_image(self,item):

        image = []
        image.append(self.images_url.format(item['contentId']))

        return image


    def get_download(self, item, is_episode=False):
        '''
        Si el contenido es serie:
        la informacion en la api para ver si se puede descargar o no
        está en cada episodio(los cuales son True siempre),
        por ende dejo "None" hardcodeado por defecto si el contenido es serie.
        Si el contenido es episodio o movie, el dato en general está para scrapearlo.
        '''
        if item['contentType'] == 'Movie':
            return item['downloadable']
        
        elif is_episode:
            try:
                return is_episode['downloadable']
            except:
                return None
        else:
            return None


    def get_seasons(self,item):
        seasons = []

        if item['contentType'] == 'Movie':
            return None
        
        else:
            for season in item['childContent']:
                seasons.append(self.payload_season(item,season))

        return seasons



    def payload_season(self,item,seas):
        payload_seasons = {
            "Id": str(seas['contentId']), 
            "Synopsis": seas['logLine'], 
            "Title": seas['title'],
            "Deeplink": self.get_deeplinks(item,is_season=seas),
            "Number": seas['order'], 
            "Year": seas['minReleaseYear'], 
            "Image": None,
            "Directors": self.get_directors(item), 
            "Cast": self.get_cast(item,is_season=seas), 
            "Episodes": seas['episodeCount'],
            "IsOriginal": seas['original']
        }

        return payload_seasons



    def get_packages(self):
        
        '''
        Esto va hardcodeado, porque no hay de donde obtener esta info
        '''
        package = [{'Type': 'subscription-vod'}]

        return package


    def get_genres(self,item, is_episode=False):
        genres = []

        if is_episode:
            for genre in is_episode['genres']:
                genres.append(genre['description'])
            return genres

        else:
            for genre in item['genres']:
                genres.append(genre['description'])
            return genres

    def get_directors(self,item, is_episode=False):
        '''
        la información certera de los directores se encuentra en cada episodio,
        por lo que el total de directores de la serie es el conjunto de directores
        de cada episodio, por eso, si el contenido, es serie, solo relleno ese campo
        en cada episodio.
        '''
        directors = []
        if item['contentType'] == 'Series with Season':
            return directors
        
        elif is_episode:
            for director in is_episode['directors']:
                directors.append(director['fullName'])
        
        else:
            for director in item['directors']:
                directors.append(director['fullName'])

        return directors


    def get_release_year(self, item):
        '''
        Este método devuelve el año de lanzamiento de un contenido,
        si es serie se puede tomar el "minReleaseYear" o el "maxReleaseYear",
        osea que este dato varía segun las temporadas.
        Por defecto dejo el "minReleaseYear"         
        '''
        if item['contentType'] == 'Series with Season':

            return int(item['minReleaseYear'])
        else:

            return int(item['releaseYear'])


    def get_content(self,url):
        response = self.session.get(url)#conexión a la url
        dictionary = response.json()#ordeno la información obtenida en formato JSON

        self.contents = []

        items_dicc = dictionary['playContentArray']

        for item in items_dicc['playContents']:

            if item in self.contents:
                print('repetido')
            else:
                self.contents.append(item)

        return self.contents


    def get_crew(self,item, is_season=False, is_episode=False):
        '''
        Guarda en un diccionario la gente del cast que no son actores ni directores,
        en el caso de que una persona cumpla más de un rol, se insertan en una lista.
        '''
        crew_episode = {}
        crew = {}
        crew['Name'] = []
        crew['Role'] = []
        crew_episode['Name'] = []
        crew_episode['Role'] = []

        if is_episode:
            for credit in is_episode['credits']:
                for role in credit['keyedRoles']:

                    if role['name'] == 'Escritor' or role['name'] == 'Productor':
                        crew_episode['Role'].append(role['name'])
                        crew_episode['Name'].append(credit['name'])

                    else:
                        pass

            return crew_episode

        else:#Si es movie o serie

            for credit_ in item['credits']:   
                for role in credit_['keyedRoles']:
                    if role['name'] == 'Escritor' or role['name'] == 'Productor':
                        crew['Role'].append(role['name'])
                        crew['Name'].append(credit_['name'])
                    else:
                        pass

            return crew


    def get_cast(self,item, is_season=False, is_episode=False):
        ''' Toma el cast completo, sacando a Escritores y productores.'''
        cast_season = []
        cast_episode = []
        cast = []

        if is_season:
            try:
                credits = is_season['credits']
                for credit in is_season['credits']:
                    for role in credit['keyedRoles']:
                        if role['name'] == 'Escritor' or role['name'] == 'Productor':
                            pass
                        else:
                            cast_season.append(credit['name'])
                
                return cast_season
            except:
                return cast_season

        elif is_episode:
            for credit in is_episode['credits']:
                for role in credit['keyedRoles']:

                    if role['name'] == 'Escritor' or role['name'] == 'Productor':
                        pass

                    else:
                        cast_episode.append(credit['name'])

            return cast_episode

        else:

            for cast_ in item['credits']:   
                for role in cast_['keyedRoles']:
                    if role['name'] == 'Escritor' or role['name'] == 'Productor':
                        pass
                    else:
                        cast.append(cast_['name'])

            return cast


    def get_deeplinks(self, item, is_episode=False, is_season=False, contador=False):
        #Verifica si es pelicula
        if item['contentType'] == 'Movie':

            deeplink = self.url + 'movies' + '/' + item['title'].lower().replace(':','').replace('?','').replace('¿','').replace(' ','-') + '-' + str(item['contentId'])

        #Verifica si es season
        elif item['contentType'] == 'Series with Season' and is_season:
            
            deeplink = self.url + 'series' + '/' + item['title'].lower().replace(':','').replace('?','').replace('¿','').replace(' ','-') + '/' + 'season-'+str(is_season['order']) + '/' + str(is_season['contentId'])
        
        #Verifica si es un episodio
        elif is_episode:

            deeplink = self.url + 'series' + '/' + item['title'].lower().replace(':','').replace('?','').replace('¿','').replace(' ','-') + '/' + 'season-'+str(is_episode['seasonNumber']) + '/' + 'episode-'+str(contador)+ '/' + str(is_episode['contentId'])

        #Si es serie...:
        else:

            deeplink = self.url + 'series' + '/' + item['title'].lower().replace(':','').replace('?','').replace('¿','').replace(' ','-') + '/' + str(item['contentId'])
        
        return deeplink


    def get_duration(self,item, is_episode=False):

        if item['contentType'] == 'Series with Season' and is_episode:
            return int(is_episode['runtime']/60)

        elif item['contentType'] == 'Series with Season':
            return None
        
        else:
            return int(item['runtime']/60)


    def get_type(self, typee):
        if typee == 'Series with Season':
            return 'serie'
        elif typee == 'Movie':
            return 'movie'