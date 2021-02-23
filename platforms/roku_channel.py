# -*- coding: utf-8 -*-
import time
import requests
import hashlib   
import pymongo 
import re
import json
import platform
from handle.replace import _replace
from common import config
from datetime import datetime
from handle.mongo import mongo
from slugify import slugify
from handle.datamanager import Datamanager
from updates.upload import Upload

class RokuChannel():
    '''
        Scraping de la plataforma The Roku Channel, la misma está asociada a una 
        serie de reproductores de medios digitales manufacturados por la empresa 
        estadounidense Roku.Inc. Presenta algunos contenidos Free to Watch, mientras 
        que se precisa suscripción para acceder a otros titulos.

        Para obtener todos los titulos primero se analiza la pagina principal, esta 
        trae aproximadamente 1300 contenidos, de los cuales se obtienen los ids de 
        los géneros de cada uno (acumula un aproximado de 114 géneros sin repetidos). 
        Luego, con los ids de cada género, se accede a la API de cada uno para traer
        todos los titulos que matchean con ese género. Esto acumula un aproximado de 
        7000 títulos.

        DATOS IMPORTANTES: 
            - ¿Necesita VPN? -> SI.
            - ¿HTML, API, SELENIUM? -> API
            - Cantidad de contenidos (ultima revisión 23/02/2021): 7023 titulos | 59627 episodios
            - Tiempo de ejecucion: 175 minutos aproximadamente
    '''
    def __init__(self, ott_site_uid, ott_site_country, type):
        self._config = config()['ott_sites'][ott_site_uid]
        self._start_url = self._config['start_url']
        self._platform_code = self._config['countries'][ott_site_country]
        self._created_at = time.strftime("%Y-%m-%d")
        self.mongo = mongo()
        
        self.titanPreScraping = config()['mongo']['collections']['prescraping']
        self.titanScraping = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodios = config()['mongo']['collections']['episode']
        
        self.skippedTitles = 0
        self.skippedEpis = 0
        
        self.sesion = requests.session()
        self.headers  = {"Accept":"application/json",
                         "Content-Type":"application/json; charset=utf-8"}

        self.content_api = self._config['content_api']
        self.content_link = self._config['content_link']
        self.genre_api = self._config['genre_api']
        
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
            self._scraping(testing=True)

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
 
    def _scraping(self, testing=False):

        payloads = []
        payloads_episodes = []

        scraped = Datamanager._getListDB(self, self.titanScraping)
        scraped_episodes = Datamanager._getListDB(self, self.titanScrapingEpisodios)

        start_time = time.time()
        titles_ids = self.get_content_ids()

        for content_id in titles_ids:
            
            content_data = Datamanager._getJSON(self, self.content_api.format(content_id))

            # Creo una lista con las 4 opciones válidas que puede tener un tipo de contenido para validarlo luego
            possible_types = ['series', 'shortformvideo', 'movie', 'tvspecial']

            if content_data['type'] not in possible_types:
                continue
            else:
                self.general_scraping(content_id, content_data, payloads, payloads_episodes, scraped, scraped_episodes)

        Datamanager._insertIntoDB(self, payloads, self.titanScraping)
        Datamanager._insertIntoDB(self, payloads_episodes, self.titanScrapingEpisodios)

        # TODO: Update

        print("--- {} seconds ---".format(time.time() - start_time))
        self.sesion.close()

    def get_content_ids(self):
        '''
            Este método se encarga de analizar la página principal, con el objetivo 
            de obtener primero los ids de todos los posibles géneros (va trayendo los 
            géneros de cada contenido analizable en la página, evitando los duplicados). 
            Al tener aprox 1000 contenidos en la página se estarían trayendo todos los
            géneros posibles.

            Una vez que obtengo los ids de los géneros, accedo con ellos a sus respectivas
            APIs que contienen los títulos asociados a dicho género. Luego hace lo mismo que 
            en el paso anterior, acumula los ids de los títulos en un Set para que no haya 
            duplicados, con esto se obtiene un total de 6600 títulos aproximadamente.

            RETURN: Set de ids de contenidos/títulos.
        '''
        main_page_data = Datamanager._getJSON(self, self._start_url)

        # Este set va a servir para ir acumulando los ids de los contenidos (sin duplicados)
        contents_id = {}
        contents_id = set()
        # Este set va a servir para ir acumulando los ids de los generos (sin duplicados)
        genres_id = {}
        genres_id = set()

        # Hago una iteración con todas las categorias de contenido que presenta la página principal
        for collection in main_page_data['collections']:
            
            category = collection['title']

            # Como las categorías "Characters",
            #  "Live TV" y "Browse Premium Subscriptions" no presentan contenidos scrapeables,
            #  las salteo
            if category == "Characters" or category == "Browse Premium Subscriptions" or category == "Live TV":
                continue

            for content in collection['view']:

                content_data = content['content']

                # Busco el tipo de contenido,
                #  si no tiene un atributo 'type' probablemente esté parado sobre alguna categoria (la página principal mezcla contenidos
                # y categorias en la misma fila de contenidos) por lo que salteo al próximo contenido que sí sea scrapeable
                if not content_data.get('type'):
                    continue

                content_id = content_data['meta']['id']
                category_objects = content_data['categoryObjects']

                for category in category_objects:
                    
                    category_id = category['meta']['id']
                    genres_id.add(category_id)

                contents_id.add(content_id)

        for genre in genres_id:

            genre_contents = Datamanager._getJSON(self,
             self.genre_api.format(genre))

            # Valido que el género tenga una colección con contenidos,
            #  de no tenerla se saltea
            if not genre_contents['collections']:
                continue
            
            content_collection = genre_contents['collections'][0]['view']

            # Para cada contenido que se corresponda con el género actual,
            #  obtengo su id para agregarlo al set de ids
            for genre_content in content_collection:

                content_data = genre_content['content']
                content_id = content_data['meta']['id']

                contents_id.add(content_id)
        return contents_id

    def general_scraping(self, content_id, content_data, payloads, payloads_episodes, scraped, scraped_episodes):
        '''
            Este método se encarga de scrapear el .json que se le pasa por parámetro, 
            si se trata de una serie además scrapea los episodios.
            Para todos los casos se utilizan las funciones checkDBAndAppend e insertIntoDB
            del DataManager (se consulta con la base de datos y se sube a la misma)

            - PARÁMETROS:
                - content_id: el ID del contenido
                - content_data: el.json del contenido
                - payloads: la lista de payloads en la que se van acumulando los contenidos
                - payloads_episodes: la lista de payloads en la que se van acumulando los episodios
                - scraped: la BD con los contenidos ya scrapeados
                - scraped_episodes: la BD con los episodios ya scrapeados
            
        '''
        # TITULO
        content_title = content_data['title']

        # LINK DEL CONTENIDO
        content_link = self.content_link.format(content_id)

        # TIPO DE CONTENIDO
        if content_data['type'] == 'series':
            content_type = 'serie'
        else:
            content_type = 'movie'

        # AÑO DE ESTRENO
        # Hago una validación para obtener el releaseYear:
        if content_data.get('releaseYear'):
            content_year = content_data['releaseYear']
        elif content_data.get('releaseDate'):
            content_year = int(content_data['releaseDate'].split("-")[0])
        else:
            content_year = None

        # DURACIÓN
        if content_data.get('runTimeSeconds'):
            content_duration = content_data['runTimeSeconds'] // 60 if content_data['runTimeSeconds'] > 0 else None
        else:
            content_duration = None
        
        # DESCRIPCION
        # Como los contenidos tienen varias descripciones (cortas y largas, en ese orden) traigo todas y luego
        # obtengo la mas larga (ubicada en el ultimo lugar de la lista)
        descriptions = content_data['descriptions']
        descriptions_text = []
        for key in descriptions:
            descriptions_text.append(descriptions[key]['text'])
        content_description = descriptions_text[-1] if descriptions_text else None
        
        # IMÁGENES
        content_images = []
        for image in content_data['images']:
            image_path = image['path']
            content_images.append(image_path)

        # RATING
        if content_data.get('parentalRatings'):
            content_rating = ""
            for rating in content_data['parentalRatings']:
                if rating['code'] != 'UNRATED':
                    rating_code = rating['code']
                    content_rating += rating_code + ", "
        else:
            content_rating = None
        
        # GÉNEROS
        content_genres = content_data['genres'] if content_data.get('genres') else None

        # CAST & DIRECTORS
        content_cast = []
        content_directors = []
        for person in content_data['credits']:
            if person['role'] == 'ACTOR':
                content_cast.append(person['name'])
            if person['role'] == 'DIRECTOR':
                content_directors.append(person['name'])

        # Para obtener datos como disponibilidad de contenido, package y provider accedo a los viewOptions del mismo:
        content_view_options = content_data['viewOptions'][0]

        # AVAILABILITY
        content_availability = content_view_options['validityEndTime']

        # PACKAGES
        if content_view_options['license'] == "Subscription":
            content_package = [{'Type': 'subscription-vod'}]
        elif content_view_options['license'] == "Free":
            content_package = [{'Type': 'free-vod'}]

        # PROVIDER
        # Aclaración: Si el contenido es gratis ("Free to watch") generalmente el provider es TheRokuChannel. Los que son
        # contenidos pagos bajo suscripción tienen otros providers.
        content_provider = content_view_options['providerDetails']['title']

        payload = {
                'PlatformCode': self._platform_code,
                'Id': content_id,
                'Title': content_title,
                'OriginalTitle': None,
                'CleanTitle': _replace(content_title),
                'Type': content_type,
                'Year': content_year,
                'Duration': content_duration,
                'ExternalIds': None,
                'Deeplinks': {
                    'Web': content_link,
                    'Android': None,
                    'iOS': None,
                },
                'Playback': None,
                'Synopsis': content_description,
                'Image': content_images if content_images else None,
                'Rating': content_rating[:-2] if content_rating else None, # elimina la ultima coma del String
                'Provider': content_provider,
                'Genres': content_genres,
                'Cast': content_cast if content_cast else None, 
                'Directors': content_directors if content_directors else None,
                'Availability': content_availability,
                'Download': None,
                'IsOriginal': None,
                'IsAdult': None,
                'IsBranded': None,
                'Packages': content_package,
                'Country': None,
                'Timestamp': datetime.now().isoformat(),
                'CreatedAt': self._created_at
        }

        # Si el contenido es de tipo serie, debo agregarle el campo "Seasons" al payload general
        # También hay que llamar a la función que scrapea los episodios
        if content_type == 'serie':
            # Esta lista va a ir acumulando los dict con los datos de cada temporada, para luego agregarla
            # al payload de la serie
            seasons_payload = []

            seasons_data = content_data['seasons']

            for season in seasons_data:

                    # Implemento un try/except porque no todas las temporadas de una serie tienen
                    # información pertinente. Si algún dato no se puede traer se saltea la temporada
                    # y pasa a la siguiente (no queda registrado en el campo Seasons del payload de 
                    # la serie). Pero toda la seasons_data se analiza aparte para el payload de los 
                    # episodios.
                    try:
                        # SEASON ID, TITULO, LINK, NÚMERO Y AÑO DE ESTRENO
                        season_id = season['meta']['id']
                        season_title = season['title'] if season.get('title') else None
                        season_link = self.content_link.format(season_id) 
                        season_number = int(season['seasonNumber'])
                        season_release_year = season['releaseNumber'] if season.get('releaseNumber') else None

                        # IMÁGENES (SEASONS)
                        season_images = []
                        for image in season['images']:
                            image_path = image['path']
                            season_images.append(image_path)

                        # CAST & DIRECTORS (SEASONS)
                        season_cast = []
                        season_directors = []
                        for person in season['credits']:
                            if person['role'] == 'ACTOR':
                                season_cast.append(person['name'])
                            if person['role'] == 'DIRECTOR':
                                season_directors.append(person['name'])

                        season_payload = {
                                'Id': season_id,
                                'Synopsis': None,
                                'Title': season_title,
                                'Deeplink': season_link,
                                'Number': season_number,
                                'Year': season_release_year,
                                'Image': season_images if season_images else None,
                                'Directors': season_directors if season_directors else None,
                                'Cast': season_cast if season_cast else None
                                }

                        seasons_payload.append(season_payload)
                    except:
                        continue
            
            # Agrego el campo "Seasons" al payload con toda la información recopilada de las temporadas
            payload['Seasons'] = seasons_payload if seasons_payload else None

            self.episodes_scraping(content_id, content_title, seasons_data, payloads_episodes, scraped_episodes)

        Datamanager._checkDBandAppend(self, payload, scraped, payloads)

    def episodes_scraping(self, content_id, content_title, seasons_data, payloads_episodes, scraped_episodes):
        '''
            Este método se encarga de analizar una fracción del .json de las series 
            (el apartado de las temporadas). Scrapea los datos de los episodios
            y luego los carga en la BD mediante las funciones del DataManager.

            - PARÁMETROS:
                - content_id: el ID de la serie padre
                - content_title: el titulo de la serie padre
                - seasons_data: fragmento del .json de la serie padre
                - payloads_episodes: la lista de payloads en la que se van acumulando los episodios
                - scraped_episodes: la BD con los episodios ya scrapeados
        '''
        # Loop doble para iterar episodio por episodio en la lista de episodios de cada temporada
        for season in seasons_data:
            # Valido que la temporada cuente con episodios
            if season.get('episodes'):
                for episode in season['episodes']:

                    # EPISODE ID, TITULO, NUMERO, NUMERO SEASON, LINK
                    episode_id = episode['meta']['id']
                    episode_title = episode['title']
                    episode_number = episode['episodeNumber']
                    season_number = episode['seasonNumber']
                    episode_link = self.content_link.format(episode_id)

                    # AÑO DE ESTRENO (EPISODE)
                    episode_year = int(episode['releaseDate'].split("-")[0])

                    # DESCRIPCION (EPISODE)
                    # Como los contenidos tienen varias descripciones (cortas y largas, en ese orden) traigo todas y 
                    # luego obtengo la mas larga (ubicada en el ultimo lugar de la lista)
                    # La otra opcion es traer la descripcion que tienen por defecto (la mas corta)
                    if episode.get('descriptions'):
                        descriptions = episode['descriptions']
                        descriptions_text = []
                        for key in descriptions:
                            descriptions_text.append(descriptions[key]['text'])
                        episode_description = descriptions_text[-1]
                    elif episode.get('description'):
                        episode_description = episode['description']
                    else:
                        episode_description = None
                    
                    # IMÁGENES (EPISODE)
                    episode_images = []
                    for image in episode['images']:
                        image_path = image['path']
                        episode_images.append(image_path)

                    # Estas variables se declaran nulas antes de validar que se puedan obtener 
                    episode_view_options = None
                    episode_availability = None
                    episode_package = None
                    # TODO: providers para los episodios
                    
                    if episode.get('viewOptions'):
                        # Para obtener datos como disponibilidad del episodio y package accedo a los viewOptions del mismo:
                        episode_view_options = episode['viewOptions'][0]

                        # AVAILABILITY (EPISODE)
                        episode_availability = episode_view_options['validityEndTime']

                        # PACKAGES (EPISODE)
                        # TODO: considerar traer el package de la serie en el caso de no encontrarlo
                        # (consultar si está bien asumir que si una serie figura como gratis o por suscripcion
                        # todos sus episodios van a tener el mismo modelo de negocios)
                        if episode_view_options['license'] == "Subscription":
                            episode_package = [{'Type': 'subscription-vod'}]
                        elif episode_view_options['license'] == "Free":
                            episode_package = [{'Type': 'free-vod'}]

                    payload_episode = {
                                'PlatformCode': self._platform_code,
                                'Id': episode_id, 
                                'ParentId': content_id,
                                'ParentTitle': content_title,
                                'Episode': episode_number, 
                                'Season': season_number, 
                                'Title': episode_title,
                                'OriginalTitle': None, 
                                'Year': episode_year, 
                                'Duration': None,
                                'ExternalIds': None,
                                'Deeplinks': {
                                    'Web': episode_link,
                                    'Android': None,
                                    'iOS': None,
                                    },
                                'Synopsis': episode_description,
                                'Image': episode_images if episode_images else None,
                                'Rating': None,
                                'Provider': None,
                                'Genres':None,
                                'Cast': None,
                                'Directors': None,
                                'Availability': episode_availability,
                                'Download': None,
                                'IsOriginal': None,
                                'IsAdult': None,
                                'Packages': episode_package,
                                'Country': None,
                                'Timestamp': datetime.now().isoformat(),
                                'CreatedAt': self._created_at
                                }
                    Datamanager._checkDBandAppend(self, payload_episode, scraped_episodes, payloads_episodes, isEpi=True)