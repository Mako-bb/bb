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
            - ¿Necesita VPN? -> SI (PureVPN USA).
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

        payloads_movies = []
        payloads_series = []
        payloads_episodes = []

        scraped = Datamanager._getListDB(self, self.titanScraping)
        scraped_episodes = Datamanager._getListDB(self, self.titanScrapingEpisodios)

        start_time = time.time()
        titles_ids = self.get_content_ids()

        for content_id in titles_ids:
            
            content_data = Datamanager._getJSON(self, self.content_api.format(content_id), showURL=False)

            # Creo una lista con las 3 opciones válidas que puede tener un tipo de contenido para validarlo luego
            possible_types = ['series', 'movie', 'tvspecial']

            if content_data['type'] not in possible_types:
                continue
            elif content_data['type'] == 'series':
                self.series_scraping(content_id, content_data, payloads_series, scraped, payloads_episodes, scraped_episodes)
            else:
                self.movies_scraping(content_id, content_data, payloads_movies, scraped)

        # Hago los insert a la base de datos para cada uno de los 3 casos (peliculas, series y episodios)
        Datamanager._insertIntoDB(self, payloads_movies, self.titanScraping)
        Datamanager._insertIntoDB(self, payloads_series, self.titanScraping)
        Datamanager._insertIntoDB(self, payloads_episodes, self.titanScrapingEpisodios)

        Upload(self._platform_code, self._created_at, testing=testing)

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
            duplicados, con esto se obtiene un total de 7000 títulos aproximadamente.

            - RETURN: Set de ids de contenidos/títulos.
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

                # Busco el tipo de contenido, si no tiene un atributo 'type' probablemente esté parado 
                # sobre alguna categoria (la página principal mezcla contenidos y categorias en la misma
                # fila de contenidos) por lo que salteo al próximo contenido que sí sea scrapeable
                if not content_data.get('type'):
                    continue

                content_id = content_data['meta']['id']
                category_objects = content_data['categoryObjects']

                for category in category_objects:
                    
                    category_id = category['meta']['id']
                    genres_id.add(category_id)

                contents_id.add(content_id)

        for genre in genres_id:

            genre_contents = Datamanager._getJSON(self, self.genre_api.format(genre))

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

    def movies_scraping(self, content_id, content_data, payloads, scraped):
        '''
            Este método se encarga de scrapear el .json que se le pasa por parámetro.
            Se utilizan las funciones checkDBAndAppend y merge_movies
            (se consulta con la base de datos y se homogeniza en caso de haber duplicados)

            - PARÁMETROS:
                - content_id: el ID del contenido
                - content_data: el .json del contenido
                - payloads: la lista de payloads en la que se van acumulando los contenidos
                - scraped: la BD con los contenidos ya scrapeados
        '''
        # TITULO
        content_title = content_data['title']

        # LINK DEL CONTENIDO
        content_link = self.content_link.format(content_id)

        # TIPO DE CONTENIDO
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
            # Algunas descripciones vienen algo sucias con caracteres como "(..." o "#"
            # por eso, si tiene alguno de esos caracteres, se la saltea como descripcion no valida
            text = descriptions[key]['text']
            if "(..." in text or "#" in text:
                continue
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
        content_provider = [content_view_options['providerDetails']['title']]

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

        self.merge_movies_payload(payload, scraped, payloads) 
 
    def series_scraping(self, content_id, content_data, payloads, scraped, payloads_episodes, scraped_episodes):
        '''
            Este método se encarga de scrapear el .json que se le pasa por parámetro, 
            como se trata de una serie además scrapea los episodios.
            Se utilizan las funciones checkDBAndAppend y merge_series
            (se consulta con la base de datos y se homogeniza en caso de haber duplicados)

            - PARÁMETROS:
                - content_id: el ID del contenido
                - content_data: el .json del contenido
                - payloads: la lista de payloads en la que se van acumulando los contenidos
                - scraped: la BD con los contenidos ya scrapeados
                - payloads_episodes: la lista de payloads en la que se van acumulando los episodios
                - scraped_episodes: la BD con los episodios ya scrapeados
        '''
        # TITULO
        content_title = content_data['title']

        # LINK DEL CONTENIDO
        content_link = self.content_link.format(content_id)

        # TIPO DE CONTENIDO
        content_type = 'serie'

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
            # Algunas descripciones vienen algo sucias con caracteres como "(..." o "#"
            # por eso, si tiene alguno de esos caracteres, se la saltea como descripcion no valida
            text = descriptions[key]['text']
            if "(..." in text or "#" in text:
                continue
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
        content_provider = [content_view_options['providerDetails']['title']]

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

        self.merge_series_payload(payload, seasons_data, scraped, payloads, payloads_episodes, scraped_episodes)

    def episodes_scraping(self, parent_data, seasons_data, payloads_episodes, scraped_episodes):
        '''
            Este método se encarga de analizar una fracción del .json de las series 
            (el apartado de las temporadas). Scrapea los datos de los episodios
            y luego los carga en la BD mediante las funciones del DataManager.

            - PARÁMETROS:
                - parent_data: dict con datos de la serie padre que pueden requerirse en el caso de que no los tenga la API 
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
                    episode_number = int(episode['episodeNumber'])
                    season_number = int(episode['seasonNumber'])
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
                    episode_provider = parent_data['Provider']
                    # En el caso de que no se pueda obtener informacion sobre el package del episodio
                    # se cuenta con el package de la serie padre como para completar
                    episode_package = parent_data['Packages']
                    
                    if episode.get('viewOptions'):
                        # Para obtener datos como disponibilidad del episodio y package accedo a los viewOptions del mismo:
                        episode_view_options = episode['viewOptions'][0]

                        # AVAILABILITY (EPISODE)
                        episode_availability = episode_view_options['validityEndTime']

                        # PACKAGES (EPISODE)
                        if episode_view_options['license'] == "Subscription":
                            episode_package = [{'Type': 'subscription-vod'}]
                        elif episode_view_options['license'] == "Free":
                            episode_package = [{'Type': 'free-vod'}]

                    payload_episode = {
                                'PlatformCode': self._platform_code,
                                'Id': episode_id, 
                                'ParentId': parent_data['Id'],
                                'ParentTitle': parent_data['Title'],
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
                                'Provider': episode_provider,
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

    def merge_series_payload(self, payload, seasons_data, scraped, payloads_list, payloads_episodes, scraped_episodes):
        '''
            Este método homogeniza series duplicadas en
            base a posibles criterios de duplicación.
            El caso principal se da cuando hay series
            separadas en temporadas que tienen distinto ID
            en la plataforma.

            En ese caso el payload dado solo va a actualizar datos
            del payload que corresponda a la serie duplicada, para 
            evitar que se suba dos veces.

            Si no hay ningun payload ingresado anteriormente 
            que coincida con el titulo de la serie (dentro de payloads_list),
            se ingresa el payload actual con el checkDBAndAppend del Datamanager.

            - PARÁMETROS:
                - payload: el payload actual, que actualizará otro payload ya
                           existente en el caso de duplicación o será ingresado
                           en el caso contrario.
                - seasons_data: extracto del .json que contiene informacion de 
                           las temporadas, en caso de ser necesario se utiliza
                           para sacar los episodios también unificados.
                - scraped: no tiene mucha importancia en esta instancia, solo sirve
                           para pasarle al checkDBAndAppend en caso de ser necesario.
                - payloads_list: la lista que contiene los payloads de series ya obtenidos.
                - payloads_episodes: la lista de payloads en la que se van acumulando los episodios.
                - scraped_episodes: la BD con los episodios ya scrapeados.
        '''
        # Encontró algun contenido duplicado?
        duplicate_value = False

        # Extraigo esta información para completar scrapear los episodios.
        # Si se trata de una serie sin duplicado por el momento, trae 
        # todas las temporadas y sus episodios. Si es el caso de un duplicado
        # solo scrapea los episodios de la temporada que no esté duplicada
        # y se haya mergeado.
        parent_data = {
                'Id': payload['Id'],
                'Title': payload['Title'],
                'Provider': payload['Provider'],
                'Packages': payload['Packages'] 
            }

        for serie_payload in payloads_list:
            if (payload['CleanTitle'] == serie_payload['CleanTitle'] and 
                payload['Synopsis'] == serie_payload['Synopsis']): # TODO: se puede mejorar el filtro? para homogenizar mejor con otro criterio
                # Si lo encontró, procedo a actualizar el payload ingresado antes
                if payload['Provider'][0] != serie_payload['Provider'][0]:
                    serie_payload['Provider'].append(payload['Provider'][0])

                if payload['Packages'][0]['Type'] != serie_payload['Packages'][0]['Type']:
                    serie_payload['Packages'].append(payload['Packages'][0])

                if serie_payload['Cast'] is None:
                    serie_payload.update({'Cast': payload['Cast']})

                if serie_payload['Directors'] is None:
                    serie_payload.update({'Directors': payload['Directors']})

                for season in payload['Seasons']:
                    if season['Number'] not in self.seasons_numbers(serie_payload['Seasons']):
                        serie_payload['Seasons'].append(season)

                        # Modifico el dict con los datos del padre para que 
                        # tenga el de la serie unificada y se vea reflejado en 
                        # los episodios
                        parent_data['Id'] = serie_payload['Id']
                        parent_data['Title'] = serie_payload['Title']
                        parent_data['Provider'] = serie_payload['Provider']
                        parent_data['Packages'] = serie_payload['Packages']

                        merged_season_data = self.find_season_data_by_number(season['Number'], seasons_data)

                        self.episodes_scraping(parent_data, merged_season_data, payloads_episodes, scraped_episodes)

                duplicate_value = True
                break

        # Si no se encontró un duplicado para este contenido, se carga en la BD   
        if not duplicate_value:
            Datamanager._checkDBandAppend(self, payload, scraped, payloads_list)
            self.episodes_scraping(parent_data, seasons_data, payloads_episodes, scraped_episodes)

    def merge_movies_payload(self, payload, scraped, payloads_list):
        '''
            Este método homogeniza peliculas duplicadas en
            base a posibles criterios de duplicación.
            El caso principal se da cuando hay duplicadas
            que tienen el mismo releaseYear pero distinto provider
            y por lo tanto distinto ID en la plataforma.

            En ese caso el payload dado solo va a actualizar datos
            del payload que corresponda a la pelicula duplicada, para 
            evitar que se suba dos veces.

            Si no hay ningun payload ingresado anteriormente 
            que coincida con el titulo de la pelicula (dentro de payloads_list),
            se ingresa el payload actual con el checkDBAndAppend del Datamanager.

            - PARÁMETROS:
                - payload: el payload actual, que actualizará otro payload ya
                           existente en el caso de duplicación o será ingresado
                           en el caso contrario.
                - scraped: no tiene mucha importancia en esta instancia, solo sirve
                           para pasarle al checkDBAndAppend en caso de ser necesario.
                - payloads_list: la lista que contiene los payloads de peliculas ya obtenidos. 
        '''
        # Encontró algun contenido duplicado?
        duplicate_value = False

        for movie_payload in payloads_list:
            if payload['CleanTitle'] == movie_payload['CleanTitle'] and payload['Year'] == movie_payload['Year']:
                # Si lo encontró, procedo a actualizar el payload ingresado antes
                if payload['Provider'][0] != movie_payload['Provider'][0]:
                    movie_payload['Provider'].append(payload['Provider'][0])

                if payload['Packages'][0]['Type'] != movie_payload['Packages'][0]['Type']:
                    movie_payload['Packages'].append(payload['Packages'][0])

                if movie_payload['Cast'] is None:
                    movie_payload.update({'Cast': payload['Cast']})

                if movie_payload['Directors'] is None:
                    movie_payload.update({'Directors': payload['Directors']})

                duplicate_value = True
                break

        # Si no se encontró un duplicado para este contenido, se carga en la BD   
        if not duplicate_value:
            Datamanager._checkDBandAppend(self, payload, scraped, payloads_list)

    def seasons_numbers(self, seasons_list):
        '''
            Dada una lista correspondiente a los campos Season
            de una serie, devuelve todos los numeros de temporadas.

            - RETURN int list 
        '''
        numbers = []
        for season in seasons_list:
            numbers.append(season['Number'])
        return numbers

    def find_season_data_by_number(self, season_number, seasons_data):
        '''
            Dado un .json con información sobre temporadas
            localiza la temporada correspondiente con el season_number

            - PARÁMETROS:
                - season_number: int, nro de temporada a localizar
                - seasons_data: .json con data sobre las temporadas de una serie
            - RETURN: list iterable con data de la temporada correspondiente    
        '''
        season_data = []
        for season in seasons_data:
            if season['seasonNumber'] == str(season_number):
                season_data = season
                break
        return [season_data]