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
    """
        Scraping de la plataforma The Roku Channel, la misma está asociada a una serie de reproductores de medios digitales manufacturados por la empresa estadounidense Roku.Inc.
        Presenta algunos contenidos Free to Watch, mientras que se precisa suscripción para acceder a otros titulos.

        Para obtener todos los titulos primero se analiza la pagina principal, esta trae aproximadamente 1300 contenidos, de los cuales se obtienen los ids de los géneros de cada uno
        (acumula un aproximado de 114 géneros sin repetidos). Luego, con los ids de cada género, se accede a la API de cada uno para traer todos los titulos que matchean con ese género.
        Esto acumula un aproximado de 6600 títulos.

        DATOS IMPORTANTES: 
            - ¿Necesita VPN? -> SI.
            - ¿HTML, API, SELENIUM? -> API
            - Cantidad de contenidos (ultima revisión): TODO
            - Tiempo de ejecucion: TODO
    """
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

        # TODO: ESTE ES EL QUE VA: titles_ids = self.get_content_ids()
        titles_ids = self.ids_de_contenidos_temp()

        for content_id in titles_ids:
            
            content_data = Datamanager._getJSON(self, self.content_api.format(content_id))

            # Creo una lista con las 4 opciones válidas que puede tener un tipo de contenido para validarlo luego
            possible_types = ['series', 'shortformvideo', 'movie', 'tvspecial']

            if content_data['type'] not in possible_types:
                continue

            content_title = content_data['title']
            content_link = "https://therokuchannel.roku.com/details/{}".format(content_id)
            content_type = content_data['type']

            # TODO: VALIDAR LA MAYORIA DE LAS COSAS PARA QUE NO ROMPA

            # AÑO DE ESTRENO
            # Hago una validación para obtener el releaseYear:
            if content_data.get('releaseYear'):
                content_year = content_data['releaseYear']
            elif content_data.get('releaseDate'):
                content_year = int(content_data['releaseDate'].split("-")[0])
            else:
                content_year = None

            # DURACIÓN
            if content_data.get('runtimeSeconds'):
                content_duration = content_data['runTimeSeconds'] // 60 if content_data['runTimeSeconds'] > 0 else None
            
            # DESCRIPCION
            # Como los contenidos tienen varias descripciones (cortas y largas, en ese orden) traigo todas y luego obtengo la mas larga (ubicada en el ultimo lugar de la lista)
            descriptions = content_data['descriptions']
            descriptions_text = []
            for key in descriptions:
                descriptions_text.append(descriptions[key]['text'])
            content_description = descriptions_text[-1]
            
            # IMÁGENES
            content_images = []
            for image in content_data['images']:
                image_path = image['path']
                content_images.append(image_path)
            
            # GÉNEROS
            content_genres = content_data['genres'] if content_data.get('genres') else None

            # RATING
            content_rating = ""
            for rating in content_data['parentalRatings']:
                rating_code = rating['code']
                content_rating += rating_code + ", "

            # CAST & DIRECTORS
            content_cast = []
            content_directors = []
            for person in content_data['credits']:
                if person['role'] == 'ACTOR':
                    content_cast.append(person['name'])
                if person['role'] == 'DIRECTOR':
                    content_directors.append(person['name'])

            # TODO: Se puede traer el availability, provider (revisar y preguntar) y package.

            # TODO: Armar funciones para los payloads, en serie que traiga las seasons y episodes.

            # BORRAR
            print("El contenido {} es de tipo {}".format(content_title, content_type))
            print("{} \n {} \n {} \n {} \n {} \n {} \n {} \n {} \n {} \n {}".format(content_id, content_year, content_duration, content_link, content_images, content_genres, content_rating, content_description, content_cast, content_directors))


    def ids_de_contenidos_temp(self):
        ids = ["776e9fa6fb54570ebbf45f137ff60935",
            "7123f3fe86bf5c67bb724ad5aa67b5fe",
            "62cbba414996515fa4743266fbd5dd36",
            "5a519a92f1155016ab1d82136d871c1e",
            "b837898b334357d6affdee66f8dfc3a7",
            "c3b515ee795d513bae679fc40809e61f",
            "b80a10fdf92e5fd6ac29c93e389f9a0a",
            "b6eef7df3dd0550f8854c27a328e8327",
            "d3eb912e78a05a89914f2bb5dda0157d",
            "9b7f411348d45722b738768c492c4f7e",
            "7a54affc1f425556a93b3b747ca7ab63",
            "c152a3b9fd09510d8a67cb351fea0829",
            "bada5e852dc35eefaaf5288fb063a2a6",
            "b6fa6e1241e755f4ba5dcde0934ff4dd",
            "32d5202609835722bdcc1743bffd6211",
            "2674db67f18e59e384c713162d44260d",
            "bb17755883035a35a8411c80bfa6f5dc",
            "127966a690ab538288af6f7a6bc46ca6",
            "92b4e9cb84565bf69c23ee6a6baccecf",
            "6982f358523a5b3d9617eb4b0cb3ade7",
            "96d2f2d4a8ba51269cfa58543c6189d6",
            "6eb176cd6b82584da8296d002f80154b",
            "d50bdb288beb50a5a954829f1593f6c3",
            "bf76177027605f269fd21ecfe5500ab6",
            "59870b02d3695ea6b9a9409475ea9c67",
            "9328bca7b70f5c82b461298a1345f549",
            "c53d9054b08a55f9aa88e35c15303ddf",
            "bdb86f4cadf75957b341390a2c6fb205",
            "e675ac4b37585a00883cf2a4b3235dfa",
            "d4eaab487a425f62b2f09f995b12f32c",
            "c6b11e886d49538da26e4e256a0363c0",
            "4c26e259b7bf50669d176cbde26727b1",
            "ea6e56ac021050fb9ba2148ae9cca7d8",
            "3485f699a3bb59dd890c2d93592611d3",
            "b39d9fb25192554c83a490edb4fe4d50",
            "ed27b53ff2f758c99db3bdeb9478d684",
            "c5f514080715595da24f5a535c4df952",
            "a6114130155d514cb96f838d7bec1203",
            "bfd46891feb7580cb200f10b85aecd60",
            "58cacd4e309a59ab839c353469edbfbc",
            "29e787feff1852fda3d8e80a2f485e1e",
            "6a7b2a4424855a97b4e879a740d0b5bc",
            "066026dd6cd456398b6fa1c3df8c39a1",
            "ac6226fd79d95a90b9172447cd98ba80",
            "3696c402c9955b678d301b1aad6dea47",
            "82e907ab109755f7ade7517c429d1046",
            "61c606d7cbec5557b60d507493c286ca",
            "9ef7c4b7b4eb5ccea959e34437511841",
            "d5871e0970fe5d4ab4f69a8c4656fc64",
            "07e66c5078c652059ff039a7bcf1a260"]

        return set(ids)

    def get_content_ids(self):
        '''
            Este método se encarga de analizar la página principal, con el objetivo de obtener primero los ids de todos los posibles géneros
            (va trayendo los géneros de cada contenido analizable en la página, evitando los duplicados). Al tener aprox 1000 contenidos en la página
            se estarían trayendo todos los géneros posibles.

            Una vez que obtengo los ids de los géneros, accedo con ellos a sus respectivas APIs que contienen los títulos asociados a dicho género. Luego 
            hace lo mismo que en el paso anterior, acumula los ids de los títulos en un Set para que no haya duplicados, con esto se obtiene un total de 
            6600 títulos aproximadamente.

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

            # Como las categorías "Characters", "Live TV" y "Browse Premium Subscriptions" no presentan contenidos scrapeables, las salteo
            if category == "Characters" or category == "Browse Premium Subscriptions" or category == "Live TV":
                continue

            for content in collection['view']:

                content_data = content['content']

                # Busco el tipo de contenido, si no tiene un atributo 'type' probablemente esté parado sobre alguna categoria (la página principal mezcla contenidos
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

            genre_contents = Datamanager._getJSON(self, self.genre_api.format(genre))

            # Valido que el género tenga una colección con contenidos, de no tenerla se saltea
            if not genre_contents['collections']:
                continue
            
            content_collection = genre_contents['collections'][0]['view']

            # Para cada contenido que se corresponda con el género actual, obtengo su id para agregarlo al set de ids
            for genre_content in content_collection:

                content_data = genre_content['content']
                content_id = content_data['meta']['id']

                contents_id.add(content_id)
        return contents_id