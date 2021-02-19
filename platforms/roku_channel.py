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

        METODOLOGIA API, HTML, SELENIUM --> API y HTML.

        NECESITA VPN --> SI.
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

    def _scraping(self, testing = False):

        payloads = []
        payloads_episodes = []

        scraped = Datamanager._getListDB(self, self.titanScraping)
        scraped_episodes = Datamanager._getListDB(self, self.titanScrapingEpisodios)

        main_page_data = Datamanager._getJSON(self, self._start_url)

        # BORRAR, sirve para ir contando los ids (sin duplicados)
        contents_id = {}

        # Hago una iteración con todas las categorias de contenido que presenta la página 
        for collection in main_page_data['collections']:
            
            category = collection['title']

            # Como las categorías "Characters", "Live TV" y "Browse Premium Subscriptions" no presentan contenidos scrapeables, las salteo
            if category == "Characters" or category == "Browse Premium Subscriptions" or category == "Live TV":
                continue

            for content in collection['view']:

                content_data = content['content']

                content_id = content_data['meta']['id']
                content_title = content_data['title']
                content_link = "https://therokuchannel.roku.com/details/{}".format(content_id)

                more_like_this_soup = Datamanager._getSoup(content_link)

                content_type = content_data['type'] if content_data.get('type') else None # Si el type es None debería saltearlo, ya que no corresponde a un contenido.
                # content_year = content_data['releaseYear'] if content_data.get('releaseYear') else int(content_data['releaseDate'].split("-")[0])
                # content_duration = content_data['runTimeSeconds'] if content_data['runTimeSeconds'] > 0 else None
                
                # DESCRIPCIONES HAY VARIAS, CUAL TENGO QUE TOMAR?
                content_images = []
                for image in content_data['images']:
                    image_path = image['path']
                    content_images.append(image_path)
                # RATINGS VIENE EN FORMATO LIST
                content_genres = content_data['genres'] if content_data.get('genres') else None

                # BORRAR
                # print("El contenido {} es de tipo {}".format(content_title, content_type))
                # print("{} | {} | {} | {} | {} | {}".format(content_id, content_year, content_duration, content_link, content_images, content_genres))
            
        # BORRAR 
        print(len(contents_id))