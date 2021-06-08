# -*- coding: utf-8 -*-
import time
import requests
from common import config
from updates.upload import Upload
from handle.replace import _replace
from handle.mongo import mongo
from datetime import datetime
from bs4 import BeautifulSoup
import platform
from pyvirtualdisplay import Display
import re

class RtlXl():
    """
    RtlXl es una plataforma de Paises Bajos que ofrece ver contenidos
    on-demand de diferentes canales de ese país.
    Es necesario tener una cuenta en Rtlxl.

    DATOS IMPORTANTES:
    - VPN: No.
    - ¿Tiene API?: Si.
    - ¿Se relaciona con scripts TP? No.
    - ¿Instanacia otro archivo de la carpeta "platforms"?: No.
    - ¿Cuanto demoró la ultima vez? Menos de 1 minuto.
    - ¿Cuanto contenidos trajo la ultima vez? 114 contenidos al 30/5/2021.

    Más información importante:
    Al 30/5/2021 no se encontró contenido exclusivo de otros
    países como DE.
    """
    def __init__(self, ott_site_uid, ott_site_country, operation):
        self.config = config()['ott_sites'][ott_site_uid]
        self.created_at = time.strftime("%Y-%m-%d")
        self.titanPreScraping = config()['mongo']['collections']['prescraping']
        self.titanScraping = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodes = config()['mongo']['collections']['episode']
        self.mongo = mongo()
        self.platform_code = self.config['countries'][ott_site_country].lower()
        self.country = ott_site_country
        self.session = requests.session()
        self.currency  = config()['currency'][ott_site_country]

        print(f"\n{self.platform_code}")

        # Variables globales de la clase:
        self.is_episode = False
        self.start_url = config()['ott_sites'][ott_site_uid]['start_url']
        self.api_url = config()['ott_sites'][ott_site_uid]['api_url']

        if operation == 'scraping':
            self.scraping()

        elif operation == 'testing':
            print("*** TESTING ***\n")
            self.scraping(is_test=True)

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
            'PlatformCode': self.platform_code,
            'CreatedAt': self.created_at
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

    def scraping(self, is_test=False):
        """Método principal del scraping.

        Args:
            testing (bool, optional): Indica si está en modo testing. Defaults to False.
        """
        # Listas de contentenido scrapeado:
        self.scraped = self.query_field(self.titanScraping, field='Id')
        self.scraped_episodes = self.query_field(self.titanScrapingEpisodes, field='Id')

        print(f"{self.titanScraping} {len(self.scraped)}")
        print(f"{self.titanScrapingEpisodes} {len(self.scraped_episodes)}")

        # Lista de contenidos a obtener:
        self.payloads = []
        self.episodes_payloads = []

        # Contenidos de la plataforma en una lista.
        contents = self.get_contents()

        # Recorrer la lista de los contenidos.
        for n, content in enumerate(contents):
            print(f"\n----- Progreso ({n}/{len(contents)}) -----\n")
            self.content_scraping(content)

        # Almaceno la lista de payloads en mongo:
        if self.payloads:
            self.mongo.insertMany(self.titanScraping, self.payloads)
        if self.episodes_payloads:
            self.mongo.insertMany(self.titanScrapingEpisodes, self.episodes_payloads)
        
        self.session.close()

        Upload(self.platform_code, self.created_at, testing=is_test)

        print("Scraping Finalizado")

    def get_contents(self):
        """Obtengo todos los items, que son los contenidos
        de la API principal, que es:
        https://api.rtl.nl/rtlxl/missed/api/missed

        Returns:
            list: Lista con metadatos en forma de diccionarios.
        """
        print(f"\nObteniendo contenidos...\n\nAPI: {self.api_url}")
        response = self.request(self.api_url)
        list_items = response.json()['items']
        print(f"\nContenidos obtenidos: {len(list_items)}")
        return list_items

    def get_contents_2(self):
        """¡NO USAR! Se intentó obtener los contenidos recorriendo
        las categorías con la apiPrincipal, pero este método
        trajo menos contenidos.

        Returns:
            list: Lista con metadatos en forma de diccionarios.
        """
        print("\nObteniendo contenidos...")
        list_items = []
        response = self.request(self.api_url)
        channels = response.json()['channels']
        
        for channel in channels:
            print(f"\nChannel: {channel.get('name')}\n")
            offset = 0
            while True:
                # Ejemplo de api:
                # https://api.rtl.nl/rtlxl/missed/api/missed?dayOffset=0&channel=RTL4
                uri = f"{self.api_url}?dayOffset={offset}&channel={channel.get('name')}"
                print(f"API: {uri}")
                response = self.request(uri)
                items = response.json().get('items')
                if items:
                    list_items += items
                    offset += 1
                else:
                    break
        print(f"\nContenidos obtenidos: {len(list_items)}")        
        return list_items

    def content_scraping(self, content_metadata):
        """Metodo que se encarga de obtener payloads y episodes
        según el content_metadata, que es un diccionario con
        metadata.

        Args:
            content_metadata (dict): Metadata en forma de dict.
        """
        content_id = None
        episode_id = None

        serie_info = content_metadata.get('series') # metadata Obtener id.
        if serie_info:
            content_id = serie_info['slug']
            episode_id = content_metadata['id']
        elif content_metadata['type'] == 'Program':
            content_id = content_metadata['id']
        else:
            raise Exception("¡Hay nuevos types de contenidos!")

        if content_id in self.scraped:
            print("Payload ya ingresado")
        elif not content_id:
            print("No es una serie")
        else:
            payload = self.get_payload(content_metadata)
            if payload:
                self.payloads.append(payload)
                self.scraped.append(content_id)
        
        if episode_id in self.scraped_episodes:
            print("Payload ya ingresado")
        elif not episode_id:
            print("No es una serie")
        else:
            payload = self.get_payload(content_metadata, is_episode=True)
            if payload:
                self.episodes_payloads.append(payload)
                self.scraped_episodes.append(episode_id)

    def request(self, url, post=False, payload=None):
        '''
        Método para hacer una petición
        '''
        requestsTimeout = 5
        while True:
            try:
                response = None
                if post and payload:
                    response = self.session.post(url, data=payload, headers=self.headers ,timeout=requestsTimeout)
                else:
                    response = self.session.get(url, timeout=requestsTimeout)
                return response
            except requests.exceptions.ConnectionError:
                print("Connection Error, Retrying")
                time.sleep(requestsTimeout)
                continue
            except requests.exceptions.RequestException:
                print('Waiting...')
                time.sleep(requestsTimeout)
                continue
            break

    def get_payload(self, content_metadata, is_episode=False):
        """Método para crear el payload. Se reutiliza tanto para
        titanScraping, como para titanScrapingEpisodes.

        Args:
            content_metadata (dict): Indica la metadata del contenido.
            is_episode (bool, optional): Indica si hay que crear un payload
            que es un episodio. Defaults to False.

        Returns:
            dict: Retorna el payload.
        """
        payload = {}

        # Indica si el payload a completar es un episodio:
        if is_episode:
            self.is_episode = True
        else:
            self.is_episode = False
        
        payload['PlatformCode'] = self.platform_code
        payload['Id'] = self.get_id(content_metadata)
        payload['Title'] = self.get_title(content_metadata)
        payload['OriginalTitle'] = None
        payload['CleanTitle'] = self.get_clean_title(content_metadata)
        payload['Type'] = self.get_type(content_metadata)

        # En el caso que sea episodio, se agregan y quitan estos campos:
        if self.is_episode:
            payload["ParentTitle"] = self.get_parent_title(content_metadata)
            payload["ParentId"] = self.get_parent_id(content_metadata)
            payload["Season"] = self.get_season(content_metadata)
            payload["Episode"] = self.get_episode(content_metadata)
            del payload["OriginalTitle"]
            del payload['CleanTitle']
            del payload['Type']

        payload['Year'] = None
        payload['Duration'] = self.get_duration(content_metadata)
        payload['Deeplinks'] = self.get_deeplinks(content_metadata)
        payload['Playback'] = self.get_playback(content_metadata)
        payload['Synopsis'] = self.get_synopsis(content_metadata)
        payload['Image'] = self.get_images(content_metadata)
        payload['Rating'] = self.get_ratings(content_metadata)
        payload['Provider'] = self.get_providers(content_metadata)
        payload['Genres'] = None
        payload['Cast'] = None
        payload['Directors'] = None
        payload['Availability'] = None
        payload['Download'] = None
        payload['IsOriginal'] = None
        payload['Seasons'] = None
        payload['IsBranded'] = None
        payload['IsAdult'] = None
        payload['Packages'] = self.get_packages(content_metadata)
        payload['Country'] = None
        payload['Timestamp'] = datetime.now().isoformat()
        payload['CreatedAt'] = self.created_at

        if self.is_episode:
            del payload['Seasons']
            del payload['IsBranded']

        # Log:
        if not self.is_episode:
            print(f"Url: {payload['Deeplinks']['Web']}")
            print(f"{payload['Type']}:\t{payload['Title']}")
        
        return payload

    def get_id(self, content):
        if not self.is_episode:
            if content['type'] == 'Program':
                return content['id']
            else:
                return content['series']['slug']
        else:
            return content['id']

    def get_title(self, content):
        if not self.is_episode:
            if content['type'] == 'Program':
                return content['title']
            else:
                return content['series']['title']
        else:
            return content['title']

    def get_clean_title(self, content):
        if not self.is_episode:
            if content['type'] == 'Program':
                title = re.sub("\(.{1,}\)$", "" , content['title'] ,flags=re.IGNORECASE)
                return _replace(title.strip())
            else:
                title = re.sub("\(.{1,}\)$", "" , content['series']['title'] ,flags=re.IGNORECASE)
                return _replace(title.strip())
        else:
            return None

    def get_original_title(self, content):
        if not self.is_episode:
            return None
        else:
            return None

    def get_type(self, content):
        if not self.is_episode:
            if content['type'] == 'Program':
                return 'movie'
            else:
                return 'serie'
        else:
            return None

    def get_duration(self, content):
        if not self.is_episode:
            if content['type'] == 'Program':
                duration = content['duration']
                if duration:
                    return duration // 60
            else:
                return None
        else:
            duration = content['duration']
            if duration:
                return duration // 60

    def get_deeplinks(self, content):
        if not self.is_episode:
            if content['type'] == 'Program':
                web_link = (self.start_url + '/video/' + content['id'])
            else:
                web_link = (self.start_url + '/programma/' + content['series']['slug'])

            deeplinks = {
                "Web": web_link,
                "Android": None,
                "iOS": None,
            }
            return deeplinks
        else:
            deeplinks = {
                "Web": (self.start_url + '/programma/' + content['series']['slug'] + f"/{content['id']}" ), 
                "Android": None,
                "iOS": None,
            }
            return deeplinks

    def get_playback(self, content):
        if not self.is_episode:
            return None
        else:
            return None

    def get_synopsis(self, content):
        if not self.is_episode:
            return content['synopsis']
        else:
            return content['synopsis']

    def get_images(self, content):
        if not self.is_episode:
            assets = content['assets']
            return [i.get("url") for i in assets if i.get('type') == 'Cover' or i.get('type') == 'Poster']
        else:
            assets = content['assets']
            return [i.get("url") for i in assets if i.get('type') == 'Still']

    def get_ratings(self, content):
        rating = content.get('rating')
        if rating:
            return rating['nicam']['ageRecommendation']

    def get_providers(self, content):
        return [content['channel']['name']]

    def get_packages(self, content):
        """ Package hardcodeado ya que no se pudo obtener referencia."""
        return [{"Type": "tv-everywhere"}]

    def get_parent_title(self, content):
        return content['series']['title']

    def get_parent_id(self, content):
        return content['series']['slug']

    def get_season(self, content):
        return None

    def get_episode(self, content):
        title = content['title']
        try:
            if 'Afl.' in title:
                epi_number = re.sub("Afl.", "", title, flags=re.IGNORECASE).strip()
                return int(epi_number)
        except:
            return None