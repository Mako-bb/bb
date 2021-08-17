# -*- coding: utf-8 -*-
import json
import time
import requests
from common import config
from updates.upload import Upload
from handle.replace import _replace
from handle.mongo import mongo
from datetime import datetime
import re

class Amediateka():
    """
    Amediateka es una ott rusa.

    DATOS IMPORTANTES:
    - VPN: Si (Recomendación: PureVPN).
    - ¿Usa Selenium?: Si. Para obtener coockies.
    - ¿Tiene API?: Si. Para obteer el contenido.
    - ¿Usa BS4?: No.
    - ¿Se relaciona con scripts TP? No.
    - ¿Instanacia otro archivo de la carpeta "platforms"?: No.
    - ¿Cuanto demoró la ultima vez? tiempo + fecha.
    - ¿Cuanto contenidos trajo la ultima vez? cantidad + fecha.

    OTROS COMENTARIOS:
    Con esta plataforma pasa lo siguiente...
    """
    def __init__(self, ott_site_uid, ott_site_country, operation):
        self.config = config()['ott_sites'][ott_site_uid]
        self.created_at = time.strftime("%Y-%m-%d")
        self.titanScraping = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodes = config()['mongo']['collections']['episode']
        self.mongo = mongo()
        self.platform_code = self.config['countries'][ott_site_country].lower()
        self.session = requests.session()

        print(f"\n{self.platform_code}")

        # Variables globales de la clase:
        self.is_episode = False
        self.start_url = config()['ott_sites'][ott_site_uid]['start_url']
        self.api_url = config()['ott_sites'][ott_site_uid]['api_url']
        self.serie_api_url = config()['ott_sites'][ott_site_uid]['serie_api_url']        
        self.content_types = config()['ott_sites'][ott_site_uid]['contents']
        self.headers = self.get_headers()
        self.api_key = self.get_apiKey()
        
        if operation == 'scraping':
            self.scraping()

        elif operation == 'testing':
            print("*** TESTING ***\n")
            self.scraping(is_test=True)

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

        if self.payloads:
            self.mongo.insertMany(self.titanScraping, self.payloads)
        if self.episodes_payloads:
            self.mongo.insertMany(self.titanScrapingEpisodes, self.episodes_payloads)
        
        self.session.close()
        Upload(self.platform_code, self.created_at, testing=is_test)

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

    def get_headers(self):
        """Método para obtener headers.

        Returns:
            dict: Diccionario con el header.
        """
        # from selenium import webdriver
        # from pyvirtualdisplay import Display        

        # if platform.system() == 'Linux':
        #     Display(visible=0, size=(1366, 768)).start()

        # browser = webdriver.Firefox()
        # browser.get(self._start_url)

        # # Retorna solo Cookie.
        # cookie = browser.execute_script("return document.cookie;")
        # print(f"\nObteniendo cookie: {cookie}\n")
        # browser.quit()
        # time.sleep(5)

        headers = {
            'authority': 'api.amediateka.tech',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.114 Safari/537.36 Edg/89.0.774.68',
            'accept': '*/*',
            'origin': 'https://www.amediateka.ru',
            'sec-fetch-site': 'cross-site',
            'sec-fetch-mode': 'cors',
            'sec-fetch-dest': 'empty',
            'referer': 'https://www.amediateka.ru/',
            'accept-language': 'en-US,en;q=0.9',
            'Cookie': 'acl="{\\"platform\\": 4\\054 \\"device_type\\": null\\054 \\"device_vendor\\": null\\054 \\"device_model\\": null}"'
            # 'Cookie': str(cookie)
        }

        return headers # TODO: Solucionar esto y no hardcodear.

    def get_url(self, url):
        """Método para hacer y validar una petición a un servidor.

        Args:
            url (str): Url a la cual realizaremos la petición.

        Returns:
            obj: Retorna un objeto tipo requests.
        """
        request_timeout = 5
        while True:
            try:
                response = self.session.get(
                    url,
                    headers=self.headers,
                    timeout=request_timeout
                )
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

    def get_apiKey(self):
        """Método para obtener la apiKey.

        Returns:
            str: Retorna la apiKey para acceder a la api.
        """
        return 'eeGaeliYah5veegh' # TODO: Solucionar esto y no hardcodear.

    def get_contents(self):
        """Método para obtener todos los contenidos de la plataforma.

        Returns:
            list: Retorna los contenidos que son dict, en una lista.
        """
        print("\nObteniendo contenidos...")
        contents = []
        page = 0
        for content_type in self.content_types:
            print(f"\n{content_type.upper()}:\n")
            while True:
                api_uri = self.api_url.format(
                    content_type=content_type,
                    api_key= self.api_key,
                    offset=page
                )
                response = self.get_url(api_uri)
                results = response.json().get('results')

                if not results:
                    page = 0
                    break

                for i in results:
                    print(i["title"])
                    contents.append(i)
                page += 20
        
        return contents

    def content_scraping(self, content):
        """Método donde ordenamos el scraping del contenido.

        Args:
            content (dict): Diccionario con info del contenido.
        """

        content_id = content['id']

        if content_id in self.scraped:
            print("Contenido ya ingresado")
        else:
            # Realizo otro request para obtener mucha más data del contenido:
            uri = content['url']
            response = self.get_url(uri)
            content_metadata = response.json()

            payload = self.get_payload(content_metadata)
            self.payloads.append(payload)
            if payload['Type'] == 'serie':
                content_metadata["ParentPackages"] = payload["Packages"]
                self.serie_info(content_metadata)

    def serie_info(self, content):
        """Método para ordear el scraping de los episodios de un contenido.

        Args:
            content (dict): Diccionario con info del contenido.
        """        
        seasons_data = content['seasons']
        for season in seasons_data:
            uri = season['url']
            response = self.get_url(uri)            
            # TODO: Errores por aqui en response.
            season_data = response.json()
            episodes = season_data['episodes']

            for episode in episodes:
                episode_id = str(episode['id'])

                if episode_id in self.scraped_episodes:
                    print("Episodio ingresado")
                else:
                    # Agrego seasonNumber a metadata del episodio.
                    episode["seasonNumber"] = season["seasonNumber"]
                    
                    # Herencia de Packages del padre.
                    episode["ParentPackages"] = content["ParentPackages"]

                    epi_payload = self.get_payload(episode, is_episode=True)
                    self.episodes_payloads.append(epi_payload)
            print(f"Insertados {len(episodes)} episodios. Season: {season['seasonNumber']}")

    def get_payload(self, content_metadata, is_episode=False):
        """Método para crear el payload.

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
        payload['OriginalTitle'] = self.get_original_title(content_metadata)
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

        payload['Year'] = self.get_year(content_metadata)
        payload['Duration'] = self.get_duration(content_metadata)
        payload['Deeplinks'] = self.get_deeplinks(content_metadata)
        payload['Playback'] = self.get_playback(content_metadata)
        payload['Synopsis'] = self.get_synopsis(content_metadata)
        payload['Images'] = self.get_images(content_metadata)
        payload['Rating'] = self.get_ratings(content_metadata)
        payload['Provider'] = self.get_providers(content_metadata)
        payload['Genres'] = self.get_genres(content_metadata)
        payload['Cast'] = self.get_cast(content_metadata)
        payload['Directors'] = self.get_directors(content_metadata)
        payload['Availability'] = None
        payload['Download'] = None
        payload['IsOriginal'] = None
        payload['Seasons'] = self.get_seasons(content_metadata)
        payload['IsBranded'] = None
        payload['IsAdult'] = None
        payload['Packages'] = self.get_packages(content_metadata)
        payload['Country'] = self.get_country(content_metadata)
        payload['Timestamp'] = datetime.now().isoformat()
        payload['CreatedAt'] = self.created_at

        if not self.is_episode:
            print(f"{payload['Type']}:\t{payload['Title']}")
        return payload

    def get_id(self, content):
        if not self.is_episode:
            return content['id']
        else:
            return content['id']

    def get_title(self, content):
        if not self.is_episode:
            return content['title']
        else:
            return content['title']

    def get_clean_title(self, content):
        if not self.is_episode:
            return _replace(content['title'])
        else:
            return None

    def get_original_title(self, content):
        if not self.is_episode:
            return content['originalTitle']
        else:
            return None

    def get_type(self, content):
        if not self.is_episode:
            return re.sub("s$", "", content['type'], flags=re.IGNORECASE)  
        else:
            return None

    def get_year(self, content):
        if not self.is_episode:
            return content['premierYear']
        else:
            return None

    def get_duration(self, content):
        if not self.is_episode:
            return content.get('duration')
        else:
            return content.get('duration')

    def get_deeplinks(self, content):
        if not self.is_episode:
            deeplinks = {
                "Deeplink": (self.start_url + content['webUrl']), 
                "Android": None,
                "iOS": None,
            }
            return deeplinks
        else:
            deeplinks = {
                "Deeplink": (self.start_url + content['season']['webUrl']), 
                "Android": None,
                "iOS": None,
            }
            return deeplinks

    def get_playback(self, content):
        if not self.is_episode:
            return content['playbackUrl']
        else:
            return content['playbackUrl']

    def get_synopsis(self, content):
        if not self.is_episode:
            return content['description']
        else:
            return content['description']

    def get_images(self, content):
        if not self.is_episode:
            return [content['assets'].get("poster_logo")]
        else:
            return [content['assets'].get("stopKadr")]

    def get_ratings(self, content):
        if not self.is_episode:
            return content['ageRestrictions']
        else:
            return None

    def get_providers(self, content):
        if not self.is_episode:
            return [studio.get("name") for studio in content['studios'] if studio.get("name")]
        else:
            return None

    def get_genres(self, content):
        if not self.is_episode:
            return [genre.get("name") for genre in content['genres'] if genre.get("name")]
        else:
            return None

    def get_cast(self, content):
        if not self.is_episode:
            return [genre.get("name") for genre in content['persons'] if genre.get("type") == 'actor']
        else:
            return None

    def get_directors(self, content):
        if not self.is_episode:
            return [genre.get("name") for genre in content['persons'] if genre.get("type") == 'director']
        else:
            return None

    def get_seasons(self, content):
        seasons_data = content.get("seasons")
        if not self.is_episode and seasons_data:
            seasons_list = []

            for season in seasons_data:
                season_dict = {}
                season_dict["Id"] = season['id']
                season_dict["Title"] = season['title']
                season_dict["Deeplink"] = (self.start_url + season['webUrl'])
                season_dict["Number"] = season['seasonNumber']
                season_dict["Image"] = season.get('assets').get('VproductBackground')
                season_dict["Episodes"] = season['episodeCount']
                seasons_list.append(season_dict)                

            return seasons_list
        else:
            return None

    def get_packages(self, content):
        if not self.is_episode:
            package = content['licenseTypes'][0]

            if package == 'SVOD':
                return [{"Type": "subscription-vod"}]
            else:
                return None
        else:
            if content.get("free"):
                raise ValueError("Ahora hay episodios gratuitos")
            return content["ParentPackages"]

    def get_country(self, content):
        if not self.is_episode:
            return [country.get("name") for country in content['countries']]
        else:
            return None

    def get_parent_title(self, content):
        return content['content']['title']

    def get_parent_id(self, content):
        return content['content']['id']

    def get_season(self, content):
        return content['seasonNumber']

    def get_episode(self, content):
        return content['number']