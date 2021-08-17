import time
import requests
from handle.replace         import _replace
from common                 import config
from handle.mongo           import mongo
from updates.upload         import Upload
from handle.payload         import Payload
from handle.datamanager     import Datamanager
from datetime import datetime
from handle.payload import Payload
# from time import sleep
# import re

class PlutoFioV():
    """
    Analizamos los contenidos (series y peliculas) para Pluto en Argentina.

    DATOS IMPORTANTES:
    - VPN: No (Recomendación: Usar ExpressVPN).
    - ¿Usa Selenium?: No.
    - ¿Tiene API?: Si.
    - ¿Usa BS4?: No.
    - ¿Cuanto demoró la ultima vez? tiempo + fecha.
    - ¿Cuanto contenidos trajo la ultima vez? cantidad + fecha.

    OTROS COMENTARIOS:
    Conseguimos la info de peliculas y series de una api general y la info sobre los episodios
     de cada serie a partir de una api por serie.
    """
    def __init__(self, ott_site_uid, ott_site_country, type):
        self.ott_site_uid = ott_site_uid
        self.ott_site_country = ott_site_country
        self._config = config()['ott_sites'][ott_site_uid]
        self._platform_code = self._config['countries'][ott_site_country]
        self._created_at = time.strftime("%Y-%m-%d")
        self.mongo = mongo()
        self.titanPreScraping = config()['mongo']['collections']['prescraping']
        self.titanScraping = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodios = config()['mongo']['collections']['episode']


        #Me guardo las API que tengo hardcodeadas en config.yaml
        self.api_url = self._config['api_url']
        self.season_api_url = self._config['season_api_url']
        self.url = self._config['url']

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
   
    def _scraping(self, testing=True):
        
        print(f"\nIniciando scraping de {self._platform_code}\n")
        # Comparando estas listas puedo ver si el elemento ya se encuentra scrapeado. 
        # LO USO LUEGO MÁS ADELANTE CUANDO YA PASÉ POR GET_CONTENTS
        self.scraped = self.query_field(self.titanScraping, field='Id')   
        self.scraped_episodes = self.query_field(self.titanScrapingEpisodios, field='Id')
        print(f"{self.titanScraping} {len(self.scraped)}")
        print(f"{self.titanScrapingEpisodios} {len(self.scraped_episodes)}")


        # Listas vacías para guardar los contenidos scrapeados:
        self.payloads = []
        self.episodes_payloads = []

        #Me traigo el contenido de la API
        contents = self.get_contents()

        for n, content in enumerate(contents):
            print(f"\n----- Progreso ({n}/{len(contents)}) -----\n") 

            # Valido que no haya duplicados usando las listas de ID que me traje antes:           
            if content['_id'] in self.scraped:
                print(content['name'] + ' ya ha sido ingresado a la Base de Datos')
                continue
            else:   
                self.scraped.append(content['_id']) #Como es nuevo, lo agrego al final de la lista de ids screapeados
                if (content['type']) == 'movie':
                    #Lleno el payload tanto para película como para serie y lo agrego al final de la lista de Payloads
                    self.payloads.append(self.get_payload(content)) 
                    #Si el contenido era una serie, lleno el payload de los episodios
                elif (content['type']) == 'series':
                    #envío la serie y lleno el payload de sus epis y guardo al final de lista de epis. 
                    self.get_epi_payloads(content) 

        if self.payloads:
            self.mongo.insertMany(self.titanScraping, self.payloads)
        else:
            print(f'\n---- Ninguna serie o pelicula para insertar a la base de datos ----\n')
        if self.episodes_payloads:
            self.mongo.insertMany(self.titanScrapingEpisodios, self.episodes_payloads)
        else:
            print(f'\n---- Ningun episodio para insertar a la base de datos ----\n')

        Upload(self._platform_code, self._created_at, testing=True)
        print("Scraping finalizado")
        self.session.close()

    def get_contents(self):
        '''
        Request a la API de películas/series (la API está en config.yaml) 
        Devuelve un diccionario con metadata en formato json
        '''
        print("\nObteniendo contenidos...\n")
        contents = [] # Contenidos a devolver.
        response = self.request(self.api_url)
        contents_metadata = response.json()        
        categories = contents_metadata["categories"]

        for categorie in categories:
            print(categorie.get("name"))
            contents += categorie["items"]
        return contents

    def get_contents_season(self,content):
        responseSeason = self.session.get(self.season_api_url.format(content['_id']))
        season_metadata = responseSeason.json()
        seasons_list = season_metadata['seasons']

        self.seas_list = []#Creo lista vacía

        for seasons in seasons_list:
            self.seas_list.append(seasons['episodes'])

        #Devuelvo una lista con todas las temporadas de la serie
        return self.seas_list

    def request(self, url):
        '''
        Método para hacer una petición
        '''
        requestsTimeout = 5
        while True:
            try:
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
    
    def get_epi_payloads(self, serie): 

        for season in self.get_contents_season(serie):
            for episode in season:
                if episode['_id'] in self.scraped_episodes: 
                    print('Capítulo ya ingresado en la Base de Datos')
                else:
                    self.scraped_episodes.append(episode['_id'])
                    self.episodes_payloads.append(self.payload_episode(serie, episode))

    def get_payload(self, content):
        print(content['name'])
        payload = {
            "PlatformCode": str(self._platform_code),
            "Id": str(content['_id']),
            "Title": self.get_title(content),
            "CleanTitle": _replace(content['name']),
            "OriginalTitle": None,
            "Type": str(self.get_type(content['type'])),
            "Year": None,
            "Duration": self.get_duration(content),
            "ExternalIds": None,
            "Deeplinks": {
            "Web": str(self.get_deeplinks(content)),
            "Android": None,
            "iOS": None
            },
            "Synopsis": str(content['description']),
            "Image": self.get_images(content),
            "Rating": str(content['rating']),
            "Provider": None,
            "Genres": self.get_genres(content),#[content['genre']],
            "Cast": None,
            "Directors": None,
            "Availability": None,
            "Download": None,
            "IsOriginal": None,
            "IsAdult": None,
            "IsBranded": None,
            "Packages": self.get_packages(),
            "Country": None,
            "Timestamp": str(datetime.now().isoformat()),
            "CreatedAt": str(self._created_at),
            }
        return payload

    def payload_episode(self, content, episodes):
        print(episodes['name'])
        payload_epi = {
            "PlatformCode": str(self._platform_code),
            "Id": str(episodes['_id']),
            "ParentId": str(content['_id']),
            "ParentTitle": str(content['name']),
            "Episode": self.get_episodes(episodes),
            "Season": int(episodes['season']),
            "Crew": None,
            "Title": str(episodes['name']),
            "OriginalTitle": None,
            "Year": None,
            "Duration": self.get_duration(content,episodes),
            "ExternalIds": None,
            "Deeplinks": {
            "Web": str(self.get_deeplinks(content, is_episode=episodes)),
            "Android": None,
            "iOS": None
            },
            "Synopsis": str(episodes['description']),
            "Image": self.get_images(episodes),
            "Rating": str(episodes['rating']),
            "Provider": None,
            "Genres": self.get_genres(content, is_episode=episodes),
            "Cast": None,
            "Directors": None,
            "Availability": None,
            "Download": None,
            "IsOriginal": None,
            "IsAdult": None,
            "IsBranded": None,
            "Packages": self.get_packages(),
            "Country": None,
            "Timestamp": str(datetime.now().isoformat()),
            "CreatedAt": str(self._created_at),
            }

        return payload_epi

    def get_title(self, content):
        if ' (' in content['name']:
            name_list = content['name'].split(' (')
            name_list.pop()
            name_list = ' '.join([str(elem) for elem in name_list])
            print(name_list)

            return name_list
        else:
            return content['name']

    def get_type(self, type_):
        if type_ == 'series':
            return 'serie'
        else:
            return type_

    def get_deeplinks(self, content, is_episode=False):
        #Película
        if content['type'] == 'movie':

            deeplink = self.url + 'movies' + '/' + content['slug']
        #Episodio
        elif content['type'] == 'series' and is_episode:

            deeplink = self.url + 'series' + '/' + content['slug'] + '/' + 'seasons' + '/' + str(is_episode['season']) + '/' + 'episode' + '/' + is_episode['slug']
        #Serie
        else:

            deeplink = self.url + 'series' + '/' + content['slug'] + '/' + 'details'

        return deeplink

    def get_duration(self,content, is_episode=False):

        if content['type'] == 'series' and is_episode:
            return int(is_episode['duration']/60000)

        elif content['type'] == 'series':
            return None
        
        else:
            return int(content['duration']/60000)

    def get_images(self, content):
        images = []
        for cover in content['covers']:
            images.append(cover['url'])
        return images

    def get_episodes(self,episode__):
        if episode__['number'] == 0:
            numb = int(1)
        else:
            numb = int(episode__['number'])
        
        return numb

    def get_genres(self, content, is_episode=False):
        '''Un compañero (pluto_mi) normaliza todos los géneros a lower, limpia todo de caracteres especiales
         y chequea si es sci-fi porque no correspondería en ese caso quitarle "-"
        '''

        if content['type'] == 'series' or content['type'] == 'movie':
            if '&' in content['genre']:
                return content['genre'].split(' & ')
        
        elif is_episode:
            if '&' in is_episode['genre']:
                return is_episode['genre'].split(' & ')
        
        else:
            return content['genre']
        
    def get_packages(self):

        '''
        Esto va hardcodeado, porque no hay de donde obtener esta info
        '''
        package = [{'Type': 'free-vod'}]

        return package