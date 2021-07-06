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

class PlutoFV():
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
        self.api_url             = self._config['api_url']
        self._created_at = time.strftime("%Y-%m-%d")
        self.mongo = mongo()
        self.titanPreScraping = config()['mongo']['collections']['prescraping']
        self.titanScraping = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodios = config()['mongo']['collections']['episode']

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
        # Listas de contentenido scrapeado:
        self.payloads = []
        self.episodes_payloads = []
        # Comparando estas listas puedo ver si el elemento ya se encuentra scrapeado.
        self.scraped = self.query_field(self.titanScraping, field='Id')   #
        self.scraped_episodes = self.query_field(self.titanScrapingEpisodios, field='Id')
        print(f"{self.titanScraping} {len(self.scraped)}")
        print(f"{self.titanScrapingEpisodios} {len(self.scraped_episodes)}")
        contents = self.get_contents()
        for n, item in enumerate(contents):
            print(f"\n----- Progreso ({n}/{len(contents)}) -----\n")            
            if item['_id'] in self.scraped:
                # Que no avance, el _id está repetido.
                print(item['name'] + ' ya esta scrapeado!')
                continue
            else:   
                self.scraped.append(item['_id'])
                if (item['type']) == 'movie':
                    self.movie_payload(item)
                elif (item['type']) == 'series':
                    self.serie_payload(item)
        # Validar tipo de datos de mongo:
        if self.payloads:
            self.mongo.insertMany(self.titanScraping, self.payloads)
        else:
            print(f'\n---- Ninguna serie o pelicula para insertar a la base de datos ----\n')
        if self.episodes_payloads:
            self.mongo.insertMany(self.titanScrapingEpisodios, self.episodes_payloads)
        else:
            print(f'\n---- Ningun episodio para insertar a la base de datos ----\n')
        #Upload(self._platform_code, self._created_at, testing=True)
        print("Scraping finalizado")
        self.session.close()

    def get_contents(self):
        """Metodo que hace reques a la api de Pluto TV y devuelve un diccionario con metadata en formato json"""
        print("\nObteniendo contenidos...\n")
        contents = [] # Contenidos a devolver.
        response = self.request(self.api_url)
        contents_metadata = response.json()        
        categories = contents_metadata["categories"]

        for categorie in categories:
            print(categorie.get("name"))
            contents += categorie["items"]
        return contents

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
    
    def serie_payload(self, item):
        image = self.get_image(item, 'serie')
        deeplink = self.get_deeplink(item, 'serie')
        print('Serie: ' + item['name'])
        seasons = self.get_seasons(item)
        serie_payload = {
            "PlatformCode": self._platform_code, #Obligatorio 
            "Id": item['_id'], #Obligatorio
            "Seasons": item['seasonsNumbers'],
            "Title": item['name'], #Obligatorio 
            "CleanTitle": _replace(item['name']), #Obligatorio 
            "OriginalTitle": item['name'], 
            "Type": item['type'], #Obligatorio 
            "Year": None, #Important! 
            "Duration": None, 
            "ExternalIds": None, 
            "Deeplinks": { 
            "Web": deeplink, #Obligatorio 
            "Android": None, 
            "iOS": None, 
            }, 
            "Synopsis": item['description'], 
            "Image": [image], 
            "Rating": item['rating'], #Important! 
            "Provider": None, 
            "Genres": [item['genre']], #Important!  
            "Cast": None, 
            "Directors": None, #Important! 
            "Availability": None, #Important! 
            "Download": None, 
            "IsOriginal": None, #Important! 
            "IsAdult": None, #Important! 
            "IsBranded": None, #Important! (ver link explicativo)
            "Packages": [{'Type':'free-vod'}],
            "Country": None, 
            "Timestamp": datetime.now().isoformat(), #Obligatorio 
            "CreatedAt": self._created_at, #Obligatorio
        }
        self.payloads.append(serie_payload)

    def get_seasons(self, item):
        season_return = []
        uri = 'https://service-vod.clusters.pluto.tv/v3/vod/series/' + str(item['_id']) + '/seasons?advertisingId=&appName=web&appVersion=5.17.1-be7b5e79fc7cad022e22627cbb64a390ca9429c7&app_name=web&clientDeviceType=0&clientID=381c83d6-6a14-44b9-897a-c4b9f0bc021a&clientModelNumber=na&country=AR&deviceDNT=false&deviceId=381c83d6-6a14-44b9-897a-c4b9f0bc021a&deviceLat=-34.6022&deviceLon=-58.3845&deviceMake=Chrome&deviceModel=web&deviceType=web&deviceVersion=91.0.4472.124&marketingRegion=VE&serverSideAds=true&sessionID=d0fb398c-db3a-11eb-8941-0242ac110002&sid=d0fb398c-db3a-11eb-8941-0242ac110002&userId=&attributeV4=foo'
        uri_req = self.request(uri)
        items = uri_req.json() 
        seasons = items['seasons'] 
        self.totalSeasons = 0
        for season in seasons:
            self.totalSeasons += 1
            #deeplink_season = self.get_deeplink(item, item['type'], 'numeber')
            season_payload = {
                "Id": items['_id'], #Importante
                "Synopsis": items['summary'], #Importante
                "Title": items['name'], #Importante, E.J. The Wallking Dead: Season 1
                "Deeplink": None, #deeplink_season, #Importante
                "Number": season['number'], #Importante
                "Year": None, #Importante
                "Image": None, 
                "Directors": None, #Importante
                "Cast": None, #Importante
                "Episodes": len(season['episodes']), #Importante
                "IsOriginal": None 
            },
            season_return.append(season_payload)
            self.episodios = 0
            for episode in season['episodes']:
                duration = self.get_duration(episode)
                #deeplink = self.get_deeplink(episode, 'episode', season)
                image = self.get_image(episode, 'episode')
                episode_payload = { 
                    "PlatformCode": self._platform_code, #Obligatorio 
                    "Id": episode['_id'], #Obligatorio
                    "ParentId": id, #Obligatorio #Unicamente en Episodios
                    "ParentTitle": None, #Unicamente en Episodios 
                    "Episode": episode['number'] if episode['number'] != 0 else None, #Obligatorio #Unicamente en Episodios 
                    "Season": episode['season'], #Obligatorio #Unicamente en Episodios
                    "Title": episode['name'], #Obligatorio o 
                    "OriginalTitle": episode['name'], 
                    "Type": episode['type'], #Obligatorio 
                    "Year": None, #Important! 
                    "Duration": duration,
                    "ExternalIds": None,
                    "Deeplinks": { 
                    "Web": None, #Obligatorio 
                    "Android": None, 
                    "iOS": None, 
                    }, 
                    "Synopsis": episode['description'], 
                    "Image": [image], 
                    "Rating": episode['rating'], #Important! 
                    "Provider": None, 
                    "Genres": [episode['genre']], #Important! 
                    "Directors": None, #Important! 
                    "Availability": None, #Important! 
                    "Download": None, 
                    "IsOriginal": None, #Important! 
                    "IsAdult": None, #Important! 
                    "IsBranded": None, #Important! (ver link explicativo)
                    "Packages": [{'Type':'free-vod'}], #Obligatorio 
                    "Country": None, 
                    "Timestamp": datetime.now().isoformat(), #Obligatorio 
                    "CreatedAt": self._created_at, #Obligatorio
                    }
                self.episodes_payloads.append(episode_payload)
                self.episodios += 1
        ('Temporadas: ' + str(self.totalSeasons))
        print('Episodios: ' + str(self.episodios))
        return season_return

    def movie_payload(self, item):
        deeplink = self.get_deeplink(item, item['type'])
        duration = self.get_duration(item)
        image = self.get_image(item, 'movie')
        print('Movie: ' + item['name'])
        payload = { 
            "PlatformCode": self._platform_code, #Obligatorio 
            "Id": item['_id'], #Obligatorio
            "Title": item['name'], #Obligatorio 
            "CleanTitle": _replace(item['name']), #Obligatorio 
            "OriginalTitle": item['name'], 
            "Type": item['type'], #Obligatorio 
            "Year": None, #Important! 
            "Duration": duration,
            "ExternalIds": None, 
            "Deeplinks": { 
            "Web": deeplink, #Obligatorio 
            "Android": None, 
            "iOS": None, 
            }, 
            "Synopsis": item['summary'], 
            "Image": [image],
            "Rating": item['rating'], #Important! 
            "Provider": None,
            "Genres": [item['genre']], #Important!
            "Cast": None, 
            "Directors": None, #Important! 
            "Availability": None, #Important! 
            "Download": None, 
            "IsOriginal": None, #Important! 
            "IsAdult": None, #Important! 
            "IsBranded": None, #Important! (ver link explicativo)
            "Packages": [{'Type':'free-vod'}],
            "Country": None, 
            "Timestamp": datetime.now().isoformat(), #Obligatorio 
            "CreatedAt": self._created_at, #Obligatorio
            }
        self.payloads.append(payload)

    def get_deeplink(self, item, type, season = None):
        if type == 'movie':
            deeplink = 'https://pluto.tv/es/on-demand/movies/' + str(item['slug']) + '/details'
        elif type == 'serie':
            deeplink = 'https://pluto.tv/es/on-demand/series/' + str(item['slug']) + '/details'
        #elif type == 'episode': #revisar!!!!!!!!!!!!!!!!!!!!
            #deeplink = 'https://pluto.tv/es/on-demand/series/' + str(item['slug']) + '/details/season/' + str(season) + '/episode/' + str(item['slug'])
        elif type == 'season':
            deeplink = 'https://pluto.tv/es/on-demand/series/' + str(item['slug']) + '/details/season/' + str('number')
        return deeplink

    def get_image(self, item, type):
        if type == 'movie':
            image = 'https://images.pluto.tv/series/' + str(item['_id']) + '/poster.jpg'
        elif type == 'serie':
            image = 'https://images.pluto.tv/series/' + str(item['_id']) + '/poster.jpg' 
        elif type == 'episode':
            image = 'https://images.pluto.tv/series/' + str(item['_id']) + '/poster.jpg'
        return image

    def get_duration(self, item):
        duration = int((item['duration']) / 60000)
        return duration