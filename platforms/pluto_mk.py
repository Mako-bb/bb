import time
from pymongo.message import insert
from handle.replace import _replace
from common import config
from handle.mongo import mongo
from updates.upload import Upload
from datetime import datetime
# from time import sleep
# import re

class Pluto_mk():
    """
    """
    def __init__(self, ott_site_uid, ott_site_country, type):
        self.ott_site_uid = ott_site_uid
        self.ott_site_country = ott_site_country
        self._config = config()['ott_sites'][ott_site_uid]
        self._platform_code = self._config['countries'][ott_site_country]
        # self._start_url             = self._config['start_url']
        self._created_at = time.strftime("%Y-%m-%d")
        self.mongo = mongo()
        self.titanPreScraping = config()['mongo']['collections']['prescraping']
        self.titanScraping = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodios = config()['mongo']['collections']['episode']
        self.api_url = self._config['api_url']
        self.season_url = self._config['season_api_url']

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
        self.payloads = []
        self.episodes_payloads = []
        # Listas de contentenido scrapeado:
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
        Upload(self._platform_code, self._created_at, testing=True)
        print("Scraping finalizado")
        self.session.close()

    def serie_payload(self, item):
        deeplink = self.get_deeplink(item, 'serie')
        image = self.get_image(item, 'serie')
        print('Serie: ' + item['name'])
        seasons = self.get_seasons(item['_id'], item['slug'])
        serie_payload = {
            "PlatformCode": self._platform_code, #Obligatorio 
            "Id": item['_id'], #Obligatorio
            "Seasons": seasons,
            "Title": item['name'], #Obligatorio 
            "CleanTitle": _replace(item['name']), #Obligatorio 
            "OriginalTitle": item['name'], 
            "Type": 'serie', #Obligatorio 
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
            "Genres": [item['genre']], #Important!  "Cast": "list", 
            "Directors": None, #Important! 
            "Availability": None, #Important! 
            "Download": None, 
            "IsOriginal": None, #Important! 
            "IsAdult": None, #Important! 
            "IsBranded": None, #Important! (ver link explicativo)
            # "Packages": "Free", #Obligatorio 
            "Packages": [{'Type':'free-vod'}],
            "Country": None, 
            "Timestamp": datetime.now().isoformat(), #Obligatorio 
            "CreatedAt": self._created_at, #Obligatorio
        }
        self.payloads.append(serie_payload)
    
    def get_seasons(self, id, parentTitle):
        season_return = []
        uri = self.season_url + str(id) + '/seasons?deviceType=web' 
        items = self.request(uri)
        seasons = items['seasons']
        synopsis = items['description']
        name = items['name']
        self.totalSeasons = 0
        for season in seasons:
            self.totalSeasons += 1
            deeplink = self.(season, 'season', season['number'], parentTitle)
            season_payload = {
                "Id": None, #Importante
                "Synopsis": synopsis, #Importante
                "Title": name, #Importante, E.J. The Wallking Dead: Season 1
                "Deeplink": deeplink, #Importante
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
                deeplink = self.get_deeplink(episode, 'episode', season, parentTitle)
                image = self.get_image(episode, 'episode')
                episode_payload = { 
                    "PlatformCode": self._platform_code, #Obligatorio 
                    "Id": episode['_id'], #Obligatorio
                    "ParentId": id, #Obligatorio #Unicamente en Episodios
                    "ParentTitle": parentTitle, #Unicamente en Episodios 
                    "Episode": episode['number'] if episode['number'] != 0 else None, #Obligatorio #Unicamente en Episodios 
                    "Season": episode['season'], #Obligatorio #Unicamente en Episodios
                    "Title": episode['name'], #Obligatorio o 
                    "OriginalTitle": episode['name'], 
                    "Type": episode['type'], #Obligatorio 
                    "Year": None, #Important! 
                    "Duration": duration,
                    # "ExternalIds": deeplink,
                    "Deeplinks": { 
                    "Web": deeplink, #Obligatorio 
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
        deeplink = self.get_deeplink(item, 'movie')
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
            "ExternalIds": None,  #No estoy seguro de si es
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
            # "Packages": 'Free', #Obligatorio 
            "Packages": [{'Type':'free-vod'}],
            "Country": None, 
            "Timestamp": datetime.now().isoformat(), #Obligatorio 
            "CreatedAt": self._created_at, #Obligatorio
            }
        self.payloads.append(payload)
        
    def get_image(self, item, type):
        if type == 'movie':
            image = 'https://api.pluto.tv/v3/images/episodes/' + str(item['_id']) + '/poster.jpg'
        elif type == 'serie':
            image = 'https://api.pluto.tv/v3/images/series/' + str(item['_id']) + '/poster.jpg' 
        elif type == 'episode':
            image = 'https://api.pluto.tv/v3/images/episodes/' + str(item['_id']) + '/poster.jpg'
        return image
    
    def get_duration(self, item):
        duration = int((item['duration']) / 60000)
        return duration
        
    def get_deeplink(self, item, type, season = None, parentTitle = None):
        if type == 'movie':
            deeplink = 'https://pluto.tv/on-demand/movies/' + item['slug']
        elif type == 'serie':
            deeplink = 'https://pluto.tv/on-demand/series/' + item['slug']
        elif type == 'episode':
            deeplink = 'https://pluto.tv/on-demand/series/' + parentTitle + '/episode/' + str(item['slug'])
        elif type == 'season':
            deeplink = 'https://pluto.tv/on-demand/series/' + parentTitle + '/season/' + str(season)
        return deeplink

    def request(self, uri):
        response = self.session.get(uri)
        contents = response.json()
        return contents

    def get_contents(self):
        """Método que trae los contenidos en forma de diccionario.

        Returns:
            list: Lista de diccionarios
        """
        content_list = []
        uri = self.api_url
        response = self.request(uri)
        list_categories = response['categories']
        for categories in list_categories:
            content_list += categories['items']

        return content_list
