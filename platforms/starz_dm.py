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
    Pluto es una ott de Estados Unidos que opera en todo el mundo.

    DATOS IMPORTANTES:
    - VPN: Si/No (Recomendación: Usar ExpressVPN).
    - ¿Usa Selenium?: No.
    - ¿Tiene API?: Si. Tiene 2, una general en donde se ven las series y peliculas,
      y otra específica de cada contenido, donde se obtienen los detalles de los mismos.
    - ¿Usa BS4?: No.
    - ¿Cuanto demoró la ultima vez? tiempo + fecha.
    - ¿Cuanto contenidos trajo la ultima vez?:

    OTROS COMENTARIOS:
    ...
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
        self.titanScrapingEpisodes = config()['mongo']['collections']['episode']

        self.url = self._config['url']
        self.api_url = self._config['api_url']


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
        
        for item in all_items:
            if item["contentId"] in self.scraped:
                    print("Ya ingresado")
            else:
                self.scraped.append(item['contentId'])
                self.payloads_list.append(self.payload(item))

                '''
                if item['contentType'] == 'Series with Season':
                    season = self.get_season(item)
                    if episodes['contentId'] in self.scraped_episodes:
                        print('capitulo ya ingresado')
                    else:
                        self.scrapedEpisodes.append(episodes['_id'])
                        self.payloads_episodes_list.append(self.payloads_episode(item, episodes))
                '''


    def payload(self,item_):
        payload = {
            "PlatformCode": str(self._platform_code),
            "Id": str(item_['contentId']),
            "Title": str(item_['title']),
            "CleanTitle": _replace(item_['title']),
            "OriginalTitle": None,
            "Type": str(self.get_type(item_['contentType'])),
            "Year": self.get_release_year(item_),
            "Duration": self.get_duration(item_),
            "ExternalIds": None,
            "Deeplinks": {
            "Web": str(self.get_deeplinks(item_)),
            "Android": None,
            "iOS": None
            },
            "Synopsis": str(item_['logLine']),
            "Image": None,
            "Rating": str(item_['ratingCode']),
            "Provider": None,
            "Genres": self.get_genres(item_),
            "Cast": self.get_cast(item_),
            "Directors": None,
            "Availability": None,
            "Download": None,
            "IsOriginal": None,
            "IsAdult": None,
            "IsBranded": None,
            "Packages": None,
            "Country": None,
            "Timestamp": str(datetime.datetime.now().isoformat()),
            "CreatedAt": str(self._created_at),
            }
        return payload

    def get_genres(self,item):
        genres = []
        for genre in item['genres']:
            genres.append(genre['description'])
        return genres

    def get_release_year(self, item):

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


    def get_cast(self,item):
        pass

    def get_deeplinks(self, item, is_episode=False):
        #Verifica si es pelicula
        if item['contentType'] == 'movie':

            deeplink = self.url + 'movies' + '/' + item['title'].replace(':','').replace(' ','-') + '-' + item['contentId']
        
        #Verifica si es un episodio
        elif item['contentType'] == 'series' and is_episode:
            pass
            #deeplink = self.url + 'series' + '/' + item['title'].replace(':','').replace(' ','-') + '/' + 'season-'+is_episode['seasonNumber'] + '/' + 'episode-'+contador_episodio + '/'+is_episode['contentId']
        
        #Si es serie...:
        else:

            deeplink = self.url + 'series' + '/' + item['title'].replace(':','').replace(' ','-') + '/' + str(item['contentId'])
        
        return deeplink



    def get_duration(self,item, is_episode=False):

        if item['contentType'] == 'Series with Season' and is_episode:
            return int(is_episode['runtine']/60)

        elif item['contentType'] == 'Series with Season':
            return None
        
        else:
            return int(item['runtime']/60)


    def get_type(self, typee):
        if typee == 'Series with Season':
            return 'serie'
        else:
            return typee