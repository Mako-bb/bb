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

class PlutoDM():
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

        #self.api_url = self._config['api_url']

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
        url = 'https://service-vod.clusters.pluto.tv/v3/vod/categories?includeItems=true&includeCategoryFields=imageFeatured%2CiconPng&itemOffset=10000&advertisingId=&appName=web&appVersion=5.17.1-be7b5e79fc7cad022e22627cbb64a390ca9429c7&app_name=web&clientDeviceType=0&clientID=5ba90432-9a1d-46d1-8f93-b54afe54cd1e&clientModelNumber=na&country=AR&deviceDNT=false&deviceId=5ba90432-9a1d-46d1-8f93-b54afe54cd1e&deviceLat=-34.5106&deviceLon=-58.7536&deviceMake=Microsoft%2BEdge&deviceModel=web&deviceType=web&deviceVersion=91.0.864.54&marketingRegion=VE&serverSideAds=true'
        response = self.session.get(url)
        dictionary = response.json()

        #Método que toma el "slug title" y lo transforma para obtener el originalTitle(revisar)
        def original_title(films):

            lista_slug = films['slug'].split('-')
            no_deseados = ['1','2','ptv1','ptv3','latam']
            for indeseado in no_deseados:
                while indeseado in lista_slug:
	                lista_slug.pop()
            lista_slug = ' '.join([str(elem) for elem in lista_slug])
            return lista_slug

        #Filtra lo que necesito de cada pelicula
        def payloads_movies(films):
            payload = {
                "PlatformCode": self._platform_code,
                "Id": films['_id'],
                "Title": films['name'],
                "OriginalTitle": original_title(films),
                "Type": films['type'],
                "Year": "Null", #No encuentro esta info
                "Duration": int(films['duration']/60000),
                "ExternalIds": "Null",
                "Deeplinks": {
                #No encuentro este dato, de momento va "Null"
                "Web": "Null",
                "Android": "Null",
                "iOS": "Null"
                },
                "Synopsis": films['description'],
                "Image": films['covers'][0]['url'],
                "Rating": films['rating'],
                "Provider": "Null",
                "Genres": [films['genre']],
                "Cast": "Null",#No encuentro esa info
                "Directors": "Null",#No encuentro esa info
                "Availability": "Null",#No encuentro esa info
                "Download": "Null",#No encuentro esa info
                "IsOriginal": "Null",#No encuentro esa info
                "IsAdult": "Null",# Esto está hardcodedado, no se si hay que implementar algun algoritmo
                "IsBranded": "Null",# No encuentro eta info
                "Packages": [{'Type': 'free-vod'}],
                "Country": ["ar"],#Revisar
                "Timestamp": datetime.datetime.now().isoformat(),
                "CreatedAt": self._created_at,
            }
            return payload

        #Filtra lo que necesito de cada serie
        def payloads_series(films):
            payload_series= {
                "PlatformCode": self._platform_code,
                "Id": films['_id'],
                "Seasons": len(films['seasonsNumbers']),
                "Title": films['name'],
                "CleanTitle": _replace(films['name']),
                "OriginalTitle": original_title(films),
                "Type": films['type'],
                "Year": None,
                "ExternalIds": "Null",
                "Deeplinks": {
                "Web": "Null",#No encuentro este dato, de momento va "Null"
                "Android": "Null",
                "iOS": "Null"
                },
                "Synopsis": films['description'],
                "Image": films['covers'][0]['url'],
                "Rating": films['rating'],
                "Provider": "Null",
                "Genres": [films['genre']],
                "Cast": "Null",#No encuentro esa info
                "Directors": "Null",#No encuentro esa info
                "Availability": "Null",#No encuentro esa info
                "Download": "Null",#No encuentro esa info
                "IsOriginal": "Null",#No encuentro esa info
                "IsAdult": "Null",# Esto está hardcodedado, no se si hay que implementar algun algoritmo
                "IsBranded": "Null",# No encuentro eta info
                "Packages": [{'Type': 'free-vod'}],
                "Country": ["ar"],#Revisar
                "Timestamp": datetime.datetime.now().isoformat(),
                "CreatedAt": self._created_at,
                }

            return payload_series


        #Filtra la metadata de todos los episodios de una serie en especifico y los inserta en la db
        def episodes(films):
            #URL de api de series con el id de la serie correspondiente
            url_series = "https://service-vod.clusters.pluto.tv/v3/vod/series/{}/seasons?advertisingId=&appName=web&appVersion=5.17.1-be7b5e79fc7cad022e22627cbb64a390ca9429c7&app_name=web&clientDeviceType=0&clientID=c636f54d-adcd-4b30-b2cd-02cf58d954f4&clientModelNumber=na&country=AR&deviceDNT=false&deviceId=c636f54d-adcd-4b30-b2cd-02cf58d954f4&deviceLat=-34.6022&deviceLon=-58.3845&deviceMake=Chrome&deviceModel=web&deviceType=web&deviceVersion=91.0.4472.114&marketingRegion=VE&serverSideAds=true&sessionID=be85a5ba-d44b-11eb-8a41-0242ac110002&sid=be85a5ba-d44b-11eb-8a41-0242ac110002&userId=&attributeV4=foo".format(films['_id'])
            response2 = self.session.get(url_series)
            dictionary_seasons = response2.json()
            epi_list = []#Creo lista vacía
            seasons_list = dictionary_seasons['seasons']

            for seasons in seasons_list:
                epi_list.append(seasons['episodes'])

            contador_episodio = 0

            for seas in epi_list:
                for episodess in seas:
                    contador_episodio += 1
                    payloads_episodios = {#Falta cargarle mas datos a este payload
                    "PlatformCode": self._platform_code,
                    "Id": str(episodess['_id']),
                    "ParentId": films['_id'],
                    "ParentTitle": str(films['name']),
                    "Episode": int(contador_episodio),
                    "Season": int(episodess['season'])
                    }
                    #Si el episodio ya se encuentra en la DB, no lo inserta
                    if self.mongo.search("titanScrapingepisodes",payloads_episodios):#Devuelve un booleano
                        print('Este episode ya esta cargado en la db')
                    else:
                        self.mongo.insert("titanScrapingepisodes",payloads_episodios)

        items_list = []#Creo lista
        categories_list = dictionary['categories']

        for categories in categories_list:
            #append al array con todas las peliculas/Series de cada categoria
            items_list.append(categories['items'])


        for cat in items_list:
            for films in cat:
                '''
                #Verifica si el contenido es pelicula o serie, y en ambos casos
                verifica que dicho contenido no este en la DB antes de insertar
                '''
                if films['type'] == 'movie':
                    if self.mongo.search("titanScraping",payloads_movies(films)):#Devuelve un booleano
                        print('esta movie ya esya en la db')
                    else:
                        #Si el contenido es una pelicula, la inserta en la DB
                        self.mongo.insert("titanScraping",payloads_movies(films))
                elif films['type'] == 'series':
                    if self.mongo.search("titanScraping",payloads_series(films)):#Devuelve un booleano
                        print('esta serie ya esta en la db')
                    else:
                        #Si el contenido es una serie, inserta la serie y los capitulos correspondientes.
                        self.mongo.insert("titanScraping",payloads_series(films))
                        episodes(films)