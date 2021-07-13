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

class StarzFEV():
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
        self.scraped = self.query_field(self.titanScraping, field='Id')
        self.scraped_episodes = self.query_field(self.titanScrapingEpisodios, field='Id')
        print(f"{self.titanScraping} {len(self.scraped)}")
        print(f"{self.titanScrapingEpisodios} {len(self.scraped_episodes)}")
        contents = self.get_contents()
        for n, content in enumerate(contents):
            print(f"\n----- Progreso ({n}/{len(contents)}) -----\n")            
            if content['contentId'] in self.scraped:
                # Que no avance, el contentId está repetido.
                print(content['title'] + ' ya esta scrapeado!')
                continue
            else:   
                self.scraped.append(content['contentId'])
                if (content['contentType']) == 'Movie':
                    self.movie_payload(content)
                elif (content['contentType']) == 'Series with Season':
                    self.serie_payload(content)
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
        """Metodo que hace reques a la api de StarZ y devuelve un diccionario con metadata en formato json"""
        print("\nObteniendo contenidos...\n")
        contents_byid = [] # Contenidos a devolver.
        response = self.request(self.api_url)
        api_contents = response.json()        
        blocks = api_contents['blocks']
        contenidos_api = blocks[-1] #nos quedamos con el último elemento de la lista que es el de los contenidos
        contents_dic = contenidos_api['playContentsById']
        print(contents_dic)

        for clave, valor in contents_dic.items():
            contents_byid.append(valor)
        return contents_byid
        
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

    def movie_payload(self, playContentsById):

        print('Movie: ' + playContentsById['title'])
        payload = { 
            "PlatformCode": self._platform_code, #Obligatorio 
            "Id": playContentsById['contentId'], #Obligatorio
            "Title": playContentsById['title'], #Obligatorio 
            "CleanTitle": _replace(playContentsById['title']), #Obligatorio 
            "OriginalTitle": playContentsById['title'], 
            "Type": playContentsById['contentType'], #Obligatorio 
            "Year": playContentsById['releaseYear'], #Important! 
            "Duration": playContentsById['runtime'],
            "ExternalIds": None, 
            "Deeplinks": { 
            "Web": None, #Obligatorio 
            "Android": None, 
            "iOS": None, 
            }, 
            "Synopsis": None, 
            "Image": None,
            "Rating": None, #Important! 
            "Provider": None,
            "Genres": None, #Important!
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

    def serie_payload(self, playContentsById):

        serie_payload = {
            "PlatformCode": self._platform_code, #Obligatorio 
            "Id":  playContentsById['contentId'], #Obligatorio
            "Seasons": None,
            "Title": playContentsById['title'], #Obligatorio 
            "CleanTitle": _replace(playContentsById['title']), #Obligatorio 
            "OriginalTitle": playContentsById['title'], 
            "Type": playContentsById['contentType'], #Obligatorio 
            "Year": playContentsById['minReleaseYear'], #Important! 
            "Duration": playContentsById['minReleaseYear'], 
            "ExternalIds": None, 
            "Deeplinks": { 
            "Web": None, #Obligatorio 
            "Android": None, 
            "iOS": None, 
            }, 
            "Synopsis": None, 
            "Image": None, 
            "Rating": None, #Important! 
            "Provider": None, 
            "Genres": None, #Important!  
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
            "CreatedAt": self._created_at #Obligatorio
            }
        self.payloads.append(serie_payload)

    def get_uri (self, playContentsById):
        uri = 'https://playdata.starz.com/metadata-service/play/partner/Web_ES/v8/content?lang=es-ES&contentIds=' + str(playContentsById['contentId']) + '&includes=title,logLine,contentType,contentId,ratingName,properCaseTitle,topContentId,releaseYear,runtime,images,credits,episodeCount,seasonNumber,childContent,order'
        return uri