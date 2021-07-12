import json
import re
from handle.replace_regex import clean_title
import time
from typing import Container
import requests
from bs4                    import BeautifulSoup, element
from handle.replace         import _replace
from common                 import config
from handle.mongo           import mongo
from updates.upload         import Upload
from handle.payload         import Payload
from handle.datamanager     import Datamanager
import datetime

class VicePFD():
    """
    ... es una ott de Estados Unidos que opera en todo el mundo.

    DATOS IMPORTANTES:
    - VPN: Si/No (Recomendación: Usar ExpressVPN).
    - ¿Usa Selenium?: No.
    - ¿Tiene API?: Si. Tiene 2, una general en donde se ven las series y peliculas,
      y otra específica de las series, donde se obtienen los cap. de las mismas.
    - ¿Usa BS4?: No.
    - ¿Cuanto demoró la ultima vez? 184.65199732780457 segundos, el 6/7/2021.
    - ¿Cuanto contenidos trajo la ultima vez?:
        -Fecha: 29/6/2021
        -Episodios: 19.990
        -Peliculas/series: 1.524

    OTROS COMENTARIOS:
    ...
    """

    def __init__(self, ott_site_uid, ott_site_country, type):

        self.initial_time = time.time()

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
        '''
        Declaro las url a utilizar.
        '''
        self.url = self._config['url']
        self.api_shows_url = self._config['api_shows']#Api de shows/series.
        self.api_videos_url = self._config['api_videos']#Api de videos.

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

        self.payloads_list = []#Payloads de shows
        self.videos_payloads = []#Payloads de videos

        pages_shows = self.get_content(self.api_shows_url)

        self.shows_validated = []#Shows validados!

        for page in pages_shows:#Por cada pagina en la lista
            for show in page:#Por cada show en cada página
                '''
                Si el show está validado, lo inserta en la lista "shows_validated".
                '''
                verifica = self.show_verification(show)
                if verifica:
                    self.shows_validated.append(verifica)


    def get_content(self, url):
        '''
        La API de shows es muy extensa, por este motivo está dividida en páginas.
        Este método recorre esas páginas(hasta que devuelva NULL)
        y va insertando los shows en una lista.

        Devuelve una lista con cada página y sus respectivos shows.
        '''
        contador = 1#Contador de páginas
        dictionary_list = []

        while True:
                request = self.get_req(url.format(contador))
                dictionary = request.json()

                if dictionary:#Si dictionary "obtuvo" contenido, lo inserta en la lista
                    dictionary_list.append(dictionary)
                    contador += 1
                else:
                    print('Todos los shows cargados en "Dictionary_list" ')
                    break
        return dictionary_list


    def get_req(self, url):
        '''
        Método para hacer una petición
        '''
        requestsTimeout = 5
        while True:
            try:
                request = self.session.get(url, timeout=requestsTimeout)
                return request
            except requests.exceptions.ConnectionError:
                print("Connection Error, Retrying")
                time.sleep(requestsTimeout)
                continue
            except requests.exceptions.RequestException:
                print('Waiting...')
                time.sleep(requestsTimeout)
                continue


    def show_verification(self, show):
        '''
        Este método valida si los shows tienen contenido. Si es así
        los devuelve.
        '''
        shows_validated = []#Shows validados!

        '''Hago request a la API de videos con la id del show, si devuelve NULL no pasa la verificación.'''
        request_videos = self.get_req(self.api_videos_url.format(show['id']))#Request
        dictionary_videos = request_videos.json()

        if dictionary_videos:
            return show
        else:
            print('Este show no pasó la verificación')
            return False
