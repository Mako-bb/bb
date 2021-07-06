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
# import re

class HboDM():
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
        self.url = self._config['url']
        self.url_movies = self._config['url_movies']
        self.url_series = self._config['url_series']

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
        slug_movies = self.get_slug_mv_titles(self.url_movies)
        self.rebotaron = []
        for slug in slug_movies:
            movie_content = self.get_mv_content(self.url+'movies'+'/', slug)


    def get_mv_content(self, url, slug):
        req_mov = self.get_req(url, is_slug=slug)
        soup_mv = BeautifulSoup(req_mov.text, 'html.parser')
        container_1 = soup_mv.find('div', {'class':'cf w-100 components/Band--band', 'data-bi-context':'{"band":"Text"}'})
        try:
            descriptionn = container_1.find('p')
            print(descriptionn.text)
        except:
            self.rebotaron.append(slug)
            print('rebotó')




    def get_slug_mv_titles(self, url):
        req = self.get_req(url)#conexión a la url

        soup = BeautifulSoup(req.text, 'html.parser')
        container = soup.find('div', {'class':'components/MovieGrid--container'})

        slug_list = []

        contents = container.find_all('div', {'class':'modules/cards/CatalogCard--container modules/cards/MovieCatalogCard--container modules/cards/CatalogCard--notIE modules/cards/CatalogCard--desktop'})

        for content in contents:
            #Obtengo título, lo depuro y lo convierto en slug.
            title = content.find('p', class_= 'modules/cards/CatalogCard--title').text
            slug_title = self.dep_slug_title(title).lower()
            slug_list.append(slug_title)

        return slug_list
            


    def get_req(self,url, is_slug=False):
        '''
        Método para hacer una petición
        '''
        requestsTimeout = 5
        while True:
            if is_slug:
                try:
                    request = self.session.get(url+is_slug, timeout=requestsTimeout)
                    return request
                except requests.exceptions.ConnectionError:
                    print("Connection Error, Retrying")
                    time.sleep(requestsTimeout)
                    continue
                except requests.exceptions.RequestException:
                    print('Waiting...')
                    time.sleep(requestsTimeout)
                    continue
            else:
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


    def dep_slug_title(self, title):

        if '&' in title:
            title = title.replace('&','and')
        elif ')' in title:
            title=title.replace(')','')

        slug = re.sub(r'[^\w]', '-', title)

        if slug[0] == '-':
            slug = slug.replace('-','',1)

        elif '--' in slug:
            while '--' in slug:
                slug = slug.replace('--','-')

        return slug