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
        self.api_mv_url = self._config['api_mv_url']
        self.api_series_url = self._config['api_series_url']
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
        slug_mv_titles = self.get_mv_slug_titles(self.url_movies)
        slug_series_titles = self.get_series_slug_title(self.url_series)

        for slug in slug_mv_titles:
            movie_content = self.get_content(self.url+'movies'+'/', slug)
            self.payloads_list.append(self.payload(movie_content[0], slug))

        for slug_sr in slug_series_titles:
            serie_content = self.get_content(self.url, slug_sr, is_serie=True)
            self.payloads_list.append(self.payload(serie_content[0], slug_sr))


    def get_content(self, url, slug, is_serie=False):
        '''
        Accedo a la url de cada movie, de ahí obtengo el "productId" de cada pelicula
        para scrapear directamente desde la API.
        '''

        req = self.get_req(url, is_slug=slug)
        soup_mv = BeautifulSoup(req.text, 'html.parser')
        container_description = soup_mv.find("noscript", id="react-data")

        data = json.loads(container_description['data-state'])
        try:
            data_id = data['bands'][1]['data']['infoSlice']['streamingId']['id']#Obtengo "streamingId", que es el mismo que "ProductId".
        except:
            print('No hay información')
        

        self.session.close()
        try:
            if is_serie:
                response_content = self.get_req(self.api_series_url, is_api=data_id)
                dictionary = response_content.json()
            else:
                response_content = self.get_req(self.api_mv_url, is_api=data_id)
                dictionary = response_content.json()

            return dictionary

        except:
            pass

    def payload(self,content, slug):
        payload = {
            "PlatformCode": str(self._platform_code),
            "Id": str(content['productId']),
            "Seasons": None,
            "Crew": self.get_crew(content),
            "Title": self.get_title(content),
            "CleanTitle": _replace(self.get_title(content)),
            "OriginalTitle": None,
            "Type": str(content['categories'][0]),
            "Year": self.get_year(content),
            "Duration": self.get_duration(content),
            "ExternalIds": None,
            "Deeplinks": {
            "Web": str(self.get_deeplinks(content, slug)),
            "Android": None,
            "iOS": None
            },
            "Synopsis": self.get_synopsis(content, slug),
            "Image": self.get_images(content),
            "Rating": str(content['mpaa']),
            "Provider": None,
            "Genres": self.get_genres(content),
            "Cast": self.get_cast,
            "Directors": self.get_directors(self, content),
            "Availability": None,
            "Download": None,
            "IsOriginal": None,#No encuentro este dato en la API
            "IsAdult": None,
            "IsBranded": None,
            "Packages": self.get_packages(),
            "Country": None,
            "Timestamp": str(datetime.datetime.now().isoformat()),
            "CreatedAt": str(self._created_at),
            }


    def get_directors(self, content):
        directors = []
        for cast in content['castCrew']:
            if cast['role'] == 'Director':
                directors.append(cast['name'])
        return directors

    def get_cast(self, content, is_episode=False):
        cast = []
        if content['categories'][0] == 'movies':
            for cast in content['castCrew']:
                if cast['role'] == 'Writer' or cast['role'] == 'Producer':
                    pass
                else:
                    cast.append(cast['name'])
        elif content['categories'][0] == 'series':
            None

    def get_genres(self, content, is_episode=False):
        genres = []
        for genre in content['genre']:
            genres.append(genre['name'])

        return genres

    def get_images(self, content, slug, is_episode=False):
        if content['categories'][0] == 'movies':

            request = self.get_req(self.url + 'movies' + '/', is_slug=slug)
            soupp = BeautifulSoup(request.text, 'html.parser')
            content_image = soupp.find('div', {'class':'components/HeroImage--heroImageContainer'})
            image = content_image.find('img')['src']
            if 'content/dam' in image:
                image = 'https://www.hbo.com'+ image
            else:
                pass
            self.session.close()
            return image

        elif content['categories'][0] == 'series':

            request = self.get_req(self.url, is_slug=slug)
            soupp = BeautifulSoup(request.text, 'html.parser')
            content_image = soupp.find('div', {'class':'components/HeroImage--heroImageContainer'})
            image = content_image.find('img')['src']
            if 'content/dam' in image:
                image = 'https://www.hbo.com'+ image
            else:
                pass
            self.session.close()
            return image

    def get_synopsis(self, content, slug, is_episode=False):
        if content['categories'][0] == 'movies':
            synop = _replace(content['summary'])
            return synop

        elif content['categories'][0] == 'series':
            request = self.get_req(self.url, is_slug=slug)

            soupp = BeautifulSoup(request.text, 'html.parser')
            description_container = soupp.find('div', {'class':'cf w-100 components/Band--band'})
            descriptionn = description_container.find('p').text
            self.session.close()
            return descriptionn


    def get_deeplinks(self,content, slug, is_episode=False):

        if content['categories'][0] == 'movies':
            deeplink = self.url + content['pageUrl']
            return deeplink

        elif content['categories'][0] == 'series':
            deeplink = self.url + slug

            return slug


    def get_year(self,content, is_episode=False):

        if content['categories'][0] == 'movies':
            date = content['publishDate']
            date.split('-')

            return int(date[0])

        elif content['categories'][0] == 'series':
            return None

    def get_duration(self,content, is_episode=False):
        if content['categories'][0] == 'movies':
            return content['duration']
        elif content['categories'][0] == 'series':
            return None


    def get_title(self, content, is_episode=False):
        if content['categories'][0] == 'movies':
            title = content['title']
            return title

        elif content['categories'][0] == 'series':
            serie_title = content['series']['title']
            return serie_title

    def get_mv_slug_titles(self, url_mv, is_episode=False):
        '''
        Devuelve una lista con todos los titulos a scrapear en formato slug.
        '''
        slug_list = []

        req = self.get_req(url_mv)#conexión a la url de movies

        soup = BeautifulSoup(req.text, 'html.parser')
        container = soup.find('div', {'class':'components/MovieGrid--container'})

        mv_contents = container.find_all('div', {'class':'modules/cards/CatalogCard--container modules/cards/MovieCatalogCard--container modules/cards/CatalogCard--notIE modules/cards/CatalogCard--desktop'})

        for content in mv_contents:#Peliculas
            '''
            Obtiene el título, lo devuelve en formato slug y se hace un append a "slug_list".
            '''
            title = content.find('p', class_= 'modules/cards/CatalogCard--title').text
            slug_title = self.dep_slug_title(title).lower()
            slug_list.append(slug_title)
        
        self.session.close()
        return slug_list

    def get_series_slug_title(self, url_series):
        '''
        Devuelve una lista con todos los titulos a scrapear en formato slug.
        '''
        slug_list = []

        req = self.get_req(url_series)#conexión a la url de series

        soup = BeautifulSoup(req.text, 'html.parser')
        container = soup.find('div', {'class':'components/MovieGrid--container'})

        series_contents = container.find_all('div', {'class':'modules/cards/CatalogCard--container modules/cards/SamplingCatalogCard--container modules/cards/CatalogCard--notIE modules/cards/CatalogCard--desktop'})
        slug_list = []
        for serie in series_contents:
            '''
            Obtiene el título, lo devuelve en formato slug y se hace un append a "slug_list".
            '''
            title = serie.find('p', class_= 'modules/cards/CatalogCard--title').text
            slug_title = self.dep_slug_title(title).lower()
            slug_list.append(slug_title)


        self.session.close()

        return slug_list
            


    def get_req(self,url, is_slug=False, is_api=False, is_serie=False):
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
            elif is_api:
                try:
                    request = self.session.get(url.format(is_api), timeout=requestsTimeout)
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
        '''
        Corrijo casos especiales...
        '''
        if '&' in title:
            title = title.replace('&','and')

        elif title[len(title)-1] == ')':
            title = title.replace(')','',title.count(")"))
        
        elif title[len(title)-1] == "'":
            title = title.replace("'",'',title.count("'"))

        elif title[len(title)-1] == ".":
            title = title.replace(".",'',title.count("."))
        '''
        Correjidos los casos especiales, reemplazo espacios y caracteres especiales por "-" usando regex.
        '''
        slug = re.sub(r'[^\w]', '-', title)

        if slug[0] == '-':
            slug = slug.replace('-','',1)#Me aseguro de no dejar ningun guión al principio
        
        elif slug[len(slug)-1] == '-':
            slug = slug.replace('-','',slug.count('-'))#Me aseguro de no dejar ningun guión al final

        elif '--' in slug:
            while '--' in slug:
                slug = slug.replace('--','-')

        return slug