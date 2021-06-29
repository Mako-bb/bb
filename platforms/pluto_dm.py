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
    Pluto es una ott de Estados Unidos que opera en todo el mundo.

    DATOS IMPORTANTES:
    - VPN: Si/No (Recomendación: Usar ExpressVPN).
    - ¿Usa Selenium?: No.
    - ¿Tiene API?: Si.
    - ¿Usa BS4?: No.
    - ¿Cuanto demoró la ultima vez? tiempo + fecha.
    - ¿Cuanto contenidos trajo la ultima vez? cantidad + fecha.

    OTROS COMENTARIOS:
    Con esta plataforma pasa lo siguiente...
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
        self.season_api_url = self._config['season_api_url']


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
        self.scrapedEpisodes = self.query_field(self.titanScrapingEpisodes, field='Id')
        
        self.payloads_list = []
        self.payloads_episodes_list = []

        contents = self.get_content(self.api_url)

        for content in contents:
            for films in content:
                '''
                verifica que el contenido contenido no este en la DB antes de insertar,
                si el contenido no esá en la DB hace un append a la lista "self.scraped"
                con la id de este contenido nuevo, rellena el payload y lo almacena en "payloads_list".

                Si el contenido es una serie, se busca insertar los episodios en la db.
                Se sigue el mismo proceso explicado anteriormente
                '''
                if films["_id"] in self.scraped:
                    print("Ya ingresado")
                else:
                    self.scraped.append(films['_id'])
                    self.payloads_list.append(self.payloads(films))

                    if films['type'] == 'series':

                        self.contador_episodio = 0

                        for seas in self.get_content_season(films,self.season_api_url):
                            for episodes in seas:
                                if episodes['_id'] in self.scrapedEpisodes: 
                                    print('capitulo ya ingresado')
                                else:

                                    self.contador_episodio += 1
                                    self.scrapedEpisodes.append(episodes['_id'])
                                    self.payloads_episodes_list.append(self.payloads_episode(films, episodes, self.contador_episodio))

        '''
        Si a las listas "payloads_list" y "payloads_episodes_list" tienen contenido,
        entonces se insertan a la DB
        '''
        if self.payloads_list: #Devuelve booleano, si no se insertó nada en la lista devuelve False
            self.mongo.insertMany(self.titanScraping, self.payloads_list)
        if self.payloads_episodes_list:#Devuelve booleano, si no se insertó nada en la lista devuelve False
            self.mongo.insertMany(self.titanScrapingEpisodes, self.payloads_episodes_list)
        
        self.session.close()
        Upload(self._platform_code, self._created_at, testing=True)



    def get_content(self,url):
        response = self.session.get(url)#conexión a la url
        self.contents = []
        dictionary = response.json()#ordeno la información obtenida en formato JSON
        categories_list = dictionary['categories']

        for categories in categories_list:
        #append al array con todas las peliculas/series de cada categoria
            self.contents.append(categories['items'])
        
        return self.contents


    #Filtra lo que necesito de cada pelicula
    def payloads(self,content):
        payload = {
            "PlatformCode": str(self._platform_code),
            "Id": str(content['_id']),
            "Title": str(content['name']),
            "CleanTitle": _replace(content['name']),
            "OriginalTitle": self.original_title(content),
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
            "Genres": [content['genre']],
            "Cast": None,
            "Directors": None,
            "Availability": None,
            "Download": False,
            "IsOriginal": False,
            "IsAdult": False,
            "IsBranded": False,
            "Packages": [{'Type': 'free-vod'}],
            "Country": [self.ott_site_country],
            "Timestamp": str(datetime.datetime.now().isoformat()),
            "CreatedAt": str(self._created_at),
            }
        
        return payload


    def get_images(self, content_, is_episode=False):
        images = []
        if is_episode:
            for cover in is_episode['covers']:
                images.append(cover['url'])
        else:
            for cover in content_['covers']:
                images.append(cover['url'])

        return images


    def get_type(self, typee):
        if typee == 'series':
            return 'serie'
        else:
            return typee


    def get_deeplinks(self, content, is_episode=False):
        if content['type'] == 'movie':

            deeplink = self.url + 'movies' + '/' + content['slug'] + '/' + 'details' + '/'

        elif content['type'] == 'series' and is_episode:

            deeplink = self.url + 'series' + '/' + content['slug'] + '/' + 'season' + '/' + str(is_episode['season']) + '/' + 'episode' + '/' + is_episode['slug'] + '/'

        else:

            deeplink = self.url + 'series' + '/' + content['slug'] + '/' + 'details' + '/'

        return deeplink


    def get_duration(self,content):

        if content['type'] == 'series':
            return None
        else:
            return int(content['duration']/60000)


    #Método que toma el "slug title" y lo transforma para obtener el originalTitle(revisar)
    def original_title(self,films):

        lista_slug = films['slug'].split('-')
        no_deseados = ['1','2','ptv1','ptv3','latam']
        for indeseado in no_deseados:
            while indeseado in lista_slug:
	            lista_slug.pop()
        lista_slug = ' '.join([str(elem) for elem in lista_slug])

        return lista_slug
    
    '''
    Metodo que accede a la api de las series con la id de la serie que se esta scrapeando
    y obtiene una lista con las temporadas de cada una.

    args:
        -content(el contenido de la serie)
        -url(la url de la api de series)

    return:
        -Devuelve una lista con las temporadas de la serie
    '''
    def get_content_season(self,content,url):
        response2 = self.session.get(url.format(content['_id']))
        dictionary_seasons = response2.json()
        seasons_list = dictionary_seasons['seasons']

        self.seas_list = []#Creo lista vacía

        for seasons in seasons_list:
            self.seas_list.append(seasons['episodes'])

        return self.seas_list

    
    def payloads_episode(self, content, episodes, contador_episodio):
        payload_epi = {
            "PlatformCode": str(self._platform_code),
            "Id": str(episodes['_id']),
            "ParentId": str(content['_id']),
            "ParentTitle": str(content['name']),
            "Episode": int(contador_episodio),
            "Season": int(episodes['season']),
            "Crew": None,
            "Title": str(episodes['name']),
            "OriginalTitle": None,
            "Year": None,
            "Duration": int(episodes['duration']/60000),
            "ExternalIds": None,
            "Deeplinks": {
            "Web": str(self.get_deeplinks(content, is_episode=episodes)),
            "Android": None,
            "iOS": None
            },
            "Synopsis": str(episodes['description']),
            "Image": self.get_images(content, is_episode=episodes),
            "Rating": str(episodes['rating']),
            "Provider": None,
            "Genres": [episodes['genre']],
            "Cast": None,
            "Directors": None,
            "Availability": None,
            "Download": False,
            "IsOriginal": False,
            "IsAdult": False,
            "IsBranded": False,
            "Packages": [{'Type': 'free-vod'}],
            "Country": [self.ott_site_country],
            "Timestamp": str(datetime.datetime.now().isoformat()),
            "CreatedAt": str(self._created_at),
            }

        return payload_epi
