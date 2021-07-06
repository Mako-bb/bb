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

                        for seas in self.get_content_season(films,self.season_api_url):
                            for episodes in seas:
                                if episodes['_id'] in self.scrapedEpisodes: 
                                    print('capitulo ya ingresado')
                                else:

                                    self.scrapedEpisodes.append(episodes['_id'])
                                    self.payloads_episodes_list.append(self.payloads_episode(films, episodes))

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

        end_time = time.time()
        time_execute = end_time - self.initial_time
        print('el tiempo de ejecución es de: '+ str(time_execute) + ' segundos.')

    '''
    Hace un request a la api_url "general" de las series/peliculas y devuelve
    una lista con todas las categorías
    
    '''
    def get_content(self,url):
        response = self.session.get(url)#conexión a la url
        self.contents = []
        dictionary = response.json()#ordeno la información obtenida en formato JSON
        categories_list = dictionary['categories']

        for categories in categories_list:
        #append al array con todas las peliculas/series de cada categoria
            self.contents.append(categories['items'])
        
        return self.contents

    #Payload de peliculas y series
    def payloads(self,content):
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
            "Timestamp": str(datetime.datetime.now().isoformat()),
            "CreatedAt": str(self._created_at),
            }
        
        return payload

    def get_title(self, content, is_episode=False):

        if content['type'] == 'series' or content['type'] == 'movie':
            if ' (' in content['name']:
                name_list = content['name'].split(' (')
                name_list.pop()
                name_list = ' '.join([str(elem) for elem in name_list])
                print(name_list)

                return name_list
            else:
                return content['name']
            
        elif is_episode:
            if ' (' in is_episode['name']:
                name_list = is_episode['name'].split(' (')
                name_list.pop()
                name_list = ' '.join([str(elem) for elem in name_list])
                print(name_list)

                return name_list
            else:
                return content['name']

        else:
            return content['genre']


    def get_genres(self, content, is_episode=False):

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

    def get_images(self, content_, is_episode=False):
        images = []
        if is_episode:
            for cover in is_episode['covers']:
                if '/poster.jpg' in cover['url']:
                    pass
                else:
                    images.append(cover['url'])
        else:
            for cover in content_['covers']:
                if '/poster.jpg' in cover['url']:
                    pass
                else:
                    images.append(cover['url'])

        return images


    def get_type(self, typee):
        if typee == 'series':
            return 'serie'
        else:
            return typee


    def get_deeplinks(self, content, is_episode=False):
        #Verifica si es pelicula
        if content['type'] == 'movie':

            deeplink = self.url + 'movies' + '/' + content['slug']
        #Verifica si es un episodio
        elif content['type'] == 'series' and is_episode:

            deeplink = self.url + 'series' + '/' + content['slug'] + '/' + 'seasons' + '/' + str(is_episode['season']) + '/' + 'episode' + '/' + is_episode['slug']
        #Si es serie...:
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


    def original_title(self,films):
        '''
        Método que toma el "slug title" y lo transforma para obtener el originalTitle,
        de momento no funciona correctamente, así que dejo "None"
        '''
        lista_slug = films['slug'].split('-')
        no_deseados = ['1','2','ptv1','ptv3','latam']
        for indeseado in no_deseados:
            while indeseado in lista_slug:
	            lista_slug.pop()
        lista_slug = ' '.join([str(elem) for elem in lista_slug])

        return lista_slug
    

    '''
    Metodo que accede a la api de las series con la id de la serie que se esta scrapeando.
    Devuelve una lista con las temporadas de la misma.

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
    

    def get_episodes(self,episode__):
        if episode__['number'] == 0:
            numb = int(1)
        else:
            numb = int(episode__['number'])
        
        return numb


    #Payload de episodios
    def payloads_episode(self, content, episodes):
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
            "Image": self.get_images(content, is_episode=episodes),
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
            "Timestamp": str(datetime.datetime.now().isoformat()),
            "CreatedAt": str(self._created_at),
            }

        return payload_epi
