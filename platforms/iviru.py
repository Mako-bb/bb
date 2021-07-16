from os import replace
import time
from pymongo.common import CONNECT_TIMEOUT, clean_node
import requests
from yaml.tokens import FlowMappingStartToken
from handle.replace import _replace
from common import config
from handle.mongo import mongo
from updates.upload import Upload
from handle.payload import Payload
from handle.datamanager import Datamanager
from bs4 import BeautifulSoup
import datetime
# from time import sleep
import re
start_time = time.time()


class Iviruu():
    def __init__(self, ott_site_uid, ott_site_country, type):
        """
        Iviru es una ott de Rusia.

        DATOS IMPORTANTES:
        - VPN: No
        - ¿Usa Selenium?: No.
        - ¿Tiene API?: Si.
        - ¿Usa BS4?: No.
        - ¿Cuanto demoró la ultima vez?. NA
        - ¿Cuanto contenidos trajo la ultima vez? NA.

        OTROS COMENTARIOS:
        """
        self.ott_site_uid = ott_site_uid
        self.ott_site_country = ott_site_country
        self._config = config()['ott_sites'][ott_site_uid]
        self._platform_code = self._config['countries'][ott_site_country]
        # self._start_url             = self._config['start_url']
        self._created_at = time.strftime("%Y-%m-%d")
        self.mongo = mongo()
        self.titanPreScraping = config()['mongo']['collections']['prescraping']
        self.titanScraping = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodes = config(
        )['mongo']['collections']['episode']

        self.api_url = self._config['api_collections_url']
        self.contents_api = self._config['api_contents_url']
        self.categories_api = self._config['api_categories_url']
        self.url = self._config['url']

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
        self.api_url = self.api_url + '?app_version=870&from={}&to={}'
        self.contents_api = self.contents_api +'?id={}&from={}&to={}&withpreorderable=true&app_version=870'
        self.categories_ids=[]
        self.categories_contents=[]
        self.payloads = []
        self.episodes_payloads = []
        self.movies = []
        self.series = []
        self.episodes = []
        self.movies_series_ids = []
        self.episodes_ids = []

        self.get_ids()
        self.get_contents()

        print('------movies&series-------')
        print(len(self.payloads))
        print(len(self.movies))
        print(len(self.series))
        print('------episodes-------')
        print(len(self.episodes))
        print(len(self.episodes_payloads))
        #self.insert_payloads_close(self.payloads, self.episodes_payloads)
        print("--- %s seconds ---" % (time.time() - start_time))

    def get_ids(self):
        self.iterate_all(self.api_url,None,99,self.categories_ids)
        
    def get_contents(self):
        for json in self.categories_ids:
            content_id = json['id']
            self.iterate_all(self.contents_api,content_id,29,self.categories_contents)
            
    def iterate_all(self,url,id,step,to_list):
        json_data = None
        init = 0
        end = init + step
        next_page = True
        while next_page:
            if id:
                api = url.format(id,init,end)
            else:
                api = url.format(init,end)
            response = self.session.get(api)
            json_data = response.json()
            if 'error' in json_data:
                next_page = False
            else:
                json_data = json_data['result']
                self.save_contents(json_data,to_list)
                init = end 
                end = init + step
                if len(json_data) < step:
                    next_page = False

    def save_contents(self,json_result,to_list):
        if len(json_result) > 1:
            for content in json_result:
                self.separate_videos_compilations(content)
                to_list.append(content)
        elif len(json_result) == 1:
            self.separate_videos_compilations(json_result[0])
            to_list.append(json_result[0])
        else:
            pass

        self.get_payloads()

    def separate_videos_compilations(self, content):
        if content['object_type'] == 'compilation':
            if not self.isDuplicate(self.movies_series_ids, content['id']):
                self.series.append(content)
                self.movies_series_ids.append(content['id'])
        else:
            if content['object_type'] == 'video':
                if content['duration_minutes'] > 4:
                    self.separate_episode_movie(content)
                else: pass
            else: pass  

    def separate_episode_movie(self, content):
        content_id = content['id']
        content_api = 'https://api.ivi.ru/mobileapi/videoinfo/v6/?id={}'.format(content_id)
        response = self.session.get(content_api)
        json_data = response.json()
        if 'error' not in json_data :
            content_result = json_data['result']
            if 'episode' in content_result or 'compilation' in content_result:
                if not self.isDuplicate(self.episodes_ids, content_id):
                    self.episodes.append(content_result)
                    self.episodes_ids.append(content_id)
                else: pass
            else:
                if not self.isDuplicate(self.movies_series_ids, content_id):
                    self.movies.append(content_result)
                    self.movies_series_ids.append(content_id)
        else: pass

    def get_payloads(self):
        '''
        '''
        if self.movies:
            print('----creando payloads movies----')
            for movie in self.movies:
                if not self.isDuplicate(self.scraped,movie['id']):
                    payload = self.generic_payload(movie,'movie')
                    self.payloads.append(payload)
                    self.scraped.append(movie['id'])     
        if self.series:
            print('----creando payloads series----')
            for serie in self.series:
                if not self.isDuplicate(self.scraped,serie['id']):
                    payload = self.generic_payload(serie,'serie')
                    self.payloads.append(payload)
                    self.scraped.append(serie['id'])
        if self.episodes:
            print('----creando payloads episodes----')
            for episode in self.episodes:
                if not self.isDuplicate(self.scraped_episodes,episode['id']):
                    payload = self.episode_payload(episode)
                    self.episodes_payloads.append(payload)
                    self.scraped_episodes.append(episode['id'])

    def insert_payloads_close(self, payloads, epi_payloads):
        '''
            El metodo checkea que las listas contengan elementos para ser subidos y corre el Upload en testing.
        '''
        if payloads:
            self.mongo.insertMany(self.titanScraping, payloads)
        if epi_payloads:
            self.mongo.insertMany(self.titanScrapingEpisodes, epi_payloads)
        self.session.close()
        Upload(self._platform_code, self._created_at, testing=True)
    
    def generic_payload(self,content,content_type):
        '''
            Aca voy a validar el argumento content_type si es serie o movie, dependiendo del type
            va a completar los campos segun corresponda en el payload.
        '''
        payload = {
            'PlatformCode': self._platform_code,
            'Id': self.get_id(content),
            'Crew': self.get_crew(content),
            'Title': self.get_title(content),
            'OriginalTitle': self.get_original_title(content),
            'CleanTitle': self.get_clean_title(content),
            'Type': content_type,
            'Year': self.get_year(content),
            'Duration': None,
            'Deeplinks': self.get_Deeplinks(content,None),
            'Synopsis': self.get_synopsis(content),
            'Image': self.get_image(content),
            'Rating': self.get_rating(content),
            'Provider': self.get_provider(content),
            'ExternalIds': self.get_external_ids(content),
            'Genres': self.get_genres(content),
            'Cast': self.get_cast(content),
            'Directors': self.get_directors(content),
            'Availability': self.get_availability(content),
            'Download': self.get_download(content),
            'IsOriginal': self.get_isOriginal(content),
            'IsBranded':self.get_isBranded(content),
            'IsAdult': self.get_isAdult(content),
            "Packages": self.get_package(content),
            'Country': [self.ott_site_country],
            'Timestamp': datetime.datetime.now().isoformat(),
            'CreatedAt': self._created_at,
        }
        if content_type == 'serie':
            seasons = self.season_payload(content)
            payload['Seasons'] = seasons
            payload['Playback'] = None
        else:
            payload['Duration'] = self.get_duration(content)
        return payload
    
    def episode_payload(self,content):
        episode = {
            'PlatformCode':self._platform_code,
            'Id': self.get_id(content),
            'ParentId': self.get_ep_parent_id(content),
            'ParentTitle': self.get_ep_parent_title(content),
            'Episode':self.get_ep_episode_number(content),
            'Season': self.get_ep_season_number(content),
            'Crew': self.get_crew(content),
            'Title':self.get_title(content),
            'OriginalTitle': self.get_original_title(content),
            'Year': self.get_year(content),
            'Duration':self.get_duration(content),
            'ExternalIds': self.get_external_ids(content),
            'Deeplinks':self.get_Deeplinks(content,None),
            'Synopsis':self.get_synopsis(content),
            'Image':self.get_image(content),
            'Rating':self.get_rating(content),
            'Provider':self.get_provider(content),
            'Genres': self.get_genres(content),
            'Cast':self.get_cast(content),
            'Directors':self.get_directors(content),
            'Availability':self.get_availability(content),
            'Download': self.get_download(content),
            'IsOriginal': self.get_isOriginal(content),
            'IsAdult': self.get_isAdult(content),
            'IsBranded':self.get_isBranded(content),
            'Packages': self.get_package(content),
            'Country': [self.ott_site_country],
            'Timestamp': datetime.datetime.now().isoformat(),
            'CreatedAt': self._created_at,
        }
        return episode

    def season_payload(self,content):
        seasons_list=[]
        for key, season in content['seasons_extra_info'].items():
            season_num = int(key) + 1
            season_str = str(season_num)
            s = {
                "Id":season['season_id'], 
                "Synopsis": self.get_seasons_synopsis(content,str(season_str)), 
                "Title": self.get_title(season),
                "Deeplink": self.get_Deeplinks(content,season_str),
                "Number": season_num, 
                "Year": self.get_year(content), 
                "Image": None, 
                "Directors": None, 
                "Cast": None, 
                "Episodes": season['max_episode'], 
                "IsOriginal": None 
            }
            seasons_list.append(s)
        return seasons_list

    def isDuplicate(self, scraped_list, key_search):
        '''
            Metodo para validar elementos duplicados segun valor(key) pasado por parametro en una lista de scrapeados.
        '''
        isDup = False
        if key_search in scraped_list:
            isDup = True
        return isDup

    def get_ep_parent_id(self, content):
        parent_id = None
        try:
            parent_id = content['compilation']
        except:
            pass
        return parent_id
    
    def get_ep_parent_title(self, content):
        parent_title = None
        try:
            parent_title = content['compilation_hru']
        except:
            pass
        return parent_title

    def get_ep_episode_number(self, content):
        ep_number = None
        try:
            ep_number = content['episode']
        except:
            pass
        return ep_number

    def get_ep_season_number(self, content):
        season_number = None
        try:
            season_number = content['season']
        except:
            pass
        return season_number

    def get_original_title(self, content):
        original_title = None
        try:
            if content['orig_title'] != '':
                original_title = content['orig_title']
        except:
            pass
        return original_title

    def get_seasons_synopsis(self,content,season_num):
        synopsis = None
        try:
            descriptions = content['seasons_description']
            if season_num in descriptions:
                synopsis = descriptions[season_num]
        except:
            pass
        return synopsis

    def get_Deeplinks(self, content, season):
        Deeplinks = {
            "Web": None,
            "Android": None,
            "Ios": None,
        }
        if season:
            if content["hru"] != '':
                Deeplinks["Web"] = 'https://www.ivi.tv/watch/{}/season{}'.format(content["hru"],season)
        else: 
            if "share_link" in content:
                Deeplinks["Web"] = content["share_link"]
            else:
                available = content['available_in_countries']
                if available:
                    if content["object_type"] == 'video':
                        Deeplinks["Web"] = 'https://www.ivi.tv/watch/{}'.format(content["id"])
                    else:
                        if content["hru"]:
                            Deeplinks["Web"] = 'https://www.ivi.tv/watch/{}'.format(content["hru"])
                        else:
                            pass
                else:
                    pass
        return Deeplinks

    def get_id(self, content):
        '''
            Paso el id a str porque asi lo pide el payload para hacer el upload
        '''
        try:
            id = str(content["id"])
            return id
        except:
            pass

    def get_title(self, content):
        try:
            title = content["title"]
            return title
        except:
            pass

    def get_clean_title(self, content):
        """
        Metodo para traer los titulos sin caracteres innecesarios.
        Seguramente se va a tener que mejorar una vez hecho el analisis.
        """
        try:
            clean_title = _replace(content["title"])
            return clean_title
        except:
            pass


    def get_year(self, content):
        year = 0
        if content['object_type'] == 'video':
            try:
                year = int(content["year"])
            except:
                pass
        else:
            pass
        if 1870 > year or year > 2022:
            year = None
        else:
            pass
        return year

    def get_duration(self, content):
        try:
            duration = int(content["duration_minutes"])
            return duration
        except:
            pass

    def get_external_ids(self, content):
        """
        Metodo para mostrar las ids externas que entrega.
        En cuanto a ID solo trae la de 'kp', después, de otras páginas
        trae el rating o la fecha de salida.

        Lo paso a lista y el id a str porque es el tipo de dato que pide el payload para hacer el upload
        """
        try:
            external_ids = [{
                "Provider": "Kp",
                "Id": str(content["kp_id"])

            }]
            return external_ids
        except:
            pass

    def get_synopsis(self, content):
        synopsis = None
        try:
            if content["synopsis"] != '':
                synopsis = content["synopsis"]
            elif content["description"] != '':
                synopsis = content["description"]
            else:
                pass
        except:
            pass
        return synopsis

    def get_image(self, content):
        """
        Metodo para conseguir las imagenes.
        Como vimos que habian posters, miniaturas e imagenes de promo, intentamos traerlas 
        con un for que deberia funcionar.

        Hago una lista de dict, porque el payload pide que sea lista para hacer el upload.
        """
        image = []  
        try:
            if "promo_images" in content:
                for img in content["promo_images"]:
                    image.append(img['url'])
            if "poster_originals" in content:
                for img in content["poster_originals"]:
                    image.append(img['path'])
        except:
            pass
        return image

    def get_rating(self, content):
        """
        Metodo para traer los generos. 

        Lo paso a str porque el payload pide este tipo de dato para el upload.       
        """
        try:
            genres = str(content["restrict"])
            return genres
        except:
            pass

    def get_provider(self, content):
        """
        Metodo para los provider.
        Por parte de la pagina no hay algo relacionado a lo pedido.
        """
        provider = None
        return provider

    def get_genres(self, content):
        """
        """
        genres = []
        content_genres=[]
        content_categories=[]
        
        for genre in content['genres']:   
            content_genres.append(genre)
        for categorie in content['categories']:
            content_categories.append(categorie)

        if content_genres and content_categories:
            response = self.session.get(self.categories_api)
            json_data = response.json()
            categories_list = json_data['result']
            for categorie in categories_list:
                if categorie['id'] in content_categories:
                    for genre in categorie['genres']:
                        if genre['id'] in content_genres:
                            genres.append(genre['hru'])
                        else: pass
                else: pass
        else: genres = None
        return genres

    def get_cast(self, content):
        """
        Por api no parece darlo, pero en el link de cada contenido, hay un apartado de cast "url + /person"
        así que seguramente se va a poder sacar por bs4.
        
        """
        return None
        deeplink = self.get_Deeplinks(content,None)

        deeplink = deeplink["Web"] + "/" + "person"

        request = self.sesion.get(deeplink)

        soup = BeautifulSoup(request.text, 'html.parser')
              
        actores_contenidos = soup.find('div', {'class':'gallery movieDetails__gallery', 'data-test':"actors_actors_block"})
       
        actores = []

        for item in actores_contenidos:
            nombre = actores_contenidos.find('div', {'class':"slimPosterBlock__title"})
            apellido = actores_contenidos.find('div', {'class':"slimPosterBlock__secondTitle"})
            actor = nombre.join(apellido)
            actores.append(actor)
            print(actores)
        #return actores
        

    def get_directors(self, content):
        return None
        deeplink = self.get_Deeplinks(content, None)

        deeplink = deeplink["Web"] + "/" + "person"

        request = self.sesion.get(deeplink)

        soup = BeautifulSoup(request.text, 'html.parser')

        directores_contenidos = soup.find('div', {'class':'gallery movieDetails__gallery', 'data-test':"actors_directors_block"})

        directores = []

        for item in directores_contenidos:
            nombre = directores_contenidos.find('div', {'class':"slimPosterBlock__title"})
            apellido = directores_contenidos.find('div', {'class':"slimPosterBlock__secondTitle"})
            directores.append(nombre, apellido)
            print(directores)

        #return directores

    def get_availability(self, content):
        """
        Metodo para chequear la disponibilidad.    
        Le paso un solo valor porque el payload pide tipo de dato str, no list.   
        """
        availability = None
        if content['object_type'] == 'video':
            try:
                availability = content["best_watch_before"]
            except:
                pass
        return availability


    def get_download(self, content):
        """
        Metodo para ver si se puede descargar.
        Devuelve un booleano        
        """
        download = False
        try:
            download = content["allow_download"]
        except:
            pass
        return download    

    def get_isOriginal(self, content):
        """
        Metodo para ver si es original de la página.
        
        Devuelve un bool

        No hay data de si es original o no, se hardcodea en None, por si algun dia cambia que no quede en False.        
        """
        isOriginal = None
        return isOriginal      

    def get_isBranded(self,content):
        '''
        No hay data de si es branded o no, se hardcodea en None, por si algun dia cambia que no quede en False.
        '''
        branded = None
        return branded

    def get_isAdult(self,content):
        isErotic = False
        try:
            isErotic = content['is_erotic']
        except:
            pass
        return isErotic

    def get_package(self,content):
        """
        Metodo para el package.      
        """
        packages = []
        package = {'Type':'free-vod'}
        try:
            for pack in content["content_paid_types"]:
                for key,val in pack.items():
                    package['Type'] =  val
                    packages.append(package)
        except:
            packages.append(package)
        return packages

def get_crew(self,content):
    """
    Script parecido al del cast.
    Se trae el cast según su apartado, productores, operadores, etc.
    Se hace un if para evitar que rompa al no encontrar alguno de los apartados.

    """
    crew = None
    return crew