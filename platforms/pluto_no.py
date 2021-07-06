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



class PlutoNO():
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
        #self._start_url = self._config['start_url']
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
            self._scraping(is_test=True)

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

    def _scraping(self, is_test=False):
        # Pensando algoritmo:
        # 1) Método request (request)-> Validar todo.
        # 2) Método payload (get_payload)-> Para reutilizarlo.
        # 3) Método para traer los contenidos (get_contents)

        # Listas de ids scrapeados:
        print(f"\nIniciando scraping de {self._platform_code}\n")
        self.scraped = self.query_field(self.titanScraping, field='Id')
        self.scraped_episodes = self.query_field(self.titanScrapingEpisodes, field='Id')
        print(f"{self.titanScraping} {len(self.scraped)}")
        print(f"{self.titanScrapingEpisodes} {len(self.scraped_episodes)}")

        # Lista de contenidos a obtener (Almacenan dict):
        self.payloads = []
        self.episodes_payloads = []

        contents = self.get_contents()
        for n, content in enumerate(contents):
            print(f"\n----- Progreso ({n}/{len(contents)}) -----\n")

            # Valido que no haya duplicados:
            if content["_id"] in self.scraped:
                print("Ya ingresado")
            else:
                self.scraped.append(content["_id"])    
                payload = self.get_payload(content)
                self.payloads.append(payload)

                # Traigo los episodios en caso de ser serie: # Preguntar mañana el tema de que si ya está la id, no corre lo otro.
                if payload["Type"] == 'serie':
                    try:
                        for contenidos in self.get_seasons(content, self.season_api_url):
                            for episode in contenidos:
                                if episode["_id"] in self.scraped_episodes:
                                    print("Ya ingresado")
                                else:
                                    self.scraped_episodes.append(episode["_id"])
                                    self.episodes_payloads.append(self.get_payload_episodes(content, episode))
                    except:
                        pass



        if self.payloads:
            self.mongo.insertMany(self.titanScraping, self.payloads)
        if self.episodes_payloads:
            self.mongo.insertMany(self.titanScrapingEpisodes, self.episodes_payloads)

        
        self.session.close()
        Upload(self._platform_code, self._created_at, testing=True)

        print("Scraping Finalizado")

    def get_contents(self): 
        """Método para obtener contenidos en forma de dict,
        almancenados en una lista.

        Returns:
            list: Lista de contenidos.
        """
        print("\nObteniendo contenidos...\n")
        contents = [] # Contenidos a devolver.
        response = self.request(self.api_url)
        contents_metadata = response.json()        
        categories = contents_metadata["categories"]

        for categorie in categories:
            print(categorie.get("name"))
            contents += categorie["items"]
        return contents

    def get_seasons(self, content, slug):
        episodes = []
        response_2 = self.request(self.season_api_url.format(content["slug"]))
        content = response_2.json()
        try:
            seasons = content['seasons']

            for season in seasons:
                episodes.append(season['episodes'])
            
            return episodes
        except:
            pass

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

    def get_payload(self, content_dict):
        """Método para crear el payload. Para titanScraping.

        Args:
            content_metadata (dict): Indica la metadata del contenido.

        Returns:
            dict: Retorna el payload.
        """
        payload = {}

        # Indica si el payload a completar es un episodio:        
        payload['PlatformCode'] = self._platform_code
        payload['Id'] = content_dict["_id"]
        payload['Title'] = content_dict["name"]
        payload['OriginalTitle'] = None
        payload['CleanTitle'] = _replace(content_dict["name"])
        payload['Duration'] = self.get_duration(content_dict)
        payload['Type'] = self.get_type(content_dict["type"]) 
        payload['Year'] = None
        payload['Deeplinks'] = self.get_deeplinks(content_dict)
        payload['Playback'] = None
        payload['Synopsis'] = content_dict["summary"]
        payload['Image'] = self.get_images(content_dict)
        payload['Rating'] = content_dict["rating"]
        payload['Provider'] = None
        payload['Genres'] = content_dict["genre"]
        payload['Cast'] = None
        payload['Directors'] = None
        payload['Availability'] = None
        payload['Download'] = None
        payload['IsOriginal'] = None
        payload['IsBranded'] = None
        payload['IsAdult'] = None
        payload['Packages'] = [{"Type":"free-vod"}]
        payload['Country'] = None
        payload['Crew'] = None        
        payload['Timestamp'] = datetime.now().isoformat()
        payload['CreatedAt'] = self._created_at

        print(f"Url: {payload['Deeplinks']['Web']}")
        print(f"{payload['Type']}:\t{payload['Title']}")

        return payload

    def get_payload_episodes(self, content_dict, episode_dict):
        """Método para crear el payload. Para titanScrapingEpisodes.

        Args:
            content_metadata (dict): Indica la metadata del contenido.

        Returns:
            dict: Retorna el payload.
        """
        episode_payloads = {}

        # Indica si el payload a completar es un episodio:        
        episode_payloads['PlatformCode'] = self._platform_code
        episode_payloads['Id'] = episode_dict["_id"]
        episode_payloads['Title'] = episode_dict["name"]
        episode_payloads['Duration'] = self.get_duration(episode_dict)
        episode_payloads["ParentTitle"] = content_dict["name"]
        episode_payloads["ParentId"] = content_dict["_id"]
        episode_payloads["Season"] = episode_dict["season"]
        episode_payloads["Episode"] = episode_dict["number"]
        episode_payloads['Year'] = None
        episode_payloads['Deeplinks'] = self.get_deeplinks(content_dict)
        episode_payloads['Playback'] = None
        episode_payloads['Synopsis'] = None
        episode_payloads['Image'] = self.get_images(episode_dict)
        episode_payloads['Rating'] = episode_dict["rating"]
        episode_payloads['Provider'] = None
        episode_payloads['Genres'] = episode_dict["genre"]
        episode_payloads['Cast'] = None
        episode_payloads['Directors'] = None
        episode_payloads['Availability'] = None
        episode_payloads['Download'] = None
        episode_payloads['IsOriginal'] = None
        episode_payloads['IsAdult'] = None
        episode_payloads['Packages'] = [{'Type': 'free-vod'}]
        episode_payloads['Country'] = None
        episode_payloads['Timestamp'] = datetime.now().isoformat()
        episode_payloads['CreatedAt'] = None

        print(f"Url: {episode_payloads['Deeplinks']['Web']}")
        print(f"{episode_payloads['Id']}:\t{episode_payloads['Title']}")
        
        return episode_payloads

    def get_deeplinks(self, content_dict):
        url = "https://pluto.tv/on-demand"
        if content_dict["type"] == 'movie':
            deeplinks = {
                    "Web": url + "/" + content_dict["type"] + "s" + "/" + content_dict["slug"],
                    "Android": None,
                    "iOS": None,
                }
            return deeplinks
        else:
            deeplinks = {
                    "Web": url + "/" + content_dict["type"] + "/" + content_dict["slug"],
                    "Android": None,
                    "iOS": None,
                }
            return deeplinks


    def get_type(self, type_):
        if type_ == 'series': # Se puede solucionar con regex.
            return 'serie'
        else:
            return type_
    
    # def get_episodes(self, content_dict):        
    #     # 1) Hacer consulta a los episodios scrapeados (self.scraped_episodes):
    #     # self.episodes_payloads
    #     # 2) Si no existen, agregar los episodios a self.episodes_payloads.
    #     if "¿Existe ese episodio?" in self.scraped_episodes:
    #         print("Ya ingresado")
    #     else:
    #         print("TRAER EPISODIO/S")
            
    def get_images(self, content_dict):
        """
        generamos un hermoso script para traer las url de las covers de las peliculas/series.

        (entiendo que es un script super obvio, pero para tomar la costumbre, jajaja).
        """

        for cover in content_dict["covers"]: 
            images = cover.get("url")
            return images            

    def get_duration(self, content_dict, is_episode=False):
        """
        Verificamos si es una serie, en el caso de que lo sea, dejamos la celda en Null;
        Si es una pelicula, colocamos la duración (habría que ponerla en segundos).
        """
                
        if content_dict["type"] == 'series' and is_episode == False:
            pass
        if content_dict["type"] == 'series' and is_episode == True:
            return int(content_dict["duration"]/60000)
        if content_dict["type"] == 'movie':
            return int(content_dict["duration"]/60000)
        