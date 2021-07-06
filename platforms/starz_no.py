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



class StarzNO():
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
        

        #self.url = self._config['url']
        self.api_url = self._config['api_url']
       # self.season_api_url = self._config['season_api_url']

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

    def _scraping(self, testing=False):

        
        url = 'https://playdata.starz.com/metadata-service/play/partner/Web_ES/v8/blocks?playContents=map&lang=es-ES&pages=MOVIES,SERIES&includes=contentId,title,contentType,releaseYear,runtime,logLine,image,ratingCode,genres,actors,directors,original,countryOfOrigin,seriesName,seasonNumber,episodeCount,details'
        
        response = self.session.get(url)

        contents_metadata = response.json()        
        
        contents = contents_metadata['blocks'][2]

        slug = []

        
        
        for content in contents:
          for content in contents:
              id = content.get("contentId")
              title = content.get("title")
              type = content.get("contentType")
              synopsis = content.get("summary")
              duration = content.get("runtime")
              rating = content.get("rating")
              genres = content.get("genre")
              covers = content["covers"]
              slug = content["slug"]
              for content in covers:
                  image = content.get("url")
                  pass


              payload_contenidos = {
                  "PlatformCode": "ar.pultotv",
                  "Id": id,
                  "Title": title,
                  "CleanTitle": _replace(title),
                  "Type": type,
                  "Duration": duration, #no pude ponerlo en segundos
                  "Synopsis" : synopsis,
                  "Image": image,
                  "Rating": rating,
                  "Genres": genres,
                  
              }
              print(payload_contenidos)                 

              self.mongo.insert("titanScraping", payload_contenidos)
              
              
              
              url_episodios = f'https://service-vod.clusters.pluto.tv/v3/vod/slugs/{slug}?advertisingId=&appName=web&appVersion=5.17.1-be7b5e79fc7cad022e22627cbb64a390ca9429c7&app_name=web&clientDeviceType=0&clientID=95b00792-ce58-4e87-b310-caaf6c8d8de4&clientModelNumber=na&country=AR&deviceDNT=false&deviceId=95b00792-ce58-4e87-b310-caaf6c8d8de4&deviceLat=-34.6022&deviceLon=-58.3845&deviceMake=Firefox&deviceModel=web&deviceType=web&deviceVersion=89.0&marketingRegion=VE&serverSideAds=true&sessionID=4987c7e3-d482-11eb-bee6-0242ac110002&sid=4987c7e3-d482-11eb-bee6-0242ac110002&userId=&attributeV4=foo'
              response_episodios = self.session.get(url_episodios)
              contents_metadata_episodios = response_episodios.json()
              temporadas = contents_metadata_episodios["seasons"]
              

              for temporada in temporadas:
                  episodes = temporada.get("episodes")
                  for episode in episodes:
                      id_episode = episode.get("_id")
                      ParentId_episode = id
                      ParentTitle_episode = title
                      Episode_episode = episode.get("number")
                      Season_episode = episode.get("season")
                      Title_episode = episode.get("name")
                      Duration_episode = episode.get("duration")
                      Rating_episode = episode.get("rating")
                      Genres_episode = episode.get("genre")
                      Cover_episode = episode["covers"]
                      for content_ep in Cover_episode:
                          Image_episode = content_ep.get("url")

                      if type == "series":
                          payload_episodes = {
                              "PlatformCode": "ar.pultotv",
                              "Id": id_episode,
                              "ParentId": ParentId_episode,
                              "ParentTitle": ParentTitle_episode,
                              "Episode": Episode_episode,
                              "Season": Season_episode,
                              "Title": Title_episode,
                              "CleanTitle": _replace(Title_episode),
                              "Duration": Duration_episode, #no pude ponerlo en segundos
                              "Image": Image_episode,
                              "Rating": Rating_episode,
                              "Genres": Genres_episode,
                              
                          }
                          print(payload_episodes)       
                          self.mongo.insert("titanScrapingEpisodes", payload_episodes)

            
                      

              

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
            if content["contentId"] in self.scraped:
                print("Ya ingresado")
            else:
                self.scraped.append(content["contentId"])    
                payload = self.get_payload(content)
                self.payloads.append(payload)

                # Traigo los episodios en caso de ser serie: # Preguntar mañana el tema de que si ya está la id, no corre lo otro.
                if payload["Type"] == 'serie':
                   # try:
                        for contenidos in self.get_seasons(content, self.api_url):
                            for episode in contenidos:
                                if episode["contentId"] in self.scraped_episodes:
                                    print("Ya ingresado")
                                else:
                                    self.scraped_episodes.append(episode["contentId"])
                                    self.episodes_payloads.append(self.get_payload_episodes(content, episode))
                  #  except:
                    #    pass



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
        response = self.session.get(self.api_url)
        contents_metadata = response.json()        
        categories = contents_metadata['blocks'][2]['playContentsById']

        for item in categories:
            all_content = categories[str(item)]
            contents.append(all_content)
        return contents

    def get_seasons(self, content, slug):
        episodes = []
        response = self.session.get(self.api_url)
        contents_metadata = response.json()        
        categories = contents_metadata['blocks'][2]['playContentsById']
        
        
        try:
            if content['childContent']:
                seasons = content['childContent']
                episodes.append(seasons)
            
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
        payload['Id'] = content_dict["contentId"]
        payload['Title'] = content_dict["title"]
        payload['OriginalTitle'] = None
        payload['CleanTitle'] = _replace(content_dict["title"])
        payload['Duration'] = self.get_duration(content_dict)
        payload['Type'] = self.get_type(content_dict["contentType"]) 
        payload['Year'] = self.get_year(content_dict)
        payload['Deeplinks'] = None #self.get_deeplinks(content_dict)
        payload['Playback'] = None
        payload['Synopsis'] = content_dict["logLine"]
        payload['Image'] = None
        payload['Rating'] = content_dict["ratingCode"]
        payload['Provider'] = None
        payload['Genres'] = self.get_genres(content_dict)
        payload['Cast'] = self.get_cast(content_dict)
        payload['Directors'] = self.get_directors(content_dict)
        payload['Availability'] = None
        payload['Download'] = None
        payload['IsOriginal'] = content_dict["original"]
        payload['IsBranded'] = None
        payload['IsAdult'] = None
        payload['Packages'] = [{"Type":"subscription-vod"}]
        payload['Country'] = self.get_country(content_dict)
        payload['Crew'] = None #self.get_crew(content_dict)        
        payload['Timestamp'] = datetime.now().isoformat()
        payload['CreatedAt'] = self._created_at

        #Tengo que crear (o mejorar) el get de:
        #Deeplinks, crew

        #print(f"Url: {payload['Deeplinks']['Web']}")
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
        if content_dict["contentType"] == 'movie':
            deeplinks = {
                    "Web": url + "/" + content_dict["contentType"] + "s" + "/" + content_dict["slug"],
                    "Android": None,
                    "iOS": None,
                }
            return deeplinks
        else:
            deeplinks = {
                    "Web": url + "/" + content_dict["contentType"] + "/" + content_dict["slug"],
                    "Android": None,
                    "iOS": None,
                }
            return deeplinks


    def get_type(self, type_):
        if type_ == 'Series with Season': # Se puede solucionar con regex.
            return 'serie'
        else:
            return type_
                
      

    def get_duration(self, content_dict, is_episode=False):
        """
        Verificamos si es una serie, en el caso de que lo sea, dejamos la celda en Null;
        Si es una pelicula, colocamos la duración (habría que ponerla en segundos).
        """
                
        if content_dict["contentType"] == 'series' and is_episode == False:
            pass
        if content_dict["contentType"] == 'series' and is_episode == True:
            return int(content_dict["runtime"]/60)
        if content_dict["contentType"] == 'movie':
            return int(content_dict["runtime"]/60)


    def get_genres(self, content_dict):
        for item in content_dict["genres"]:
            all_genres = str(item)
            
            return all_genres

    
    def get_cast(self, content_dict):
        try:
            for item in content_dict["actors"]:
                all_cast = str(item)
                
                return all_cast
        except:
            pass        

    def get_directors(self, content_dict):
        try:
            for item in content_dict["directors"]:
                all_directors = str(item)
                
                return all_directors
        except:
            pass


    def get_year(self, content_dict):
        try:
            year = content_dict["releaseYear"]
            return year
        except:
            pass    

    def get_country(self, content_dict):
        try:
            country = content_dict["countryOfOrigin"]
            return country
        except:
            pass    
