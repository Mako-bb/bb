import json
import logging
import threading
import time
import requests
from handle.replace         import _replace
from common                 import config
from handle.mongo           import mongo
from updates.upload         import Upload
from handle.payload         import Payload
from handle.datamanager     import Datamanager
from datetime               import datetime
from bs4                    import BeautifulSoup, element
# from time import sleep
# import re
from concurrent.futures     import ThreadPoolExecutor, as_completed
class HBOPQ():
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
        self.titanScrapingEpisodes = config()['mongo']['collections']['episode']

        self.all_movies_url = self._config['all_movies']
        self.info_movie_url = self._config['info_movie']

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
    
    # NO DIGAN COMO PROGRAMO :(
    def _scraping(self, testing=False):
        
        self.payloads = []
        response = self.request(self.all_movies_url)
        soup = BeautifulSoup(response.content, "html.parser")
        all_movies_title = soup.find_all("p", class_="modules/cards/CatalogCard--title")
        print("Traje "+str(len(all_movies_title))+" peliculas")
       
        ini = time.time()
        self.preparing_to_look_for_info(all_movies_title)
        fin = time.time()
        print(fin - ini)
        self.session.close()
    
    #preparing_to_look_for_info toma cada title y lo limpia para acceder al la pagina individual. Ej: https://www.hbo.com/movies/13-going-on-30
    #En processes pongo cada thread que va a tener el trabajo de buscar la info con get_info.
    def preparing_to_look_for_info(self, list_movies_title):
        processes = []
        with ThreadPoolExecutor(max_workers=8) as executor:
            i = 0
            for title in list_movies_title:
                title_ok = self.clean_title(title.text)
                processes.append(executor.submit(self.get_info, title_ok))
                if i == 10:
                    break
                i += 1            
    
    #get_info toma la info de cada contenito en su pag individual.
    def get_info(self, title):
        url = self.info_movie_url+title.replace(" ", "-")
        res = self.request(url)
        if res.status_code != 404:
            try:
                soup = BeautifulSoup(res.content, "html.parser")
                payload = self.get_payload(soup)
                #Aca voy a tener que poner algo para las series
                self.payloads.append(payload)         
            except:
                pass
        else:
            print("ERROR")

    
    def get_payload(self, soup):
        """Método para crear el payload. Para titanScraping.

        Args:
            soup (objeto de bs4): donde esta contenita la info que necesito.

        Returns:
            dict: Retorna el payload.
        """
        title = soup.find("a", class_="bands/MainNavigation--logoName")
        
        
        payload = {}
        payload = { 
            "PlatformCode": self._platform_code,   #Obligatorio 
            "Id": None,  #Obligatorio
            "Seasons": None, #Lo hago aparte
            "Crew": None,
            "Title": None, #Obligatorio 
            "CleanTitle": None, #Obligatorio 
            "OriginalTitle": None, 
            "Type": None, #Obligatorio #movie o serie 
            "Year": None, #Important! 1870 a año actual 
            "Duration": "", #en minutos 
            "ExternalIds": None, #consultar
            "Deeplinks": { 
                "Web": None, #Obligatorio 
                "Android": None, 
                "iOS": None, 
            }, 
            "Synopsis": None, 
            "Image": None, 
            "Rating": None, #Important!  "Provider": "list", 
            "Genres": None, #Important! 
            "Provider": None,
            "Cast": None, #Important! 
            "Directors": None, #Important! 
            "Availability": None, #Important! 
            "Download": None, 
            "IsOriginal": None, #Important! 
            "IsAdult": None, #Important! 
            "IsBranded": None, #Important! (ver link explicativo)
            "Packages": [{"Type":"subscription-vod"}], #Obligatorio 
            "Country": None, 
            "Timestamp": datetime.now().isoformat(), #Obligatorio 
            "CreatedAt": self._created_at, #Obligatorio
        }

        return payload

    
    def clean_title(self, title):
       #Método para acomodar el title, sacando / , * , &
       valid_chars = [" ", "&","/"]
       new_string = ''.join(char for char in title if char.isalnum() or char in valid_chars)
       new_string = new_string.lower()
       new_string = new_string.replace("  ", " ")
       new_string = new_string.replace("/","-").replace("&","and")
       return new_string
         
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
    
    """
        Sale con la API, me costo tanto encontrarla que no quiero borrarla je
        
        res = self.request(self.all_movies_url)
        soup = BeautifulSoup(res.content, "html.parser")
        noScript = soup.find("noscript", id="react-data")
        noScript_json = json.loads(noScript["data-state"])
        i = 0
        pepe = []
        try:
            for id in noScript_json["bands"][1]["data"]["entries"]:
                print(id["streamingId"]["id"])
   
                
                i += 1
        except:
            pepe.append(i)
        """























