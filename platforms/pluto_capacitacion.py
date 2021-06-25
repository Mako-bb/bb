import time
import requests
from handle.replace         import _replace
from common                 import config
from handle.mongo           import mongo
from updates.upload         import Upload
from handle.payload         import Payload
from handle.datamanager     import Datamanager
# from time import sleep
# import re

class PlutoCapacitacion():
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
        self.titanScrapingEpisodios = config()['mongo']['collections']['episode']

        self.api_url = self._config['api_url']

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
        # 1) Obtener la API.
        # 2) BS4.
        # 3) Selenium.

        # Puequeños tips para BS4:
        # from bs4                    import BeautifulSoup
        # req = self.session.get('https://www.hbo.com/movies/catalog')
        # print(req.status_code, req.url)

        # soup = BeautifulSoup(req.text, 'html.parser')
        
        # contenedors = soup.find_all('div', {'class':'modules/cards/CatalogCard--description'})

        # for tag in contenedors:
        #     print(tag)
        #     print(tag["href"])

        # print(contenedor.attrs)

        # print(contenedor.children.contents)
        # print(" ")
        # print(contenedor.parent.attrs)

        url = self.api_url
        
        response = self.session.get(url)
        contents_metadata = response.json()        
        # print([i.get("name") for i in dict_of_pluto["categories"]])
        
        # Recorrer dict_of_pluto e imprimir todos los datos que se
        # puedan de sus contenidos

        categories = contents_metadata["categories"]

        contents = []
        # Ejemplo:
        for categorie in categories:
            print(categorie.get("name"))
            contents += categorie["items"]

        for content in contents:
            from pprint import pprint as print_lindo
            # print_lindo(content)
            # # Imprimir los payloads:
            payload = {
                "Id": content["_id"],
                "Title": content["name"],
                "PlatformCode": self._platform_code,
                "Type": "serie"
                # Lo pueden hacer completo.
            }
            # Inserto payload:
            self.mongo.insert("titanScraping", payload)
            print("Insert")

            # En caso de ser serie, inserto los capitulos.
            if payload["Type"] == 'serie':
                epi_payload = {
                    "Id": "1",
                    "PlatformCode": self._platform_code,                    
                    "ParentId": content["_id"],
                    "Title": content["name"],
                    "Type": "serie"
                    # Lo pueden hacer completo.
                }
                print(payload)
                self.mongo.insert("titanScrapingEpisodes", epi_payload)
                print("Insert")