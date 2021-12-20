import time
import requests
from bs4                import BeautifulSoup as BS
from handle.replace     import _replace
from common             import config
from datetime           import datetime
from handle.mongo       import mongo
from handle.datamanager import Datamanager
from updates.upload     import Upload

#Comando para correr el Script: python main.py Shoutfactorytv --c US --o testing

class Shoutfactorytv():
    """
    Shoutfactorytv es una ott de Estados Unidos.

    DATOS IMPORTANTES:
    - Versión Final: No.
    - VPN: No.
    - ¿Usa Selenium?: No.
    - ¿Tiene API?: No.
    - ¿Usa BS4?: Si.
    - ¿Se relaciona con scripts TP? No.
    - ¿Instanacia otro archivo de la carpeta "platforms"?: No.
    - ¿Cuanto demoró la ultima vez? 
    - ¿Cuanto contenidos trajo la ultima vez?
    """
    #Constructor, instancia variables
    def __init__(self, ott_site_uid, ott_site_country, type):
        self._config                = config()['ott_sites'][ott_site_uid]
        self._platform_code         = self._config['countries'][ott_site_country]
        self._created_at            = time.strftime("%Y-%m-%d")
        self.mongo                  = mongo()
        self.titanPreScraping       = config()['mongo']['collections']['prescraping']
        self.titanScraping          = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodios = config()['mongo']['collections']['episode']
        self.skippedEpis            = 0
        self.skippedTitles          = 0
        self.payloads_movies        = []    #Inicia payloads movies vacio, luego de la primer ejecución obtenemos contenido
        self.payloads_shows         = []    #Inicia payloads shows vacio, luego de la primer ejecución obtenemos contenido
        self.payloads_episodes      = []    #Inicia payloads episodios vacio, luego de la primer ejecución obtenemos contenido
        self.deeplinkBase           = "https://www.shoutfactorytv.com"
        self.url                    = self._config['url']   #URL YAML 
        self.testing                = False
        self.sesion                 =  requests.session()
        self.headers                 = {"Accept": "application/json",
                                        "Content-Type": "application/json; charset=utf-8"}

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
            self.testing = True
            self._scraping()

    #Se encarga de instanciar las DB y llamar a los métodos que hacen el scraping
    def _scraping(self):
        self.list_db_episodes = Datamanager._getListDB(self, self.titanScrapingEpisodios)
        self.list_db_movies_shows = Datamanager._getListDB(self, self.titanScraping)
        
        self.get_payload_movies(requests.get(self.url))     #Se obtiene la request de la pagina y se extrae la información necesaria

        '''
        self.get_payload_shows(show_data) 
        self.get_payload_episodes(episode_data)
        '''

        self.sesion.close()
        #Upload(self._platform_code, self._created_at, testing=self.testing) #Sube a Misato (OJO, NO LO USAMOS TODAVÍA)

    #Se encargar de llenar extraer información para llenar los payloads de peliculas y series
    def get_payload_movies(self, response):

        if response.status_code == 200:
            print("Status code: " + str(response.status_code))

            soup = BS(response.text, 'lxml')    #Se trae todo el contenido de la plataforma y lo convierte en un objeto de BS
            categories = soup.find_all("div", class_ = 'divRow')  #Se queda con los tags que contienen las catergorias en una lista
            links_movies = categories[0].find_all("a")  #Obtiene una lista con todos los links de las peliculas
            links_shows = categories[1].find_all("a")   #Obtiene una lista con todos los links de las series

            deeplinks_movies = self.get_categories_movies(links_movies)
            deeplinks_series = self.get_categories_shows(links_shows)

            '''
            for link in deeplinks_movies:

            for link in deeplinks_series:
            '''

        else:
            print("Status code: " + str(response.status_code))
            
    #Getter que extraen las categorias de las peliculas
    def get_categories_movies(self, links_movies):
         deeplinks = []
         for item in links_movies:    
             deeplinks.append(self.deeplinkBase + item['href'])   #Agrega los links de las categorías de las peliculas en una lista  
         return deeplinks

    #Getter que extraen las categorias de las series
    def get_categories_shows(self, links_series):
         deeplinks = []
         for item in links_series:    
             deeplinks.append(self.deeplinkBase + item['href'])   #Agrega los links de las categorías de las series en una lista  
         return deeplinks