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
        
        response_url = requests.get(self.url)   #Se obtiene la request de la pagina
        self.get_payload_movies_shows(response_url)     #Se extrae la información necesaria para peliculas y series

        '''
        self.get_payload_episodes(episode_data)
        '''

        self.sesion.close()
        #Upload(self._platform_code, self._created_at, testing=self.testing) #Sube a Misato (OJO, NO LO USAMOS TODAVÍA)

    #Se encargar de llenar extraer información para llenar los payloads de peliculas y series
    def get_payload_movies_shows(self, response):

        if (self.verify_status_code(response)):

            soup_categories = BS(response.text, 'lxml')    #Se trae todo el contenido de la plataforma
            categories = soup_categories.find_all("div", class_='divRow')  #Se queda con los tags que contienen las categorias en una lista
    
            list_categories_movies = categories[0].find_all("a")  #Obtiene una lista con todos los links de las categorias de peliculas sin limpiar
            list_categories_shows = categories[1].find_all("a")   #Obtiene una lista con todos los links de las categorias de series sin limpiar

            links_categories_movies = self.get_links_categories_movies_shows(list_categories_movies)  #Obtiene la lista de categorias para peliculas limpios       
            '''
            links_categories_series = self.get_links_categories_movies_shows(list_categories_shows)    #Obtiene la lista de categorias para series
            '''
            for link_categorie in links_categories_movies:  #Recorre cada categoria de la lista de categorias de peliculas
                response_categorie = requests.get(link_categorie)   #Realiza la petición HTTP

                if (self.verify_status_code(response_categorie)):   #Verifica la petición HTTP
                    soup_link_categorie = BS(response_categorie.text, 'lxml')   #Se trae todo el contenido de las categoria
                    content_categorie = soup_link_categorie.find_all("div", class_='img-holder')    #Se queda con los tags que contienen la lista de peliculas
                    
                    links_movies = []
                    for content in content_categorie:   #Recorre cada pelicula de la lista de peliculas 
                        list_links_movies = content.find_all("a")   #Obtiene una lista con todos los links de las peliculas sin limpiar
                        links_movies.append(self.get_links_movies(list_links_movies))   #Obtiene la lista de peliculas limpios

                    #print(links_movies)
                                
                else:
                    break
            '''
            for link in links_categories_series:
            '''

    #Comprobación del estado de la respuesta a la petición HTTP
    def verify_status_code(self, response):     
        if response.status_code == 200:
            print("Status code: " + str(response.status_code))
            return True
        else:
            print("Status code: " + str(response.status_code))  #Cualquier respuesta que no sea 200 entra por acá
            return False
            
    #Getter que extrae los links de categorias de las peliculas y series
    def get_links_categories_movies_shows(self, content):
         links = []
         for item in content:    
             links.append(self.deeplinkBase + item['href'])   #Agrega los links de las categorías de las peliculas en una lista  
         return links

    #Getter que extrae los links de las peliculas
    def get_links_movies(self, content):
        links = []
        for item in content:    
            links.append(self.deeplinkBase + item['href'])   #Agrega los links de las peliculas en una lista 
        return links