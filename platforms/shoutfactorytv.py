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
        self.payloads_movies        = []                                                                    #Inicia payloads movies vacio, luego de la primer ejecución obtenemos contenido
        self.payloads_shows         = []                                                                    #Inicia payloads shows vacio, luego de la primer ejecución obtenemos contenido
        self.payloads_episodes      = []                                                                    #Inicia payloads episodios vacio, luego de la primer ejecución obtenemos contenido
        self.deeplink_base          = "https://www.shoutfactorytv.com"                                      #Se necesita para concatenar
        self.search_page            = "https://www.shoutfactorytv.com/videos?utf8=✓&commit=submit&q="       #Se necesita para concatenar
        self.url                    = self._config['url']                                                   #URL YAML 
        self.testing                = False
        self.sesion                 = requests.session()
        self.headers                = {"Accept": "application/json", "Content-Type": "application/json; charset=utf-8"}

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
        
        response_url = requests.get(self.url)                                                               #Se obtiene la request de la pagina
        if(self.verify_status_code(response_url)):                                                          #Verifica la response
            self.get_payload_movies(response_url)                                                           #Se extrae la información necesaria para peliculas y series
            
            #self.get_payload_shows()
            #self.get_payload_episodes()

        self.sesion.close()
        #Upload(self._platform_code, self._created_at, testing=self.testing) #Sube a Misato (OJO, NO LO USAMOS TODAVÍA)

    #Se encargar de llenar extraer información para llenar los payloads de peliculas y series
    def get_payload_movies(self, response):
        
        links_categories_movies = self.get_categories(response, 0)                                          #Obtiene la lista con todos los links de categorias para peliculas limpias 
    
        count = 0
        for link_categorie in links_categories_movies:                                                      #Recorre cada categoria de la lista de categorias
            response_categorie = requests.get(link_categorie)                                   

            if (self.verify_status_code(response_categorie)):                                   
                soup_link_categorie = BS(response_categorie.text, 'lxml')                                   #Se trae todo el contenido de las categoria
                content_categorie = soup_link_categorie.find_all("div", class_='img-holder')                #Se queda con los tags que contienen la lista de peliculas
              
                for item in content_categorie:                                                              #Recorre cada pelicula
                    list_links = item.find_all("a")                                                         #Obtiene una lista con todos los links de las peliculas sin limpiar

                    for item in list_links:
                        deeplink = self.deeplink_base + item['href']                                         #Acá obtiene el link limpio de cada pelicula                
                        link_search_page = self.search_page + deeplink.split("/")[3]                        #Link de la pelicula para buscar en el search
                        response_link_search = requests.get(link_search_page)                               #Obtiene links para buscar en el search cada pelicula
                        
                        count =+ 1
                        if (self.verify_status_code(response_link_search)):                                
                            soup_link_search = BS(response_link_search.text, 'lxml')                        #Se trae todo el contenido de la búsqueda con el link en el search
                            content_link_search = soup_link_search.find_all("article", class_='post')       #Se queda con los tags que contienen la información de las peliculas

                            for content in content_link_search:
                                list_movies = content.find_all("h2")                                        #Se queda con los tags que contienen los titulos de las peliculas

                                for item in list_movies:
                                    if(str(item.text).upper()                             
                                        == str(link_search_page.split("=")[3]).replace("-", " ").upper()):  #Comprueba que el titulo de la pelicula de la pagina search sea igual al del link
                                        title = content.h2.text                                             #Acá obtiene el titulo
                                        image = content.img['src']                                          #Acá obtiene el link de la imagen
                                        duration = content.time.text                                        #Acá obtiene la duración
                                        description = content.p.text                                        #Acá obtiene la descripción
                                        print(deeplink + " - "
                                            + title + " - "
                                            + image + " - "
                                            + duration + " - "
                                            + description)
                                            
                        else:
                            break
            else:
                break   
        print("\n###############################################")
        print("Cantidad de contenido encontrado: " + str(count))
        print("#################################################\n")
        time.sleep(10)

        #links_categories_series = self.get_categories(response, 1)                                         #Obtiene la lista con todos los links de categorias para series limpias

        '''
        search_pages_shows = []
        for link in links_shows:
            search_pages_movies.append("self.search_page" + link.split("/")[4])
        print(search_pages_shows)
        '''

    #Comprobación del estado de la respuesta a la petición HTTP
    def verify_status_code(self, response):     
        if response.status_code == 200:
            return True
        else:
            print("Status code " + str(response.status_code) + " -> ¡Fail!")                                #Cualquier respuest que no sea 200 entra por acá
            return False    

    #Getter que extrae los links de categorias de las peliculas y series    
    def get_categories(self, response, position):   
        soup_categories = BS(response.text, 'lxml')                                                         #Se trae todo el contenido de la plataforma
        categories = soup_categories.find_all("div", class_='divRow')                                       #Se queda con los tags que contienen las categorias en una lista
        list_categories = categories[position].find_all("a")                                                #Obtiene una lista con todos los links de las categorias

        links_categories = []         
        for item in list_categories:        
             links_categories.append(self.deeplink_base + item['href'])                                      #Agrega los links de las categorías en una lista
        return links_categories