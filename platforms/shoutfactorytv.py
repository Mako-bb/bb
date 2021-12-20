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
        self.url                    = self._config['url']      #URL YAML
        self._format_url            = self._config['format_url'] 
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
    def scraping(self):
        self.list_db_episodes = Datamanager._getListDB(self, self.titanScrapingEpisodios)
        self.list_db_movies_shows = Datamanager._getListDB(self, self.titanScraping)
        
        #Se pasa por parametro el contenido de la plataforma y la función se encarga de extraer la información
        self.get_payload_movies(requests.get(self.url)) 

        '''
        self.get_payload_shows(show_data) 
        self.get_payload_episodes(episode_data)
        '''

        self.sesion.close() #Se cierra la session
        #Upload(self._platform_code, self._created_at, testing=self.testing) #Sube a Misato (OJO, NO LO USAMOS TODAVÍA)

    def get_payload_movies(self, response):

        print("Status code: " + response.status_code)
        if response.status_code == '200':
            soup = BS(response.text, 'lxml')    #Se trae todo el contenido de la plataforma y lo convierte en un objeto de BS
            contenedor = soup.find_all("div",{"class" : "divRow"})  #Contenido completo 
            print(contenedor)
        
