# -*- coding: utf-8 -*-
import requests # Si el script usa requests/api o requests/bs4
import time
import platform
from bs4                import BeautifulSoup # Si el script usa bs4
from selenium           import webdriver # Si el script usa selenium
from handle.datamanager import Datamanager # Opcional si el script usa Datamanager
from common             import config
from handle.mongo       import mongo
from updates.upload     import Upload
from handle.replace     import _replace
from handle.payload     import Payload
from pyvirtualdisplay   import Display

class Template():

    """
    Plantilla de muestra para la definición de la clase de una plataforma nueva.

    Los imports van arriba de todo, de acuerdo a las convenciones de buenas prácticas.

    Esta es la descripción de la clase, donde se indica toda la información pertinente al script.
    Siempre debe ir, como mínimo, la siguiente información:
    - Status: (Si aún se está trabajando en la plataforma o si ya se terminó)
    - VPN: (La plataforma requiere o no VPN)
    - Método: (Si la plataforma se scrapea con Requests, BS4, Selenium o alguna mezcla)
    - Runtime: (Tiempo de corrida aproximado del script)

    El __init__ de la clase define los atributos de la clase al instanciar un objeto de la misma.
    Los parámetros que se le pasan al momento de instanciar una clase son los que se insertan desde la terminal
    y siempre son los mismos:
    - ott_platforms: El nombre de la clase, debe coincidir con el nombre que se encuentra en el config.yaml
    - ott_site_country: El ISO code de 2 dígitos del país a scrapear. ejm: AR (Argentina), US (United States)
    - ott_operation: El tipo de operación a realizar. Cuando estamos desarrollando usamos 'testing', cuando
    se corre en el server usa 'scraping'
    Al insertar el comando en la terminal, se vería algo así:
    python main.py --o [ott_operation] --c [ott_site_country] [ott_platforms]

    Los atributos de la clase que use Datamanager siempre deben mantener el nombre, ya que Datamanager
    accede a ellos por nombre. Por ejemplo, si el script usa Datamanager, entonces self.titanScrapingEpisodios
    debe llamarse tal cual, no se le puede cambiar el nombre a self.titanScrapingEpisodes o algo así, porque
    Datamanager no lo va a reconocer y va a lanzar una excepción.
    """

    def __init__(self, ott_platforms, ott_site_country, ott_operation):
        self.test = ott_operation in ("testing", "return") #
        config_ = config()['ott_sites'][ott_platforms] # Obligatorio
        self.country = ott_site_country # Opcional, puede ser útil dependiendo de la lógica del script.
        self._created_at = time.strftime('%Y-%m-%d')
        self._platform_code = config_['countries'][ott_site_country]
        self.mongo = mongo()
        self.sesion                 = requests.session() # Requerido si se va a usar Datamanager
        self.titanPreScraping       = config()['mongo']['collections']['prescraping'] # Opcional
        self.titanScraping          = config()['mongo']['collections']['scraping'] # Obligatorio. También lo usa Datamanager
        self.titanScrapingEpisodios = config()['mongo']['collections']['episode'] # Obligatorio. También lo usa Datamanager
        self.skippedTitles          = 0 # Requerido si se va a usar Datamanager
        self.skippedEpis            = 0 # Requerido si se va a usar Datamanager

        # si se usa selenium, el browser debe declararse como un atributo de clase para que nos sea más fácil cerrarlo con el destructor
        self.browser = webdriver.Firefox()

        #### este bloque de abajo es OBLIGATORIO si se usa Selenium para que pueda correr en los servers
        #### se debe copiar y pegar tal cual está para que funcione
        # primero declarando la variable self.display en una línea
        # y luego llamando al método start de esa variable en la línea que le sigue
        try:
            if platform.system() == 'Linux':
                self.display = Display(visible=0, size=(1366, 768))
                self.display.start()
        except Exception:
            pass
        ####

        """
        La operación 'return' la usamos en caso que se nos corte el script a mitad de camino cuando
        testeamos, sea por un error de conexión u otra cosa. Nos crea una lista de ids ya insertados en
        nuestro Mongo local, la cual podemos usar para saltar los contenidos scrapeados y volver rápidamente
        a donde había cortado el script.
        """
        if ott_operation == 'return':
            return_params = {'PlatformCode' : self._platform_code}
            last_item = self.mongo.lastCretedAt('titanPreScraping', return_params)
            if last_item.count() > 0:
                for last_content in last_item:
                    self._created_at = last_content['CreatedAt']
            self.prescraped_ids = [pay["Id"] for pay in Datamanager._getListDB(self, self.titanPreScraping)]

            self.scraping()
        else:
            self.prescraped_ids = list()

        if ott_operation in ('testing', 'scraping'):
            self.scraping()

    def __del__(self):
        # este es el destructor de clase, realmente solo es necesario añadirlo en scripts donde se use selenium
        # ya que lo usamos para asegurarnos que se cierren todas las instancias de selenium y los virtual displays
        # el destructor de clase se encarga de cerrarlos independientemente de si el script corrió hasta el final o
        # si cortó por un error

        try:
            self.browser.quit()
        except Exception:
            pass

        try:
            self.display.stop()
        except Exception:
            pass

    def scraping(self):
        print("estoy corriendo el Template")
        #self.testConnectionToKaji()

    def testConnectionToKaji(self):
        payload = self.build_payload_movie()
        self.mongo.insert(self.titanScraping, payload)
        print("Insertado a mongo local. Ejecutando Upload...")
        
        Upload(self._platform_code, self._created_at, testing=True, server=2)

    def build_payload_movie(self):

        payload = Payload()

        payload.platform_code = self._platform_code
        payload.id = "12345" # (str) debe ser único para este contenido de esta plataforma
        payload.title = "Payload for testing connection to Kaji" # (str)
        payload.clean_title = _replace(payload.title) # (str)
        payload.deeplink_web = "https://www.google.com" # (str)
        payload.packages = [
            {
                "Type":"free-vod"
            }
        ]
        payload.createdAt = self._created_at
        
        return payload.payload_movie()
