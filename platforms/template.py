# -*- coding: utf-8 -*-
import requests # Si el script usa requests/api o requests/bs4
from bs4                import BeautifulSoup # Si el script usa bs4
from selenium           import webdriver # Si el script usa selenium
from handle.datamanager import Datamanager # Opcional si el script usa Datamanager
from common             import config
from handle.mongo       import mongo
from updates.upload     import Upload
from handle.replace     import _replace

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
    - ott_site_uid: El nombre de la clase, debe coincidir con el nombre que se encuentra en el config.yaml
    - ott_site_country: El ISO code de 2 dígitos del país a scrapear. ejm: AR (Argentina), US (United States)
    - operation: El tipo de operación a realizar. Cuando estamos desarrollando usamos 'testing', cuando
    se corre en el server usa 'scraping'
    Al insertar el comando en la terminal, se vería algo así:
    python main.py --o [operation] --c [ott_site_country] [ott_site_uid]

    Los atributos de la clase que use Datamanager siempre deben mantener el nombre, ya que Datamanager
    accede a ellos por nombre. Por ejemplo, si el script usa Datamanager, entonces self.titanScrapingEpisodios
    debe llamarse tal cual, no se le puede cambiar el nombre a self.titanScrapingEpisodes o algo así, porque
    Datamanager no lo va a reconocer y va a lanzar una excepción.
    """

    def __init__(self, ott_site_uid, ott_site_country, operation):
        self.test = operation in ("testing", "return") #
        config_ = config()['ott_sites'][ott_site_uid] # Obligatorio
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

        #### OBLIGATORIO si se usa Selenium para que pueda correr en los servers
        try:
            if platform.system() == 'Linux':
                Display(visible=0, size=(1366, 768)).start()
        except Exception:
            pass
        ####

        """
        La operación 'return' la usamos en caso que se nos corte el script a mitad de camino cuando
        testeamos, sea por un error de conexión u otra cosa. Nos crea una lista de ids ya insertados en
        nuestro Mongo local, la cual podemos usar para saltar los contenidos scrapeados y volver rápidamente
        a donde había cortado el script.
        """
        if operation == 'return':
            return_params = {'PlatformCode' : self._platform_code}
            last_item = self.mongo.lastCretedAt('titanPreScraping', return_params)
            if last_item.count() > 0:
                for last_content in last_item:
                    self._created_at = last_content['CreatedAt']
            self.prescraped_ids = [pay["Id"] for pay in Datamanager._getListDB(self, self.titanPreScraping)]

            self.scraping()
        else:
            self.prescraped_ids = list()

        if operation in ('testing', 'scraping'):
            self.scraping()
