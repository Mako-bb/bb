import time
import requests
from handle.replace import _replace
from common import config
from handle.mongo import mongo
# from time import sleep
# import re

class Pluto():
    """
    """
    def __init__(self, ott_site_uid, ott_site_country, type):
        self._config = config()['ott_sites'][ott_site_uid]
        self._platform_code = self._config['countries'][ott_site_country]
        # self._start_url             = self._config['start_url']
        self._created_at = time.strftime("%Y-%m-%d")
        self.mongo = mongo()
        self.titanPreScraping = config()['mongo']['collections']['prescraping']
        self.titanScraping = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodios = config()['mongo']['collections']['episode']

        self.api_url = self._config['api_url']
        
        self.sesion = requests.session()

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
    
    def _scraping(self, testing=False):
        uri = self.api_url
        response = self.sesion.get(uri)
        if response.status_code == 200:
            from pprint import pprint
            dict_contents = response.json()
            list_categories = dict_contents['categories']

        for categories in list_categories:
            contents = categories['items']

            # Traduccion de generos
            for content in contents:
                if (content['genre']) == 'Children & Family':
                    genero = 'Famila y ninos'
                elif (content['genre']) == 'Comedy':
                    genero = 'Comedia'
                elif (content['genre']) ==  'Instructional & Educational':
                    genero = 'Educacion e Instructivos'
                elif (content['genre']) ==  'Documentaries':
                    genero = 'Documentales'
                elif (content['genre']) ==  'Sci-Fi & Fantasy':
                    genero = 'Ciencia Ficcion y Fantasia'
                elif (content['genre']) ==  'Action & Adventure':
                    genero = 'Accion y aventura'
                else:
                    genero = content['genre']

                # Traduccion de tipos
                if (content['type'] == 'series'):
                    tipo = 'Serie'
                elif (content['type'] == 'movie'):
                    tipo = 'Pelicula'
                else:
                    tipo = content['type']

                # Traduccion de clasificacion
                if (content['rating'] == 'Not Rated'):
                    clasificacion = 'Sin clasificar'
                else:
                    clasificacion = content['rating']

                if tipo == 'Serie':                 # Si es una serie, se muestran las temporadas
                    pprint('Nombre: ' + content['name'] + ' - Tipo: ' + tipo + ' - Temporadas: ' + str(content['seasonsNumbers']) +
                    ' - Genero: ' + genero + ' - Clasificacion: ' + clasificacion  + ' - Resumen: ' + content['summary'])
                elif tipo == 'Pelicula':            # Si es una pelicula, no se muestran las temporadas
                    pprint('Nombre: ' + content['name'] + ' - Tipo: ' + tipo + ' - Genero: ' + genero + ' - Clasificacion: ' + clasificacion
                    + ' - Resumen: ' + content['summary'])