# -*- coding: utf-8 -*-
import json
import time
import requests
import hashlib   
from common import config
#from bs4 import BeautifulSoup
from datetime import datetime
from handle.mongo import mongo
from updates.upload import Upload
from handle.datamanager import Datamanager
from handle.replace import _replace
import pandas as pd


class WeTV():
    '''
        WeTV es una plataforma de EEUU, la misma provee unicamente series, contenido pago por suscripción 
            y contenido gratuito (son extras o detras de camara), por lo tanto no nos interesan.
        Se accede sin requerir VPN.
        Forma de obtener la info: API.
        Estructura de la web/apis: La web presenta un "Show All Series" y "Show All Episodes"
            dentro de las cuales podemos obtener los datos, desde show all series podemos obtener la 
            data principal (titles, synopsis, etc), lamentablemente desde show all episodes solo se obtiene
            la data de la ultima temporada de cada serie (ya que en la web se muestran las ultimas temporadas como novedades), 
            por lo tanto hay que visitar multiples URL's de las restantes temporadas.

        Duración: Dependiendo la pc, aprox. 3 minutos.

        Cosas por hacer: Externalizar urls.
    '''
    def __init__(self, ott_site_uid, ott_site_country, type):
        self._config                = config()['ott_sites'][ott_site_uid]
        self._created_at            = time.strftime("%Y-%m-%d")
        self.mongo                  = mongo()
        self._platform_code         = self._config['countries'][ott_site_country]   
        self.titanScraping          = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodios = config()['mongo']['collections']['episode']
        self.sesion                 = requests.session()
        self.skippedEpis            = 0
        self.skippedTitles          = 0

        
        if type == 'scraping':
            self._scraping()

        if type == 'testing':
            self._scraping(testing=True)
        
        if type == 'return':
            '''
            Retorna a la Ultima Fecha
            '''

            params = {"PlatformCode" : self._platform_code}
            lastItem = self.mongo.lastCretedAt(self.titanScraping, params)
            if lastItem.count() > 0:
                for lastContent in lastItem:
                    self._created_at = lastContent['CreatedAt']
                    
            self._scraping()


    def _scraping(self, testing=False):
 
        link_principal_info = ('https://content-delivery-gw.svc.ds.amcn.com/api/v2/content/'
                                'amcn/wetv/url/shows?cacheHash=6fbe285914ba1b125a543cb2a78a8e5d2b'
                                '8bf1962737c01b1fd874e87f8dbdb9&device=web')

        self.get_response(link_principal_info)

        self.sesion.close()

        Upload(self._platform_code, self._created_at, testing = testing)
        

    def get_principal_info(self, url):
        '''Esta funcion extrae la data de la api principal (y mas facil de detectar),
        desde la url en _scraping, paralelamente llama a otras funciones para extraer
        datos restantes'''

        series_info = []

        data = url.json()

        content = data['data']['children'][4]['children'] #Es el indice 4 de la lista la que contiene la info completa

        for a in content:
            try:
                link_to_cast = a['properties']['cardData']['meta'].get('permalink', None).replace('--', '/').split('/')
                link = a['properties']['cardData']['meta'].get('permalink', None)

                series_info.append([a['properties']['cardData']['meta'].get('nid', None),
                                a['properties']['cardData']['text'].get('title', None),
                                a['properties']['cardData']['text'].get('description', None),
                                a['properties']['cardData'].get('images', None),
                                link,
                                self.get_seasons(link),
                                self.get_cast(link_to_cast[2], link_to_cast[3])]) #Cast se extrae de otra api (función aparte)
                                                              

            except KeyError:
                print(f'Error al extraer los datos.')
                pass


        self.payloads(series_info, 'series')

        pandas = pd.DataFrame(series_info, columns=['ID', 'Title', 'Descripción', 'Image', 'Url', 'Seasons', 'Cast'])
        links = pandas['Url']

        self.api_each_serie(links)


    def get_cast(self, title, _id):
        '''Esta función recibe como parametros el title e id y hace el request correspondiente,
            el JSON de esta api esta compuesto por varios indices, el 4 es justamente el que tiene
            data del cast, se calcula el len de este indice (ya que varia para distintas series) y
            se recorre extrayendo los nombres, notese que hay series que no tienen cast'''

        cast = []

        try:
            r = self.sesion.get(f'https://content-delivery-gw.svc.ds.amcn.com/api/v2/content/amcn/wetv/url/shows/{title}/explore--{_id}')
            data = r.json()
            try:
                content = data['data']['children'][4]['children']
                for c in content:
                    cast.append(c['properties']['cardData']['text']['title'])
                    
            except KeyError as e:
                print(f'Error {title}, no se encuentra la key {e}, no hay cast disponible.')
                pass
        except:
            pass
        
        if not cast:
            return None
        else:
            return cast
    
    def get_seasons(self, url):
        '''Funcion que obtiene len de series'''

        content = None
        try:
            r = self.sesion.get(f'https://content-delivery-gw.svc.ds.amcn.com/api/v2/content/amcn/wetv/url{url}')
            data = r.json()
            try:
                content = data['data']['children'][3]['properties']['dropdownItems']

            except KeyError as e:
                print(f'Error, no se encuentra la key {e}, no hay seasons disponibles.')
                pass
        except:
            pass
        
        if content:
            return int(len(content))
        else:
            return None
            

    def api_each_serie(self, links):
        '''Esta funcion hace request a la api general de cada serie, 
            las cuales presentan las url de las distintas temporadas,
            estas url serian usadas por la funcion get_final_episodes
            para extraer toda la data de cada episodio.'''

        list_parameters = []

        for link in links:
            try:
                r = self.sesion.get(f'https://content-delivery-gw.svc.ds.amcn.com/api/v2/content/amcn/wetv/url{link}')
                data = r.json()

                _id = [int(s) for s in link.split('--') if s.isdigit()]
                try:
                    #list_parameters.append(data['data']['children'][2]['properties']['tabs'][0]['permalink']) #Extrae parametro
                    content = data['data']['children'][3]['properties']['dropdownItems']
                    for data in content:
                        list_parameters.append([_id[0],
                                    data['properties'].get('nid', None),
                                    data['properties'].get('title', None),
                                    data['properties'].get('permalink', None)])

                except Exception as e:
                    print(f'Error al extraer datos de {r.url}, no cuenta con episodios.')
                    pass

            except Exception as e:
                print(e)
                pass
        
        
        self.get_final_episodes(list_parameters)

    '''def get_pre_episodes(self, parameters):

        FUNCIÓN REEMPLAZADA POR GET EPISODES.

        list_parameters = []

        for parameter in parameters:
            try:
                r = self.sesion.get(f'https://content-delivery-gw.svc.ds.amcn.com/api/v2/content/amcn/wetv/url{parameter}')
                data = r.json()
                content = data['data']['children'][3]['properties']['dropdownItems']

                for a in content:
                    list_parameters.append(a['properties']['permalink'])
                     
            except KeyError: #Algunas no tienen series por lo tanto no hay dropdownItems
                pass
        
        
        self.get_final_episodes(list_parameters)'''

    def get_final_episodes(self, parameters):
        '''Finalmente esta función es la que obtiene los datos de todas las seasons'''

        list_final_episodes = []

        for parameter in parameters:
            try:
                r = requests.get(f'https://content-delivery-gw.svc.ds.amcn.com/api/v2/content/amcn/wetv/url{parameter[3]}')
                data = r.json()

                content = data['data']['children'][4]['children']
                titles = data['data']['children'][1]['properties']['cardData']['text']

                for a in content:

                    season_episode = a['properties']['cardData']['text'].get('seasonEpisodeNumber', None)

                    list_final_episodes.append([parameter[0], #Id serie
                                                titles.get('showTitle', None), #Titulo serie
                                                parameter[1], #Id season
                                                parameter[2], #Title Season
                                                titles.get('description', None), #Descripcion season
                                                parameter[3], #Link season
                                                a['properties']['cardData']['meta'].get('nid', None), #Id. epi
                                                a['properties']['cardData']['text'].get('title', None), #Title epi
                                                a['properties']['cardData']['text'].get('description', None),
                                                a['properties']['cardData']['meta'].get('permalink', None),
                                                a['properties']['cardData'].get('images', None),
                                                season_episode.split(',')[0].replace('S', '') if season_episode else None,
                                                season_episode.split(',')[1].replace('E', '') if season_episode else None,
                                                int(len(content))]) #len de contenidos (cada contenido es un episode)              
            except KeyError as e:
                print(f'Error campo no encontrado {e} en {r.url}')
                pass
        
        #pandas = pd.DataFrame(list_final_episodes, columns=['ID_serie', 'Title_serie', 'Id_season', 'Title_season', 'Description_season',
                                                            #'Link_season', 'Id_cap', 'Title_cap', 'Descripcion', 'Link_cap', 
                                                            #'Image', 'Season', 'Episode','Episodes_per_season'])
        #pandas.to_excel("series.xlsx",sheet_name='series') Si queremos previsualizar datos


        self.payloads(list_final_episodes, 'episodes')

    
    def payloads(self, list_info, _type=None):
        '''Funcion payloads'''

        payloads = []

        packages = [
                            {
                                "Type": "subscription-vod"
                            }
                        ]

        if _type == 'series':

            list_db_series = Datamanager._getListDB(self, self.titanScraping)


            for serie in list_info:

                payload = {
                                    'PlatformCode':  self._platform_code,
                                    'Id':            str(serie[0]),
                                    'Title':         serie[1],
                                    'OriginalTitle': None,
                                    'CleanTitle':    _replace(serie[1]),
                                    'Type':          'serie',
                                    'Year':          None,
                                    'Duration':      None,
                                    'Seasons':      serie[5],
                                    'Deeplinks': {
                                        'Web':       'https://www.wetv.com' + serie[4],
                                        'Android':   None,
                                        'iOS':       None,
                                        },
                                    'Playback':      None,
                                    'Synopsis':      serie[2],
                                    'Image':         [serie[3]],
                                    'Rating':        None,
                                    'Provider':      None,
                                    'Genres':        None,
                                    'Cast':          serie[6],
                                    'Directors':     None,
                                    'Availability':  None,
                                    'Download':      None,
                                    'IsOriginal':    None,
                                    'IsAdult':       None,
                                    'Packages':      packages,
                                    'Country':       None,
                                    'Timestamp':     datetime.now().isoformat(),
                                    'CreatedAt':     self._created_at
                    }

                Datamanager._checkDBandAppend(self, payload, list_db_series, payloads)

            Datamanager._insertIntoDB(self, payloads, self.titanScraping)
        
        elif _type == 'episodes':

            list_db_episodes = Datamanager._getListDB(self, self.titanScrapingEpisodios)

            for epi in list_info:

                payload_epi =   {      
                                    "PlatformCode":  self._platform_code,      
                                    "Id":            int(epi[6]), 
                                    "ParentId":      int(epi[0]), 
                                    "ParentTitle":   epi[1],  
                                    "Episode":       int(epi[12]) if epi[12] else None,   
                                    "Season":        int(epi[11]) if epi[11] else None, 
                                    "Seasons":       [ 
                                                        {
                                                        "Id": epi[2],           
                                                        "Synopsis": epi[4],     
                                                        "Title": epi[1] + ' ' + epi[3],        
                                                        "Deeplink":  'https://www.wetv.com' + epi[5] if epi[5] else None,    
                                                        "Number": None,       
                                                        "Year": None,         
                                                        "Image": None, 
                                                        "Directors": None,   
                                                        "Cast": None,       
                                                        "Episodes": int(epi[13]) if epi[13] else None,      
                                                        "IsOriginal": None    
                                                        },
                        
                                    ],
                                    "Title":         epi[7],      
                                    "CleanTitle":    _replace(epi[7]),      
                                    "OriginalTitle": epi[7],                          
                                    "Type":          'serie',           
                                    "Year":          None,          
                                    "Duration":      None,      
                                    "ExternalIds":   None,      
                                    "Deeplinks": {          
                                        "Web":       'https://www.wetv.com' + epi[9] if epi[9] else None,                 
                                        "Android":   None,          
                                        "iOS":       None,      
                                    },      
                                    "Synopsis":      epi[8],      
                                    "Image":         ['https://www.wetv.com' + epi[10]] if epi[10] else None,      
                                    "Rating":        None,           
                                    "Provider":      None,      
                                    "Genres":        None,          
                                    "Cast":          None,      
                                    "Directors":     None,          
                                    "Availability":  None,           
                                    "Download":      None,      
                                    "IsOriginal":    None,          
                                    "IsAdult":       None,    
                                    "IsBranded":     None,    
                                    "Packages":      packages,          
                                    "Country":       None,      
                                    "Timestamp":     datetime.now().isoformat(),       
                                    "CreatedAt":     self._created_at, 
                                }


                Datamanager._checkDBandAppend(self, payload_epi, list_db_episodes, payloads)

            Datamanager._insertIntoDB(self, payloads, self.titanScrapingEpisodios)
        
        else:
            print('Error, argumento desconocido, instancie el metodo con argumento series o episodes.')

    def get_response(self, url):

        '''Esta función hace la conexion a la api principal y arroja los errores en caso de haberlos. 
        Si request.status_code == 200 pasa a la funciones que obtienen info.'''
        
        r = self.sesion.get(url)
        
        if r.status_code == 200:
            self.get_principal_info(r)

        elif r.status_code == 301:
            print(f'Error {r.status_code}, el servidor esta tratando de redirigirte hacia otra URL. Quizas haya cambiado de dominio.')
            
        elif r.status_code == 400:
            print(f'Error {r.status_code}, request erroneo, por favor verificar sintaxis y url escrita.')
            
        elif r.status_code == 401:
            print(f'Error {r.status_code}, autenticación incorrecta, verifica credenciales o token.')
            
        elif r.status_code == 403:
            print(f'Error {r.status_code}, no tienes permiso para ver el/los recurso/s.')
            
        elif r.status_code == 404:
            print(f'Error {r.status_code}, recurso no encontrado.')

        elif r.status_code == 503:
            print(f'Error {r.status_code}, el servidor no pudo procesar la petición.')

        else:
            print(f'Error {r.status_code}.')
