import time
import re
import requests
import json
import html
from handle.payload import Payload
from bs4 import BeautifulSoup
from common import config
from datetime import datetime
from handle.mongo import mongo
from updates.upload import Upload
from handle.datamanager import Datamanager
from handle.replace import _replace
from handle.payload import Payload
from handle.datamanager import RequestsUtils

class TvNow():
    """
        - DATOS IMPORTANTES:
            - VPN: NO
            - ¿Usa Selenium?: NO
            - ¿Tiene API?: SI
            - ¿Usa BS4?: NO
            - ¿Se relaciona con scripts TP? NO
            - ¿Instancia otro archivo de la carpeta "platforms"?: NO
            - ¿Cuanto demoró la ultima vez (08/06/2021)? 1hr30min
            - ¿Cuantos contenidos trajo la ultima vez (08/06/2021)? 389 contenidos | 36050 episodios
        
        OTROS COMENTARIOS:
            El script se divide en prescraping y scraping, el primero se encarga
            de hacer todas las requests (detallado en la docu del método) y el segundo
            hace el procesamiento de los datos que vienen 'crudos' para cargarlos
            en la lista de payloads y subirlos a la BBDD.
            Packages: De momento es todo svod, si bien en la página los contenidos
            se diferencian entre "Premium" y no, cuando uno quiere acceder a esos 
            contenidos no premium si o si te pide ingresar con suscripción (08/06/2021)
    """
    def __init__(self, ott_site_uid, ott_site_country, operation):
        self._config                = config()['ott_sites'][ott_site_uid]
        self._platform_code         = self._config['countries'][ott_site_country]
        self._created_at            = time.strftime("%Y-%m-%d")
        
        # urls:
        self.start_url = self._config['urls']['start_url']
        self.contents_api = self._config['urls']['contents_api']
        self.serie_info = self._config['urls']['serie_info']
        self.season_list = self._config['urls']['season_list']
        self.movie_info = self._config['urls']['movie_info']
        self.annual_season = self._config['urls']['annual_season']
        self.common_season = self._config['urls']['common_season']

        self.mongo                  = mongo()
        self.titanPreScraping       = config()['mongo']['collections']['prescraping']
        self.titanScraping          = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodios  = config()['mongo']['collections']['episode']

        self.skippedTitles = None
        self.skippedEpis = None

        self.scraped = Datamanager._getListDB(self,self.titanScraping)
        self.scraped_epi = Datamanager._getListDB(self,self.titanScrapingEpisodios)
        self.payloads = []
        self.payloads_epi = []

        self.packages = [{'Type': 'subscription-vod'}]

        self.sesion                 = requests.session()
        self.country_code           = ott_site_country
        self.addheader              = False
        self.req_utils              = RequestsUtils()
        
        if operation == "return":
            params = {"PlatformCode": self._platform_code}
            lastItem = self._mongo.lastCretedAt(self._titanScraping, params)
            if lastItem.count() > 0:
                for lastContent in lastItem:
                    self._created_at = lastContent["CreatedAt"]
            self._prescraping()
            self._scraping()

        elif operation == "scraping":
            self._prescraping()
            self._scraping()

        elif operation == 'testing':
            prescraping_list = self.mongo.search(
            self.titanPreScraping, {'PlatformCode': self._platform_code})
            if not prescraping_list:
                self._prescraping() 
            self._scraping(testing=True)

        else:
            print("Operacion no valida.")
            return

    def __query_field(self, collection, field, extra_filter=None):
        if not extra_filter:
            extra_filter = {}

        find_filter = {
            'PlatformCode': self._platform_code,
            'CreatedAt': self._created_at,
        }

        find_filter.update(extra_filter)

        query = self.mongo.db[collection].find(
            filter=find_filter,
            projection={
                '_id': 0,
                field: 1,
            },
            no_cursor_timeout=False
        )

        query = {item[field] for item in query}

        return query

    def _prescraping(self):
        """Este método hace todas las requests por api necesarias para 
        almacenar los prepayloads de contenidos diferenciando su tipo:
        movie, serie o episodio.

        - La plataforma usa apis para traer la informacion
        - Tiene una apì donde trae los contenidos desde la 0-9 a la z 
        - Dependiendo el tipo de contenido se hacen mas o menos requests, las peliculas
        necesitan una request extra mientras que las series necesitan 2 (una para los datos
        y otra para obtener los nros de temporadas)
        """
        deleted = self.mongo.delete(self.titanPreScraping, {
                                    'PlatformCode': self._platform_code})
        print('############################################')
        print(f'### Se eliminaron {deleted} items de PreScraping ###')
        print('############################################')

        contents = self.get_contents()
        print('Obtenidos {} contenidos en total\n'.format(len(contents)))

        counter = 1
        for content in contents:
            content['Id'] = content['url'].split('-')[-1] # la url tiene /nombre-bla-bla-id, me quedo con la ultima componente de la lista
            if content['type'] == 'film':
                # MOVIES
                content['Type'] = 'movie'
                content['InfoURL'] = self.movie_info + content['Id']
            else:
                # SERIES 
                content['Type'] = 'serie'
                content['InfoURL'] = self.serie_info + content['url']
                # Traigo los datos de las temporadas mediante async_requests
                season_list_url = self.season_list + content['Id']
                print('Trayendo episodios de: {}'.format(content['name']))
                seasons_data = Datamanager._getJSON(self, season_list_url, showURL=False)

                if not seasons_data.get('items'):
                    # Si no tiene temporadas se saltea la serie
                    continue
                if seasons_data['navigationType'] == 'annual':
                    seasons_urls = []
                    for year in seasons_data['items']: 
                        # los "items" son años en los que se estrenaron las temporadas
                        for month in year['months']:
                            season_url = self.annual_season.format(
                                id_=content['Id'],
                                year=year['year'],
                                month=month['month'])
                            seasons_urls.append(season_url)
                else:
                    seasons_urls = [self.common_season.format(
                        id_=content['Id'], season=item['season']) for item in seasons_data['items']]

                seasons = self.req_utils.async_requests(seasons_urls)
                episodes = []
                for season in seasons:
                    season_data = season.json()
                    for episode in season_data['items']:
                        epi_prepayload = {
                            'PlatformCode': self._platform_code,
                            'ParentTitle': content['name'],
                            'ParentId': content['Id'],
                            'JSON': episode,
                            'Type': 'episode'
                        }
                        episodes.append(epi_prepayload)
                self.mongo.insertMany(self.titanPreScraping, episodes)
                print(f"   ∟{len(episodes)} episodios insertados en PreScraping")
            
            content_json = Datamanager._getJSON(self, content['InfoURL']) 
            content_prepayload = {
                'PlatformCode': self._platform_code,
                'Id': content['Id'],
                'Title': content['name'],
                'Type': content['Type'],
                'JSON': content_json,
                'DeeplinkWeb': self.start_url + content['url'],
                'Genres': content['genres']
            }

            self.mongo.insert(self.titanPreScraping, content_prepayload)
            print('\x1b[1;32;40mINSERTADO EN PRESCRAPING: \x1b[0m {} ({}/{})'.format(
                content['name'], counter, len(contents)))
            counter += 1

    def _scraping(self, testing=False):
        """Este método se ejecuta una vez que se obtuvieron todos los
        datos mediantes las requests. La función principal es procesar
        la metadata e insertar en la BBDD los payloads generados.
        """
        prescraping_list = self.mongo.search(
           self.titanPreScraping, {'PlatformCode': self._platform_code})

        for item in prescraping_list:
            if item['Type'] == 'episode':
                payload = self.build_epi_payload(item)
            elif item['Type'] == 'serie':
                payload = self.build_serie_payload(item)
            else:
                payload = self.build_movie_payload(item)

        Datamanager._insertIntoDB(self, self.payloads, self.titanScraping)
        Datamanager._insertIntoDB(self, self.payloads_epi, self.titanScrapingEpisodios)

        self.sesion.close()
        Upload(self._platform_code, self._created_at, testing=testing)

        if not testing:
            deleted = self.mongo.delete(
                self.titanPreScraping, {'PlatformCode': self._platform_code})
            print(f'Se eliminaron {deleted} items de PreScraping')

    def build_movie_payload(self, movie):
        """En base a un prepayload este método
        genera un Payload de tipo movie y lo
        ingresa a la lista de payloads.
        """
        payload = Payload()
        payload.createdAt = self._created_at
        payload.platform_code = self._platform_code
        payload.packages = self.packages
        payload.title = movie['Title']
        payload.id = movie['Id']
        payload.genres = movie['Genres'] if movie['Genres'] else None
        payload.deeplink_web = movie['DeeplinkWeb']

        movie_data = movie['JSON']
        # Si el JSON vino vacío significa que la pelicula no cuenta
        # con estos datos:
        if movie_data:
            cast = [actor['name'] for actor in movie_data['cast']]
            payload.cast = cast if cast else None
            payload.synopsis = movie_data['description']
            payload.image = [movie_data['image']] if movie_data['image'] else None

            info = movie_data['info']
            info = info.split('|')
            # Formato: COUNTRY, YEAR | DURACIÓN, RATING
            payload.year = info[0].split(',')[-1]                
            country = [info[0].split(',')[0].strip()] if [info[0].split(',')[0]] != [] else None
            # Quitale año a los countries.
            for c in country:
                try:
                    num = int(c)
                    country = None
                except:
                    pass
            payload.country = country

            duration_filtered = info[1].split(',')[0]
            payload.duration = int(re.findall(r'\d+', duration_filtered)[0])
            payload.rating = info[1].split(',')[-1].split(' ')[-1]

        Datamanager._checkDBandAppend(
            self, payload.payload_movie(), self.scraped, self.payloads)
 
    def build_serie_payload(self, serie):
        """En base a un prepayload este método
        genera un Payload de tipo serie y lo
        ingresa a la lista de payloads.
        """
        payload = Payload()
        payload.createdAt = self._created_at
        payload.platform_code = self._platform_code
        payload.packages = self.packages
        payload.title = serie['Title']
        payload.id = serie['Id']
        payload.genres = serie['Genres'] if serie['Genres'] else None
        payload.deeplink_web = serie['DeeplinkWeb']

        serie_data = serie['JSON']
        information_text = serie_data['seo']['text']
        if 'Schauspieler' in information_text:
            """
            Algunas series tiene los actores en una tabla y como la informacion aparece en formato html tengo que modificarlo.
            Para eso me quedo con la palabra actores en aleman (Schauspieler) ya que tambien esta el rol (Role en aleman)
            entonces filtro por actores y me quedo con la parte de final que tiene los datos, despues filtro por la etiqueta de
            cierre de la tabla y me quedo con la primera parte
            """
            # son los actores en una lista, tambien estan los roles. Los rol se encuentra en posiciones pares y los actores
            # en posiciones impar. Me quedo con lo impar
            actores_lista = information_text.split('Schauspieler')[1].split('</table>')[0].split('</td>')
            actores = []
            i = 1
            while i < len(actores_lista):
                actores.append(self.clean_actores(self.clean_html(actores_lista[i])))
                i=i+2
            payload.cast = actores if actores else None

        # Algunas series traen la descripción separada en párrafos, en esos casos me quedo solo con el primero.
        if '<p>' in information_text:
            synopsis = information_text.split('</p>')[0]
            synopsis = self.clean_html(synopsis)
            payload.synopsis = synopsis
        else:
            payload.synopsis = self.clean_html(information_text)

        payload.image = [serie_data['seo']['jsonLd']['webpage']['image']['url']]

        # En el JSON de las series puedo determinar si es original de la plataforma:
        payload.is_original = 'Original' in serie_data['reporting']['classification']

        Datamanager._checkDBandAppend(
            self, payload.payload_serie(), self.scraped, self.payloads)

    def build_epi_payload(self, episode):
        """En base a un prepayload este método
        genera un Payload de tipo episodio y lo
        ingresa a la lista de payloads.
        """
        payload_epi = Payload()
        payload_epi.createdAt = self._created_at
        payload_epi.platform_code = self._platform_code
        payload_epi.packages = self.packages
        payload_epi.parent_id= episode['ParentId']
        payload_epi.parent_title = episode['ParentTitle']
        epi_data = episode['JSON']

        payload_epi.id = str(epi_data['id'])

        if not epi_data['ecommerce'].get('teaserEpisodeName'):
            # Si el episodio no tiene titulo se saltea
            return
        else:
            payload_epi.title = epi_data['ecommerce']['teaserEpisodeName']
  
        # TRY/CATCH EPI & SEASON NUMBER
        try:
            season = epi_data['ecommerce']['teaserSeason']
            payload_epi.season = int(re.findall(r'\d+', season)[0])
        except:
            payload_epi.season = None
        try:
            episode = epi_data['ecommerce']['teaserEpisodeNumber']
            payload_epi.episode = int(re.findall(r'\d+', episode)[0])
        except:
            payload_epi.episode = None

        img = [image['src'] for image in epi_data['images']]
        payload_epi.image = img if img else None
        payload_epi.deeplink_web = self.start_url + epi_data['url']

        Datamanager._checkDBandAppend(
            self, payload_epi.payload_episode(), self.scraped_epi, self.payloads_epi,
            isEpi=True)

    # -- AUXILIARES --
    def clean_html(self, string):
        """Este método borra todos los tags html (cualquiera contenido entre
        < o >). Como la plataforma es alemana también utiliza el unescape 
        para recuperar los caracteres especiales como ß, ä, entre otros.
        """
        string = re.sub('<.+?>', '', string)
        string = html.unescape(string)
        return string.strip()    

    def clean_actores(self, string):    
        return string.split('(')[0].strip()
    
    def get_contents(self):
        """Este método hace una request a una API que tiene un
        listado de todos los contenidos de la plataforma agrupados 
        y ordenados alfabéticamente.

        Return: [dict] - lista con dicts de contenidos.
        """
        contents = []
        contents_list = Datamanager._getJSON(self, self.contents_api)
        for content_group in contents_list:
            contents.extend(content_group['formats'])
        return contents