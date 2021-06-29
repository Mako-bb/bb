import time
import requests
from handle.replace import _replace
from common import config
from handle.mongo import mongo
from updates.upload import Upload
from handle.payload import Payload
from handle.datamanager import Datamanager
import datetime
# from time import sleep
import re


class PlutoMI():
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
        self.titanScrapingEpisodes = config()['mongo']['collections']['episode']

        self.api_url = self._config['api_url']
        self.url=self._config['url']

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
        
        self.scraped = self.query_field(self.titanScraping, field='Id')
        self.scraped_episodes = self.query_field(self.titanScrapingEpisodes, field='Id')
        self.payloads = []
        self.episodes_payloads = []
        contents=self.get_contents()
        key_search='_id'
        
        for content in contents:
            if self.isDuplicate(self.scraped,content[key_search])==False:
                self.scraped.append(content[key_search])    
                newPayload = self.get_payload(content)
                self.payloads.append(newPayload)
                print('\n---Contenido ingresado---\n')
                print(content['name']+':'+ content['type'])
            else:
                print('\nContenido ya existe.\n')

        self.insert_payloads_close(self.payloads,self.episodes_payloads)
            

    def get_contents(self):
        url_api = self.api_url
        contents=[]
        response = self.session.get(url_api)
        json_data=response.json()
        for content in json_data["categories"]:
            for item in content["items"]:
                contents.append(item)
        return contents

    def isDuplicate(self, scraped_list, key_search):
        isDup=False
        if key_search in scraped_list:
            isDup = True
        return isDup
    
    def insert_payloads_close(self,payloads,epi_payloads):    
        if payloads:
            self.mongo.insertMany(self.titanScraping, payloads)
            print('\nSe cargaron nuevos registros de peliculas y series\n')
        if epi_payloads:
            self.mongo.insertMany(self.titanScrapingEpisodes, epi_payloads)
            print('\nSe cargaron nuevos registros de peliculas y series\n')
        self.session.close()
        Upload(self._platform_code, self._created_at, testing=True)
    
    def get_payload(self, content):
        payload = self.generic_payload(content)
        if content['type']=='movies':
            seconds=content['duration']
            minutes=seconds/60
            duration= int(minutes)
            payload['Duration']=duration
        elif content['type']=='series':
            payload['Type']= 'serie'
            payload['Seasons']= len(content['seasonsNumbers'])
            payload['Playback']=None
            payload['Duration']=None
            parent_id=content['_id']
            parent_title=content['name']
            series_api='https://service-vod.clusters.pluto.tv/v3/vod/series/{}/seasons?advertisingId=&appName=web&appVersion=5.17.1-be7b5e79fc7cad022e22627cbb64a390ca9429c7&app_name=web&clientDeviceType=0&clientID=820bd17e-1326-4985-afbf-2a75398c0e4e&clientModelNumber=na&country=AR&deviceDNT=false&deviceId=820bd17e-1326-4985-afbf-2a75398c0e4e&deviceLat=-39.0576&deviceLon=-67.5301&deviceMake=Firefox&deviceModel=web&deviceType=web&deviceVersion=89.0&marketingRegion=VE&serverSideAds=true&sessionID=1ddd9448-d514-11eb-b85e-0242ac110002&sid=1ddd9448-d514-11eb-b85e-0242ac110002&userId=&attributeV4=foo'.format(parent_id)
            self.episodes_payload(series_api,parent_id,parent_title)
        return payload

    def generic_payload(self, content):
        genericPl={
                "PlatformCode": self._platform_code,
                "Id": content['_id'],
                "Title": content['name'],
                "CleanTitle": _replace(content['name']),
                "OriginalTitle": content['slug'],
                "Type": content['type'],
                "Year": None,
                "Duration": None,
                "ExternalIds": None,
                "Deeplinks": {
                    "Web": self.get_deepLinks(self.url,content),
                    "Android": None,
                    "iOS": None,
                },
                "Synopsis": content['summary'],
                'Image': None,
                "Rating": content['rating'],
                "Provider": None,
                "Genres": [content['genre']],
                "Cast": None,
                "Directors": None,
                "Availability": None,
                "Download": None,
                "IsOriginal": None,
                "IsAdult": None,
                "IsBranded": None,
                "Packages": [{'Type': 'free-vod'}],
                "Country": ["AR"],#self.ott_site_country,
                "Timestamp":datetime.datetime.now().isoformat(),
                "CreatedAt": self._created_at,
        }
        return genericPl
    

    def episodes_payload(self,series_api,parent_id,parent_title):
        response_episodes = self.session.get(series_api).json()
        key_search='_id'
        for seasonValue in response_episodes['seasons']:
            for epValue in seasonValue['episodes']:
                if self.isDuplicate(self.scraped_episodes,epValue[key_search])==False:
                    seconds=epValue['duration']
                    minutes=seconds/60
                    duration= int(minutes) 
                    episode = {
                        'PlatformCode':self._platform_code,
                        'ParentId': parent_id,
                        'ParentTitle': parent_title,
                        'Id': epValue['_id'] ,
                        'Title':epValue['name'] ,
                        'Episode':epValue['number'],
                        'Season': epValue['season'],
                        'Year': None,
                        'Image':None ,
                        'Duration': duration,
                        'Deeplinks':{
                            'Web':self.get_deepLinks(self.url,epValue),
                            'Android': None,
                            'iOS':None ,
                        },
                        'Synopsis':epValue['description'],
                        'Rating':epValue['rating'] ,
                        'Provider':None ,
                        'ExternalIds': None,
                        'Genres': [epValue['genre']],
                        'Cast':None ,
                        'Directors':None ,
                        'Availability':None ,
                        'Download':None ,
                        'IsOriginal': None,
                        'IsAdult': None,
                        'Country': ["AR"],#self.ott_site_country,
                        'Packages': [{'Type': 'free-vod'}],
                        'Timestamp': datetime.datetime.now().isoformat(),
                        'CreatedAt': self._created_at,
                    }
                    self.scraped_episodes.append(epValue[key_search])    
                    self.episodes_payloads.append(episode)
                    print('\n---Contenido ingresado---\n')
                    print('{}: Season - {} - Episode {}'.format(epValue['name'],epValue['season'],epValue['number']))
                else:
                    print('\nContenido ya existe.\n')

    def depurate_title(self, title):
        chars=' *,./|&¬!"£$%^()_+{@:<>?[]}`=;¿'
        title=title.lower()#paso el titulo original a minusculas
        if '-' in title:#primero elimino los guiones que vengan con el titulo original
            title=title.replace('-'," ")
        for c in chars:#luego elimino el resto de los caracteres especiales
            title=title.replace(c,'-')
        if "'" in title:#elimino los apostrofes simples que quedan fuera de la lista de caracteres especiales, este paso quizas se pueda evitar de otro modo.
            title=title.replace("'","")
        return title
    
    def get_deepLinks(self,url,content):
        content_title=_replace(content['name'])
        clean_title= self.depurate_title(content_title)
        if 'season' in content:
            deeplink=self.url+'{}/{}/{}/{}/{}/{}'.format('series',clean_title,'seasons',content['season'],'episode',clean_title)
        else:
            deeplink=self.url+'/{}/{}'.format('movies',clean_title)
        return deeplink