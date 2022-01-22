import os
import sys
import json
import pymongo
from common import config
try:
    from root import servers
except ModuleNotFoundError:
    path = os.path.abspath('.')
    sys.path.insert(1, path)
    from root import servers
try:
    import settings
except ModuleNotFoundError:
    path = os.path.abspath('.')
    sys.path.insert(1, path)
from settings import get_logger
from settings import settings

class import_scraping():
    '''
    Este script sirve para traer a tu db local el scraping realizado en el server de la plataforma que desees.
    Se le puede especificar una fecha, sinó por defecto trae el último scraping que se hizo.

    python main.py ClasePlataforma --c CountryCode --o import_scraping --date XXXX-XX-XX

    El argumento --date es OPCIONAL.

    Si agregamos el argumento --testing el script va a bsucar en el sv de testing.
    '''
    JSON_RESOURCES = servers.DICT_RESOURCES_JSON

    def __init__(self, ott_platforms, ott_site_country, provider, logat, testing):
        ''''''
        self.testing = testing
        self.date = logat
        self.client_local = pymongo.MongoClient("mongodb://localhost:27017")
        self.db_local = self.client_local["titan"]
        self.collection_name = 'titanScraping'

        self._config = config()['ott_sites'][ott_platforms]
        self.platform_code = self._config['countries'][ott_site_country]

        if self.testing:#Si se quiere obtener los datos del sv de testing. Se llama al método de conexión directamente
            sv = 'DE-Test1'
            self.connect_mongo(sv)
            quit()

        for sv_location in self.JSON_RESOURCES:
            '''
            Busca todas las plataformas que están en el root, cuando se dá la coincidencia con la que se está
            buscando se llama al método connect_mongo.
            '''
            with open(self.JSON_RESOURCES[sv_location], 'r') as file:
                data = json.load(file)
        
            for item in data:
                    platforms = item['platforms']
                    for platform in platforms:
                        if ott_platforms == 'JustWatch':
                            if platform.get('Provider') == provider[0] and platform['Country'] == ott_site_country:
                                self.connect_mongo(sv_location)
                        else:
                            if platform['PlatformName'] == ott_platforms and platform['Country'] == ott_site_country:
                                self.connect_mongo(sv_location)
        

    def connect_mongo(self, sv_location):
        '''
        Método que hace la conexión al mongo del servidor correspondiente e inserta los datos en
        nuestra DB.
        '''
        username = settings.DEFAULT_USER_DATA
        port = settings.DEFAULT_PORT

        ssh_connection = servers.Connection(server_name=sv_location)

        with ssh_connection.connect(username=username, port=port) as server:#Conexión por ssh al servidor correspondiente
            client   = pymongo.MongoClient('127.0.0.1', server.local_bind_port)#Conexión al mongo del server
            titan = client['titan']
            collections = ['titanScraping', 'titanScrapingEpisodes']

            for collection_name in collections:
                collection = titan[collection_name]
                if self.date:#Si se inserta fecha...
                    if "-" not in self.date:
                        self.date = "{}-{}-{}".format(self.date[0:4],self.date[4:6],self.date[6:8])
                    createdAt = self.date
                else:
                    createdAt = self.get_last_CreatedAt(collection)#Obtengo la fecha mas reciente de scraping.

                payload = {'PlatformCode': self.platform_code, 
                            'CreatedAt': createdAt
                }
                last_contents = collection.find(payload, no_cursor_timeout=True)
                last_contents = [item for item in last_contents]
                
                if last_contents:
                    print('INSERTANDO CONTENIDO EN '+collection_name+' LOCAL .... ESTO PUEDE DEMORAR UN RATO')
                    self.db_local[collection_name].insert_many(last_contents)
                else:
                    print('NO EXISTE CONTENIDO DE {} PARA LA FECHA {}'.format(self.platform_code, createdAt))
                    break

                print('####     INSERTADOS '+str(len(last_contents))+' CONTENIDOS     ####')

            if self.date and last_contents:
                print('OBTENIDOS LOS DATOS DE {} DEL {}'.format(self.platform_code, createdAt))
            elif last_contents:
                print('OBTENIDOS LOS ULTIMOS DATOS DE {} :)'.format(self.platform_code))

        
    def get_last_CreatedAt(self, collection):
        '''
        Obtengo el último createdAt de la plataforma. Así aseguramos siempre traer el último scraping.
        '''
        try:
            self.platform_code.get('platform_code')
            payload_api = {'PlatformCode': self.platform_code.get('platform_code')}
            self.platform_code = self.platform_code.get('platform_code')
        except:
            payload_api = {'PlatformCode': self.platform_code}
        sort   = [("CreatedAt", pymongo.DESCENDING)]#Filtra de manera descendente
        last_item = collection.find(payload_api, limit=1).sort(sort)
        if collection.count_documents(payload_api, limit = 1) > 0:
            for lastContent in last_item:
                created_at = lastContent['CreatedAt']
        return created_at