import pymongo
import argparse
import os
import sys
try:
    from root import servers
    from settings import settings
except ModuleNotFoundError:
    path = os.path.abspath('.')
    sys.path.insert(1, path)
    from root import servers
    from settings import settings
from updates.upload import Upload
from pymongo.errors import ConnectionFailure
from pymongo.errors import ExecutionTimeout
from difflib import SequenceMatcher

"""
KAJI TO MISATO ٩(˘◡˘)۶
COMO USAR: python handle/kaji_to_misato.py --platform --server --testing
--platform ---> puede ser una plataforma, ej: mx.canelatv o multiples, ej: mx.canelatv us.canelatv us.mubi (SOLAMENTE SEPARAR)
--server por DEFAULT KAJI (No especificar)
--testing True/False, por DEFAULT esta en TRUE asi que va a hacer el upload

"""

class MongoDB():
    """
    Clase que nos conecta al servidor Kaji o Localhost y devuelve
    el ""cursor", también detecta que tengamos MongoDB corriendo
    si no lanza un ConnectionFailure
    """
    def __init__(self):
        self.servers = servers
        self.settings = settings

    def server(self, name='kaji'):
        try:
            if name == 'kaji':
                print('--- CONNECTING TO KAJI ---\n')
                ssh_connection = servers.KajiConnection()
                server = ssh_connection.connect()
                cursor = pymongo.MongoClient('127.0.0.1', server.local_bind_port)
            elif name == 'localhost':
                cursor = pymongo.MongoClient('mongodb://localhost:27017')
            if 'cursor' in locals():
                return cursor
            else:
                raise AssertionError('--- NOMBRE DE SERVER INCORRECTO (KAJI/LOCALHOST) ---')
        except ConnectionFailure:
            print(f'--- CONNECTION FAILURE TO {name.upper()}: PLEASE CHECK IF MONGODB IS RUNNING ---')

class Collections():
    """
    Clase que dependiendo el argumento name
    llama a la clase MongoDB para que devuelva la conexión
    a kaji o local, luego busca en la db titan (en un futuro deberia hacerse para mas db) 
    las colecciones tScraping y tScrapingEpisodes, conecta y devuelve el cursor
    """
    def __init__(self):
        self.mongo = MongoDB()
        self.kaji_settings = settings.KAJI_SERVER_NAME
    
    def connect(self, name='kaji'):
        cursor = self.mongo.server(name=name)
        db = 'titan'
        if not db in cursor.list_database_names():
            raise Exception(f'--- ATENCIÓN: DB {db.upper()} NO EXISTE EN {name.upper()} ---')

        print(f'--- SERVER --> {name.upper()} --> DB -> {db.upper()} --- \n')
        database = cursor[db]
        content_collection = 'titanScraping'
        epi_collection = 'titanScrapingEpisodes'
        if not content_collection in database.collection_names(include_system_collections=False):
            raise Exception(f'--- ATENCIÓN: COLLECTION {content_collection.upper()} NO EXISTE EN {db.upper()} ---')

        collection = database[content_collection]
        print(f'--- DB --> {name.upper()} --> COLLECTION -> {content_collection.upper()} ---\n')
        episodes_collection = database[epi_collection]
        print(f'--- DB --> {name.upper()} --> COLLECTION -> {epi_collection.upper()} ---\n')
        return collection,  episodes_collection

class Export():
    """
    Clase principal que hace todo el trabajo, recibe el nombre del server,
    instancia a las demas clases para que retornen el cursor y ejecuta metodo
    search().
    search() ---> En un principio si pusimos mal el platform va a hacer una busqueda
    en Kaji y nos va a dar una lista para que eligamos el correcto, y asi llamar de vuelta a 
    la clase con el platformname correcto.
    En caso de que la data en Kaji ya este en Local (filtra por platformcode y createdAt),
    nos da la opción de uplodear desde local, o desde kaji.
    Si elegimos Kaji, hace un delete de la data local, baja de Kaji a local y uplodea.
    En al caso de que no haya data en local desde un principio, se hace un Upload normal
    de la data en Kaji (bajandola a local primero y luego uplodeando).
    Este metodo siempre tiene en cuenta si la ott tiene series o no.
    """
    def __init__(self, platformcodes, server, testing):
        self.collections = Collections()
        self.server = server
        self.platformcodes = platformcodes
        self.collection, self.epi_collection = self.collections.connect(self.server)
        self.testing = testing
        self.search()

    def search(self):
        for number, platform in enumerate(self.platformcodes):
            print(f'\n--- EN PROCESO: {platform} ---\n')
            kaji_data = self.collection.find({'PlatformCode': platform})
            if kaji_data.count() == 0:
                names = self.collection.distinct('PlatformCode')
                matches = []
                for name in names:
                    if SequenceMatcher(None, platform, name).ratio() > 0.90:
                        matches.append(name)
                print(f'PLATFORM NO ENCONTRADA {platform}, QUIZAS QUISITE ESCRIBIR?:\n')
                for index, name in enumerate(matches):
                    print(index, ': ', name)
                choise = input('\nINGRESAR NUMERO DE PLATFORMCODE CORRECTO O EXIT SI NINGUNO CONCUERDA: ').lower().strip()
                if choise == 'exit':
                    continue
                else:
                    try:
                        index = int(choise)
                        platformcodes[number] = matches[index]
                        Export(platformcodes, server, self.testing)
                    except ValueError:
                        print('--- ARGUMENTO ERRONEO (DEBE SER INT) ---')
            else:
                kaji_episodes = self.epi_collection.find({'PlatformCode': platform})
                created_at = kaji_data.distinct('CreatedAt')[0]
                tscraping, tscraping_episodes = self.collections.connect(name='localhost')
                local_data = tscraping.find({'PlatformCode': platform, 'CreatedAt':created_at})
                if local_data.count() > 0:
                    types = local_data.distinct('Type')
                    print(f'--- DATA YA SE ENCUENTRA EN LOCAL {platform} ---')
                    choice = input('¿DESEA SUBIR LA DATA DESDE LOCAL? (YES, NO): ').lower().strip()
                    if choice == 'yes':
                        if 'serie' in types:
                            has_episodes = True
                        else:
                            has_episodes = False
                        self.upload(platform, created_at, self.testing, has_episodes=has_episodes)
                        continue
                    else:
                        print('\n--- CONTINUING ---')
                        tscraping.delete_many({'PlatformCode': platform, 'CreatedAt':created_at})
                        if 'serie' in types:
                            tscraping_episodes.delete_many({'PlatformCode': platform, 'CreatedAt':created_at})
                        print('--- DELETING LOCAL DATA ---')
                        self.insert_local(tscraping, tscraping_episodes, kaji_data, kaji_episodes, platform, created_at)
                        continue
                else:
                    self.insert_local(tscraping, tscraping_episodes, kaji_data, kaji_episodes, platform, created_at)
        print('\n--- DONE :) ---')        
        sys.exit(0)

    def insert_local(self, tscraping, tscraping_episodes, kaji_data, kaji_episodes, platform, created_at):
        types = kaji_data.distinct('Type')
        if 'serie' not in types:
            print('--- DATA INSERTED IN LOCAL (tScraping) ---')
            tscraping.insert_many(kaji_data)
            self.upload(platform, created_at, self.testing, has_episodes=False)
        else:
            tscraping.insert_many(kaji_data)
            print('--- DATA INSERTED IN LOCAL (tScraping) ---')
            tscraping_episodes.insert_many(kaji_episodes)
            print('--- DATA INSERTED IN LOCAL (tScrapingEpisodes) ---')
            self.upload(platform, created_at, self.testing)

    def upload(self, platform, created_at, testing, has_episodes=True):
        Upload(platform, created_at, testing=testing, has_episodes=has_episodes)

class ArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        self.print_help(sys.stderr)
        self.exit(2, '%s: error: %s\n' % (self.prog, message))

if __name__ =='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--platform', help = 'Plataforma a exportar', type=str, nargs='*', default=False)
    parser.add_argument('--server', help='Server desde cual exportar', type=str, default='kaji')
    parser.add_argument('--testing', help='Testing T/F', type=bool, default=False)
    args = parser.parse_args()
    if not args.platform:
        raise Exception('--- ESPECIFICAR PLATFORMCODE/S VIA ARGUMENTO --platform ---')
    if args.testing != False and args.testing != True:
        raise Exception('--- ESPECIFICAR True o False (defualt) en --testing ---')
    else:
        platformcodes = args.platform
        server = args.server
        mode = args.testing
        Export(platformcodes, server, mode)