import csv
import os
import sys
import time
import pymongo
import argparse
import hashlib
try:
    from root import servers
except ModuleNotFoundError:
    path = os.path.abspath('.')
    sys.path.insert(1, path)
    from root import servers


SEPARATOR = "___SEPARATOR___"


def show_help():
    print('Este script sirve para comparar dos dbs.') 
    print('Pueden estar como archivos csv o se puede buscar los datos al mongo local y api presence(u otra fecha del local si se pone a comparar dos fechas)')
    print('Imprime en pantalla los match entre ambos. ')
    print('Como opcional tambien imprime los contenidos que no estan en el otro.\n')
    print('Para ejecutar:\n')
    print('\tSi se comparan csv:\n')
    print('\t\033[1mpython handle/comparar_scraping.py archivo1.csv archivo2.csv --col NombreColumna --sinMatch --export NombreArchivo\033[0m\n')

    print('\t\tcsv1 y csv2 son los archivos a comparar')
    print('\t\t\033[1m--col\033[0m\t\t columna del csv a comparar (por default es Title)')
    print('\t\t\033[1m--sinMatch\033[0m\t es un opcional para imprimir lista de los contenidos diferentes')
    print('\t\t\033[1m--export\033[0m\t es un opcional para que guarde lo que se imprime en pantalla en un .log\n')
    
    print('\tSi se busca la data de mongo:\n')
    print('\t\033[1mpython handle/comparar_scraping.py PlatformCode --at CreatedAt --col NombreColumna --sinMatch --mongo --export NombreArchivo --epis\033[0m\n')
    print('\t\tPlatformCode inclyendo iso-code.')
    print('\t\t\033[1m--at\033[0m\t\t Fecha del scraping local en formato YYYY-MM-DD (por default, la ultima del mongo local). \n\t\t\t\t Si son 2 fechas diferentes se compara el scraping de local de una fecha y de otra, en vez de local y apiPresence:')
    print('\t\t\t\t python handle/comparar_scraping.py PlatformCode --at \033[1mCreatedAt1 CreatedAt2\033[0m --col NombreColumna --sinMatch --mongo --export NombreArchivo --epis\n')
    print('\t\t\033[1m--col\033[0m\t\t columna a comparar (por default es Title)')
    print('\t\t\033[1m--sinMatch\033[0m\t es un opcional para imprimir lista de los contenidos diferentes')
    print('\t\t\033[1m--mongo\033[0m\t\t necesario para que busque en la db')
    print('\t\t\033[1m--export\033[0m\t es un opcional para que guarde lo que se imprime en pantalla en un .log')
    print('\t\t\033[1m--epis\033[0m\t\t es un opcional para que compare episodios ')


class Comparator():

    def __init__(self, dbs_or_source_files: list, column: str, use_mongo: bool, list_created_at=[], epis=False) -> None:
        self.__dbs_source_file = dbs_or_source_files
        self.__column = column
        self.__use_mongo = use_mongo
        self.__list_created_at = list_created_at
        self.__epis = epis
        self.__file = None

    def compare(self, export_file: str, sin_match=False) -> None:
        if export_file:
            if '.' in export_file:
                print('El nombre del archivo no puede contenter puntos')
                self.__file = None
            else:
                export = export_file + '.log'
                self.__file = open(export, 'w', encoding="utf-8")
        else:
            self.__file = None

        column = self.__column
        dbs = self.__dbs_source_file
        list_created_at = self.__list_created_at
        epis = self.__epis

        if self.__use_mongo:
            self.compare_mongo_db(dbs, list_created_at, column, sin_match, self.__file, epis)
        else:
            if len(self.__dbs_source_file) == 2:
                filename_tp = dbs[0]
                filename_scraping = dbs[1]
                self.compare_csv(filename_tp, filename_scraping, column, sin_match, export_file)
            else:
                print('Solo se pueden comparar 2 csv')

        if self.__file:
            self.__file.close()

    @classmethod
    def compare_csv(cls, filename_tp: str, filename_scraping: str, col: str, sin_match=False, export_file=None) -> None:
        titulos_tp = []
        with open(filename_tp, newline='', encoding='utf-8') as csvfile:
            lector = csv.reader(csvfile)
            headers = lector.__next__()
            if col in headers:
                col_index = headers.index(col)
                for row in lector:
                    title = row[col_index]
                    titulos_tp.append(title)
            else:
                cls.print_or_write('valor incorrecto de col')
                cls.print_or_write('Disponibles:')
                cls.print_or_write(headers)

        titulos_scraping = []
        match_scraping = 0

        titulos_scraping_no_tp = []
        with open(filename_scraping, newline='', encoding='utf-8') as csvfile:
            lector = csv.reader(csvfile)
            headers = lector.__next__()
            if titulos_tp and col in headers:
                col_index = headers.index(col)

                for row in lector:
                    title = row[col_index]
                    titulos_scraping.append(title)
                    if any(title == title_tp for title_tp in titulos_tp):
                        match_scraping += 1
                    else:
                        titulos_scraping_no_tp.append(title)
            else:
                cls.print_or_write('valor incorrecto de col')
                cls.print_or_write('Disponibles:')
                cls.print_or_write(headers) 

        cls.print_or_write(f'{filename_scraping} Matcheo {match_scraping} de {len(titulos_scraping)}', export_file)

        titulos_tp_no_scraping = []
        match_tp = 0
        if titulos_scraping:
            for titulo in titulos_tp:
                if any(titulo == title_s for title_s in titulos_scraping):
                    match_tp += 1
                else:
                    titulos_tp_no_scraping.append(titulo)
            cls.print_or_write(f'{filename_tp} Matcheo {match_tp} de {len(titulos_tp)}', export_file)

        if sin_match:
            cls.print_or_write('')
            cls.print_or_write(f'TITULOS EN {filename_scraping.upper()} QUE NO ESTAN EN {filename_tp.upper()}', export_file)
            cls.print_or_write(titulos_scraping_no_tp, export_file)
            cls.print_or_write('')
            cls.print_or_write(f'TITULOS EN {filename_tp.upper()} QUE NO ESTAN EN {filename_scraping.upper()}', export_file)
            cls.print_or_write(titulos_tp_no_scraping, export_file)

    @classmethod
    def compare_mongo_db(cls, dbs: list, list_created_at: list, col:str, sin_match=False, export_file=None, epis=False) -> None:
        """Función utilizada para comparar el scraping que se encuentra localmente frente a lo disponible en apiPresence.\n
        También tiene la opción de comparar entre 2 fechas localmente en el caso de comparar diferencias entre fechas.
        
        Args:
            - list_created_at :class:`list`: Lista de fechas. Si son 2 fechas se compara localmente. En el caso que sea 1 fecha se asume como segunda DB a apiPresence.
            - col :class:`str`: Columna que se desea comparar
            - sin_match :class:`bool`: Indica si se desea imprimir en pantalla la lista de contenidos que no hicieron match
            - export_file :class:`str`: Nombre del archivo a guardar los resultados como log
            - epis :class:`bool`: Indica si se desea comparar episodios
        """
        client_local = pymongo.MongoClient("mongodb://localhost:27017")
        db_local = client_local["titan"]
        collection_name = 'titanScrapingEpisodes' if epis else 'titanScraping'
        collection = db_local[collection_name]

        platform_code = dbs[0]

        sub_col = None
        if '.' in col:
            col = col.split('.')
            sub_col = col[1]
            col = col[0]

        dict_dbs = {
            "db1": [],  # local fecha1
            "db2": [],  # local fecha2 ó apipresence
        }

        # Si se ingresa 2 fechas se comparan localmente
        for counter, created_at in enumerate(list_created_at):
            if counter >= 2:
                break
            if created_at == 'last':
                params = {'PlatformCode': platform_code}
                sort   = [("CreatedAt", pymongo.DESCENDING)]
                limit = 1
                lastItem = collection.find(params).sort(sort).limit(limit)
                if collection.count_documents(params, limit=limit) > 0:
                    for lastContent in lastItem:
                        created_at = lastContent['CreatedAt']
            cls.print_or_write('Mongo local: ' + created_at)
            payload = {
                'PlatformCode'  : platform_code,
                'CreatedAt'     : created_at
            }
            cls.print_or_write('Obteniendo DB de Mongo local ...')
            col = 'CleanTitle' if col == 'Title' and not epis else col
            if col == 'HashUnique':
                if epis:
                    projection = ['ParentId', 'Episode', 'PlatformCode']
                    projection_scraping = ['Id', 'CleanTitle', 'Year', 'Type']
                    scraping = db_local['titanScraping'].find(payload, no_cursor_timeout=True, projection=projection_scraping).batch_size(100)
                    scraping = [dict(item) for item in scraping if item['Type'] == 'serie']
                else:
                    projection = ['CleanTitle','Type','Year','PlatformCode']
            else:
                projection = [col,'ParentId'] if epis else [col]

            local = collection.find(payload, no_cursor_timeout=True, projection=projection).batch_size(100)
            local = [item for item in local]
            if col == 'HashUnique':
                if epis:
                    for epi in local:
                        for serie in scraping:
                            if epi['ParentId'] == serie['Id']:
                                epi['Year'] = serie['Year']
                                epi['Title'] = serie['CleanTitle']
                    # print('No se puede realizar la comparacion por HashUnique (por ahora)')
                    # return
                local = [cls._generate_hash_unique(item, epis) for item in local]

            db_name = 'db1' if counter == 0 else 'db2'
            dict_dbs[db_name] = local

        # Si solo se ingresó 1 fecha se comparará con apiPresence
        if len(list_created_at) == 1:
            apipresence = []
            try:
                ssh_connection = servers.MisatoConnection()
                with ssh_connection.connect() as server:
                    client   = pymongo.MongoClient('127.0.0.1', server.local_bind_port)
                    business = client['business']
                    db_api = business['apiPresence']
                    
                    payload_api = {
                        'PlatformCode': platform_code,
                        'Status': {'$ne': 'inactive'}
                    }
                    cls.print_or_write('Obteniendo DB de apiPresence ...')
                    if epis:
                        episodios = []
                        payload_api['Type'] = 'serie'
                        apipresence = db_api.find(payload_api, no_cursor_timeout=True, projection=['Episodes', 'Status']).batch_size(100)
                        for doc in apipresence:
                            for epi in doc['Episodes']:
                                episodios.append({col:epi[col], 'ParentId':epi['ParentId']})
                        apipresence = episodios
                    else:
                        col = 'Title' if col == 'CleanTitle' else col
                        col = 'ContentId' if col == 'Id' else col
                        apipresence = db_api.find(payload_api, no_cursor_timeout=True, projection=[col, 'Status']).batch_size(100)
                        apipresence = [item for item in apipresence]

            except Exception as e:
                cls.print_or_write(f'No se pudo conectar con apiPresence -> {e}')
            else:
                dict_dbs['db2'] = apipresence
            finally:
                client.close()
                server.stop()

        print("COMPARANDO...")
        if dict_dbs['db2']:
            match_local = 0
            match_api = 0
            api_no_local = []
            local_no_api = []

            if col == 'ContentId':
                col_local = 'Id'
            elif col == 'Title' and not epis:
                col_local = 'CleanTitle'
            else:
                col_local = col

            tmp_dict_fields = {
                "local": {},
                "api": {},
            }
            for name in tmp_dict_fields:
                temporary_dict = tmp_dict_fields[name]
                db_name = 'db1' if name == 'local' else 'db2'
                current_db = dict_dbs[db_name]  # local1 | local2 | apipresence
                for item in current_db:
                    custom_col_name = col_local if name == "local" else col
                    first_part = item['ParentId'] if epis else ''
                    second_part = item[col][sub_col] if sub_col else item[custom_col_name]
                    custom_field_value = f"{first_part}{SEPARATOR}{second_part.lower()}"

                    # Se almacenan los títulos que coinciden con el campo. Puede contener una lista de 1 o más(repetidos)
                    # Ej: "__SEPARATOR__title1": ['Title1", 'TITLE1', 'title1']
                    list_column_values = temporary_dict.setdefault(custom_field_value, [])
                    list_column_values.append(second_part)
                    # temporary_dict[custom_field_value] = list_column_values

            ##############################################
            #  Método de comparación usando set          #
            ##############################################
            # >>> a = {1, 2, 3, 4, 5, 6, 7}
            # >>> b = {1, 2, 4, 5, 8, 9}
            # >>> 
            # >>> a - b
            # {3, 6, 7}
            # >>> b - a
            # {8, 9}
            ##############################################
            set_apipresence = set(tmp_dict_fields['api'].keys())
            set_local = set(tmp_dict_fields['local'].keys())

            # Se compara la diferencia entre local vs apiPresence o local vs local(otra fecha)
            api_no_local = set_apipresence - set_local  # Contenidos que están en apiPresence, pero no en local
            local_no_api = set_local - set_apipresence  # Contenidos que están en local, pero no en apiPresence

            list_all_apipresence = []
            list_diff_apipresence = []
            list_all_local = []
            list_diff_local = []
            for k, list_values in tmp_dict_fields['api'].items():
                for field_value in list_values:
                    list_all_apipresence.append(field_value)
                    if k in api_no_local:
                        list_diff_apipresence.append(field_value)
            for k, list_values in tmp_dict_fields['local'].items():
                for field_value in list_values:
                    list_all_local.append(field_value)
                    if k in local_no_api:
                        list_diff_local.append(field_value)

            match_api = len(list_all_apipresence) - len(list_diff_apipresence)
            match_local = len(list_all_local) - len(list_diff_local)
            cls.print_or_write(f'apiPresence/otra DB matcheó {match_api} de {len(list_all_apipresence)}', export_file)
            cls.print_or_write(f'Local matcheó {match_local} de {len(list_all_local)}', export_file)

            if sin_match:
                cls.print_or_write('-------Titulos apiPresence que no estan en Local-------', export_file)
                cls.print_or_write(list_diff_apipresence, export_file)
                cls.print_or_write('')
                cls.print_or_write('-------Titulos Local que no estan en apiPresence-------', export_file)
                cls.print_or_write(list_diff_local, export_file)

    @staticmethod
    def print_or_write(text, file=None):
        if file:
            if type(text) == list:
                for item in text:
                    file.write(str(item) + '\n')
            else:
                file.write(str(text) + '\n')
        print(text)

    @staticmethod
    def _generate_hash_unique(item, epis):
        if epis:
            text = f'{item["Title"]}{item["Episode"]}{item["Year"]}{item["PlatformCode"]}'
        else:    
            text =  f'{item["CleanTitle"]}{item["Type"]}{item["Year"]}{item["PlatformCode"]}'
        hash_unique = hashlib.md5(text.encode('UTF-8')).hexdigest()        
        return {'HashUnique': hash_unique, 'ParentId': item["ParentId"]} if epis else {'HashUnique': hash_unique}


if __name__ == '__main__':
    parser =  argparse.ArgumentParser()
    parser.add_argument('dbs', help='archivos a comparar o PlatformCode de la DB', type=str, nargs="+")
    parser.add_argument('--col',help='Columna a comparar', type=str, default='Title')
    parser.add_argument('--sinMatch', help='Opcional para mostrar diferencia entre archivos/dbs', nargs='?', default=False, const=True)
    parser.add_argument('--mongo', help='Comparar dbs de mongo local y misato', nargs='?', default=False, const=True)
    parser.add_argument('--at', help='CreatedAt', type=str, nargs="+", default=['last'])
    parser.add_argument('--export', help='Nombre de archivo donde guardar lo que se imprime en pantalla', type=str)
    parser.add_argument('--epis', help='Si se desea comparar episodios', nargs='?', default=False, const=True)
    parser.add_argument('--h', help='Ver ayuda', nargs='?', default=False, const=True)

    try:
        args = parser.parse_args()
        dbs             = args.dbs
        col             = args.col
        sin_match       = args.sinMatch
        mongo           = args.mongo
        list_created_at = args.at
        export          = args.export
        epis            = args.epis
        _help           = args.h
        if _help:
            show_help()
        else:
            comparator = Comparator(dbs_or_source_files=dbs, column=col, use_mongo=mongo, list_created_at=list_created_at, epis=epis)
            comparator.compare(export_file=export, sin_match=sin_match)
    except Exception as e:
        show_help()
        raise
