# -*- coding: utf-8 -*-
import os
import sys
import time
import json
import re
import argparse
import pymongo
from datetime   import datetime, timedelta
from bson.regex import Regex
try:
    from root import servers
except ModuleNotFoundError:
    path = os.path.abspath('.')
    sys.path.insert(1, path)
    from root import servers

"""
    Herramienta para obtener estadísticas de plataformas/roots:
    ===========================================================
    Obtener información de una plataforma(--provider si es de JW):
        - dado un rango de fecha:
            python handle/rootstats.py --platform <PLATFORM> --country <COUNTRY> --provider <PROVIDER> --datefrom <CREATEDAT> --dateto <CREATEDAT>
        
        - ordenar dado un campo(por defecto CreatedAt en fomra ascendente):
            python handle/rootstats.py --platform <PLATFORM> --country <COUNTRY> --provider <PROVIDER> --sort CreatedAt
        
        - obtener solo los que tienen un status especificado(ok,error,upload error, stopped):
            python handle/rootstats.py --platform <PLATFORM> --country <COUNTRY> --provider <PROVIDER> --status <STATUS>

        - obtener información de forma detallada(muestra en gráfico de barras):
            python handle/rootstats.py --platform <PLATFORM> --country <COUNTRY> --provider <PROVIDER> --pretty

    Obtener información de un server:
        - dado un rango de fecha:
            python handle/rootstats.py --source <SERVER> --datefrom <CREATEDAT> --dateto <CREATEDAT>

"""

MAX_COLS_MSG = 70
VALID_SOURCES = ["CA", "CA2", "DE", "DE2", "DE3", "DE-Test1", "GB", "GB2", "NL", "SG", "US", "MX",]
COLLECTION_NAME = "rootStats"
DEFAULT_STATUS = ("ok", "error", "upload error", "not found", "not running", "stopped",)

# DEBUG: Cambiar esto
file_json = {
    "dlv-ca"    : "root/rootCA.json",
    "dlv-ca2"   : "root/rootCA2.json",
    "dlv-gb"    : "root/rootGB.json",
    "dlv-gb2"   : "root/rootGB2.json",
    "dlv-sg"    : "root/rootSG.json",
    "dlv-nl"    : "root/rootNL.json",
    "dlv-us"    : "root/rootUS.json",
    "dlv-de"    : "root/rootDE.json",
    "dlv-de2"   : "root/rootDE2.json",
    "dlv-de3"   : "root/rootDE3.json",
    "MX"        : "root/rootMexico.json",
    "dlv-de-test1" : "root/rootDE-Test1.json",
}


def __obtain_docs(query, sort_field):
    docs = []
    column_names = ['Source', 'CreatedAt', 'ToUpdate', 'AlreadyUpdated', 'NumberRunnedPlatforms', 'ElapsedSeconds',]
    sort_field = "CreatedAt" if sort_field not in column_names else sort_field
    if sort_field not in column_names:
        print(f"CAMPO '{sort_field}' NO RECONOCIDO. ORDENANDO POR FECHA. VÁLIDOS: {column_names}")
        sort_field = "CreatedAt"

    def format_date(input_date):
        created_at = input_date
        if isinstance(created_at, (str, )):
            created_at = created_at.split(' ')[0]
        elif isinstance(created_at, (datetime, time, )):
            created_at = created_at.strftime("%Y-%m-%d")
        return created_at

    ssh_connection = servers.MisatoConnection()
    with ssh_connection.connect() as server:
        business = pymongo.MongoClient(port=server.local_bind_port).business
        collection_stats = business[COLLECTION_NAME]
        cursor = collection_stats.find(query, no_cursor_timeout=True).sort(sort_field, pymongo.ASCENDING)
        try:
            for doc in cursor:
                doc = {k: v if v else '' for k, v in doc.items() if k in column_names}
                for col in column_names:
                    if not col in doc:
                        doc.setdefault(col, '')
                doc['CreatedAt'] = format_date(input_date=doc['CreatedAt'])
                docs.append(doc)
        except Exception as e:
            print(e)
        finally:
            cursor.close()
    
    return docs

def validate_date(input_date):
    regex_date = re.compile(r"^(?P<year>\d{4})(\-|/|)(?P<month>\d{2})(\-|/|)(?P<day>\d{2})$")
    try:
        match = regex_date.search(input_date)
        year = match.group('year')
        month = match.group('month')
        day = match.group('day')
        datetime(int(year), int(month), int(day))
        is_valid_date = True
    except Exception:
        is_valid_date = False
    return is_valid_date


def determine_location(ott_platform, ott_site_country, ott_provider):
    location = None
    try:
        for root in file_json:
            with open(file_json[root], 'r') as file:
                data = json.load(file)
            for item in data:
                for country in item:
                    for platform in item[country]:
                        if ott_platform == 'JustWatch' and platform.get('Provider'):
                            if platform['Provider'] == ott_provider and platform['Country'] == ott_site_country:
                                location = root
                                break
                        elif platform['PlatformCode'] == ott_platform and platform['Country'] == ott_site_country:
                            location = root
                            break
    except Exception:
        pass
    return location


def draw_bars(data):
    for d in data:
        status = d["Status"]
        d["StatusColor"] = "\033[32;1m" if status == "OK" else "\033[31;1m"
        d["StatusBackgroundColor"] = "\033[42m" if status == "OK" else "\033[41m"
        print("{CreatedAt:^10} {StatusColor}[ {Status:^12} ]\033[30m {StatusBackgroundColor}{ProgressBar:<45} {ElapsedTime:^20}".format(**d))


def generate_detailed_info():
    pass

def generate_table_info():
    pass

def print_table(platform, source, date_from, date_to=None, status=None, pretty=False, sort=None):
    info_for_platform = False
    # root = determine_location(**platform)

    query = {}
    platform_name = platform.get('ott_platform')
    platform_country = platform.get('ott_site_country')
    platform_provider = platform.get('ott_provider')
    if not (platform_name and platform_country) and source not in VALID_SOURCES:
        print(f"\033[31;1mERROR:\033[0m\033[31m Root/Plataforma no encontrado/a: [{source}].\n\033[0mDebe especificar el root/plataforma correctamente: {VALID_SOURCES}")
        exit(1)

    if platform_name and platform_country:
        query.update({ 'ToUpdate.Platform': platform_name, 'ToUpdate.Country': platform_country})
        info_for_platform = True
    if source:
        source = "DESKTOP-4NJ6RB8" if source == "MX" else f"dlv-{source.lower()}"
        query.update({ 'Source': source })
    if status:
        query.update({ 'Status': status })
    
    if date_from or date_to:
        sub_query = {
            '$gte': date_from, 
            '$lte': date_to
        }
        if not validate_date(date_from):
            del sub_query["$gte"]
        if not validate_date(date_to):
            del sub_query["$lte"]
        if sub_query:
            query.update({ 'CreatedAt': sub_query })
    
    docs = __obtain_docs(query=query, sort_field=sort)
    if docs:
        if info_for_platform:
            if not pretty:  # Para mostrar una tabla con información básica
                header = "+      Source      |  Platform  |  Country  |       Provider       |      Status      |    Elapsed time   |  CreatedAt +"
                columns = len(header)
                print('='*columns)
                print(header)
                print('='*columns)
            
            dict_platforms_in_roots = {}  # Una plataforma pudo haber estado en varios roots.
            for doc in docs:
                source = doc["Source"]
                format = {
                    "Source": source,
                    "CreatedAt": doc["CreatedAt"],
                    "Provider": "",
                }
                for item in doc["ToUpdate"]:
                    try:
                        if platform_name == 'JustWatch' and platform.get('Provider'):
                            if platform['Provider'] == platform_provider and platform['Country'] == platform_country:
                                format.update(item)
                        else:
                            if item["Platform"] == platform_name and item["Country"] == platform_country:
                                format.update(item)
                            else:
                                continue
                    except:
                        continue
                    if 'Status' not in format:
                        continue
                    if not pretty:
                        print("|{Source:^17} | {Platform:^10} | {Country:^9} | {Provider:^20} | {Status:^16} | {ElapsedTime:^17} | {CreatedAt:^9} |".format(**format))
                        print('¯'*columns)
                    else:
                        list_platform_current_root = dict_platforms_in_roots.setdefault(source, [])
                        list_platform_current_root.append(format)
                        dict_platforms_in_roots[source] = list_platform_current_root.copy()

            # Mostrar información detallada de una plataforma
            if pretty and dict_platforms_in_roots:
                for counter, source in enumerate(dict_platforms_in_roots.keys()):
                    dict_status = {}
                    dict_durations = {}
                    
                    data = []
                    for doc in dict_platforms_in_roots[source]:
                        elapsed_seconds = doc["ElapsedSeconds"]
                        elapsed_time = doc["ElapsedTime"]
                        width_bar = (elapsed_seconds // 3600) * 2 + 1
                        
                        status = doc["Status"].replace(" ", "_")
                        dict_status[status] = dict_status.setdefault(status, 0) + 1
                        data.append({
                            "Status": status.replace("_", " ").upper(),
                            "CreatedAt": doc["CreatedAt"],
                            "ElapsedTime": elapsed_time,
                            "ProgressBar": "_"*width_bar + "\033[0m",
                        })
                        dict_durations[elapsed_seconds] = {
                            "CreatedAt": doc["CreatedAt"],
                            "Source": source,
                        }
                    
                    ############################################################
                    draw_bars(data=data)
                    ############################################################
                    print(f"\n\033[1m{platform_name.upper()}  {platform_country.upper()} - ROOT: {source}\033[0m")
                    for status in DEFAULT_STATUS:
                        k_status = status.replace(" ", "_")
                        dict_status[k_status] = dict_status.setdefault(k_status, 0)
                    print("\n\033[1mSTATUS\tOK: {ok:<10} ERROR: {error:<10} UPLOAD ERROR: {upload_error:<10} STOPPED: {stopped:<10}\033[0m".format(**dict_status))
                    if dict_durations:
                        max_duration = max(dict_durations.keys())
                        created_at = dict_durations[max_duration]["CreatedAt"]
                        max_duration = str(timedelta(seconds=max_duration))
                        print("\033[1mMAX DURATION: {:<10} DATE: {:<10}\033[0m".format(max_duration, created_at))  
                    if counter + 1 == len(dict_platforms_in_roots):
                        print("\n")
                    else:
                        print("="*100 + "\n\n")

        else:
            if not pretty:  # Para mostrar una tabla con información básica
                header = "+      Source      | Created at | Number of platforms | To update | Already update |    Elapsed time   +"
                columns = len(header)
                print("\n\n\033[1;40m{collection:^{columns}}\033[0m".format(collection=COLLECTION_NAME.upper(), columns=columns))
                print('='*columns)
                print(header)
                print('='*columns)
                for doc in docs:
                    elapsed_seconds = doc["ElapsedSeconds"]
                    doc["ToUpdate"] = len(doc["ToUpdate"])
                    doc["AlreadyUpdated"] = len(doc["AlreadyUpdated"])
                    doc["ElapsedTime"] = str(timedelta(seconds=elapsed_seconds))
                    print("|{Source:^17} | {CreatedAt:^10} | {NumberRunnedPlatforms:^19} | {ToUpdate:^9} | {AlreadyUpdated:^14} | {ElapsedTime:^17} |".format(**doc))
                    print('¯'*columns)
            else:
                pass  # DEBUG: Pensar cómo mostrar información detallada de los roots.

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--platform',   help='PlatformName.', type=str)
    parser.add_argument('--country',    help='Country.', type=str)
    parser.add_argument('--provider',   help='Provider.', type=str)
    parser.add_argument('--source',     help='Source. Ej: DE, DE2, GB, MX, etc.', type=str)
    parser.add_argument('--datefrom',   help='Fecha desde. Ej: 2021-06-23', type=str)
    parser.add_argument('--dateto',     help='Fecha hasta. Si no se especifica se selecciona la actual.', type=str)
    parser.add_argument('--status',     help='Status. Ej: ok, error, stopped, etc.', type=str)
    parser.add_argument('--pretty',     help='Muestra información de una plataforma de forma detallada.', nargs='?', default=False, const=True)
    parser.add_argument('--sort',       help='Ordenar por un campo.', default="CreatedAt", type=str)
    args = parser.parse_args()

    platform = {
        "ott_platform": args.platform,
        "ott_site_country": args.country,
        "ott_provider": args.provider,
    }
    source          = args.source
    date_from       = args.datefrom
    date_to         = args.dateto
    status          = args.status
    pretty          = args.pretty
    sort            = args.sort

    print_table(platform, source, date_from, date_to, status, pretty, sort)

