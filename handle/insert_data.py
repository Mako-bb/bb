import json
import time
import requests
import hashlib
import platform
import sys, os
import re
from pprint import pprint as print_lindo  
from bs4                    import BeautifulSoup
from datetime               import datetime
import argparse
import pymongo

"""
Este programa agarra los datos de un platformcode que se encuentra en la base local y los copia
a diferentes platformcode que se pasan por archivo al programa.

la ejecucion del programa es la siguiente (estando en la carpeta agentes):

python handle/insert_data.py <PlatformCode a sacar los datos> --countries <Nombre archivo de texto de PlatformCode en donde copiar los datos> --at <CreatAt de los datos del platformcode>

Formato del archivo de platformCode con N platformcodes:

platformcode1
platformcode2
...
platformcodeN

Ejemplo: Supongamos que tenemos una plataforma pepito con un platformcode sa.pepito, que ejecute el codigo 
el dia 20-04-2021 entonces el createdat seria "2021-04-20". Ahora analizando la plataforma me di cuenta que
tiene el mismo contenido para otros paises pero con un platform code del estilo dz.pepito,ps.pepito y fr.pepito
entonces lo que quiero es copiar estos datos de sa.pepito que tengo en mi base local en los otros platformcode
pero manteniendo el createdAt en mi base local. Para ese ejecuto el comando:

python handle/insert_data.py sa.pepito --countries lista_platformcode.txt --at 2021-04-20

con listaplatformcode.txt:
dz.pepito
ps.pepito
fr.pepito

y automaticamente copia los datos con los otros platformcode para luego poder subir manualmente los platformcode.
"""




def check_id(payload,scrapeds):

    for scr in scrapeds:
        if scr['Id'] == payload['Id']:
            return True
    return False

def _query_field(_platform_code,_created_at,mongo, collection, field=None, extra_filter=None):
        find_filter = {'PlatformCode': _platform_code,
                       'CreatedAt': _created_at}

        if extra_filter:
            find_filter.update(extra_filter)

        find_projection = {'_id': 0, field: 1, } if field else None

        query = mongo[collection].find(
            filter=find_filter,
            projection=find_projection,
            no_cursor_timeout=False
        )

        if field:
            # query = [item[field] for item in query]
            query = [item[field] for item in query if item.get(field)] # Este funciona en el server.
        else:
            query = list(query)

        return query

def insert_data_in_differtent_countries(db_local,created_at,news_platform_code,titanScraping,
                                        titanScrapingEpisodes,datos_a_copiar,datos_a_copiar_epi):
    """
    Este metodo, agarra los datos a acopiar de la platarforma pasada y los copia en el resto de los PlatformCode
    pasado en la variable news_platform_code
    """
    
    for country in news_platform_code:      
        payload_fun = []
        payload_epis = []
        new_platform_code = country.replace('\n','')        
        
        datos_a_revisar = _query_field(new_platform_code,created_at,db_local,titanScraping)
        datos_a_revisar_epi = _query_field(new_platform_code,created_at,db_local,titanScrapingEpisodes)

        for scr in datos_a_copiar_epi:
            scr['PlatformCode'] = new_platform_code
            try:
                del scr['_id']
            except:
                pass
            if check_id(scr,datos_a_revisar_epi) == False:
                payload_epis.append(scr)
            else:
                print("Existente")
            

        for scr in datos_a_copiar:
            
            scr['PlatformCode'] = new_platform_code
            try:
                del scr['_id']
            except:
                pass
            if check_id(scr,datos_a_revisar) == False:
                payload_fun.append(scr)
            else:
                print("Existente")
        try: # puede pasar que tenga los datos y no se pueda hacer el insert_many de una lista vacia por lo que paso
            db_local[titanScraping].insert_many(payload_fun)
        except:
            pass
        print("titanScraping")
        print("Insertados {} con PlatformCode: {}".format(len(payload_fun),new_platform_code))
        try: # puede pasar que tenga los datos y no se pueda hacer el insert_many de una lista vacia por lo que paso
            db_local[titanScrapingEpisodes].insert_many(payload_epis)
        except:
            pass
        print("titanScrapingEpisodios")
        print("Insertados {} con PlatformCode: {}".format(len(payload_epis),new_platform_code))
        
        
if __name__ == '__main__':
    client_local = pymongo.MongoClient("mongodb://localhost:27017")
    db_local = client_local.titan
    
    parser =  argparse.ArgumentParser()
    parser.add_argument('dbs', help = 'PlatformCode de donde sacar los datos a copiar',type=str)
    parser.add_argument('--countries',help = 'Nombre del archivo con la lista de PlatformCode a ingresar', type=str)
    parser.add_argument('--at',help ='CreatedAt', type=str,default='last')
    args = parser.parse_args()


    with open(args.countries,'r') as file:
        news_platform_code = file.readlines()
    
    titanScraping = 'titanScraping'
    titanScrapingEpisodes = 'titanScrapingEpisodes'
    datos_a_copiar = _query_field(args.dbs,args.at,db_local,titanScraping)
    datos_a_copiar_epi = _query_field(args.dbs,args.at,db_local,titanScrapingEpisodes)

    insert_data_in_differtent_countries(db_local,args.at,news_platform_code,titanScraping,
                                        titanScrapingEpisodes,datos_a_copiar,datos_a_copiar_epi)