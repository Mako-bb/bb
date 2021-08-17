# -*- coding: utf-8 -*-
import argparse
import logging
from datetime import datetime
from common import config
#from handle.logchecker import LogChecker

# 1) Evito importar todo con importlib.
from importlib import import_module

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if __name__ == '__main__':
    inicio = datetime.now()
    parser = argparse.ArgumentParser()
    # 2) El nombre de la Clase ingresada, debe ser igual en config.yaml -> ott_sites
    new_site_choices = list(config()['ott_sites'].keys())
    # 3) Parser de argumentos de la terminal:
    parser.add_argument('--c', help = 'País para Scrapear', type=str)
    parser.add_argument('--o', help = 'Operación', type=str)
    parser.add_argument('--dIni', help = 'Fecha Inicio', type=str)
    parser.add_argument('--dEnd', help = 'Fecha Fin', type=str)
    parser.add_argument('ott_site', help = 'Sitios Para Scrapear', type=str, choices=new_site_choices)
    parser.add_argument('--p', help = 'Codigo de Plataformas', type=str)
    parser.add_argument('--date', help = 'Fecha del log buscado', type=str)
    parser.add_argument('--t', help = 'Tipo Update', type=str)
    parser.add_argument('--m', help = 'Varios Paises', type=str, nargs='*')
    parser.add_argument('--provider', help = 'Provider para JustWatch', type=str, nargs='+')
    parser.add_argument('--postscraping', nargs='?', default=False, const=True)
    parser.add_argument('--skip', nargs='?', default=False, const=True)

    args = parser.parse_args()

    ott_site_country = args.c
    ott_operation    = args.o
    ott_platforms    = args.ott_site
    fechaInicio      = args.dIni
    fechaFin         = args.dEnd
    plataformas      = args.p
    tipo             = args.t
    lista            = args.m
    provider         = args.provider
    skip             = args.skip
    postscraping     = args.postscraping
    logat            = args.date
    countries        = args.m

    # 4) Indico en formato "string", el nombre del módulo a importar.
    module = None
    module = 'platforms.' + ott_platforms.lower()
    #####################################################
    # IMPORTANTE: El nombre del archivo, debe ser igual #
    # al nombre de la clase.                            #
    # Ejemplo: Clase-> "Pepe", archivo: pepe.py         #
    #                                                   #
    # module = 'platforms.pepe'                         #
    #####################################################

    # 5) Valido si aún el script pertenece a un "3rd-party".
    # Ej: 'platforms.reelgood_v3'
    tp_sites = list(config()['tp_sites'].items())
    for tp, ott in tp_sites:
        if ott_platforms in ott:
            module = 'platforms.' + tp
            break

    status_code = 0
    # 6) Obtengo el módulo a importar:
    MODULE_NAME = module
    try:
        # 7) Importo el módulo correcto.
        module = import_module(MODULE_NAME)

        # 8) Hago la instancia de la Clase. La clase == ott_platforms
        PlatformClass = getattr(module, ott_platforms)
    except ModuleNotFoundError as exc:
        print(exc)
        print("\n¡¡¡El nombre del archivo y la clase no coinciden!!!\n")
        print(f"Para importar: \"{ott_platforms}\"...")
        print(f"El archivo debe llamarse: \"{ott_platforms.lower()}.py\"")

        ott_operation = 'no operation'
        status_code = 3

    # python main.py --o scraping --c [ott_site_country] [ott_platforms]
    # Para correr plataformas comunes.
    # 'scraping' corre el script, hace los chequeos de los Paylods e intenta subir el scraping a Misato al terminar
    # 'testing' corre el script y hace los chequeos de los Payloads, pero no intenta subir a Misato
    # 'return' sirve para seguir el script donde haya quedado en caso de que se haya interrumpido
    if ott_operation in ('scraping', 'return', 'testing', 'generos', 'top-ten'):
        try:
            PlatformClass(ott_platforms, ott_site_country, ott_operation, countries)
        except TypeError:
            PlatformClass(ott_platforms, ott_site_country, ott_operation)

    # python main.py --o jwscraping --c [ott_site_country] [ott_platforms]
    # Para plataformas que se scrapeen por Third Party
    # se cambia 'jwscraping' por 'jwreturn' para el modo return o 'jwtesting' para el modo testing
    if ott_operation in ('jwscraping', 'jwreturn', 'jwtesting'):
        PlatformClass(ott_platforms, ott_site_country, ott_operation, provider, postscraping, skip)

    if ott_operation in ('guia', 'year'):
        PlatformClass(ott_platforms, ott_operation)

    # python main.py --o log --c [ott_site_country] [ott_platforms] --date [logat]
    # Para visualizar en la terminal el log de la plataforma/país indicada en la fecha dada.
    # Si no se indica fecha, se muestra el log más reciente.
    # Se puede reemplazar "log" por "logd" para descargar el log en un archivo de texto
    if ott_operation in ('log', 'logd'):
        LogChecker(ott_platforms, ott_site_country, ott_operation, provider, logat)

    # python main.py --o excel --c [ott_site_country] [ott_platforms]
    # Para exportar el scraping de una plataforma a un archivo de excel.
    if ott_operation in ('excel'):
        platform_code = config()['ott_sites'][ott_platforms]["countries"].get(ott_site_country)
        if platform_code:
            from analysis.utils.excel_template import ExcelTemplate
            ExcelTemplate.export_excel(platform_code, logat)
        else:
            print(f"Error in PlatformCode. See config.yaml")

    if ott_operation == 'producciones':
        PlatformClass(ott_platforms, fechaInicio, fechaFin)

    fin = datetime.now()

    print('Tiempo transcurrido:', fin - inicio)
    exit(status_code)
