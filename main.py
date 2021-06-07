# -*- coding: utf-8 -*-
from platforms.amazon_login import AmazonLogin

from platforms.pluto_tomas import Pluto_tomas

from platforms.pluto_ariel import Pluto_ari
from platforms.pluto_mv import Pluto_mv
from platforms.optimum_test import OptimumTest
from platforms.boomerang import Boomerang
from platforms.fandango import FandangoNOW
from platforms.cartoonnetwork import CartoonNetwork
from platforms.pantaya import Pantaya
from platforms.cwseed import CwSeed
from platforms.myoutdoortv import MyOutdoorTV
from platforms.freeform import Freeform
from common import config
from platforms.cwtv import CWtv
import argparse
import logging

from platforms.pongalo import Pongalo
from platforms.cinema_uno import CinemaUno
from platforms.acorntv import AcornTV
from platforms.adultswim import AdultSwim
from platforms.flixfling import FlixFling
from platforms.trutv import TruTV
from platforms.quibi import Quibi
from platforms.optimum import Optimum
#from platforms.acorntv_test         import AcornTV_Test
from platforms.pluto_ariel import Pluto
from platforms.cwtv import CWtv
from common import config
from platforms.freeform import Freeform
from platforms.myoutdoortv import MyOutdoorTV
from platforms.cwseed import CwSeed
from platforms.pantaya import Pantaya
from platforms.cartoonnetwork import CartoonNetwork
from platforms.fandango import FandangoNOW
from platforms.boomerang import Boomerang
from platforms.optimum_test import OptimumTest
from platforms.amazon_login import AmazonLogin
from platforms.abc import Abc
from platforms.hbo_prueba import HboPrueba
from platforms.pluto_capacitacion import PlutoCapacitacion
from platforms.pluto_ggarcia import Pluto_gg
from platforms.hbo_prueba            import HboPrueba
from platforms.starz_tom import Starz
from platforms.pluto_mv import Pluto_mv

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    new_site_choices = list(config()['ott_sites'].keys())
    parser.add_argument('--c', help='País para Scrapear', type=str)
    parser.add_argument('--o', help='Operación', type=str)
    parser.add_argument('--date', help='Fecha del log buscado', type=str)
    parser.add_argument('ott_site', help='Sitios Para Scrapear',
                        type=str, choices=new_site_choices)
    args = parser.parse_args()

    ott_site_country = args.c
    ott_operation = args.o
    ott_platforms = args.ott_site
    logat = args.date

    if ott_operation in ('scraping', 'return', 'testing'):
        locals()[ott_platforms](ott_platforms, ott_site_country, ott_operation)

    if ott_operation in ('excel'):
        platform_code = config()['ott_sites'][ott_platforms]["countries"].get(
            ott_site_country)
        if platform_code:
            from analysis.utils.excel_template import ExcelTemplate

            ExcelTemplate.export_excel(platform_code, logat)
        else:
            print(f"Error in PlatformCode. See config.yaml")           
