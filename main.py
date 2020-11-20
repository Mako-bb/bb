# -*- coding: utf-8 -*-
import argparse
import logging

from platforms.pongalo              import Pongalo
from platforms.cinema_uno           import CinemaUno
from platforms.acorntv              import AcornTV
from platforms.adultswim            import AdultSwim
from platforms.flixfling            import FlixFling
from platforms.trutv                import TruTV
from platforms.quibi                import Quibi
from platforms.optimum              import Optimum
from platforms.acorntv_test         import AcornTV_Test
from platforms.acorntv_test_diego   import AcornTV_Test_Diego
from platforms.acorntv_test_carlos  import AcornTV_Test_Carlos
from platforms.optimum_test_diego   import Optimum_test_diego
from platforms.optimum_test_carlos  import Optimum_Test_Carlos
from platforms.amazon_login         import AmazonLogin
from common                         import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if __name__ == '__main__':
    parser =  argparse.ArgumentParser()
    new_site_choices    = list(config()['ott_sites'].keys())
    parser.add_argument('--c',        help = 'País para Scrapear', type = str)
    parser.add_argument('--o',        help = 'Operación', type = str)
    parser.add_argument('ott_site',   help = 'Sitios Para Scrapear', type = str, choices = new_site_choices)
    args = parser.parse_args()

    ott_site_country = args.c
    ott_operation    = args.o
    ott_platforms    = args.ott_site

    if ott_operation in ('scraping', 'return', 'testing'):
        locals()[ott_platforms](ott_platforms, ott_site_country, ott_operation)
