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
# from platforms.acorntv_test         import AcornTV_Test
from platforms.cwtv                 import CWtv
from common                         import config
from platforms.freeform             import Freeform
from platforms.myoutdoortv          import MyOutdoorTV
from platforms.cwseed               import CwSeed
from platforms.pantaya              import Pantaya
from platforms.cartoonnetwork       import CartoonNetwork
from platforms.fandango             import FandangoNOW
from platforms.boomerang            import Boomerang

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
