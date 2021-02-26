# -*- coding: utf-8 -*-
from platforms.Oxygen import Oxygen
from platforms.AmcSeries import AmcSeries
from platforms.amazon_login import AmazonLogin
from platforms.sundancetv_test import SundanceTvTest
from platforms.cmt import Cmt
from platforms.discoverylife import DiscoveryLife
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
from platforms.optimum_test         import OptimumTest
from platforms.indieflix            import Indieflix
from platforms.comedy_central       import Comedy_central
from platforms.vh1                  import Vh1
from platforms.shoutfactorytv       import Shoutfactorytv
from platforms.discoverylife        import DiscoveryLife
from platforms.sundancetv_test      import SundanceTvTest
from platforms.cmt                  import Cmt
from platforms.fxnow                import Fxnow
from platforms.tvland               import TvLand
from platforms.amazon_login         import AmazonLogin
from platforms.abc                  import Abc
from platforms.bet_test             import BetTest
from platforms.amc_networks        import AMCNetworks
from platforms.syfy import Syfy
from platforms.logo import Logo
from platforms.roku_channel         import RokuChannel
from platforms.wwenetwork           import WWENetwork
from platforms.telemundo            import Telemundo
from platforms.bravotv              import BravoTv


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    new_site_choices = list(config()['ott_sites'].keys())
    parser.add_argument('--c',        help='País para Scrapear', type=str)
    parser.add_argument('--o',        help='Operación', type=str)
    parser.add_argument('ott_site',   help='Sitios Para Scrapear',
                        type=str, choices=new_site_choices)
    args = parser.parse_args()

    ott_site_country = args.c
    ott_operation = args.o
    ott_platforms = args.ott_site

    if ott_operation in ('scraping', 'return', 'testing'):
        locals()[ott_platforms](ott_platforms, ott_site_country, ott_operation)
