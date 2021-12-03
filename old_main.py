# -*- coding: utf-8 -*-
import argparse
import logging
from datetime                       import datetime
from platforms.paramountplus        import ParamountPlus
from platforms.hulujp               import HuluJP
from platforms.hulutest2          import HuluTest2
from platforms.skyit                import SkyIT
from platforms.skygb                import SkyGB
# from platforms.netflix              import Netflix
from platforms.claro                import Claro
from platforms.qubit                import Qubit
from platforms.youtube              import Youtube
from platforms.blim                 import Blim
from platforms.cinear               import Cinear
from platforms.google               import Google
from platforms.atresplayer          import Atresplayer
from platforms.crackle              import Crackle
from platforms.itunes               import Itunes
from platforms.peacock              import Peacock
from platforms.beeline              import Beeline
from platforms.cnt                  import CNT
from platforms.amazonprimevideo               import AmazonPrimeVideo
from platforms.crunchyroll          import Crunchyroll
from platforms.globoplay            import Globoplay
from platforms.cinemaxgo            import CinemaxGo
# reelgood queda comentado mientras reelgood_v3 funcione bien
#from platforms.reelgood             import (
from platforms.reelgood_v3          import (
                                        YoutubePremiumUS,
                                        YoutubeUs,
                                        GoogleUs,
                                        HBOUsNow,
                                        HBOUs,
                                        AmazonUs,
                                        ItunesUs,
                                        NetflixUs,
                                        MicrosoftUs,
                                        Hulu,
                                        DisneyNOW,
                                        MTV,
                                        NationalGeographic,
                                        NBCUniverso,
                                        PlayStationVideo,
                                        SproutNow,
                                        TelemundoNow,
                                        UrbanMovieChannel,
                                        Velocity,
                                        Viceland,
                                        ShoutFactoryTV, # En desarrollo Samuel.
                                        # VH1, # En desarrollo Samuel.
                                        # WEtv, # En desarrollo Tadeo.
                                        # BounceTV # En desarrollo Tadeo.
                                        # IndieFlix,
                                        # Showtime,
                                        # Starz,
                                        # CMT,
                                        # CuriosityStream,
                                        # DiscoveryLifeGO,
                                        # BBCAmerica,
                                        # BET,
                                        # GaiamTV,
                                        # IFC,
                                        # Logo,
                                        # NickJr,
                                        # Oxygen,
                                        # ParamountNetwork,
                                        # PBSKids,
                                        # Sundance,
                                        # TBS,
                                        # TVLand,
                                        # Viewster,
                                    )
from platforms.zattoo               import Zattoo
from platforms.nickjr               import NickJr
from platforms.contv                import ConTv
from platforms.thecw                import TheCW
from platforms.bongo                import Bongo
from platforms.fptplay              import FPTPLAY
from platforms.caracol              import Caracol
from platforms.enterplay            import Enterplay
from platforms.dkids                import DKids
from platforms.teatrix              import Teatrix
from platforms.retinalatina         import Retinalatina
from platforms.guidedoc             import Guidedoc
from platforms.nsnow                import NsNow
from platforms.filmbox              import Filmbox
from platforms.ae                   import AE
from platforms.lifetime             import Lifetime
from platforms.enow                 import eNow
from platforms.history              import History
from platforms.cartoon            import Cartoon
from platforms.tnt                  import TNT
from platforms.bioscope             import Bioscope
from platforms.spacego              import Space
from platforms.hbo                  import HBO
from platforms.hotgo                import HotGo
from platforms.pongalo              import Pongalo
from platforms.directv            import Directv
from platforms.microsoft            import Microsoft
from platforms.movistar             import Movistar
# from platforms.netflix_unogs        import NetflixUnogs
from platforms.maxdome              import Maxdome
from platforms.flow                 import Flow
from platforms.cinemauno           import CinemaUno
from platforms.oi       import OI
from platforms.fox                  import Fox
from platforms.playstation          import Playstation
from platforms.looke                import Looke
from platforms.telecine             import Telecine
from platforms.filmin               import Filmin
from platforms.flixpatrol           import FlixPatrol
from platforms.moovimex             import Moovimex
from platforms.axn                  import AXN
from platforms.universaltv          import UniversalTv
from platforms.canalsony            import CanalSony
from platforms.mundonick            import MundoNick
from platforms.smartvod             import Smartvod
from platforms.viki                 import Viki
from platforms.nowonline            import NowOnline
from platforms.mubi                 import Mubi
from platforms.comedycentral        import ComedyCentral
from platforms.mtv                  import MTV
from platforms.contar               import Contar
from platforms.ipla                 import IPLA
from platforms.philos               import Philos
from platforms.netmovies            import Netmovies
from platforms.vtr              import VTR
from platforms.rtvc                 import RTVC
from platforms.globosat             import (
                                        CanalOff,
                                        Globosat
                                    )
from platforms.mcgolive             import MCGoLive
from platforms.netflixtest         import NetflixTest
from platforms.channel5             import Channel5
from platforms.channel4             import Channel4
from platforms.itv                  import ITV
from platforms.bbc                  import BBC
from platforms.jiocinema            import Jiocinema
from platforms.justwatch            import JustWatch
from platforms.zee5                 import Zee5
from platforms.erosnow              import Erosnow
from platforms.hooq                 import Hooq
from platforms.viu                  import Viu
from platforms.hotstar              import Hotstar
from platforms.tubitv               import TubiTV
from platforms.voot                 import Voot
from platforms.sonyliv              import SonyLiv
from platforms.sunnxt               import SunNxt
from platforms.altbalaji            import Altbalaji
from platforms.uktv                 import UKTV
from platforms.nowtv                import NowTV
from platforms.hayu                 import Hayu
from platforms.pantaflix            import Pantaflix
from platforms.chili                import Chili
from platforms.skystore             import SkyStore
from platforms.bfiplayer            import BFIPlayer
from platforms.rakuten              import Rakuten
from platforms.curzon               import Curzon
from platforms.lifetimetest       import LifetimeTest
from platforms.ocs                  import OCS
from platforms.canalplayvod         import CanalPlayVOD
from platforms.canalplus            import CanalPlus
#from platforms.flex                 import Flex
from platforms.cinemasalademande    import Cinemasalademande
from platforms.filmotv              import FilmoTV
from platforms.cinetek              import Cinetek
from platforms.bbciplayer          import BBCIPlayer
from platforms.sfrplay              import SFRPlay
from platforms.francetv             import FranceTV
from platforms.mytf1                import Mytf1
from platforms.sixplay              import Sixplay
from platforms.orangevod            import OrangeVOD
from platforms.arteboutique        import ArteBoutique
from platforms.fubotv               import FuboTV
from platforms.appletv              import AppleTV
from platforms.movistarplus         import MovistarPlus
#from platforms.vodafonetv           import VodafoneTV
from platforms.hboespana            import HBOEspana
from platforms.yupptv               import YuppTV
from platforms.sky                  import Sky
from platforms.historytest         import HistoryTest
from platforms.skyticket            import SkyTicket
from platforms.sundancenow         import SundanceNow
from platforms.sundancetv           import SundanceTv
from platforms.popcornflix          import PopcornFlix
from platforms.hoopla               import Hoopla
from platforms.videoload            import Videoload
from platforms.epix                 import Epix
from platforms.criterionchannel    import CriterionChannel
from platforms.gatotv               import GatoTV
from platforms.infinityplus         import InfinityPlus
from platforms.acorntv              import AcornTV
from platforms.playbrands           import PlayBrands
from platforms.pickbox              import Pickbox
from platforms.adultswim            import AdultSwim
from platforms.timvision            import Timvision
from platforms.dcuniverse          import DCUniverse
from platforms.cinepolisklic        import CinepolisKlic
from platforms.funimation           import Funimation
from platforms.flixfling            import FlixFling
from platforms.britbox              import BritBox
from platforms.tlc                  import TLC
from platforms.ahc                  import AHC
from platforms.destinationamerica  import DestinationAmerica
from platforms.discoverynetworks    import DiscoveryNetworks
from platforms.fandangonow             import FandangoNOW
from platforms.redbox               import Redbox
from platforms.dishanywhere         import DishAnywhere
from platforms.trutv                import TruTV
from platforms.ziggogo             import ZiggoGo
from platforms.pureflix             import Pureflix
from platforms.sharetv              import ShareTV
from platforms.videoland            import Videoland
from platforms.verizonondemand      import VerizonOnDemand
from platforms.hallmarkchannel     import HallmarkChannel
from platforms.aol                  import Aol
from platforms.xfinity              import Xfinity
from platforms.oratv                import OraTv
from platforms.poptv                import PopTv
from platforms.curiositystream      import CuriosityStream
from platforms.moviesanywhere       import MoviesAnywhere
from platforms.ewtn                 import EWTN
from platforms.redbulltv            import RedBullTV
from platforms.npostart             import NPOStart
from platforms.tudiscovery          import TuDiscovery
from platforms.videobuster          import Videobuster
from platforms.magentatv            import MagentaTV
from platforms.flimmit              import Flimmit
from platforms.netzkino             import Netzkino
from platforms.kividoo              import Kividoo
from platforms.nuplin               import Nuplin
from platforms.gaia                 import Gaia
from platforms.daserstemediathek  import DasersteMediathek
from platforms.watchbox             import Watchbox
from platforms.alleskino            import Alleskino
from platforms.realeyz              import Realeyz
from platforms.universcine          import Universcine
from platforms.arte                 import Arte
from platforms.stan                 import Stan
from platforms.sieteplus           import SietePlus
from platforms.quickflix            import Quickflix
from platforms.cineplex             import Cineplex
from platforms.dovechannel          import DoveChannel
from platforms.abciview            import ABCIview
from platforms.fandor               import Fandor
from platforms.icitoutv            import IciTouTv
from platforms.cmore                import Cmore
from platforms.pluto                import Pluto
from platforms.facebookwatch        import FacebookWatch
from platforms.kanopy               import Kanopy
from platforms.watcha               import Watcha
# from platforms.netflix_top_ten      import NetflixTopTen
#from platforms.fox_test             import FoxTest
from platforms.crave                import Crave
from platforms.tenplay              import Tenplay
from platforms.volta                import Volta
from platforms.sbsondemand          import SbsOnDemand
from platforms.shudder              import Shudder
from platforms.neontv               import NeonTV
from platforms.lightbox             import Lightbox
from platforms.pathethuis           import PatheThuis
from platforms.youtubepremium         import YoutubePremium
from platforms.cbs                  import CBS
from platforms.hollystar            import Hollystar
from platforms.quibi                import Quibi
from platforms.skyplay              import SkyPlay
from platforms.horizon              import Horizon
from platforms.yeloplay             import YeloPlay
from platforms.catchplay            import Catchplay
from platforms.miteleplus           import MiTelePlus
from platforms.boxoffice            import BoxOffice
from platforms.talktalktv           import TalkTalkTV
from platforms.disneyplus           import DisneyPlus
from platforms.disneyplusjp         import DisneyPlusJP
from platforms.iflix                import Iflix
from platforms.hulutest            import HuluTest
from platforms.vudu                 import Vudu
from platforms.mediaset             import Mediaset
from platforms.unext                import UNext
from platforms.raiplay              import Raiplay
from platforms.hbomax               import HBOMax
from platforms.viaplay              import Viaplay
from platforms.vrtnu                import VRTNu
from platforms.gyao                 import Gyao
from platforms.vodpl                import Vodpl
from platforms.tntus                import TNTUS
from platforms.go3                  import Go3
from platforms.wavve                import Wavve
from platforms.espn                 import ESPN
from platforms.espnplus            import EspnPlus
from platforms.espnwatch           import EspnWatch
from platforms.plex                 import Plex
from platforms.vix                  import Vix
from platforms.megogo               import Megogo
from platforms.osn                  import OSN
from platforms.shahidplus          import ShahidPlus
from platforms.tving                import TVING
from platforms.iqiyi                import Iqiyi
from platforms.beinconnect          import Beinconnect
from platforms.voyo                 import Voyo
from platforms.nickelodeon       import Nickelodeon
from platforms.starzplay            import Starzplay
from platforms.netflixjw           import NetflixJW
from platforms.mediacom             import Mediacom
from platforms.vimeo                import Vimeo
from platforms.optimum              import Optimum
from platforms.weyyak               import Weyyak
from platforms.screambox            import ScreamBox
from platforms.pantaya              import Pantaya
from platforms.freeform             import Freeform
from platforms.myoutdoortv          import MyOutdoorTV
from platforms.cartoonnetwork       import CartoonNetwork
from platforms.cwseed               import CwSeed
from platforms.boomerang            import Boomerang
from platforms.ertflix              import Ertflix
from platforms.addatimes            import Addatimes
from platforms.banglaflix           import Banglaflix
from platforms.cinespot             import Cinespot
from platforms.keeng                import Keeng
from platforms.jagobd               import Jagobd
from platforms.teleflix             import TeleFlix
from platforms.jagobd                import Jagobd
from platforms.hoichoi              import Hoichoi
from platforms.cliptv               import ClipTV
from platforms.discoveryplus        import DiscoveryPlus
from platforms.guiaflow             import GuiaFlow
from platforms.tvnow                import TvNow
from platforms.boxer                import Boxer
from platforms.guiareportv       import GuiaReporTV
# from platforms.netflix_unogs1       import Netflix_Unogs1
from platforms.directv_us           import DirecTV_US
from platforms.saltotv              import SaltoTV
from platforms.wetvasia             import WeTVAsia
from platforms.sparklight           import Sparklight
from platforms.watchit              import WatchIt
from common                         import config
from platforms.tvn            import TVN
from platforms.danet                import Danet
from platforms.galaxyplay          import GalaxyPlay
from platforms.cellcom              import Cellcom
from handle.logchecker              import LogChecker
from platforms.spectrum import Spectrum
from platforms.tbs                  import TBS
from platforms.naver2                import Naver2
from platforms.abc_us                   import Abc
from platforms.cmt                  import Cmt
from platforms.discoverylifego        import DiscoveryLifeGO
from platforms.wwenetwork           import WWENetwork
from platforms.dtv                  import DTV
from platforms.tvland               import TvLand
from platforms.tvdorange            import TvDOrange
from platforms.tfoumax import TFOUMax
from platforms.combateplay import CombatePlay
from platforms.bet import BET
from platforms.amc import Amc
from platforms.syfy import Syfy
from platforms.oxygen import Oxygen
from platforms.logo import Logo
from platforms.allente import Allente
from platforms.sumotv import SumoTV
from platforms.dimsum import Dimsum
from platforms.starhubgo import StarhubGo
from platforms.o2tv import O2TV
from platforms.teliaplay import TeliaPlay
from platforms.dazn import Dazn
from platforms.animedigitalnetwork import AnimeDigitalNetwork
from platforms.rokuchannel import RokuChannel
from platforms.docomoanimestore  import DocomoAnimeStore
from platforms.hikaritv import HikariTV
from platforms.skyat import SkyAT
from platforms.bravotv import BravoTv
from platforms.slingtv import SlingTV
# from platforms.nbcnetworks import NBCNetworks
from platforms.fxnow import Fxnow
# from platforms.amcnetworks import AMCNetworks
from platforms.tigouneplay import TigoUnePlay
from platforms.indieflix import Indieflix
from platforms.comedy_central import Comedy_Central
from platforms.vh1 import Vh1
# from platforms.shoutfactorytv import Shoutfactorytv
from platforms.spuul          import Spuul
from platforms.kakaotv import KakaoTV
from platforms.binge import Binge
from platforms.actvila import AcTVila
from platforms.showtime import Showtime
from platforms.fibetv import FibeTV
from platforms.kayosports import KayoSports
from platforms.orangetvgo import OrangeTVGO
from platforms.amediateka import Amediateka
from platforms.fanatiz import Fanatiz
from platforms.mytvsuper import MyTvSuper
from platforms.hmvod import HMVod
from platforms.ncplusgo import NcPlusGo
from platforms.telasa import Telasa
from platforms.bbcamerica import BBCAmerica
from platforms.pbskids import PBSKids
from platforms.bouncetv import BounceTV
from platforms.wetv import WEtv
from platforms.divantv import DivanTV
#from platforms.vodafonetvonline import VodafoneTR
from platforms.ifc import IFC
from platforms.blutv import BluTV
from platforms.hamivideo import HamiVideo
from platforms.paramountnetwork import ParamountNetwork
from platforms.dsmartgo import DSmartGo
from platforms.multimediago import MultimediaGo
from platforms.ant1next import Ant1next
from platforms.invivo import InVivo
from platforms.telekomtv import TelekomTV
from platforms.antenaplay import AntenaPlay
from platforms.voliatv import VoliaTV
from platforms.tvbarrandov import TVBarrandov
from platforms.nlziet import NLZiet
from platforms.upctv import UPCtv
from platforms.directvsports import DirecTVSports
from platforms.mytv import MyTV
from platforms.voomotion import VooMotion
from platforms.mewatch import MeWatch
from platforms.playnow import PlayNow
from platforms.ollehtv import OllehTV
from platforms.litv import LiTv
from platforms.nowplayer import NowPlayer
from platforms.myvideo import MyVideo
from platforms.telesatlivetv import TeleSatLiveTv
from platforms.kino1tv import Kino1Tv
from platforms.maxtv import MaxTv
from platforms.tv2play import Tv2Play
from platforms.illico import Illico
from platforms.kuki import Kuki
from platforms.hallmarkmoviesnow import HallMarkMoviesNow
from platforms.seezn    import Seezn
from platforms.seezn2    import Seezn2
from platforms.tver import TVer
from platforms.paravi import Paravi
from platforms.mts import MTS
from platforms.estadiocdf import EstadioCDF
from platforms.genflix import Genflix
from platforms.megafontv import MegafonTv
from platforms.virgintvgo import VirginTvGo
from platforms.tsutayatv            import TsutayaTV
from platforms.iroko import Iroko
from platforms.ntv import Ntv
from platforms.hboeasteurope import HBOEastEurope
from platforms.showmax import Showmax
from platforms.esporteinterativoplus import EsporteInterativoPlus
from platforms.tivibu import Tivibu
from platforms.nosplay import NosPlay
from platforms.rtlxl import RtlXl
from platforms.darkmattertv import DarkMattertv
from platforms.olltv import OllTv
from platforms.fod import Fod
from platforms.abema import Abema
from platforms.amctheatres import AMCTheatres
from platforms.crunchyrollbeta import CrunchyrollBeta
from platforms.allblk import Allblk
from platforms.nationalgeographic import Natgeotv
from platforms.totalplay import Totalplay
from platforms.swisscom import Swisscom


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if __name__ == '__main__':
    inicio = datetime.now()
    parser =  argparse.ArgumentParser()
    new_site_choices    = list(config()['ott_sites'].keys())
    parser.add_argument('--c',        help = 'País para Scrapear', type = str)
    parser.add_argument('--o',        help = 'Operación', type = str)
    parser.add_argument('--dIni',     help = 'Fecha Inicio', type = str)
    parser.add_argument('--dEnd',     help = 'Fecha Fin', type = str)
    parser.add_argument('ott_site',   help = 'Sitios Para Scrapear', type = str, choices = new_site_choices)
    parser.add_argument('--p',        help = 'Codigo de Plataformas', type = str)
    parser.add_argument('--date',     help = 'Fecha del log buscado', type = str)
    parser.add_argument('--t',        help = 'Tipo Update', type = str)
    parser.add_argument('--m',        help = 'Varios Paises', type = str, nargs = '+')
    parser.add_argument('--provider', help = 'Provider para JustWatch', type = str, nargs = '+')
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

    # _import_platforms()

    if ott_operation in ('scraping', 'return', 'testing', 'generos', 'top-ten'):
        locals()[ott_platforms](ott_platforms, ott_site_country, ott_operation)

    if ott_operation in ('jwscraping', 'jwreturn', 'jwtesting'):
        locals()[ott_platforms](ott_platforms, ott_site_country, ott_operation, provider, postscraping, skip)

    if ott_operation in ('guia', 'year'):
        locals()[ott_platforms](ott_platforms, ott_operation)

    if ott_operation in ('log', 'logd'):
        LogChecker(ott_platforms, ott_site_country, ott_operation, provider, logat)

    if ott_operation in ('excel'):
        platform_code = config()['ott_sites'][ott_platforms]["countries"].get(ott_site_country)
        if platform_code:
            from analysis.utils.excel_template import ExcelTemplate
            ExcelTemplate.export_excel(platform_code, logat)
        else:
            print(f"Error in PlatformCode. See config.yaml")

    if ott_operation == 'producciones' : locals()[ott_platforms](ott_platforms, fechaInicio, fechaFin),

    fin = datetime.now()

    print('Tiempo transcurrido:', fin - inicio)
