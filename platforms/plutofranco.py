import requests
import time
import re
import hashlib
from handle.datamanager import Datamanager
from common             import config
from handle.mongo       import mongo
from updates.upload     import Upload
from handle.replace     import _replace
from handle.payload     import Payload
class PlutoFranco():
    """
    Status: Completado
    VPN: No
    Método: API
    Runtime: 0:05:39.811863
    """

    def __init__(self, ott_platforms, ott_site_country, ott_operation):
        self.test = ott_operation in ("testing", "return") #
        config_ = config()['ott_sites'][ott_platforms] # Obligatorio
        self.country = ott_site_country # Opcional, puede ser útil dependiendo de la lógica del script.
        self._created_at = time.strftime('%Y-%m-%d')
        self._platform_code = config_['countries'][ott_site_country]
        self.mongo = mongo()
        self.sesion                 = requests.session() # Requerido si se va a usar Datamanager
        self.titanPreScraping       = config()['mongo']['collections']['prescraping'] # Opcional
        self.titanScraping          = config()['mongo']['collections']['scraping'] # Obligatorio. También lo usa Datamanager
        self.titanScrapingEpisodios = config()['mongo']['collections']['episode'] # Obligatorio. También lo usa Datamanager
        self.skippedTitles          = 0 # Requerido si se va a usar Datamanager
        self.skippedEpis            = 0 # Requerido si se va a usar Datamanager
        self.url                    = config_['url']
        self.url_api_token          = config_['url_api_token']
        self.url_api_categories     = config_['url_api_categories']
        self.url_api_series         = config_['url_api_series']
        self.payloads               = list()
        self.payloads_episodes      = list()
        self.ids_scrapeados         = Datamanager._getListDB(self,self.titanScraping)
        self.ids_scrapeados_episodios = Datamanager._getListDB(self,self.titanScrapingEpisodios)
        self.authorization          = self.getApiToken()
        self.first_episode_year     = int()
        self.first_season_year      = int()

        """
        Crea una lista de IDs ya scrapeados en nuestra bd local de Mongo para no
        repetir el contenido al volver a ejecutarse
        """
        if ott_operation == 'return':
            return_params = {'PlatformCode' : self._platform_code}
            last_item = self.mongo.lastCretedAt('titanPreScraping', return_params)
            if last_item.count() > 0:
                for last_content in last_item:
                    self._created_at = last_content['CreatedAt']
            self.prescraped_ids = [pay["Id"] for pay in Datamanager._getListDB(self, self.titanPreScraping)]

            self.scraping()
        else:
            self.prescraped_ids = list()

        if ott_operation in ('testing', 'scraping'):
            self.scraping()

    def getApiToken(self) -> dict:
        url = self.url_api_token
        response = Datamanager._getJSON(self, url)
        header = {
                "authority": "service-vod.clusters.pluto.tv",
                "sec-ch-ua": "^\^",
                "Authorization": "Bearer" + response["sessionToken"]
        }
        return header

    def scraping(self):
        all_items           = self.requestAllItems()
        _ids_obtenidas      = list()
        for category in all_items.get("categories"):
            for item in category.get("items"):
                _id = item.get("_id")
                if _id not in _ids_obtenidas:
                    _ids_obtenidas.append(_id)
                    _type = item.get("type")
                    if _type == "series":
                        series                  = item.get("slug")
                        all_seasons_episodes    = self.requestSeries(series)
                        payload                 = self.buildPayloadSerie(all_seasons_episodes)
                    elif _type == "movie":
                        movie                   = item
                        payload                 = self.buildPayloadMovie(movie)
                    # else: ¿Y si no llega ningún "type"?
                    Datamanager._checkDBandAppend(self,payload,self.ids_scrapeados,self.payloads)
        Datamanager._insertIntoDB(self,self.payloads,self.titanScraping)
        Datamanager._insertIntoDB(self,self.payloads_episodes,self.titanScrapingEpisodios)

        Upload(self._platform_code,self._created_at,testing=False)

    def requestAllItems(self) -> dict:
        """
        Es llamada una sola vez, y contiene todas las "categories". Estas categories vienen
        con sus "items", los cuales contienen toda la información disponible de las "movies"
        y "series", pero no trae las "seasons" ni sus "episodes".
        """
        url = self.url_api_categories
        headers = self.authorization

        return Datamanager._getJSON(self, url, headers)

    def requestSeries(self, series_slug=None) -> dict:
        """
        Es llamada una vez por cada serie, y contiene toda la información disponible de una
        "series", incluidos sus "episodes".
        """
        url = self.url_api_series + series_slug + "/seasons?offset=1000&page=1"
        headers = self.authorization

        return Datamanager._getJSON(self, url, headers)

    def buildPayloadMovie(self, content_metadata) -> dict:
        payload                 = Payload()
        payload.platform_code   = self._platform_code
        payload.id              = self.getId(content_metadata)
        payload.title           = self.getTitle(content_metadata)
        payload.clean_title     = _replace(payload.title)
        payload.year            = self.getYear(content_metadata)
        payload.duration        = self.getDuration(content_metadata)
        payload.deeplink_web    = self.getDeeplink(content_metadata)
        payload.synopsis        = self.getSynopsis(content_metadata)
        payload.image           = self.getImage(content_metadata)
        payload.rating          = self.getRating(content_metadata)
        payload.genres          = self.getGenres(content_metadata)
        payload.packages        = [{"Type":"free-vod"}]
        payload.createdAt       = self._created_at

        return payload.payload_movie()

    def buildPayloadSerie(self, content_metadata) -> dict:
        seasons                 = list()
        for season in content_metadata.get("seasons"):
            for episode in season.get("episodes"):
                payload_episode = self.buildPayloadEpisode(episode, content_metadata)
                Datamanager._checkDBandAppend(self,payload_episode,self.ids_scrapeados_episodios,self.payloads_episodes, isEpi=True)
            seasons.append(self.buildPayloadSeason(season, content_metadata))
        payload                 = Payload()
        payload.platform_code   = self._platform_code
        payload.id              = self.getId(content_metadata)
        payload.seasons         = seasons
        payload.title           = self.getTitle(content_metadata)
        payload.clean_title     = _replace(payload.title)
        payload.year            = self.getYearSeries()
        payload.deeplink_web    = self.getDeeplink(content_metadata)
        payload.synopsis        = self.getSynopsis(content_metadata)
        payload.image           = self.getImage(content_metadata)
        payload.rating          = self.getRating(content_metadata)
        payload.genres          = self.getGenres(content_metadata)
        payload.packages        = [{"Type":"free-vod"}]
        payload.createdAt       = self._created_at

        return payload.payload_serie()

    def buildPayloadSeason(self, content_metadata, parent_metadata) -> dict:
        payload                 = Payload()
        payload.id              = self.getSeasonId(content_metadata, parent_metadata)
        payload.number          = self.getNumber(content_metadata)
        payload.year            = self.getYearSeason()
        if payload.number == 1:
            self.first_season_year = payload.year
        payload.deeplink_web    = self.getDeeplinkSeason(content_metadata, parent_metadata)
        payload.episodes        = self.getEpisodes(content_metadata)

        return payload.payload_season()

    def buildPayloadEpisode(self, content_metadata, parent_metadata) -> dict:
        payload                 = Payload()
        payload.platform_code   = self._platform_code
        payload.parent_id       = self.getId(parent_metadata)
        payload.parent_title    = self.getTitle(parent_metadata)
        payload.id              = self.getId(content_metadata)
        payload.title           = self.getTitle(content_metadata)
        payload.season          = self.getSeasonNumber(content_metadata)
        payload.episode         = self.getNumber(content_metadata)
        payload.year            = self.getYear(content_metadata)
        if payload.episode == 1:
            self.first_episode_year = payload.year
        payload.deeplink_web    = self.getDeeplinkEpisode(content_metadata, parent_metadata)
        payload.synopsis        = self.getSynopsis(content_metadata)
        payload.image           = self.getImage(content_metadata)  # << !!
        payload.rating          = self.getRating(content_metadata)
        payload.genres          = self.getGenres(content_metadata)
        payload.packages        = [{"Type":"free-vod"}]
        payload.createdAt       = self._created_at

        return payload.payload_episode()

    def getId(self, content_metadata) -> str:
        return content_metadata.get("_id")

    def getTitle(self, content_metadata) -> str:
        return content_metadata.get("name")

    def getDuration(self, content_metadata) -> int:
        return content_metadata.get("allotment")//60

    def getSynopsis(self, content_metadata) -> str:
        return content_metadata.get("description")

    def getDeeplink(self, content_metadata) -> str:
        _type = content_metadata.get("type")

        if _type == "movie":
            slug_movie      = content_metadata.get("slug")
            return self.url + "movies/" + slug_movie
        elif _type == "series":
            slug_series     = content_metadata.get("slug")
            return self.url + "series/" + slug_series

    def getDeeplinkSeason(self, content_metadata, parent_metadata) -> str:
        slug_series     = parent_metadata.get("slug")
        season_number   = content_metadata.get("number")

        return self.url + "series/" + slug_series + "/details/season/" + str(season_number)

    def getDeeplinkEpisode(self, content_metadata, parent_metadata) -> str:
        slug_series     = parent_metadata.get("slug")
        slug_episode    = content_metadata.get("slug")
        season_number   = content_metadata.get("season")

        return self.url + "series/" + slug_series + "/season/" + str(season_number) + "/episode/" + slug_episode

    def getNumber(self, content_metadata) -> int:
        return content_metadata.get("number")

    def getEpisodes(self, content_metadata) -> int:
        """
        No deberían haber tráilers y recaps realmente.
        """
        return len(content_metadata.get("episodes"))

    def getImage(self, content_metadata) -> list:
        """
        Hay muy pocos casos en los que devuelve una imagen negra que
        dice PlutoTV, determinado desde el backend de la página.
        """
        covers = content_metadata.get("covers")
        images = []
        for image in covers:
            i = image.get("url")
            if "screenshot4_3" not in i:
                images.append(i)
        return images

    def getRating(self, content_metadata) -> str:
        rating = content_metadata.get("rating")
        if rating == "Not Rated":
            return None
        else:
            return rating

    def getYear(self, content_metadata) -> int:
        """
        El year proviene del "slug" de la metadata.
        No viene en las series, pero sí en sus episodios.
        """
        slug = content_metadata.get("slug")
        regex = re.compile(r'\W(\d{4})\W')
        if regex.search(slug):
            year = int(regex.search(slug).group(1))
            if year >= 1878 and year <= time.localtime().tm_year:
                return year
        else:
            return None

    def getYearSeries(self) -> int:
        year_series             = self.first_season_year
        self.first_season_year  = None

        return year_series

    def getYearSeason(self) -> int:
        year_season             = self.first_episode_year
        self.first_episode_year = None

        return year_season

    def getGenres(self, content_metadata) -> list:
        """
        Los "genre" pueden venir varios en un solo string, y depende del
        idioma si van separados con un '&' o una 'y'.
        """
        genres      = content_metadata.get("genre")
        r_genres    = re.split(r"\s[\sy&]+", genres)

        return r_genres

    def getSeasonNumber(self,content_metadata) -> int:
        return content_metadata.get("season")

    def getSeasonId(self, content_metadata, parent_metadata) -> str:
        """
        Las seasons de la metadata no vienen con id propia.
        """
        series_slug     = parent_metadata.get("slug")
        season_number   = content_metadata.get("number")
        season_id       = str(season_number)\
                         + series_slug

        return hashlib.md5(season_id.encode("utf-8")).hexdigest()