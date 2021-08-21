# -*- coding: utf-8 -*-
import requests # Si el script usa requests/api o requests/bs4
import time
from handle.payload     import Payload
from bs4                import BeautifulSoup # Si el script usa bs4
from handle.datamanager import Datamanager # Opcional si el script usa Datamanager
from common             import config
from handle.mongo       import mongo
from updates.upload     import Upload
from handle.replace     import _replace

class Showtime():

    """
    - Status: En desarrollo
    - VPN: No
    - Método: BS4
    - Runtime: 
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

        self.HOME = config_["urls"]["HOME"]

        if ott_operation == 'return':
            return_params = {'PlatformCode' : self._platform_code}
            last_item = self.mongo.lastCretedAt('titanPreScraping', return_params)
            if last_item.count() > 0:
                for last_content in last_item:
                    self._created_at = last_content['CreatedAt']
            self.prescraped_section_ids = [pay["Id"] for pay in Datamanager._getListDB(self, self.titanPreScraping)]

            self.scraping()
        else:
            self.prescraped_section_ids = list()

        if ott_operation in ('testing', 'scraping'):
            self.scraping()


    def scraping(self):
        self.checkSections()

        content = {
            "series": self.getSeries(),
            "movies": self.getMovies()
        }

        print(f"Obtenidas {len(content['series'])} series y {len(content['movies'])} películas")

        self.payloads = list()
        self.payloads_db = Datamanager._getListDB(self, self.titanScraping)
        self.payloads_episodes = list()
        self.payloads_episodes_db = Datamanager._getListDB(self, self.titanScrapingEpisodios)

        for content_type in content:
            for href in content[content_type]:
                if content_type == "series":
                    payload = self.buildPayloadSerie(href)
                else:
                    payload = self.buildPayloadMovie(href)

                if payload:
                    #Datamanager._checkDBandAppend(self, payload, self.payloads, self.payloads_db)
        
        #Datamanager._insertIntoDB(self, self.payloads)
        #Datamanager._insertIntoDB(self, self.payloads_episodes)

        #Upload(self._platform_code, self._created_at, testing=True, has_episodes=bool(self.payloads_episodes))


    def checkSections(self):
        MESSAGE = "Una nueva sección podría haber sido añadida. Revisar script y sitio web, luego implementar scraping de la sección nueva y añadir a section_ids si corresponde"

        res = requests.get(self.HOME)
        soup = BeautifulSoup(res.text, features="html.parser")

        section_ids =  {"home", "series", "movies", "sports", "docs", "comedy"}
        for li in soup.find_all("li", {"class":"global-navigation__primary-mobile-menu-item"}):
            section_id = li.find("a")["id"]
            if section_id not in section_ids:
                raise Exception(MESSAGE)

        print("No hay secciones nuevas.")


    def buildPayloadMovie(self, href):

        def get_id(): return deeplink.split("/")[-2]

        def get_title(): return hero.find("h1", "hero__headline").text.strip()

        def get_year(): return int(metadata["Released"].text)

        def get_synopsis(): return hero.find("p",{"class":"hero__description"}).text

        def get_image():
            gallery_href = hero.find("a", {"class":"cta--gallery"})["href"],
            gallery_url = f"{deeplink}{gallery_href}"

            gallery_soup = BeautifulSoup(requests.get(gallery_url).text, features="html.parser")
            
            return [img["src"] for img in gallery_soup.find_all("img", {"class":"pswp__img"})] or None

        def get_rating(): return hero.find("p", {"class":"hero__subtitle"}).text.split("•")[0].strip()

        def get_genres():
            genres = set()
            for genre in metadata["Genre"].find_all("span"):
                genres.add(genre.text.replace(",", "").strip().title())
            return list(genres) or None

        def get_crew():
            crew = list()

            if metadata.get("Written by"):
                writer = {
                    "Role":"Writer",
                    "Name":crew.append(metadata["Written by"].text)
                }
                crew.append(writer)

            return crew
        
        def get_is_adult(): return "Adult" in payload.genres

        deeplink = f"{self.HOME}{href}"
        print(deeplink)

        res = requests.get(deeplink)
        soup = BeautifulSoup(res.text, features="html.parser")

        hero = soup.find("section", {"class":"hero"})
        metadata_section = hero.find("dl", {"class":"metadata"})
        
        dt_list = metadata_section.find_all("dt", "metadata__key")
        dd_list = metadata_section.find_all("dd", "metadata__value")
        metadata = {element[0].text:element[1] for element in zip(dt_list, dd_list)}

        payload = Payload()

        payload.platform_code = self._platform_code
        payload.id = get_id()
        payload.title = get_title()
        payload.clean_title = _replace(payload.title)
        payload.year = 2021 # (int)
        payload.deeplink_web = deeplink
        payload.synopsis = get_synopsis()
        payload.image = get_image()
        payload.rating = get_rating()
        payload.genres = get_genres()
        payload.crew = get_crew()
        payload.cast = None
        payload.directors = None
        payload.availability = None
        payload.download = None
        payload.is_original = None
        payload.is_adult = get_is_adult()
        payload.is_branded = None
        payload.packages = [
            {
                "Type":"subscription-vod"
            }
        ]
        payload.country = None
        payload.createdAt = self._created_at
        
        return payload.payload_movie()


    def buildPayloadSerie(self, href):

        def get_id():
            return hero.find("a", {"class":"hero__cta"})["href"][-2]

        def get_title():
            pass

        deeplink = f"{self.HOME}{href}"
        res = requests.get(deeplink)
        soup = BeautifulSoup(res.text, features="html.parser")

        hero = soup.find("section", {"class":"hero"})

        payload = Payload()

        payload.platform_code = self._platform_code
        payload.id = get_id()
        payload.title = "Mi serie" # (str)
        payload.original_title = "My series" # (str)
        payload.clean_title = _replace(payload.title) # (str)
        payload.year = 2021 # (int)
        payload.deeplink_web = deeplink
        payload.playback = True # (bool)
        payload.synopsis = "Mi sinopsis" # (str)
        payload.image = [ # lista de strings.
            "https://bb.vision/img/imagen1.jpg",  # (str)
            "https://bb.vision/img/imagen2.jpg"  # (str)
        ]
        payload.rating = "18+" # (str)
        payload.provider = [ # lista de strings
            "Netflix", # (str)
            "NBC" # (str)
        ]
        payload.external_ids = [ # lista de diccionarios. Muy raro, por lo general no está y se deja en None
            {
                "Provider": "IMDb", # (str)
                "Id": "tt12345678" # (str)
            },
            {
                "Provider": "tvdb", # (str)
                "Id": "12345678" # (str)
            }
        ]
        payload.genres = [ # lista de strings
            "Acción", # (str)
            "Suspenso" # (str)
        ]
        payload.crew = [ # lista de diccionarios
            {
                "Role":"writer", # (str)
                "Name":"Charlie Kaufman" # (str)
            },
            {
                "Role":"compositor", # (str)
                "Name":"Jon Brion" # (str)
            }
        ]
        payload.cast = [ # lista de strings
            "Bill Murray", # (str)
            "Scarlett Johansson" # (str)
        ]
        payload.directors = [ # lista de strings
            "Sofia Coppola", # (str)
            "Gaspar Noé" # (str)
        ]
        payload.availability = "2022-08-20" # (str)
        payload.download = True # (bool)
        payload.is_original = True # (bool)
        payload.is_adult = True # (bool)
        payload.is_branded = True # (bool)
        payload.packages = [ # lista de diccionarios. Para más info sobre los posibles packages, chequear el pdf
            {
                "Type":"subscription-vod" # (str)
            },
            {
                "Type":"transaction-vod", # (str)
                "BuyPrice":9.99, # (float)
                "RentPrice":4.99, # (float)
                "Definition":"HD" # (str)
            }
        ]
        payload.country = [ # lista de strings
            "USA", # (str)
            "Argentina" # (str)
        ]
        payload.createdAt = "2021-08-20" # (str) directamente lo asignamos al self._created_at que definimos en el init de la clase de la plataforma
        
        season_hrefs = [a["href"] for a in soup.find_all("a", {"data-context":"slider:season"})]
        if not season_hrefs:
            return
        seasons = self.getSeasons(season_hrefs, payload.id, payload.clean_title, deeplink)

        payload.seasons = seasons
        return payload.payload_serie()


    def buildPayloadEpisode(self, soup, series_id, series_title, season_deeplink):
        payload = Payload()

        def get_deeplink(): return season_deeplink + soup.find("a", {"class":"promo__stream-btn"})["href"]

        payload.platform_code = self._platform_code
        payload.parent_id = series_id
        payload.parent_title = series_title
        payload.id = "54321" # (str) debe ser único para este contenido de esta plataforma
        payload.title = "Mi episodio" # (str)
        payload.original_title = "My episode" # (str)
        payload.season = 1 # (int) el número de la temporada a la que pertenece
        payload.number = 1 # (int) el número de episodio
        payload.year = 2021 # (int)
        payload.deeplink_web = get_deeplink()
        payload.playback = True # (bool)
        payload.synopsis = "Mi sinopsis" # (str)
        payload.image = [ # lista de strings.
            "https://bb.vision/img/imagen1.jpg",  # (str)
            "https://bb.vision/img/imagen2.jpg"  # (str)
        ]
        payload.rating = "18+" # (str)
        payload.provider = [ # lista de strings
            "Netflix", # (str)
            "NBC" # (str)
        ]
        payload.genres = [ # lista de strings
            "Acción", # (str)
            "Suspenso" # (str)
        ]
        payload.crew = [ # lista de diccionarios
            {
                "Role":"writer", # (str)
                "Name":"Charlie Kaufman" # (str)
            },
            {
                "Role":"compositor", # (str)
                "Name":"Jon Brion" # (str)
            }
        ]
        payload.cast = [ # lista de strings
            "Bill Murray", # (str)
            "Scarlett Johansson" # (str)
        ]
        payload.directors = [ # lista de strings
            "Sofia Coppola", # (str)
            "Gaspar Noé" # (str)
        ]
        payload.availability = "2022-08-20" # (str)
        payload.download = True # (bool)
        payload.is_original = True # (bool)
        payload.is_adult = True # (bool)
        payload.is_branded = True # (bool)
        payload.packages = [ # lista de diccionarios. Para más info sobre los posibles packages, chequear el pdf
            {
                "Type":"subscription-vod" # (str)
            },
            {
                "Type":"transaction-vod", # (str)
                "BuyPrice":9.99, # (float)
                "RentPrice":4.99, # (float)
                "Definition":"HD" # (str)
            }
        ]
        payload.country = [ # lista de strings
            "USA", # (str)
            "Argentina" # (str)
        ]
        payload.createdAt = "2021-08-20" # (str) directamente lo asignamos al self._created_at que definimos en el init de la clase de la plataforma
        
        return payload.payload_episode()


    def getSeasons(self, season_hrefs, series_id, series_title, deeplink):

        seasons = list()
        for href in season_hrefs:
            season = {"Payload":None, "Episodes":list()}
            season_deeplink = f"{deeplink}{href}"

            res = requests.get(season_deeplink)
            soup = BeautifulSoup(res.text, features="html.parser")

            season_group = soup.find("div", {"class":"promo-season-group"})
            for episode_soup in season_group.find_all("a", {"class":"promo__body"}):
                episode_payload = self.buildPayloadEpisode(episode_soup, series_id, series_title, season_deeplink)
                
                if episode_payload:
                    season["Episodes"].append(episode_payload)


            payload = Payload()

            payload.id = "S123"
            payload.synopsis = "La sinopsis de una temporada"
            payload.title = "Mi serie Temporada X"
            payload.deeplink_web = season_deeplink
            payload.number = None
            payload.image = None
            payload.directors = None
            payload.cast = None
            payload.episodes = len(season["Episodes"])
            payload.is_original = None
            payload.year = None

            season["Payload"] = payload.payload_season()
            seasons.append(season)


    def getSeries(self):
        SERIES_URL = f"{self.HOME}/series"

        res = requests.get(SERIES_URL)
        soup = BeautifulSoup(res.text, features="html.parser")

        section = soup.find("section", {"data-context":"promo group:All Showtime Series"})
        all_series = section.find_all("div", {"class":"promo--square"})
        #all_series = section.find_all("a", {"class":"promo__link"})

        hrefs = set()
        for div in all_series:

            thumbnail = div.find("div", {"class":"promo__image"})
            if "comingsoongeneric" in thumbnail["data-bgset"]:
                print("salteando contenido aún no disponible.")
                continue

            hrefs.add(div.a["href"])

        return hrefs


    def getMovies(self):
        MOVIES_URL = f"{self.HOME}/movies"

        res = requests.get(MOVIES_URL)
        soup = BeautifulSoup(res.text, features="html.parser")

        categories_section = soup.find("section", {"data-context":"slider:genres"})
        all_categories = categories_section.find_all("a", {"class":"promo__link"})

        categories = {category["href"] for category in all_categories}

        page = 1
        hrefs = set()
        for category in categories:

            while True:
                res = requests.get(f"https://www.sho.com{category}/page/{page}")
                if res.status_code == 404:
                    break

                category_soup = BeautifulSoup(res.text, features="html.parser")

                gallery = category_soup.find("section", {"class":"movies-gallery"})

                for a in gallery.find_all("a", {"class":"movies-gallery__item"}):
                    hrefs.add(a["href"])

                page += 1

        return hrefs
