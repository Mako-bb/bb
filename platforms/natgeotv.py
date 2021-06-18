from handle.payload import Payload
import time
import regex as re
from pymongo.message import insert
import requests
from bs4 import BeautifulSoup
from handle.replace import _replace
from common import config
from re import split
from handle.mongo import mongo
from updates.upload         import Upload
from datetime import datetime
# from time import sleep
# import re

class Natgeotv():
    """
    DATOS IMPORTANTES:
    - VPN: Si, de USA (Yo utilicé HMA y no tuve problemas).
    - ¿Usa Selenium?: No.
    - ¿Tiene API?: Si. Para obtener movies y series. No trae mucha info
    - ¿Usa BS4?: Si. Debido a que la API trae escasa info, utilizo BS4 para complementar, y traer episodios
    - ¿Se relaciona con scripts TP? No.
    - ¿Instanacia otro archivo de la carpeta "platforms"?: No.
    - ¿Cuanto demoró la ultima vez? tiempo + fecha.
    - ¿Cuanto contenidos trajo la ultima vez? cantidad + fecha.

    OTROS COMENTARIOS:
    Tuve que hacer un pip install regex
    """
    def __init__(self, ott_site_uid, ott_site_country, type):
        self.ott_site_uid = ott_site_uid
        self.ott_site_country = ott_site_country
        self._config = config()['ott_sites'][ott_site_uid]
        self._platform_code = self._config['countries'][ott_site_country]
        # self._start_url             = self._config['start_url']
        self._created_at = time.strftime("%Y-%m-%d")
        self.mongo = mongo()
        self.titanPreScraping = config()['mongo']['collections']['prescraping']
        self.titanScraping = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodios = config()['mongo']['collections']['episode']
        self.api_url = self._config['api_url']

        self.session = requests.session()

        if type == 'return':
            '''
            Retorna a la Ultima Fecha
            '''
            params = {"PlatformCode": self._platform_code}
            lastItem = self.mongo.lastCretedAt(self.titanScraping, params)
            if lastItem.count() > 0:
                for lastContent in lastItem:
                    self._created_at = lastContent['CreatedAt']
            self._scraping()

        if type == 'scraping':
            self._scraping()

        if type == 'testing':
            self._scraping(testing=True)
            
            
    def query_field(self, collection, field=None):
        """Método que devuelve una lista de una columna específica
        de la bbdd. Utilizo este método para saber si el contenido ya fue scrapeado

        Args:
            collection (str): Indica la colección de la bbdd.
            field (str, optional): Indica la columna, por ejemplo puede ser
            'Id' o 'CleanTitle. Defaults to None.

        Returns:
            list: Lista de los field encontrados.
        """
        find_filter = {
            'PlatformCode': self._platform_code,
            'CreatedAt': self._created_at
        }

        find_projection = {'_id': 0, field: 1, } if field else None

        query = self.mongo.db[collection].find(
            filter=find_filter,
            projection=find_projection,
            no_cursor_timeout=False
        )

        if field:
            query = [item[field] for item in query if item.get(field)]
        else:
            query = list(query)

        return query

        
    def _scraping(self, testing=False):
        """Método principal del scraping.
        Desde acá se hacen los updates a la DB, entre otras cosas
        Args:
            testing (bool, optional): Indica si está en modo testing. Defaults to False.
        """
        self.payloads = []
        self.episode_payloads = []
        # Listas de contentenido scrapeado:
        # Comparando estas listas puedo ver si el elemento ya se encuentra scrapeado.
        self.scraped = self.query_field(self.titanScraping, field='Id')   #
        self.scraped_episodes = self.query_field(self.titanScrapingEpisodios, field='Id')
        print(f"{self.titanScraping} {len(self.scraped)}")
        print(f"{self.titanScrapingEpisodios} {len(self.scraped_episodes)}")
        self.year = None
        self.rating = None
        metadata = self.get_contents(self.api_url)
        for n, element in enumerate(metadata):
            print(f"\n----- Progreso ({n+1}/{len(metadata)}) -----\n") 
            for content in [element]:
                if content["show"]["id"] in self.scraped:
                    print(content["show"]["title"] + ' ya esta scrapeado!')
                else:
                    soup = self.bs4request(("https://www.nationalgeographic.com" + content["link"]["urlValue"]))
                    isSerie = self.season_request(soup)
                    if isSerie != []:                   # SI TIENE SEASONS, ES PORQUE ES UNA SERIE. SINO, ES UN EPISODIO
                        self.serie_payload(content, soup)
                    elif "show" in content["link"]["urlValue"]:
                        self.serie_payload(content, soup)     # SI ES UN SHOW, TAMBIEN ES UNA SERIE
                    else:
                        self.movie_payload(content, soup)
        if self.payloads:
            self.mongo.insertMany(self.titanScraping, self.payloads)
        else:
            print(f'\n---- Ninguna serie o pelicula para insertar a la base de datos ----\n')
        if self.episode_payloads:
            self.mongo.insertMany(self.titanScrapingEpisodios, self.episode_payloads)
        else:
            print(f'\n---- Ningun episodio para insertar a la base de datos ----\n')
        Upload(self._platform_code, self._created_at, testing=True)
        print("----- Web Scraping finalizado -----")
        self.session.close()
        
    def movie_payload(self, content, soup):
        """Método para hacer un payload de movies
            Al final del método, hace un append del payload a la lista que se
            va a subir a la DB

        Args:
            content: Argumento que trae un dict la información de la API
            soup: Argumento que trae la metadata de bs4 
        """
        image = self.get_image(content, "Movie")
        year = self.get_year(soup, "Movie")
        duration = self.get_duration(soup, "Movie")
        rating = self.get_rating(soup, "Movie")
        payload = {
            "PlatformCode": self._platform_code,
            "Id": content["show"]["id"],
            "Title": content["show"]["title"],
            "CleanTitle": _replace(content["show"]["title"]),
            "OriginalTitle": content["show"]["title"], 
            "Type": "movie",
            "Year": year,
            "Duration": duration,
            "ExternalIds": None,  #No estoy seguro de si es
            "Deeplinks": { 
            "Web": "https://www.nationalgeographic.com" + content["link"]["urlValue"], #Obligatorio 
            "Android": None, 
            "iOS": None, 
            },
            "Synopsis": content['show']['aboutTheShowSummary'],
            "Image": [image],
            "Rating": rating,
            "Provider": None, 
            "Genres": [content['show']['genre']],
            "Directors": None, #Important! 
            "Availability": None, #Important! 
            "Download": None, 
            "IsOriginal": None, #Important! 
            "IsAdult": None, #Important! 
            "IsBranded": True, #Important! (ver link explicativo)
            "Packages": [{'Type':'subscription-vod'}],
            "Country": ["US"], 
            "Timestamp": datetime.now().isoformat(), #Obligatorio 
            "CreatedAt": self._created_at, #Obligatorio
        }
        self.payloads.append(payload)
        print("Movie: " + content["show"]["title"] + " // Scraped!")

    def serie_payload(self, content, soup):
        """Método para hacer un payload de series
            Al final del método, hace un append del payload a la lista que se
            va a subir a la DB

        Args:
            content: Argumento que trae un dict la información de la API
            soup: Argumento que trae la metadata de bs4 
        """
        self.total_seasons = 0
        self.total_episodes = 0
        seasons = self.seasons_data(soup, content["show"]["id"], content["show"]["title"])
        image = self.get_image(content, "Serie")
        try:
           genre = content['show']['genre']
        except:
            genre = None
        payload = {
            "PlatformCode": self._platform_code,
            "Id": content["show"]["id"],
            "Seasons": seasons,
            "Title": content["show"]["title"],
            "CleanTitle": _replace(content["show"]["title"]),
            "OriginalTitle": content["show"]["title"],  
            "Type": 'serie',
            "Year": self.year,
            "ExternalIds": None, 
            "ExternalIds": None,
            "Deeplinks": { 
                "Web": "https://www.nationalgeographic.com" + content["link"]["urlValue"], 
                "Android": None, 
                "iOS": None,
                }, 
            "Synopsis": content['show']['aboutTheShowSummary'],
            "Image": [image],
            "Rating": self.rating,
            "Provider": None, 
            "Genres": [genre],
            "Directors": None, #Important! 
            "Availability": None, #Important! 
            "Download": None, 
            "IsOriginal": None, #Important! 
            "IsAdult": None, #Important! 
            "IsBranded": True, #Important! (ver link explicativo)
            "Packages": [{'Type':'subscription-vod'}],
            "Country": ["US"], 
            "Timestamp": datetime.now().isoformat(), #Obligatorio 
            "CreatedAt": self._created_at, #Obligatorio
            }        
        self.payloads.append(payload)
        print("Serie: " + content["show"]["title"] + " // Scraped!")
        if self.total_seasons:
            print("(Temporadas: " + str(self.total_seasons) + " - Episodios: " + str(self.total_episodes) + ")")
    
    def seasons_data(self, soup, parentId, parentTitle):
        """Método para obtener la informacion de las temporadas
        Para que la data esté dentro del payload de series

        Args:
            soup: Argumento que trae la metadata de bs4 
            parentId: el id de la serie a la cual pertenecen las temporadas
            parentTitle: el nombre de la serie a la cual pertenecen las temporadas
            
        Return:
            Una lista con toda la información de las seasons, lista para incluirla en los payloads de series
        """
        seasons = []
        allSeasons = soup.find_all("div", class_="tilegroup tilegroup--shows tilegroup--carousel tilegroup--landscape")
        for season in allSeasons:
            title = self.get_title(season)
            deeplink = self.get_deeplink(season, "Season")
            number = self.get_season_number(title)
            episodes = self.get_episodes(season, parentId, parentTitle, number)
            payload = {
                "Id": None,
                "Title": title, #Importante, E.J. The Wallking Dead: Season 1
                "Deeplink": deeplink, #Importante
                "Number": number, #Importante
                "Year": self.year, #Importante
                "Image": None, 
                "Directors": None, #Importante
                "Cast": None, #Importante
                "Episodes": episodes, #Importante
                "IsOriginal": None
            } 
            seasons.append(payload)
            self.total_seasons += 1
        return seasons
    
    def get_episodes(self, season, parentId, parentTitle, seasonNumber):
        """Método para obtener los episodios de una serie   
        NO HACE EL PAYLOAD.
        desde este método se invocan las funciones que hacen los payloads de episodios 

        Args:
            season: Argumento que trae la metadata de la season en bs4 
            parentId: el id de la serie a la cual pertenecen las temporadas
            parentTitle: el nombre de la serie a la cual pertenecen las temporadas
            seasonNumber: numero de temporada

        """
        if seasonNumber == "":
            seasonNumber = "Latest Clips"
        episodes = season.find_all("a", "AnchorLink CarouselSlide relative pointer tile tile--video tile--hero-inactive tile--landscape")
        n = 0
        for n, episode in enumerate(reversed(episodes)):
            self.episode_payload(episode, n, parentId, parentTitle, seasonNumber)
            n += 1
        self.last_episode_payload(season, n, parentId, parentTitle, seasonNumber)
        episodes_count = len(episodes) + 1 #porque falta el ultimo
        return episodes_count

    def episode_payload(self, episode, n, parentId, parentTitle, seasonNumber):
        """Método para hacer el payload de los episodios desde el primero hasta el anteultimo
        (El ultimo lo tengo que traer con otro metodo por cuestiones del bs4)

        Args:
            n: variable que cuenta el nro de episodios, para desp saber ctos episodios tiene cada season
            season: Argumento que trae la metadata de la season en bs4 
            parentId: el id de la serie a la cual pertenecen las temporadas
            parentTitle: el nombre de la serie a la cual pertenecen las temporadas
            seasonNumber: numero de temporada
        """
        title = episode.find("span", "tile__details-season-data")
        original_title = self.get_episode_title(title)
        year = self.get_year(episode, "Episode")
        self.year = year
        duration = self.get_duration(episode, "Episode")
        deeplink = self.get_deeplink(episode, "Episode")
        image = self.get_image(episode, "Episode")
        rating = self.get_rating(episode, "Episode")
        self.rating = rating
        episode_payload = { 
                "PlatformCode": self._platform_code, #Obligatorio 
                "Id": None, #Obligatorio
                "ParentId": parentId,
                "ParentTitle": parentTitle, 
                "Episode": n+1,
                "Season": seasonNumber,
                "Title": title.text.strip(),
                "OriginalTitle": original_title,
                "Type": "episode",
                "Year": year, 
                "Duration": duration,
                "Deeplinks": { 
                    "Web": deeplink, #Obligatorio 
                    "Android": None, 
                    "iOS": None, 
                },
                "Synopsis": None,
                "Image": [image],
                "Rating": rating, 
                "Provider": None, 
                "Genres": None, #Important! 
                "Directors": None, #Important! 
                "Availability": None, #Important! 
                "Download": None, 
                "IsOriginal": None, #Important! 
                "IsAdult": None, #Important!
                "IsBranded": True, #Important! (ver link explicativo)
                "Packages": [{'Type':'subscription-vod'}], #Obligatorio 
                "Country": ["US"], 
                "Timestamp": datetime.now().isoformat(), #Obligatorio 
                "CreatedAt": self._created_at, #Obligatorio 
                }
        self.total_episodes += 1
        self.episode_payloads.append(episode_payload)
    
    def last_episode_payload(self, season, n, parentId, parentTitle, seasonNumber):
        """Método para hacer el payload del último episodio
        (Este último lo tengo que traer con otro metodo por cuestiones del bs4)
        Al final del método, hace un append del payload a la lista que se
        va a subir a la DB

        Args:
            n: variable que cuenta el nro de episodios
            season: Argumento que trae la metadata de la season en bs4 
            parentId: el id de la serie a la cual pertenecen las temporadas
            parentTitle: el nombre de la serie a la cual pertenecen las temporadas
            seasonNumber: numero de temporada
        """
        try:
            last_episode = season.find("a", "AnchorLink CarouselSlide relative pointer tile CarouselSlide--active tile--video tile--hero-inactive tile--landscape")
            title = last_episode.find("span", "tile__details-season-data")
            original_title = self.get_episode_title(title)
            year = self.get_year(last_episode, "Episode")
            duration = self.get_duration(last_episode, "Episode")
            deeplink = self.get_deeplink(last_episode, "Episode")
            image = self.get_image(last_episode, "Episode")
            rating = self.get_rating(last_episode, "Episode")
            if self.year == None:
                self.year = year
            if self.rating == None:
                self.rating = rating
            last_episode_payload = {
                    "PlatformCode": self._platform_code, #Obligatorio 
                    "Id": None, #Obligatorio
                    "ParentId": parentId,
                    "ParentTitle": parentTitle, 
                    "Episode": n+1,
                    "Season": seasonNumber,
                    "Title": title.text.strip(),
                    "OriginalTitle": original_title,
                    "Type": "episode",
                    "Year": year, 
                    "Duration": duration, 
                    "Deeplinks": { 
                        "Web": deeplink, #Obligatorio 
                        "Android": None, 
                        "iOS": None, 
                    },
                    "Synopsis": None,
                    "Image": [image], 
                    "Rating": rating, 
                    "Provider": None, 
                    "Genres": None, #Important! 
                    "Directors": None, #Important! 
                    "Availability": None, #Important! 
                    "Download": None, 
                    "IsOriginal": None, #Important! 
                    "IsAdult": None, #Important! 
                    "IsBranded": True, #Important! (ver link explicativo)
                    "Packages": [{'Type':'subscription-vod'}], #Obligatorio 
                    "Country": "US", 
                    "Timestamp": datetime.now().isoformat(), #Obligatorio 
                    "CreatedAt": self._created_at, #Obligatorio 
                    } 
            self.total_episodes += 1
            self.episode_payloads.append(last_episode_payload)
        except: 
            pass
        
    def get_year(self, content, type):
        """Método para obtener el año que trae el BS4

        Args:
            content: Argumento que trae la metadata de bs4 
            type: tipo de contenido
            
        Return: 
            devuelve el año
        """
        if type == "Episode":
            year = content.find("span", "tile__details-date-duration")
            year = year.text.strip()
            year = re.search(r"\d{4}",year).group()
        elif type == "Movie":
            try:
                year = content.find("div", "Video__Metadata")
                year = year.text.strip()
                print(year)
                year = re.search(r"\d{2}.\d{2}.\d{2}",year).group()
                year = year[6:]
                if int(year) > 60:
                    year = int(year) + 1900
                else:
                    year = int(year) + 2000
            except:
                try: 
                    year = content.find("span", "tile__details-date-duration")
                    year = year.text.strip()
                    year = re.search(r"\d{4}",year).group()
                except:
                    year = None
        if year != None:
            return int(year) 
        else:
            return None


    def get_duration(self, content, type):
        """Método para obtener la duracion que trae el BS4

        Args:
            content: Argumento que trae la metadata de bs4 
            type: tipo de contenido
            
        Return: 
            devuelve la duracion
        """
        if type == "Episode":
            duration = content.find("div", "tile__video-duration")
            duration = duration.text.strip()
        else:
            try:
                duration = content.find("div", "Video__Metadata")
                duration = duration.text.strip()
                if "TV-G" in duration:
                    duration = duration[18:]
                else: 
                    duration = duration[19:]
                duration = duration[:8]
                print(duration)
                if "|" in duration:
                    duration = duration[:5]
            except:
                try:
                    duration = content.find("div", "tile__video-duration")
                    duration = duration.text.strip()
                except: 
                    duration = None
        # FÓRMULA PARA OBTENER LA DURACIÓN EN MINUTOS, PORQUE VIENE EN OTRO FORMATO
        if duration == None:                # Si la duracion es None, no hagas nada
            return None
        else:                               # si hay una duración, traela en minutos
            if len(duration) == 5:
                duration = duration[:2]
                duration = duration.replace(":","")
            elif len(duration) == 4:
                duration = duration[:1]
                duration = duration.replace(":","") 
            elif len(duration) == 8:
                duration = duration[:5]
                hours = duration[:2]
                minutes = duration[3:]
                duration = int(hours) * 60 + int(minutes)
            if (duration == ""):
                return None
            return int(duration)

    def get_rating(self, content, type):
        if type == "Episode":
            rating = content.find("span", "tile__details-date-duration")
            rating = rating.text.strip()
            rating = rating[:5]
            if "NR" in rating:
                rating = "NR"
        elif type == "Movie":
            try:
                rating = content.find("div", "Video__Metadata")
                rating = rating.text.strip()
                rating = rating[:5]
            except:
                rating = "None"
        return(rating)
            
    
    def get_episode_title(self, title):
        """Método para limpiar el titulo de los episodios

        Args:
            title: trae el título "sucio". por ejemplo: S2 E4 Episodio
            
        Return: 
            devuelve el título limpio. por ejemplo: Episodio (sin el S2 E4)
        """
        title = title.text.strip()
        title = re.sub("\E1 |\E2 |\E3 |\E4 |\E5 |\E6 |\E7 |\E8 |\E9 |\E10 |\E11 |\E12 |\E13 |\E14 |\E15 ","",title)
        title = re.sub("\S1 |\S2 |\S3 |\S4 |\S5 |\S6 |\S7 |\S8 |\S9 |\S10 |\S11 |\S12 |\S13 |\S14 |\S15 ","",title)
        title = title.replace("- ", "")
        return title

    def get_season_number(self, title):
        """Método que me trae el número de una temporada
        Me sirve para hacer el payload de las series. y para validar que sea el nro correcto de temporada

        Args:
            title: trae el nombre de la temporada "sucio". por ejemplo: SEASON 2
            
        Return: 
            devuelve el numero de la temporada limpio. por ejemplo:  2
        """
        number = split("\D+", title)
        number = number[1]
        return number

    def get_title(self, content):
        """Método para obtener el título de un elemento mediante BS4

        Args:
            content: metadata bs4 a la cual le voy a extraer el título
            
        Return: 
            devuelve el título
        """
        title = content.find('span', class_='titletext')
        title = title.text.strip()
        return title

    def get_deeplink(self, content, type):
        """Método para obtener el deeplink del bs4 de un elemento

        Args:
            content: metadata bs4 a la cual le voy a extraer el deeplink
            type: tipo de elemento
            
        Return: 
            devuelve el deeplink
        """
        if type == "Episode":
            try:
                deeplink = ("https://www.nationalgeographic.com" + content.get("href"))
            except:
                deeplink = None
        elif type == "Season":
            atag = content.div.a
            try:
                deeplink = ("https://www.nationalgeographic.com" + atag.get("href"))
            except:
                deeplink = None
        return deeplink

    def season_request(self, soup):
        """Método para saber si un elemento tiene seasons.
        Es una de las dos validaciones que utilizo para saber si es una serie o una movie
        Si el request season devuelve algo, es porque el elemento tiene seasons
        Y si tiene seasons, es una serie
        
        Args:
            soup: metadata bs4 a la cual le voy a hacer el analisis a ver si tiene temporadas
            
        Return: 
            devuelve una lista con los nombres de las temporadas
        """
        allSeasons = soup.find_all('span', class_='titletext')
        seasons = []
        for season in allSeasons:
            season = season.text.strip()
            if (season != 'You May Also Like'):
                seasons.append(season)
            else:
                pass
        return seasons
        
    def get_image(self, content, type):
        """Método para obtener la imagen de un elemento

        Args:
            content: metadata bs4 a la cual le voy a extraer el deeplink
            type: tipo de elemento
            
        Return: 
            devuelve la url de la imagen
            ****** en realidad la devuelve en base64 ********
            TODO: Fixear esto
        """
        if type == "Episode":
            image = content.find("img")
            image = image.get("src")
        else:
            images_list = content['images']
            image = None
            for all_images in images_list:
                for images in [all_images]:
                    if 'showimages' in images['value']:
                        image = images['value']
        return image

    def bs4request(self, uri):
        """Método para hacer una request con bs4
        
        Args:
            uri: uri a la cual le vamos a hacer la request
            
        Return: 
            devuelve el bs4
        """
        page = requests.get(uri)
        soup = BeautifulSoup(page.content, 'html.parser')
        return soup

    def request(self, uri):
        """Método para hacer una request a la API

        Args:
            uri: uri a la cual le voy a hacer la request
            
        Return: 
            devuelve los contenidos en formato .json()
        """
        response = self.session.get(uri)
        contents = response.json()
        return contents

    def get_contents(self, uri):
        """Método para obtener los contenidos 
        invoca la función request, y "limpia" el dict.json() para que devuelva el dict con la metadata

        Args:
           uri: uri de la cual vamos a obtener los contenidos
            
        Return: 
            devuelve un dict con la lista de contenidos
        """
        data_dict = self.request(uri) 
        content_list = data_dict["tiles"]
        return content_list
