import requests # Si el script usa requests/api o requests/bs4
import time
import platform
import json
from bs4                import BeautifulSoup # Si el script usa bs4
from selenium           import webdriver # Si el script usa selenium
from handle.datamanager import Datamanager # Opcional si el script usa Datamanager
from common             import config
from handle.mongo       import mongo
from updates.upload     import Upload
from handle.replace     import _replace
from handle.payload     import Payload
from pyvirtualdisplay   import Display
import re
import hashlib

class allblkLE:
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
        self.all_titles_url = config_['all_titles_url']
        self.check_id = list()
        self.all_contents = self.all_content()

        self._headers               = {
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:78.0) Gecko/20100101 Firefox/78.0',
                'Accept': '*/*',
                'Accept-Language': 'en-US,en;q=0.5',
                'Referer': 'https://pluto.tv/en/on-demand',
                'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsImtpZCI6IjJiYjNkYTc3LWRhMTktNGVmZC05ODJiLWVjMGI4MDNkY2JlYyIsInR5cCI6IkpXVCJ9.eyJzZXNzaW9uSUQiOiI5NjY3NzI3Mi0wZmRkLTExZWMtOGNhOS0wMjQyYWMxMTAwMDMiLCJjbGllbnRJUCI6IjE4Ni4yMi4yMzguMTEiLCJjaXR5IjoiTG9tYXMgZGUgWmFtb3JhIiwicG9zdGFsQ29kZSI6IjE4MzIiLCJjb3VudHJ5IjoiQVIiLCJkbWEiOjAsImFjdGl2ZVJlZ2lvbiI6IlZFIiwiZGV2aWNlTGF0IjotMzQuNzY2MSwiZGV2aWNlTG9uIjotNTguMzk1NywicHJlZmVycmVkTGFuZ3VhZ2UiOiJlcyIsImRldmljZVR5cGUiOiJ3ZWIiLCJkZXZpY2VWZXJzaW9uIjoiOTMuMC45NjEiLCJkZXZpY2VNYWtlIjoiZWRnZS1jaHJvbWl1bSIsImRldmljZU1vZGVsIjoid2ViIiwiYXBwTmFtZSI6IndlYiIsImFwcFZlcnNpb24iOiI1LjEwMy4xLThmYWQ5ZjU1MjNmZWY1N2RjZTA2MTc5ZGE2MDU2ODJjMDM1YWZlOTkiLCJjbGllbnRJRCI6ImQ5NzA5MTg2LTcyZDYtNDhkZC1hZDliLTA0YWI3MzEwM2FjNSIsImNtQXVkaWVuY2VJRCI6IiIsImlzQ2xpZW50RE5UIjpmYWxzZSwidXNlcklEIjoiIiwibG9nTGV2ZWwiOiJERUZBVUxUIiwidGltZVpvbmUiOiJBbWVyaWNhL0FyZ2VudGluYS9CdWVub3NfQWlyZXMiLCJzZXJ2ZXJTaWRlQWRzIjp0cnVlLCJlMmVCZWFjb25zIjpmYWxzZSwiZmVhdHVyZXMiOnt9LCJhdWQiOiIqLnBsdXRvLnR2IiwiZXhwIjoxNjMxMDQ5MjkyLCJqdGkiOiIxOThlNjViZi0wNjU5LTQ5ZjQtODY4Yy1lYWQwYWZkODAzNjAiLCJpYXQiOjE2MzEwMjA0OTIsImlzcyI6ImJvb3QucGx1dG8udHYiLCJzdWIiOiJwcmk6djE6cGx1dG86ZGV2aWNlczpWRTpaRGszTURreE9EWXROekprTmkwME9HUmtMV0ZrT1dJdE1EUmhZamN6TVRBellXTTEifQ.9R2a2xOCagQwYw0tcwxDziwx2xjkQseokeTev47bXzM',
                'Origin': 'https://pluto.tv',
                'Connection': 'keep-alive',
                'TE': 'Trailers'
                }
        self.payloads = []
        self.payloads_episodes = []
        self.ids_scrapeados = Datamanager._getListDB(self,self.titanScraping)
        self.ids_episcrap = Datamanager._getListDB(self,self.titanScrapingEpisodios)

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

    def all_content(self):
        res = requests.get(self.all_titles_url)
        soup = BeautifulSoup(res.text,"html.parser")
        return soup

    def scraping(self):
        urls_content = self.get_urls()
        self.get_content(urls_content)
        Datamanager._insertIntoDB(self,self.payloads_episodes,self.titanScrapingEpisodios)
        Datamanager._insertIntoDB(self,self.payloads,self.titanScraping)
        Upload(self._platform_code,self._created_at,testing=True)

    def get_urls(self):
        res = requests.get(self.all_titles_url)
        soup = BeautifulSoup(res.text,"html.parser")
        urls_content = soup.find_all("a", href=True,itemprop="url")
        return urls_content

    def get_content(self,urls_content):
        for link in urls_content:
            url = link.get("href")
            res = requests.get(url)
            soup = BeautifulSoup(res.text,"html.parser")
            check_content = soup.find("script",type="application/ld+json")
            json_data = json.loads(check_content.text)
            if json_data["@type"] == "Movie":
                payload = self.build_payload_movies(soup,link,url,json_data)
                Datamanager._checkDBandAppend(self,payload,self.ids_scrapeados,self.payloads)
            else:
                if(json_data["name"] != "Black History Month Public Service Announcements" and json_data["name"] != "We Went To...The Series"):
                    payload = self.build_payload_series(soup,link,url,json_data)
                    Datamanager._checkDBandAppend(self,payload,self.ids_scrapeados,self.payloads)

    def build_payload_movies(self,soup,urls_content,url,json_data):
        payload = Payload()
        payload.platform_code = self._platform_code
        payload.id = self.get_id(json_data)
        payload.title = self.get_title(json_data)
        payload.clean_title = self.get_clean_title(json_data)
        payload.duration = self.get_duration(json_data)
        payload.type = self.get_type(json_data)
        payload.deeplink_web = self.get_deep_link_movie(soup)
        payload.synopsis = self.get_synopsis(json_data)
        payload.image = self.get_image(soup,urls_content,url,json_data)
        payload.cast = self.get_cast(json_data)
        payload.directors = self.get_director(json_data)
        payload.packages = "subscription-vod"
        payload.createdAt = self._created_at
        return payload.payload_movie()

    def build_payload_series(self,soup,urls_content,url,json_data):
        payload = Payload()
        payload.platform_code = self._platform_code
        _id = self.get_id(json_data)
        payload.id = _id
        title = self.get_title(json_data)
        payload.title = title
        payload.seasons = self.call_payload_season(soup,json_data,_id,title)
        payload.clean_title = self.get_clean_title(json_data)
        payload.type = self.get_type(json_data)
        payload.deeplink_web = self.get_deep_link_serie(json_data)
        payload.synopsis = self.get_synopsis(json_data)
        payload.image = self.get_image(soup,urls_content,url,json_data)
        payload.cast = self.get_cast(json_data)
        payload.directors = self.get_director(json_data)
        payload.packages = "subscription-vod"
        payload.createdAt = self._created_at
        return payload.payload_serie()

    def get_type(self,json_data):
        if json_data["@type"] == "Movie":
            return "movie"
        else:
            return "serie"

    def get_duration(self,json_data):
        try:
            regex = re.compile(r'(T)(\d)(H)')
            duration = json_data["duration"]
            hours = re.search(regex, duration).group(2)
            regex = re.compile(r'(H)(\d{2}|\d)(M)')
            minutes = re.search(regex, duration).group(2)
        except:
            hours = 0
            regex = re.compile(r'(T)(\d\d|\d)(M)')
            duration = json_data["duration"]
            minutes = re.search(regex, duration).group(2)
        return int(hours)*60 + int(minutes)

    def get_duration_episode(self,json_data):
        try:
            regex = re.compile(r'(T)(\d\d|\d)(M)')
            duration = json_data["timeRequired"]
            minutes = re.search(regex, duration).group(2)
        except:
            try:
                regex = re.compile(r'(T)(\d)(H)')
                duration = json_data["timeRequired"]
                hours = re.search(regex, duration).group(2)
                regex = re.compile(r'(H)(\d{2}|\d)(M)')
                minutes = re.search(regex, duration).group(2)
            except:
                minutes = None
        return minutes

    def call_payload_season(self,soup,json_data,id_parent,title_parent):
        seasons = soup.find_all("span", class_="episode-content-strip")
        list_seasons = list()
        for season in seasons:
            number_season = self.get_number_season(season)
            link_season = season.find("a", class_="btn btn-link")
            res = requests.get(link_season.get("href"))
            soup_season = BeautifulSoup(res.text,"html.parser")
            payload = self.build_payload_season(soup_season,season,json_data)
            list_seasons.append(payload)
            episodios = season.find_all("a", itemprop="url")
            for episodio in episodios:
                href = episodio.get("href")
                res = requests.get(href)
                soup_epi = BeautifulSoup(res.text,"html.parser")
                content = soup_epi.find("script", type = "application/ld+json")
                json_data = json.loads(content.text)
                payload_epi = self.build_payload_episode(json_data,soup_epi,id_parent,title_parent,number_season)
                Datamanager._checkDBandAppend(self,payload_epi,self.ids_episcrap,self.payloads_episodes,isEpi=True)
        return list_seasons

    def build_payload_season(self,soup,season,json_data):
        payload = Payload()
        payload.platform_code = self._platform_code
        payload.id = self.get_id_season(json_data,season)
        payload.title = self.get_title_season(soup)
        payload.number = self.get_num_season(season)
        payload.clean_title = _replace(self.get_title_season(soup))
        payload.type = "season"
        payload.deeplink_web = self.get_deep_link_season(season)
        payload.synopsis = self.get_synopsis_season(soup)
        payload.image = self.get_image_season(soup)
        payload.packages = "subscription-vod"
        payload.createdAt = self._created_at
        return payload.payload_season()
               
    def build_payload_episode(self,json_data,soup,id_parent,title_parent,number_season):
        payload = Payload()
        payload.platform_code = self._platform_code
        payload.parent_id = id_parent
        payload.parent_title = title_parent
        number_episode = self.get_number_episode(soup)
        payload.id = self.get_id_episode(json_data,soup,title_parent,number_episode,number_season)
        payload.title = self.get_title(json_data)
        payload.season = number_season 
        payload.episode = number_episode
        payload.cast = self.get_cast(json_data)
        payload.clean_title = _replace(self.get_title(json_data))
        payload.type = "episode"
        payload.rating = self.get_rating(soup)
        payload.duration = self.get_duration_episode(json_data)
        payload.deeplink_web = self.get_deep_link_episode(json_data)
        payload.synopsis = self.get_synopsis(json_data)
        payload.image = self.get_image_episode(json_data)
        payload.packages = "subscription-vod"
        payload.createdAt = self._created_at
        return payload.payload_episode()

    def get_deep_link_season(self,season):
        link = season.find("a",class_="btn btn-link")
        return link.get("href") or None

    def get_num_season(self,season):
        num_season = season.find("meta", itemprop="seasonNumber")
        return num_season.get("content") or None

    def get_id_season(self,json_data,season):
        title = self.get_title(json_data)
        synopsis = self.get_synopsis(json_data)
        num_season = self.get_num_season(season)
        var = str(title) + str(synopsis) + str(num_season) + "season"
        return self.hashing(var) or None

    def get_id(self,json_data):
        title = self.get_title(json_data)
        synopsis = self.get_synopsis(json_data)
        _type = self.get_type(json_data)
        var = str(title) + str(synopsis) + str(_type)
        return self.hashing(var) or None


    def get_title(self,json_data):
        json = json_data
        tipo = type(json_data)
        return json_data["name"] or None

    def get_synopsis(self,json_data):
        return json_data["description"] or None

    def hashing(self, var):
        return hashlib.md5(var.encode("utf-8")).hexdigest()

    def get_clean_title(self,json_data):
        return _replace(json_data["name"]) or None
    
    def get_deep_link_movie(self,soup):
        try:
            button = soup.find("a",class_="inline btn btn-primary")
            button = button.get("href")
        except:
            button = None
        return button or None
    
    def get_image(self,soup,urls_content,url,json_data): # hay dos imagenes definidas en dos partes diferentes
        images = list()
        #content_principal = self.all_contents
        tag_container = self.all_contents.find("a",href=url)
        img = tag_container.find("img", itemprop = "image")
        images.append(img.get("src"))
        images.append(json_data["image"])
        return images or None

    def get_cast(self,json_data):
        cast = list()
        try:
            cast_json = json_data["actor"]
            for actor in cast_json:
                cast.append(actor["name"])
        except:
            cast = [None]
        return cast 

    def get_director(self,json_data):
        directors=list()
        director_json = json_data["director"]
        for director in director_json:
            directors.append(director["name"]) 
        return directors

    def get_title_season(self,soup):
        title = soup.find("h4", class_="subnav2")
        return title.text or None

    def get_synopsis_season(self,soup):
        synopsis = soup.find("p", id="franchise-description")
        return synopsis.text or None

    def get_image_season(self,soup):
        image = soup.find("img",class_="wp-post-image")
        return image.get("src") or None

    def get_number_episode(self,soup):
        number = soup.find("meta", itemprop="episodeNumber")
        return number.get("content") or None

    def get_id_episode(self,json_data,soup,title_parent,number_episode,number_season):
        title = self.get_title(json_data)
        description = self.get_synopsis(json_data)
        if (description is None):
            div_desc = soup.find("div", id="eps-desc")
            description = div_desc.find("p").text
        var = title + str(number_episode) + title_parent + description + str(number_season)
        return self.hashing(var) or None

    def get_number_season(self,soup):
        number = soup.find("meta", itemprop="seasonNumber")
        return number.get("content") or None

    def get_rating(self,soup):
        try:
            content = soup.find("div", id="eps-tags")
            divs = content.find_all("div")
            div_rating = divs.pop()
            rating = div_rating.text
            rating = rating.split(":")
            rating = rating.pop().strip()
        except:
            rating = None
        return rating or None

    def get_deep_link_episode(self,json_data):
        return json_data["url"] or None

    def get_image_episode(self,json_data):
        return json_data["image"] or None

    def get_deep_link_serie(self,json_data):
        return json_data["url"] or None

