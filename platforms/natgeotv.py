import time
import re
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
        
    def _scraping(self, testing=False):
        self.payloads = []
        self.episode_payloads = []
        metadata = self.get_contents(self.api_url)
        for element in metadata:
            for content in [element]:
                soup = self.bs4request(("https://www.nationalgeographic.com" + content["link"]["urlValue"]))
                isSerie = self.season_request(soup)
                if isSerie != []:                   # SI TIENE SEASONS, ES PORQUE ES UNA SERIE. SINO, ES UN EPISODIO
                    self.serie_payload(content, soup)
                else:
                    pass
                
                
    def serie_payload(self, content, soup):
        seasons = self.seasons_data(soup, content["show"]["id"], content["show"]["title"])
        image = self.get_image(content, "Serie")
        payload = {
            "PlatformCode": "us.national-geographic",
            "Id": content["show"]["id"],
            "Seasons": seasons,
            "Title": content["show"]["title"],
            "CleanTitle": _replace(content["show"]["title"]),
            "OriginalTitle": content["show"]["title"],  
            "Type": 'Show with Seasons/Episodes',
            "Year": 'ver si con BS4',
            "ExternalIds": None, 
            "ExternalIds": None,
            "Deeplinks": { 
                "Web": "https://www.nationalgeographic.com" + content["link"]["urlValue"], 
                "Android": None, 
                "iOS": None,
                }, 
            "Synopsis": content['show']['aboutTheShowSummary'],
            "Image": image,
            "Rating": 'ver si con BS4',
            "Provider": None, 
            "Genres": content['show']['genre'],
            "Directors": None, #Important! 
            "Availability": None, #Important! 
            "Download": None, 
            "IsOriginal": None, #Important! 
            "IsAdult": None, #Important! 
            "IsBranded": None, #Important! (ver link explicativo)
            "Packages": [{'Type':'subscription-vod'}],
            "Country": None, 
            "Timestamp": datetime.now().isoformat(), #Obligatorio 
            "CreatedAt": self._created_at, #Obligatorio
        }        
        #print(payload)
        
    def seasons_data(self, soup, parentId, parentTitle):
        seasons = []
        allSeasons = soup.find_all("div", class_="tilegroup tilegroup--shows tilegroup--carousel tilegroup--landscape")
        for season in allSeasons:
            title = self.get_title(season)
            deeplink = self.get_deeplink(season, "Season")
            number = self.get_number(title)
            episodes = self.get_episodes(season, parentId, parentTitle, number)
            payload = {
                "Id": None,
                "Title": title, #Importante, E.J. The Wallking Dead: Season 1
                "Deeplink": deeplink, #Importante
                "Number": number, #Importante
                "Year": None, #Importante
                "Image": None, 
                "Directors": None, #Importante
                "Cast": None, #Importante
                "Episodes": episodes, #Importante
                "IsOriginal": None
            } 
            seasons.append(payload)
        return seasons

    def get_episodes(self, season, parentId, parentTitle, seasonNumber):
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
        title = episode.find("span", "tile__details-season-data")
        original_title = self.get_episode_title(title)
        year = self.get_year(episode, "Episode")
        duration = self.get_duration(episode, "Episode")
        deeplink = self.get_deeplink(episode, "Episode")
        image = self.get_image(episode, "Episode")
        rating = self.get_rating(episode, "Episode")
        episode_payload = { 
                "PlatformCode": self._platform_code, #Obligatorio 
                "Id": None, #Obligatorio
                "ParentId": parentId,
                "ParentTitle": parentTitle, 
                "Episode": n+1,
                "Season": seasonNumber,
                "Title": title.text.strip(),
                "OriginalTitle": original_title,
                "Type": "Episode",
                "Year": year, 
                "Duration": duration,
                "Deeplinks": { 
                    "Web": deeplink, #Obligatorio 
                    "Android": None, 
                    "iOS": None, 
                },
                "Synopsis": None,
                "Image": image,
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
        print(episode_payload)
            


    def last_episode_payload(self, season, n, parentId, parentTitle, seasonNumber):
        last_episode = season.find("a", "AnchorLink CarouselSlide relative pointer tile CarouselSlide--active tile--video tile--hero-inactive tile--landscape")
        title = last_episode.find("span", "tile__details-season-data")
        original_title = self.get_episode_title(title)
        year = self.get_year(last_episode, "Episode")
        duration = self.get_duration(last_episode, "Episode")
        deeplink = self.get_deeplink(last_episode, "Episode")
        image = self.get_image(last_episode, "Episode")
        rating = self.get_rating(last_episode, "Episode")
        last_episode_payload = {
                    "PlatformCode": self._platform_code, #Obligatorio 
                    "Id": None, #Obligatorio
                    "ParentId": parentId,
                    "ParentTitle": parentTitle, 
                    "Episode": n+1,
                    "Season": seasonNumber,
                    "Title": title.text.strip(),
                    "OriginalTitle": original_title,
                    "Type": "Episode",
                    "Year": year, 
                    "Duration": duration, 
                    "Deeplinks": { 
                        "Web": deeplink, #Obligatorio 
                        "Android": None, 
                        "iOS": None, 
                    },
                    "Synopsis": None,
                    "Image": image, 
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
        

    def get_rating(self, content, type):
        if type == "Episode":
            rating = content.find("span", "tile__details-date-duration")
            rating = rating.text.strip()
            rating = rating[:5]
        return(rating)

    def get_duration(self, content, type):
        if type == "Episode":
            duration = content.find("div", "tile__video-duration")
            duration = duration.text.strip()
            return duration

    def get_year(self, content, type):
        if type == "Episode":
            year = content.find("span", "tile__details-date-duration")
            year = year.text.strip()
            year = re.search(r"\d{4}",year).group()
        return year
    
    def get_episode_title(self, title):
        title = title.text.strip()
        title = re.sub("\E1 |\E2 |\E3 |\E4 |\E5 |\E6 |\E7 |\E8 |\E9 |\E10 |\E11 |\E12 |\E13 |\E14 |\E15 ","",title)
        title = re.sub("\S1 |\S2 |\S3 |\S4 |\S5 |\S6 |\S7 |\S8 |\S9 |\S10 |\S11 |\S12 |\S13 |\S14 |\S15 ","",title)
        title = title.replace("- ", "")
        return title

    def get_number(self, title):
        number = split("\D+", title)
        number = number[1]
        return number

    def get_title(self, content):
        title = content.find('span', class_='titletext')
        title = title.text.strip()
        return title

    def get_deeplink(self, content, type):
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
        allSeasons = soup.find_all('span', class_='titletext')
        seasons = []
        for season in allSeasons:
            season = season.text.strip()
            if (season != 'You May Also Like') & (season != "Latest Clips"):
                seasons.append(season)
            else:
                pass
        return seasons
        
    def get_image(self, content, type):
        if type == "Serie":
            images_list = content['images']
            image = None
            for all_images in images_list:
                for images in [all_images]:
                    if 'showimages' in images['value']:
                        image = images['value']
        elif type == "Episode":
            image = content.find("img")
            image = image.get("src")
            
        return image

    def bs4request(self, uri):
        page = requests.get(uri)
        soup = BeautifulSoup(page.content, 'html.parser')
        return soup

    def request(self, uri):
        response = self.session.get(uri)
        contents = response.json()
        return contents

    def get_contents(self, uri):
        data_dict = self.request(uri) 
        content_list = data_dict["tiles"]
        return content_list
