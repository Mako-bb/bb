# -*- coding: utf-8 -*-
import json
import time
import requests
import re
from bs4 import BeautifulSoup
from common import config
from datetime import datetime
from handle.mongo import mongo
from updates.upload import Upload
from urllib.parse import parse_qs, urlparse
from concurrent.futures import ThreadPoolExecutor


class NetflixNonMember():
    def __init__(self, platform_code, created_at, unogs=False):
        self._testing = False

        self._platform_code = platform_code
        self._country_code = platform_code.split(".")[0]
        self._created_at = created_at
        self.unogs = unogs

        self._mongo = mongo()
        self._titanPreScraping = config()['mongo']['collections']['prescraping']
        self._titanScraping = config()['mongo']['collections']['scraping']
        self._titanScrapingEpisodes = config()['mongo']['collections']['episode']

        if not unogs:
            cursor = self._mongo.search(self._titanScraping, {
                "PlatformCode": self._platform_code,
                "CreatedAt": self._created_at,
                "Genres": { "$type": 10 }, # null
            }) or list()

            prescraping = [{"Id": item["Id"], "CreatedAt": self._created_at, "PlatformCode": self._platform_code} for item in cursor]

            if prescraping:
                delete = self._mongo.delete(self._titanPreScraping, {
                    "PlatformCode": self._platform_code,
                    "CreatedAt": self._created_at,
                })

                if delete:
                    print("Eliminados {} items".format(delete))

                self._mongo.insertMany(self._titanPreScraping, prescraping)
                print("{} Netflix IDs obtenidas en {}".format(len(prescraping), self._created_at))

                del prescraping

                delete = self._mongo.delete(self._titanScraping, {
                    "PlatformCode": self._platform_code,
                    "CreatedAt": self._created_at,
                    "Genres": None,
                })
                print("Eliminados {} items".format(delete))

                delete = self._mongo.delete(self._titanScrapingEpisodes, {
                    "PlatformCode": self._platform_code,
                    "CreatedAt": self._created_at,
                    "Genres": None,
                })
                print("Eliminados {} episodios".format(delete))

        cursor = self._mongo.search(self._titanPreScraping, {
            "PlatformCode": self._platform_code,
            "CreatedAt": self._created_at,
        }) or list()

        self.netflix_ids = [item["Id"] for item in cursor]
        print("{} Netflix IDs obtenidas en {}".format(len(self.netflix_ids), self._created_at))

    def scraping(self):
        if self._testing:
            print("*** TESTING ***")

        package = {
            "Type": "subscription-vod"
        }

        cursor = self._mongo.search(self._titanScraping, {
            "PlatformCode": self._platform_code,
            "CreatedAt": self._created_at,
        }) or list()

        scraped = [item["Id"] for item in cursor]
        print("{} titulos scrapeados en {}".format(len(scraped), self._created_at))

        cursor = self._mongo.search(self._titanScrapingEpisodes, {
            "PlatformCode": self._platform_code,
            "CreatedAt": self._created_at,
        }) or list()

        scraped_episodes = [item["Id"] for item in cursor]
        print("{} episodios scrapeados en {}".format(len(scraped_episodes), self._created_at))

        base_url = "https://www.netflix.com"

        s = requests.session()
        a = requests.adapters.HTTPAdapter(max_retries=3, pool_connections=20, pool_maxsize=20)
        s.mount(base_url, a)

        payloads = list()
        for netflix_id in self.netflix_ids:
            if netflix_id in scraped:
                continue

            scraped.append(netflix_id)

            path = "/{}/title/{}"
            while True:
                url = base_url + path.format(self._country_code, netflix_id)
                r = s.get(url)
                print(r.url)

                if r.status_code == 404:
                    path = "/{}-en/title/{}"
                else:
                    break

            soup = BeautifulSoup(r.text, "html.parser")

            json_react = soup.find("script", string=re.compile(r"netflix.reactContext"))
            json_react = json_react.text.split("netflix.reactContext = ")[-1][:-1]
            json_react = json_react.replace("\\\"", "'")
            json_react = json_react.replace("\\n", " ")
            json_react = json_react.replace("\\t", "")
            json_react = json_react.encode().decode('unicode_escape')
            json_react = json.loads(json_react)

            if not json_react["models"].get("nmTitle"):
                print("No se consiguio nmTitle en nfxid {}".format(netflix_id))
                continue

            title_data = json_react["models"]["nmTitle"]["data"]

            content_type = "movie" if title_data["metaData"]["isMovie"] else "serie"

            images = [
                title_data["artwork"].get("logo", {}).get("url"),
                title_data["artwork"].get("billboard", {})["large"]["url"],
                title_data["artwork"].get("boxShot", {})["url"],
                title_data["artwork"].get("storyArt", {})["url"],
            ]

            directors = title_data["persons"]["directors"] + title_data["persons"]["creators"]
            directors = [p["name"] for p in directors]

            payload = {
                "PlatformCode":  self._platform_code,
                "Id":            netflix_id,
                "Title":         title_data["copy"]["title"],
                "OriginalTitle": None,
                "Type":          content_type,
                "Year":          title_data["metaData"]["year"],
                "Duration":      title_data["metaData"].get("runtime", 0) // 60 or None,
                "Deeplinks": {
                    "Web":       url,
                    "Android":   None,
                    "iOS":       None,
                },
                "Playback":      "https://www.netflix.com/watch/{}".format(netflix_id),
                "Synopsis":      title_data["copy"]["synopsis"],
                "Image":         images,
                "Rating":        title_data["metaData"]["maturityDetails"]["value"],
                "Provider":      None,
                "Genres":        [g["name"] for g in title_data["genreInfo"]["genres"]],
                "Tags":          [t["displayName"] for t in title_data["tags"]],
                "Cast":          [p["name"] for p in title_data["persons"]["fullCast"]],
                "Directors":     directors,
                "Availability":  None,
                "Download":      title_data["metaData"]["isAvailableForDownload"],
                "IsOriginal":    title_data["metaData"]["isOriginal"],
                "IsAdult":       None,
                "Packages":      [package],
                "Country":       None,
                "Timestamp":     datetime.now().isoformat(),
                "CreatedAt":     self._created_at
            }

            payloads.append(payload)

            if len(payloads) >= 40:
                self._mongo.insertMany(self._titanScraping, payloads)
                print("Insertados {} titulos".format(len(payloads)))
                payloads.clear()

            if content_type != "serie":
                continue

            payloads_episodes = list()
            for season in title_data["seasons"]:

                for episode in season["episodes"]:
                    if episode["episodeId"] in scraped_episodes:
                        continue

                    scraped_episodes.append(episode["episodeId"])

                    payload_episode = {
                        "PlatformCode":  self._platform_code,
                        "Id":            episode["episodeId"],
                        "Title":         episode["title"],
                        "OriginalTitle": None,
                        "ParentId":      netflix_id,
                        "ParentTitle":   title_data["copy"]["title"],
                        "Season":        season["num"],
                        "Episode":       episode["episodeNum"],
                        "Year":          episode["year"],
                        "Duration":      episode["runtime"] // 60,
                        "Deeplinks": {
                            "Web":       None,
                            "Android":   None,
                            "iOS":       None,
                        },
                        "Playback":      None,
                        "Synopsis":      episode["synopsis"],
                        "Image":         episode["artworkUrl"],
                        "Rating":        None,
                        "Provider":      None,
                        "Genres":        [g["name"] for g in title_data["genreInfo"]["genres"]],
                        "Cast":          None,
                        "Directors":     None,
                        "Availability":  None,
                        "Download":      None,
                        "IsOriginal":    None,
                        "IsAdult":       None,
                        "Packages":      [package],
                        "Country":       None,
                        "Timestamp":     datetime.now().isoformat(),
                        "CreatedAt":     self._created_at
                    }

                    payloads_episodes.append(payload_episode)

            if payloads_episodes:
                self._mongo.insertMany(self._titanScrapingEpisodes, payloads_episodes)
                print("Insertados {} episodios".format(len(payloads_episodes)))

        if payloads:
            self._mongo.insertMany(self._titanScraping, payloads)
            print("Insertados {} titulos".format(len(payloads)))

        # with ThreadPoolExecutor(max_workers=20) as executor:
        #     [executor.submit(self._extract_data, session=s, netflix_id=netflix_id) for netflix_id in netflix_ids]

        ##
        # Upload
        ##
        if not self._testing:
            Upload(self._platform_code, self._created_at, False)
