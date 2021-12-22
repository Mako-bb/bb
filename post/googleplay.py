# -*- coding: utf-8 -*-
import pymongo
import re
import requests
from bs4 import BeautifulSoup
from common import config


class PostScraping():
    def __init__(self, platform_code, created_at):
        self._platform_code         = platform_code
        self._created_at            = created_at
        self._mongo                 = config()['mongo']['host']
        self._titanScraping         = config()['mongo']['collections']['scraping']
        self._titanScrapingEpisodes = config()['mongo']['collections']['episode']

    def run(self, testing=False):
        if testing:
            print('*** TESTING ***')

        connection = pymongo.MongoClient(self._mongo, connect=False, maxPoolSize=None)
        db = connection.titan

        cursor = db[self._titanScraping].find(
            filter={
                'PlatformCode': self._platform_code,
                'CreatedAt': self._created_at,
                'Image': {'$type': 10}
            },
            projection={
                '_id': 0,
                'Id': 1,
                'Type': 1,
                'Deeplinks': 1
            }
        )

        s = requests.session()

        for item in cursor:
            r = s.get(item['Deeplinks']['Web'])
            soup = BeautifulSoup(r.text, 'lxml')

            cover = soup.find('img', {'alt': 'Cover art'})['src']
            cover = cover.split('=w')[0]

            update = self._update_image(db[self._titanScraping], item['Id'], cover)
            print('Imagen {} - {} actualizada'.format(item['Id'], update.modified_count))

            if item['Type'] == 'movie':
                continue

            update_episodes = []

            for ep in self._episodes(soup):
                update_episodes.append(ep)

            seasons = soup('div', {'data-value': re.compile(r'id=tvseason-')})
            seasons =  ['https://play.google.com' + x['data-value'] for x in seasons]

            for season in seasons:
                r = s.get(season)
                for ep in self._episodes(BeautifulSoup(r.text, 'lxml')):
                    update_episodes.append(ep)

            for ep in update_episodes:
                update = self._update_image(db[self._titanScrapingEpisodes], ep['Id'], ep['Image'])
                print('Imagen episodio {} - {} actualizada'.format(ep['Id'], update.modified_count))

        connection.close()
        s.close()

    def _episodes(self, soup):
        episodes = soup('div', {'class': ['uMConb', 'k7gQ7e']})
        for e in episodes:
            ep_id = e.find('a', {'href': re.compile(r'id=tvepisode-')})

            if not ep_id:
                continue

            ep_id = ep_id['href'].split('gdid=')[-1]
            ep_img = e.find('img', {'class': ['T75of', 'MlYX0']})['src'].split('=w')[0]

            yield {'Id': ep_id, 'Image': ep_img}

    def _update_image(self, collection, content_id, image):
        update = collection.update_one(
            filter={
                'PlatformCode': self._platform_code,
                'CreatedAt': self._created_at,
                'Id': content_id,
            },
            update={
                '$set': {
                    'Image' : [image]
                }
            }
        )

        return update
