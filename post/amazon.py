# -*- coding: utf-8 -*-
import pymongo
import re
import requests
from bs4 import BeautifulSoup
from common import config
from selenium              import webdriver
from pyvirtualdisplay      import Display


class PostScraping():
    def __init__(self, platform_code, created_at):
        self._platform_code         = platform_code
        self._created_at            = created_at
        self._mongo                 = config()['mongo']['host']
        self._titanScraping         = config()['mongo']['collections']['scraping']
        self._titanScrapingEpisodes = config()['mongo']['collections']['episode']

    def _get_headers(self, url):
        # display = Display(visible=1, size=(1366, 768))
        # display.start()

        js_code = """
            var req = new XMLHttpRequest();
            req.open('GET', document.location, false);
            req.send(null);
            return req.getAllResponseHeaders();
        """

        driver = webdriver.Firefox()
        driver.get(url)
        headers = driver.execute_script(js_code)
        driver.quit()

        headers = headers.strip()[:-1]
        headers = [tupl.split(': ') for tupl in headers.split('\n') if tupl]
        headers = dict((k.strip(), v.strip()) for k,v in headers)

        return headers

    def run(self):
        headers = self._get_headers('https://www.amazon.com')

        connection = pymongo.MongoClient(self._mongo, connect=False, maxPoolSize=None)
        db = connection.titan

        cursor = db[self._titanScraping].find(
            filter={
                'PlatformCode': self._platform_code,
                'CreatedAt': self._created_at,
                # 'Image': {'$type': 10}
            },
            projection={
                '_id': 0,
                'Id': 1,
                'Type': 1,
                'Deeplinks': 1
            }
        )

        s = requests.session()

        # headers  = {
        #     'Accept-Encoding': 'gzip, deflate, br',
        #     'Connection': 'keep-alive',
        #     'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:70.0) Gecko/20100101 Firefox/70.0',
        #     'Referer': 'https://www.amazon.com'
        # }

        for item in cursor:
            r = s.get(item['Deeplinks']['Web'], headers=headers)
            print(r.url)
            soup = BeautifulSoup(r.text, 'lxml')

            images = soup('div', {'class': ['dv-fallback-packshot-image', 'av-hero-background']})

            for image in images:
                cover = image.img['src']

            update = self._update_image(db[self._titanScraping], item['Id'], cover)
            print('Imagen {} - {} actualizada'.format(item['Id'], update.modified_count))

            if item['Type'] == 'movie':
                continue

            update_episodes = []

            for ep in self._episodes(soup):
                update_episodes.append(ep)

            seasons = soup.find('div', {'class': 'dv-node-dp-seasons'})
            seasons = [self._get_host(r.url) + li.a['href'] for li in seasons('li')] if seasons else []

            for season in seasons:
                r = s.get(season)
                for ep in self._episodes(BeautifulSoup(r.text, 'lxml')):
                    update_episodes.append(ep)

            for ep in update_episodes:
                update = self._update_image(db[self._titanScrapingEpisodes], ep['Id'], ep['Image'])
                print('Imagen episodio {} - {} actualizada'.format(ep['Id'], update.modified_count))

        connection.close()
        s.close()

    def _get_host(self, deeplink):
        return 'https://' + deeplink.split('/')[2]

    def _episodes(self, soup):
        episodes = soup('li', {'id': re.compile('av-ep-episodes-')})
        for e in episodes:
            ep_id = e['data-aliases'].split(',')[-1] # da mas de una id a veces
            ep_img = e.img['srcset']
            ep_img = ep_img.split(', ')[-1].split(' ')[0]

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
