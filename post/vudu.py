# -*- coding: utf-8 -*-
import pymongo
import re
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

        images_pre = [
            'https://images2.vudu.com/poster2/{id}-168.jpg',
            'https://images2.vudu.com/background/{id}-1280a.jpg'
        ]
        image_ep_pre = 'https://images2.vudu.com/assets/content/placard/{id}-301.jpg'

        serie_id_regex = re.compile(r'#!content\/([0-9]+)\/')

        for item in cursor:
            if item['Type'] == 'movie':
                content_id = item['Id']
            else:
                content_id = serie_id_regex.search(item['Deeplinks']['Web']).group(1)

            images = [i.format(id=content_id) for i in images_pre]
            update = self._update(db[self._titanScraping], item['Id'], images)
            print('Type {} Id {} -> {} actualizacion'.format(item['Type'], item['Id'], update.modified_count))

        # Episodios
        ############
        cursor = db[self._titanScrapingEpisodes].find(
            filter={
                'PlatformCode': self._platform_code,
                'CreatedAt': self._created_at,
            },
            projection={
                '_id': 0,
                'Id': 1,
                'Deeplinks': 1
            }
        )

        for item in cursor:
            image_ep = image_ep_pre.format(id=item['Id'])
            update = self._update(db[self._titanScrapingEpisodes], item['Id'], [image_ep])
            print('EpisodeId {} -> {} actualizacion'.format(item['Id'], update.modified_count))

        connection.close()

    def _update(self, collection, content_id, image):
        update = collection.update_one(
            filter={
                'PlatformCode': self._platform_code,
                'CreatedAt': self._created_at,
                'Id': content_id,
            },
            update={
                '$set': {
                    'Image' : image
                }
            }
        )

        return update
