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

        for item in cursor:
            if item['Type'] == 'movie':
                re_id = re.compile(r'\/([a-z0-9]{12})(\?|$)')
                content_id = re_id.search(item['Deeplinks']['Web']).group(1)
                image = 'https://musicimage.xboxlive.com/catalog/video.movie.{id}/image?locale=en-us&mode=crop&purposes=BoxArt&q=90&h=600&w=400&format=jpg'
            else:
                content_id = item['Id']
                image = 'https://musicimage.xboxlive.com/catalog/video.tvseason.{id}/image?locale=en-us&mode=crop&purposes=BoxArt&q=90&h=300&w=200&format=jpg'

            image = image.format(id=content_id.upper())

            update = self._update(db[self._titanScraping], item['Id'], content_id, image)
            print('Id {} -> {} - {} actualizacion'.format(item['Id'], content_id, update.modified_count))

            # if item['Type'] == 'serie':
            #     update = self._update_parents(db[self._titanScrapingEpisodes], item['Id'], content_id)
            #     print('ParentId {} -> {} - {} actualizacion'.format(item['Id'], content_id, update.modified_count))

        # Episodios
        ############
        # cursor = db[self._titanScrapingEpisodes].find(
        #     filter={
        #         'PlatformCode': self._platform_code,
        #         'CreatedAt': self._created_at,
        #     },
        #     projection={
        #         '_id': 0,
        #         'Id': 1,
        #         'Deeplinks': 1
        #     }
        # )

        # for item in cursor:
        #     re_id = re.compile(r'\/([a-z0-9]{12})(\?|$)')
        #     content_id = re_id.search(item['Deeplinks']['Web']).group(1) # el id del deeplink es de temporada

        #     update = self._update(db[self._titanScrapingEpisodes], item['Id'], content_id, None)
        #     print('EpisodeId {} -> {} - {} actualizacion'.format(item['Id'], content_id, update.modified_count))

        connection.close()

    def _update(self, collection, content_id, new_id, image):
        update = collection.update_one(
            filter={
                'PlatformCode': self._platform_code,
                'CreatedAt': self._created_at,
                'Id': content_id,
            },
            update={
                '$set': {
                    'Id' : new_id,
                    'Image' : [image]
                }
            }
        )

        return update

    def _update_parents(self, collection, parent_id, new_parent_id):
        update = collection.update_many(
            filter={
                'PlatformCode': self._platform_code,
                'CreatedAt': self._created_at,
                'ParentId': parent_id,
            },
            update={
                '$set': {
                    'ParentId' : new_parent_id,
                }
            }
        )

        return update
