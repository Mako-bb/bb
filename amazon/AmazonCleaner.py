import time
import re
import os
import pymongo
import bson
import random
from handle.mongo import mongo
from common import config
from pymongo import MongoClient


class AmazonCleaner:
    def __init__(self, ott_site_uid, ott_site_country):
        self._config = config()['ott_sites'][ott_site_uid]
        self._created_at = time.strftime("%Y-%m-%d")
        self.mongo = mongo()
        self._platform_code = self._config['countries'][ott_site_country]
        self.titanScraping = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodios = config()['mongo']['collections']['episode']
        self.country = ott_site_country
        params = {"PlatformCode": self._platform_code}
        lastItem = self.mongo.lastCretedAt(self.titanScraping, params)
        if lastItem.count() > 0:
            for lastContent in lastItem:
                self._created_at = lastContent['CreatedAt']

        self.eliminar_repetidos()

    def get_country_platform(self):
        return '^{}.amazon'.format(self.country.lower())

    def eliminar_repetidos(self):
        # Con episodios es imposible trabajar todos de una, así que se hace por cada platformcode
        platformsCodes = self.get_platforms_amazon()
        self.delete_dups_in_titanScraping(platformsCodes)
        self.delete_dups_in_titansScrapingEpisodes(platformsCodes)
        self.hash_dups_with_different_parentId_in_TitanScrapingEpisodes(platformsCodes)

    def delete_dups_in_titanScraping(self, platformsCodes):
        for platform_code in platformsCodes:
            # Elimina todos los ids duplicados separados por  platformcode
            amazons = list(self.mongo.db[self.titanScraping].aggregate(
                [
                    {'$match': {'PlatformCode': platform_code['_id'], 'CreatedAt': self._created_at}},
                    {'$group': {
                        '_id': {
                            "PlatformCode": "$PlatformCode",
                            "Id": "$Id"
                        },
                        'dups': {'$push': "$_id"},
                        'sum': {'$sum': 1}
                    }
                    },
                    {'$match': {'sum': {'$gt': 1}}}
                ])
            )

            for amazon in amazons:
                amazon['dups'].pop()
                self.mongo.db[self.titanScraping].remove({'_id': {'$in': amazon['dups']}})
        # ------------------ End -----------------

    def get_platforms_amazon(self):
        # Recupera los platformcode de amazon porque es muy dificil trabajarlo en episodios todo junto
        platformsCodes = list(self.mongo.db[self.titanScraping].aggregate(
            [
                {'$match': {'PlatformCode': self.get_country_platform(), 'CreatedAt': self._created_at}},
                {'$group': {
                    '_id': '$PlatformCode'
                }
                }
            ])
        )
        return platformsCodes
        # ------------------ End -----------------

    def delete_dups_in_titansScrapingEpisodes(self, platformsCodes):
        for platform_code in platformsCodes:
            # Elimina todos los ids duplicados separados por  platformcode y mismo parentId
            amazon_eps = list(self.mongo.db[self.titanScrapingEpisodios].aggregate(
                [
                    {'$match': {'PlatformCode': platform_code['_id'], 'CreatedAt': self._created_at}},
                    {'$group': {
                        '_id': {
                            "PlatformCode": "$PlatformCode",
                            "Id": "$Id",
                            "ParentId": "$ParentId"
                        },
                        'dups': {'$push': "$_id"},
                        'sum': {'$sum': 1}
                    }
                    },
                    {'$match': {'sum': {'$gt': 1}}}
                ])
            )

            for eps in amazon_eps:
                eps['dups'].pop()
                self.mongo.db[self.titanScrapingEpisodios].remove({'_id': {'$in': eps['dups']}})
            # ------------------ End -----------------

    def hash_dups_with_different_parentId_in_TitanScrapingEpisodes(self, platformsCodes):
        for platform_code in platformsCodes:
            # Hashea Ids repetidos con diferentes parentsId
            dups_diferent_parent_id = list(
                self.mongo.db[self.titanScrapingEpisodios].aggregate(
                    [
                        {'$match': {'PlatformCode': platform_code['_id'], 'CreatedAt': self._created_at}},
                        {'$group': {
                            '_id': {
                                "PlatformCode": "$PlatformCode",
                                "Id": "$Id",
                            },
                            'dups': {'$push': "$_id"},
                            'sum': {'$sum': 1}
                        }
                        },
                        {'$match': {'sum': {'$gt': 1}}}
                    ])
            )

            for dup in dups_diferent_parent_id:
                dup['dups'].pop()
                for dup_ in dup['dups']:
                    element = self.mongo.db[self.titanScrapingEpisodios].find_one({'_id': dup_})
                    self.mongo.db[self.titanScrapingEpisodios].update_many(
                        {'_id': {'$eq': dup_}},
                        {'$set': {'Id': element['Id'] + ':' + element['ParentId']}}
                    )
