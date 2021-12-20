# -*- coding: utf-8 -*-
import pymongo
from settings import settings
from handle.season_helper import SeasonHelper

class mongo():
    def __init__(self):
        hostMongo  = settings.MONGODB_DATABASE_URI
        connection = pymongo.MongoClient(hostMongo, connect=False, maxPoolSize=None)
        self.db = connection.titan

    def lastCretedAt(self, collection, params):
        sort   = [("CreatedAt", pymongo.DESCENDING)]
        limit  = 1
        lastItem = self.db[collection].find(params).sort(sort).limit(limit)
        return lastItem

    def insert(self, collection, payload):
        query =  self.db[collection].insert_one(payload)
        if query:
            return True
        else:
            return False

    def insertMany(self, collection, payload):
        '''
            Insert many payloads in collection.

            Parameters:
                - collection:       Specify a collection where data is
                                    going to be saved.

                - payload   :       Payload to be stored.
        '''

        query =  self.db[collection].insert_many(payload)
        if query:
            return True
        else:
            return False

    def delete(self, collection, payload):
        query =  self.db[collection].delete_many(payload)
        return query.deleted_count

    def deleteOne(self, collection, payload):
        query =  self.db[collection].delete_one(payload)
        return query.deleted_count

    def comparation(self, collection, payload):
        query =  self.db[collection].find_one(payload)
        if query:
            return query
        else:
            return False

    def distinct(self, collection, field, params):
        query = self.db[collection].distinct(field, params)
        return query

    def count(self, collection, payload):
        query = self.db[collection].count_documents(payload)
        return query

    def search(self, collection, payload):
        query =  self.db[collection].find(payload, no_cursor_timeout=True).batch_size(10)
        # items = self.dbTitan['titanScraping'].find(params, no_cursor_timeout=True).batch_size(10)
        if query.count() > 0:
            return query
        else:
            return False

    def searchSort(self, collection, payload, sort):
        query = self.db[collection].find(payload, no_cursor_timeout=True, sort=sort).batch_size(10)
        return query if query.count() else False

    def update(self, collection, payload):
        query = self.db[collection].update_many(
            {'Hash': payload['Hash']},
            {
                "$set": {
                    'RentTime' : payload['RentTime'],
                    'Download' : payload['Download'],
                    'Packages' : payload['Packages']
                }
            }
        )
        if query:
            return True
        else:
            return False

    def gralUpdate(self, collection, find, payload):
        query = self.db[collection].update_many(
            find,
            payload
        )
        if query:
            return True
        else:
            return False

    @staticmethod
    def generate_payload_season(plataforma, titan_scraping, titan_scraping_episodes, created_at, platform_code):
        # genera los payloads de las temporadas donde corresponde porque tiene las mismas separadas
        print('--------- Comienza el armado de temporadas ---------')
        series = plataforma.mongo.db[titan_scraping].find(
            {'PlatformCode': platform_code, 'CreatedAt': created_at, 'Type': 'serie'})
        series = list(series)
        for serie in series:
            episodes = plataforma.mongo.db[titan_scraping_episodes].find(
                {'PlatformCode': platform_code, 'CreatedAt': created_at, 'ParentId': serie['Id']})
            episodes = list(episodes)
            seasons = SeasonHelper().get_seasons_complete(episodes)
            if seasons:
                for season in seasons:
                    season['Deeplink'] = serie['Deeplinks']['Web']
            query = {'PlatformCode': platform_code, 'CreatedAt': created_at, 'Id': serie['Id']}
            new_value = {"$set": {"Seasons": seasons}}
            plataforma.mongo.db[titan_scraping].update(query, new_value)
        print('--------- Finalizado el armado de temporadas ---------')
