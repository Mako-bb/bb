# -*- coding: utf-8 -*-
import pymongo
from common    import config

class Mongo():
    def conect(self):               
        hostMongo  = config()['mongo']['host']
        connection = pymongo.MongoClient(hostMongo, connect=False, maxPoolSize=None)
        db = connection.content_api

        return db

    def conectTitan(self):
        hostMongo  = config()['mongo']['host']
        connection = pymongo.MongoClient(hostMongo, connect=False, maxPoolSize=None)
        db = connection.titan

        return db

    def check_if_exists_platform(self, db_connection, platform_hash):
	    if db_connection['pPruebas'].find_one({"platform_hash": platform_hash}): 
		    return True

	    return False