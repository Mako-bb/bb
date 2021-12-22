import time
import requests
from handle.mongo           import mongo
from handle.datamanager     import Datamanager
from common                         import config
import pymongo


#### Como usar la clase
######## checkSeries._checkEpis_(self.mongo, self.titanScraping, self.titanScrapingEpisodes, self._platform_code, self._created_at)
########## Args opcionales: delete (default=False), elimina directamente las series sin episodios
########## Args opcionales: getList (default=False), devuelve la lista con las series sin episodios
class checkSeries():

    def __init__(self, ott_site_uid, ott_site_country, type):
        self._config                = config()['ott_sites'][ott_site_uid]
        self._created_at            = time.strftime("%Y-%m-%d")
        self.mongo                  = mongo()
        self._platform_code         = self._config['countries'][ott_site_country]   
        self.titanScraping          = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodios = config()['mongo']['collections']['episode']
        self.sesion                 = requests.session()
        self.skippedTitles          = 0
        self.skippedEpis            = 0
        self.addheader              = False
        self.currentSession         = requests.session() 


    @staticmethod
    def _checkEpis_( mongo, db, dbEpi,  platformCode, createdAt, delete=False, getList=False):

        series_with_ep = list(mongo.db[dbEpi].aggregate(
            [
                {'$match': {'PlatformCode': platformCode, 'CreatedAt': createdAt}},
                {'$group': {
                    '_id': {
                        "ParentId": "$ParentId",
                        }
                    }
                }
            ] )
        )


        ids = [ p['_id']['ParentId'] for p in series_with_ep ]


        series =  list(mongo.db[db].find(
            {'PlatformCode': platformCode, 'Type':'serie','CreatedAt': createdAt,'Id':{'$nin':ids}},
            projection={'_id': 0, 'Id': 1, 'Title': 1}
        ))
        
        if series:

            if not getList:
                print("--------| Series sin Episodios |---------")
                print(series)

            if delete:
                list(mongo.db[db].remove(
                    {'PlatformCode': platformCode, 'Type':'serie','CreatedAt': createdAt,'Id':{'$nin':ids}}
                ))  

                print("------- Series Eliminadas --------")
            # elif not delete and getList:
            #     return series
            if getList:
                return series
        else:
            print("--------| Todas las series tienen epi |---------")

    def _checkEpis(self):
        print('\n\n')
        #listDBMovie = Datamanager._getListDB(self, self.titanScraping)

        myclient = pymongo.MongoClient("mongodb://localhost:27017/")
        titan = myclient['titan']
        titanScraping = titan['titanScraping']
        listDBMovie = titanScraping.find({'PlatformCode':self._platform_code ,'CreatedAt': self._created_at, 'Type':'serie'})
        listDBEpisodes = Datamanager._getListDB(self, self.titanScrapingEpisodios)
        
        #listDBMovie = self.titanScraping.find({'Type':'serie','CreatedAt': self._created_at})
        serie_with_epis = False
        series = []
        for title in listDBMovie:
            serie_with_epis = False
            for epis in listDBEpisodes:
                if title['Id'] == epis['ParentId']:
                    serie_with_epis = True
            if serie_with_epis == False:
                series.append(title)
        if series != []:
            #print('Las siguientes series no tienen episodios en titanScrapingEpisodes:')
            print("\x1b[1;33;40m SERIES SIN EPISODIOS : \x1b[0m") 
            for items in series:
                print("\x1b[1;31;40m SERIE \x1b[0m {} : {}".format(items['Id'],items['Title']))
                #print('Id: ' + items['Id'])
                #print('Title: ' + items['Title'])
                #print(' ')
        else:
            print("\x1b[1;32;40m TODAS LAS SERIES TIENEN EPISODIOS CARGADOS. \x1b[0m")
            #print('Todas las series tienen episodios cargados.')

        episodios = []
        for epis in listDBEpisodes:
            epis_with_serie = False
            for seri in listDBMovie:
                if epis['ParentId'] == seri['Id']:
                    epis_with_serie = True
            if epis_with_serie == False:
                episodios.append(epis)
        if episodios != []:
            #print('Las siguientes series no tienen episodios en titanScrapingEpisodes:')
            print("\x1b[47m;33;40m EPISODIOS SIN SERIE : \x1b[0m") 
            for items in episodios:
                print("\x1b[1;31;40m EPISODIO \x1b[0m {} : {} >> {}".format(items['Id'],items['Title'],items['ParentId']))

        else:
            print("\x1b[1;32;40m TODAS LOS EPISODIOS TIENEN SERIE. \x1b[0m")
    
