# -*- coding: utf-8 -*-
import time
import requests
import hashlib
from common                import config
from datetime              import datetime
from handle.mongo          import mongo
from updates.upload        import Upload
from handle.replace        import _replace

class Pongalo():
    def __init__(self, ott_site_uid, ott_site_country, type):
        self._config                = config()['ott_sites'][ott_site_uid]
        self._start_url             = self._config['start_url']
        self._categories            = self._config['categories']
        self._url_detail            = self._config['url_detail']
        self._platform_code         = self._config['countries'][ott_site_country]
        self._created_at            = time.strftime("%Y-%m-%d")
        self.mongo                  = mongo()
        self.titanScraping          = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodes  = config()['mongo']['collections']['episode']
        # self.titanScrapingBash      = config()['mongo']['collections']['bash']

        self.currentSession = requests.session()

        if type == 'scraping':
            self._scraping()

        elif type == 'return':
            params = {"PlatformCode" : self._platform_code}
            lastItem = self.mongo.lastCretedAt(self.titanScraping, params)
            if lastItem.count() > 0:
                for lastContent in lastItem:
                    self._created_at = lastContent['CreatedAt']

            self._scraping()

    def _scraping(self):
        cursor = self.mongo.search(self.titanScraping, {
            "PlatformCode": self._platform_code,
            "CreatedAt": self._created_at
        }) or list()

        scraped = [item["Id"] for item in cursor]

        cursor = self.mongo.search(self.titanScrapingEpisodes, {
            "PlatformCode": self._platform_code,
            "CreatedAt": self._created_at
        }) or list()

        scraped_eps = list({item["Id"] for item in cursor})

        payloads = list()

        response = self.getUrl(url=self._start_url)
        data = response.json()

        categories = [category['key'] for category in data['categories']]

        for category in categories:
            catUrl = self._categories.format(category=category)
            print(category, catUrl)
            response = self.getUrl(url=catUrl)
            data = response.json()

            for item in data['shows']:
                ### ### ### ### ### ### ### ### ### ### ###
                if item['mediaKey'] in scraped:
                    continue
                else:
                    scraped.append(item['mediaKey'])
                ### ### ### ### ### ### ### ### ### ### ###

                detail_url = self._url_detail.format(category = item['categoryKey'], mediaKey = item['mediaKey'])
                print(detail_url)
                response = self.getUrl(url=detail_url)
                content = response.json()

                _type = 'serie' if content['isEpisodic'] else 'movie'
                _year = None if not content['year'] else int(content['year'])
                _seasons = None if content.get('seasons') == None else len(content.get('seasons'))
                _runtime = None if not content['runningTimeSeconds'] else int(content['runningTimeSeconds'] // 60)
                _deeplink = self._config['url'].format(category=category, id=content['mediaKey'])

                cast = self.cast(content=content)

                if _type == 'movie' and content['episodes'][0]['director'] != []:
                    director = content['episodes'][0]['director']
                elif _type == 'serie' and content['seasons'][0]['episodes'][0]['director'] != []:
                    director = content['seasons'][0]['episodes'][0]['director']
                else:
                    director = None

                synopsis = content['summary_ES']

                if synopsis == '':
                    synopsis = None

                payload = {
                    'PlatformCode'   : self._platform_code,
                    'Id'             : content['mediaKey'],
                    'Type'           : _type,
                    'Title'          : content['title'],
                    'CleanTitle'     : _replace(content['title']),
                    'OriginalTitle'  : None,
                    'Year'           : _year,
                    'Duration'       : _runtime,
                    'Deeplinks'      : {
                                       'Web'     : _deeplink,
                                       'Android' :  None,
                                       'iOS'     : None
                    },
                    'Synopsis'       : synopsis,
                    'Rating'         : content['rating'],
                    'Provider'       : None,
                    'Genres'         : [genre.strip() for genre in content['genre'].split('|')],
                    'Cast'           : cast,
                    'Directors'      : director,
                    'Availability'   : None,
                    'Download'       : None,
                    'IsOriginal'     : None,
                    'IsAdult'        : None,
                    # 'Season'         : _seasons,
                    # 'Episode'        : content['episodesTotal'],
                    # 'Crew'           : content['writer'],
                    'Packages'       : [{ "Type" : 'subscription-vod'}],
                    'Country'        : None,
                    'Timestamp'      : datetime.now().isoformat(),
                    'CreatedAt'      : self._created_at
                }
                payloads.append(payload)

                if len(payloads) > 100:
                    self.mongo.insertMany(self.titanScraping, payloads)
                    print("!{} Titles Inserted ".format(len(payloads)))
                    payloads.clear()

                #Episodios
                if _type == 'serie':
                    epipayloads = list()

                    for e in content['seasons']:
                        for episode in e['episodes']:
                            episode_hash = hashlib.sha224('{parentID}{episodeID}{SeasonID}'.format(parentID = episode['mediaKey'], episodeID = episode['number'], SeasonID = episode['season']).encode('UTF-8')).hexdigest()
                            ### ### ### ### ### ### ### ### ### ###
                            if episode_hash in scraped_eps:
                                continue
                            else:
                                scraped_eps.append(episode_hash)
                            ### ### ### ### ### ### ### ### ### ###

                            epicast = self.cast(content=episode)
                            epidirector = episode['director'] if episode['director'] != [] else None
                            epi_runtime = None if not episode['runningTimeSeconds'] == None else int(episode['runningTimeSeconds'] // 60)
                            epi_synopsis = episode.get('summary_ES')
                            epi_deeplink = 'https://pongalo.com/{}/{}/{}/{}'.format(category, episode['mediaKey'], episode['season'], episode['number'])

                            season = int(episode['season'])

                            if season == 0:
                                continue

                            if epi_synopsis == '':
                                epi_synopsis = None

                            episode_number = episode['number'] if episode['number'] != 0 else None

                            payload = {
                                'PlatformCode'   : self._platform_code,
                                'ParentId'       : episode['mediaKey'],
                                'ParentTitle'    : content['title'],
                                'Id'             : episode_hash,
                                'Title'          : episode['title'],
                                'Episode'        : episode_number,
                                'Season'         : episode['season'],
                                'Year'           : None,
                                'Duration'       : epi_runtime,
                                'Deeplinks'      : {
                                                    'Web'     : epi_deeplink,
                                                    'Android' : None,
                                                    'iOS'     : None
                                },
                                'Synopsis'       : epi_synopsis,
                                'Rating'         : None,
                                'Provider'       : None,
                                'Genres'         : None,
                                'Cast'           : epicast,
                                'Directors'      : epidirector,
                                'Availability'   : None,
                                'Download'       : None,
                                'IsOriginal'     : None,
                                'IsAdult'        : None,
                                'Country'        : None,
                                'Packages'       : [{ "Type" : 'subscription-vod'}],
                                'Timestamp'      : datetime.now().isoformat(),
                                'CreatedAt'      : self._created_at
                            }
                            epipayloads.append(payload)

                    if epipayloads:
                        self.mongo.insertMany(self.titanScrapingEpisodes, epipayloads)
                        print("! {} Episodes Inserted ".format(len(epipayloads)))

        if payloads:
            self.mongo.insertMany(self.titanScraping, payloads)
            print("!{} Titles Inserted ".format(len(payloads)))

        self.currentSession.close()

        print('Finished')
        Upload(self._platform_code, self._created_at, False)


    def cast(self, content):
        if content['cast'] != []:
            cast = []
            for people in content['cast']:
                if ',' in people:
                    people = people.split(',')
                    for p in people:
                        cast.append(p.strip())
                else:
                    cast.append(people)
        else:
            cast = None

        return cast

    def getUrl(self, url):
        requestsTimeout = 5
        while True:
            try:
                response = self.currentSession.get(url, timeout=requestsTimeout)
                return response
            except requests.exceptions.ConnectionError:
                print("Connection Error, Retrying")
                time.sleep(requestsTimeout)
                requestsTimeout = requestsTimeout + 5
                if requestsTimeout == 45:
                    print('Timeout has reached 45 seconds.')
                    break
                continue
            except requests.exceptions.RequestException:
                print('Waiting...')
                time.sleep(requestsTimeout)
                requestsTimeout = requestsTimeout + 5
                if requestsTimeout == 45:
                    print('Timeout has reached 45 seconds.')
                    break
                continue
            break


