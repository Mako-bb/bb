# -*- coding: utf-8 -*-
import time
import requests
import hashlib   
import pymongo 
import re
import json
import platform
from handle.replace         import _replace
from common                 import config
from datetime               import datetime
from handle.mongo           import mongo
from slugify                import slugify
from bs4                    import BeautifulSoup
from selenium               import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from handle.datamanager     import Datamanager
from updates.upload         import Upload
'''
Categories:
    - Shows
        -Franchises
            -Playlist
            -Movies
            -Series
                -Seasons
                    -Episodes
    - Movies
    - Playlists
'''
class MyOutdoorTV():
    def __init__(self, ott_site_uid, ott_site_country, type):
        self._config                = config()['ott_sites'][ott_site_uid]
        self._created_at            = time.strftime("%Y-%m-%d")
        self.mongo                  = mongo()
        self._platform_code         = self._config['countries'][ott_site_country]   
        self.titanScraping          = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodios = config()['mongo']['collections']['episode']
        self.sesion                 = requests.session()
        self.skippedEpis            = 0
        self.skippedTitles          = 0
        
        if type == 'scraping':
            self._scraping()

        if type == 'testing':
            self._scraping(testing = True)

    def _scraping(self, testing = False):
        listDBMovie = Datamanager._getListDB(self,self.titanScraping)
        listDBEpi = Datamanager._getListDB(self,self.titanScrapingEpisodios)

        listPayload = []
        listPayloadEpi = []
        #### GET MOVIES OF MOVIES CATEGORY####
        page = 1
        check = []
        req = requests.get('https://watch.boomerang.com/api/5/1/collection-items/c196?page='+str(page)+'&trans=en', headers=headers)
        soup = req.json()
        total_page = soup['num_pages']
        while page<total_page:
            for item in soup['values']:
                id_movie = item['item']['uuid']
                typeOf = 'movie' if item['item']['is_film'] else 'serie'
                title_movie = item['item']['title']
                rating_movie = item['item']['tv_rating']
                duration_movie = item['item']['runtime']
                year_movie = item['item']['year']
                synopsis_movie = item['item']['description']
                provider_movie = item['item']['network']
                originalTitle_movie = item['item']['native_lang_title'] if item['item'].get('native_lang_title') else None
                root_weblink = 'https://watch.boomerang.com'
                deeplink_movie = root_weblink + item['item']['url']
                image_movie = (item['item']['base_asset_url']+'/poster.jpg').split(',')
                check.append(title_movie)
                print(duration_movie)
                print(title_movie)
                #print(rating_movie)
                #print(provider_movie)
            page += 1
            req = requests.get('https://watch.boomerang.com/api/5/1/collection-items/c196?page='+str(page)+'&trans=en', headers=headers)
            soup = req.json()
            total_page = soup['num_pages']

        #### GET FRANCHISE OF SHOWS CATEGORY ####
        headers = {
        'X-Consumer-Key': 'DA59dtVXYLxajktV'
        }
        franchise_list = []
        page = 1
        req = requests.get('https://watch.boomerang.com/api/5/1/collection-items/c195?page='+str(page)+'&trans=en', headers=headers)
        json_response = req.json()
        num_pages = json_response['num_pages']
        while num_pages>=page:
            for franchise in json_response['values']:
                franchise_list.append(franchise['item']['slug'])
            page += 1
            req = requests.get('https://watch.boomerang.com/api/5/1/collection-items/c195?page='+str(page)+'&trans=en', headers=headers)
            json_response = req.json()

        #### GET CONTENT(Serie,Movie,Playlist) OF FRANCHISE ITEMS ####
        content_list = []
        for elem in franchise_list:
            req = requests.get('https://watch.boomerang.com/api/5/1/collection-items/slug/'+elem, headers=headers)
            json_response = req.json()
            for item in json_response['values']:
                content_list.append(item['item']['url'].replace('/browse/genre/',''))
                
        #### GET URL OF CONTENT ####
        urls_item = {}
        for item in content_list:
            if not 'playlist' in item:
                page = 1
                req = requests.get('https://watch.boomerang.com/api/5/1/collection-items/slug/'+item+'/?page='+str(page)+'&trans=en', headers=headers)
                json_response = req.json()
                total_page = json_response['num_pages']
                while total_page>=page:
                    for elem in json_response['values']:
                        urls_item[elem['item']['title']] = {'url':elem['item']['url'],'num_seasons':elem['item']['num_seasons']}
                    page += 1
                    req = requests.get('https://watch.boomerang.com/api/5/1/collection-items/slug/'+item+'/?page='+str(page)+'&trans=en', headers=headers)
                    json_response = req.json()

        #### GET SEASONS/EPISODES ####
        root_weblink = 'https://watch.boomerang.com'
        for h in urls_item:
            url = urls_item[h]['url']
            seasons = urls_item[h]['num_seasons']
            typeOf = urls_item[h]['type']
            page = 1
            while seasons >= page:
                req = requests.get('https://watch.boomerang.com/api/5/series/'+url+'/seasons/'+str(page)+'?trans=en',headers=headers)
                json_response = req.json()
                for elem in json_response['values']:
                    id_episode = elem['video_uuid']
                    parent_id = elem['series_uuid']
                    title_episode = elem['title']
                    number_episode = elem['number']
                    number_season = elem['season']
                    duration_episode = int(elem['duration'].split(':')[1])
                    image_episode = (elem['base_asset_url']+'/thumb.jpg').split(',')
                    synopsis_episode = elem['description']
                    rating_episode = elem['tv_rating']
                    
                    deeplink_episode = root_weblink + elem['url']
                    
                    packages = {
                                'Type': 'subscription-vod'
                                }
                    
                    payloadEpi = {
                                'PlatformCode'  : self._platform_code,
                                'Id'            : id_episode,
                                'Title'         : title_episode,
                                'OriginalTitle' : None,
                                'ParentId'      : parent_id,
                                'ParentTitle'   : None,
                                'Season'        : number_season,
                                'Episode'       : number_episode,
                                'Year'          : None,
                                'Duration'      : duration_episode,
                                'Deeplinks'     : {
                                    'Web': deeplink_episode,
                                    'Android': None,
                                    'iOS': None
                                },
                                'Playback'      : None,
                                'Synopsis'      : synopsis_episode,
                                'Image'         : None,
                                'Rating'        : rating_episode,
                                'Provider'      : None,
                                'Genres'        : None,
                                'Cast'          : None,
                                'Directors'     : None,
                                'Availability'  : None,
                                'Download'      : None,
                                'IsOriginal'    : None,
                                'IsAdult'       : None,
                                'Country'       : None,
                                'Packages'      : packages,
                                'Timestamp'     : datetime.now().isoformat(),
                                'CreatedAt'     : self._created_at
                                }
                    
                    payload_list.append(payloadEpi)
                page += 1
                req = requests.get('https://watch.boomerang.com/api/5/series/'+url+'/seasons/'+str(page)+'?trans=en',headers=headers)
                json_response = req.json()