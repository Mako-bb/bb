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
from handle.datamanager  import Datamanager
from updates.upload         import Upload

class Optimum():
    def __init__(self, ott_site_uid, ott_site_country, type):
        self._config                = config()['ott_sites'][ott_site_uid]
        self._platform_code         = self._config['countries'][ott_site_country]
        #self._start_url             = self._config['start_url']
        self._created_at            = time.strftime("%Y-%m-%d")
        self.mongo                  = mongo()
        self.titanPreScraping       = config()['mongo']['collections']['prescraping']
        self.titanScraping          = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodes  = config()['mongo']['collections']['episode']

        self.currentSession = requests.session()
        self.headers  = {"Accept":"application/json",
                         "Content-Type":"application/json; charset=utf-8"}

        if type == 'return':
            '''
            Retorna a la Ultima Fecha
            '''
            params = {"PlatformCode" : self._platform_code}
            lastItem = self.mongo.lastCretedAt(self.titanScraping, params)
            if lastItem.count() > 0:
                for lastContent in lastItem:
                    self._created_at = lastContent['CreatedAt']

            self._scraping()
        
        if type == 'scraping':
            self._scraping()
        if type == 'testing':
            self._scraping(testing == True)

    def __query_field(self, collection, field, extra_filter=None):
        if not extra_filter:
            extra_filter = {}

        find_filter = {
            'PlatformCode': self._platform_code,
            'CreatedAt': self._created_at,
        }

        find_filter.update(extra_filter)

        query = self.mongo.db[collection].find(
            filter=find_filter,
            projection={
                '_id': 0,
                field: 1,
            },
            no_cursor_timeout=False
        )

        query = {item[field] for item in query}

        return query

    def _scraping(self):

        if platform.system() == 'Linux':
            Display(visible=0, size=(1366, 768)).start()

        payloads = []
        payloads_episodes = []
        series_guardadas = {}

        scraped = self.__query_field(self.titanScraping, 'Id')
        scraped_episodes = self.__query_field(self.titanScrapingEpisodes, 'Id')

        categorias_pelis = {
            'Optimum': '48265001',
            'HBO': '213279202',
            'Starz': '175970002',
            'Starz Encore': '175982002',
            'Showtime': '210895202',
            'Cinemax': '7301002',
            'TOKU': '16841002',
            'here!': '167988002',
            'Eros Now': '23300002',
            'TMC': '44165002'
        }

        #Peliculas

        for c in categorias_pelis:

            offset = 0

            while True:

                req = self.getUrl('https://www.optimum.net/api/vod-webapp/services/v1/onyx/getTitlesForPagination/{}/20/{}?sort=1&filter=0'.format(categorias_pelis[c], offset))
                print(req.status_code, req.url)

                if req.json()['data'] == {}:
                    break

                data = req.json()['data']['result']

                for i, peli in enumerate(data['titles']):

                    id_ = peli.get('asset_id') if peli.get('asset_id') else peli['sd_asset']

                    title = peli.get('tms_title') if peli.get('tms_title') else peli['title']

                    directors = peli.get('directors')
                    if directors:
                        directors = directors.split(', ') if directors.split(', ') != [] else None

                    actors = peli.get('actors')
                    if actors:
                        actors = actors.split(', ') if actors.split(', ') != [] else None

                    genres = peli.get('genres')
                    if genres:
                        genres = genres.split(', ') if genres.split(', ') != [] else None

                    rent_price = float(peli['price']) if float(peli['price']) != 0 else None

                    payload = {
                        'PlatformCode':  self._platform_code,
                        'Id':            str(id_),
                        'Title':         title,
                        'OriginalTitle': None,
                        'CleanTitle':    _replace(title),
                        'Type':          'movie',
                        'Year':          peli['release_year'] if peli['release_year'] >= 1870 and peli['release_year'] <= datetime.now().year else None,
                        'Duration':      peli['stream_length'] if peli['stream_length'] > 0 else None,
                        'Deeplinks': {
                            'Web':       'https://www.optimum.net/tv/asset/#/movie/{}'.format(id_),
                            'Android':   None,
                            'iOS':       None,
                        },
                        'Playback':      None,
                        'Synopsis':      peli['long_desc'] if peli['long_desc'] != '' else None,
                        'Image':         None,
                        'Rating':        peli['rating_system'],
                        'Provider':      [c] if c != 'Optimum' else None,
                        'Genres':        genres,
                        'Cast':          actors,
                        'Directors':     directors,
                        'Availability':  None,
                        'Download':      None,
                        'IsOriginal':    None,
                        'IsAdult':       None,
                        'Packages':      [{'Type':'transaction-vod', 'RentPrice': rent_price}],
                        'Country':       None,
                        'Timestamp':     datetime.now().isoformat(),
                        'CreatedAt':     self._created_at
                    }

                    if payload['Id'] in scraped:

                        print("Ya existe el id {}".format(payload['Id']))
                    else:
                        payloads.append(payload)
                        scraped.add(payload['Id'])
                        print("Insertado {} - ({} / {})".format(payload['Title'], i + 1, len(data['titles'])))

                offset += 20

        # print(len(payloads))

        #Series

        browser = webdriver.Firefox()

        browser.get('https://www.optimum.net/tv/on-demand/#/')

        action = webdriver.common.action_chains.ActionChains(browser)
        action.move_by_offset(5, 5).click().perform()
        time.sleep(30)

        if browser.find_element_by_xpath('//a[@class="alert-minor alert-drawer__handle hbeam-inline badge-notification badge-primary"]').is_displayed():
            browser.find_element_by_xpath('//a[@class="alert-minor alert-drawer__handle hbeam-inline badge-notification badge-primary"]').click()

        time.sleep(2)

        action = webdriver.common.action_chains.ActionChains(browser)
        section = browser.find_element_by_xpath("//section[@class='on-demand']", )
        div_cont = section.find_elements_by_xpath(".//div[@ng-show='showMegaMenu']")
        div_cont = div_cont[1].find_element_by_xpath(".//div[@class='container']")
        boton = div_cont.find_element_by_tag_name('button')
        action.move_to_element(boton).click().perform()


        section = browser.find_element_by_xpath("//section[@class='on-demand']", )
        div_cont = section.find_elements_by_xpath(".//div[@ng-show='showMegaMenu']")
        div_cont = div_cont[1].find_element_by_xpath(".//div[@class='container']")
        div_menu = div_cont.find_element_by_xpath(".//div[@class='row channels-wrap']")
        lista = div_menu.find_elements_by_xpath(".//li[@class='ng-scope']")
        # print(lista)

        ids_ = []
        has_seasons = False

        for i, l in enumerate(lista):
            a = l.find_element_by_tag_name('a')
            provider = a.text
            a.click()
            time.sleep(1)
            sub_lista = l.find_elements_by_xpath(".//li[@class='items']")

            for sub_l in sub_lista:
                a = sub_l.find_element_by_tag_name('a')
                if 'Series' in a.text:
                    a.click()
                    time.sleep(1)

                    if 'Showtime' in provider:
                        has_seasons = True
                    else:
                        has_seasons = False

                    if 'Starz' in provider:
                        sub_l = sub_l.find_elements_by_xpath(".//li[@class='items']")
                        sub_l = sub_l[0]
                        a = sub_l.find_element_by_tag_name('a')
                        browser.execute_script("arguments[0].click();", a)
                        # a.click()
                        time.sleep(1)

                    try:
                        total = sub_l.find_element_by_xpath(".//li[@class='seeMore']")
                        total = total.get_attribute('ng-show')
                        total = int(total[total.index("<") + 1:])
                    except:
                        total = 0

                    if total == 0 and ('Cinemax' not in provider and 'Starz' not in provider):
                        print("entre al continue perri")
                        continue

                    titulos = self.more_titles(sub_l, 0, total, has_seasons, div_menu, browser)

                    for t in titulos:
                        t = t[:-1].replace('getDetails(', '')
                        jsont = json.loads(t)
                        ids_.append(jsont['menu_id'])

                    back = div_menu.find_element_by_xpath(".//a[@class='dropdown-control prevList']")
                    while True:
                        try:
                            back.click()
                        except:
                            break
                else:
                    continue

            if i == 7:
                break

        browser.quit()

        print(len(ids_))


        # asd = input()

        for i, id_ in enumerate(ids_):

            if str(id_) in scraped:
                print("Ya existe {}".format(id_))
                continue

            req = self.getUrl('https://www.optimum.net/api/vod-webapp/services/v1/onyx/getSeriesDetails/{}/menu/'.format(id_))
            print(req.status_code, req.url)

            data = req.json()

            try:
                datos_serie = data['episodeList']['result']['titles'][0]
            except:
                continue

            if datos_serie.get('directors'):
                directors = datos_serie['directors'].split(',') if datos_serie['directors'].split(',') != [] else None
            else:
                directors = None

            title = datos_serie.get('seriesName')

            if title == None:
                continue

            if title in series_guardadas:
                id_ = series_guardadas[title]

            payload = {
                'PlatformCode':  self._platform_code,
                'Id':            str(id_),
                'Title':         title,
                'OriginalTitle': None,
                'CleanTitle':    _replace(title),
                'Type':          'serie',
                'Year':          datos_serie['release_year'] if datos_serie['release_year'] >= 1870 and datos_serie['release_year'] <= datetime.now().year else None,
                'Duration':      None,
                'Deeplinks': {
                    'Web':       'https://www.optimum.net/tv/asset/#/series/{}'.format(id_),
                    'Android':   None,
                    'iOS':       None,
                },
                'Playback':      None,
                'Synopsis':      None,
                'Image':         None,
                'Rating':        None,
                'Provider':      None,
                'Genres':        datos_serie['genres'].split(', ') if datos_serie['genres'].split(', ') != [] else None,
                'Cast':          None,
                'Directors':     directors,
                'Availability':  None,
                'Download':      None,
                'IsOriginal':    None,
                'IsAdult':       None,
                'Packages':      [{'Type':'tv-everywhere'}],
                'Country':       None,
                'Timestamp':     datetime.now().isoformat(),
                'CreatedAt':     self._created_at
            }

            if payload['Id'] in scraped:

                print("Ya existe el id {}".format(payload['Id']))
            else:
                payloads.append(payload)
                scraped.add(payload['Id'])
                series_guardadas[payload['Title']] = payload['Id']
                print("Insertado {} - ({} / {})".format(payload['Title'], i + 1, len(ids_)))


            episodios = data['episodeList']['result']['titles']

            for epi in episodios:

                if epi.get('actors'):
                    epi_actors = epi['actors'].split(', ') if epi['actors'].split(', ') != [] else None
                else:
                    epi_actors = None

                if epi.get('directors'):
                    epi_directors = epi['directors'].split(', ') if epi['directors'].split(', ') != [] else None
                else:
                    epi_directors = None


                payload_episodio = {

                    'PlatformCode':  self._platform_code,
                    'Id':            str(epi['asset_id']),
                    'Title':         epi['title'],
                    'OriginalTitle': None,
                    'ParentId':      str(id_),
                    'ParentTitle':   title,
                    'Season':        epi.get('season_number'),
                    'Episode':       epi.get('episode_number'),
                    'Year':          epi['release_year'] if epi['release_year'] >= 1870 and epi['release_year'] <= datetime.now().year else None,
                    'Duration':      epi['stream_length'] if epi['stream_length'] != 0 else None,
                    'Deeplinks': {
                        'Web':       'https://www.optimum.net/tv/asset/#/episode/{}'.format(epi['asset_id']),
                        'Android':   None,
                        'iOS':       None,
                    },
                    'Playback':      None,
                    'Synopsis':      epi['long_desc'] if epi['long_desc'] != '' else None,
                    'Image':         None,
                    'Rating':        None,
                    'Provider':      None,
                    'Genres':        epi['genres'].split(', ') if epi['genres'].split(', ') != [] else None,
                    'Cast':          epi_actors,
                    'Directors':     epi_directors,
                    'Availability':  None,
                    'Download':      None,
                    'IsOriginal':    None,
                    'IsAdult':       None,
                    'Packages':      [{'Type':'tv-everywhere'}],
                    'Country':       None,
                    'Timestamp':     datetime.now().isoformat(),
                    'CreatedAt':     self._created_at
                }

                if payload_episodio['Id'] in scraped_episodes:

                    print("Ya existe el episodio id {}".format(payload_episodio['Id']))
                    continue
                else:
                    payloads_episodes.append(payload_episodio)
                    scraped_episodes.add(payload_episodio['Id'])
                    print("Insertado episodio {}".format(payload_episodio['Id']))

        print(len(payloads))
        print(len(payloads_episodes))

        if payloads:
            self.mongo.insertMany(self.titanScraping, payloads)
            print('Insertados {} en {}'.format(len(payloads), self.titanScraping))

        if payloads_episodes:
            self.mongo.insertMany(self.titanScrapingEpisodes, payloads_episodes)
            print('Insertados {} en {}'.format(len(payloads_episodes), self.titanScrapingEpisodes))

        self.currentSession.close()

        Upload(self._platform_code, self._created_at, testing=True)



    def more_titles(self, lista, count, total, has_seasons, div_menu, browser):

        titulos = lista.find_elements_by_xpath(".//li[@class='items']")

        lista_ids = []

        for t in titulos:
            a = t.find_element_by_tag_name('a')

            if has_seasons:
                # a.click()
                texto = a.get_attribute('ng-click')
                # print(type(texto), "\n", texto, '\n\n')
                if 'next_level_index' in texto:
                    browser.execute_script("arguments[0].click();", a)
                    ides = t.find_elements_by_xpath(".//li[@class='items']")

                    for i in ides:
                        a = i.find_element_by_tag_name('a')
                        id_ = a.get_attribute('ng-click')
                        if id_ not in lista_ids:
                            lista_ids.append(id_)

                    back = div_menu.find_element_by_xpath(".//a[@class='dropdown-control prevList']")
                    back.click()
                else:
                    id_ = texto
                    if id_ not in lista_ids:
                        lista_ids.append(id_)

            else:
                id_ = a.get_attribute('ng-click')
                if id_ not in lista_ids:
                    lista_ids.append(id_)

            count += 1

        if count < total:
            lista.find_element_by_xpath('.//li[@class="seeMore"]').click()
            lista_mas_ids = self.more_titles(titulos[-1], count, total, has_seasons, div_menu, browser)

            return lista_ids + lista_mas_ids

        else:
            return lista_ids


    def getUrl(self, url):
        requestsTimeout = 5
        while True:
            try:
                response = self.currentSession.get(url, timeout=requestsTimeout)
                return response
            except requests.exceptions.ConnectionError:
                print("Connection Error, Retrying")
                time.sleep(requestsTimeout)
                continue
            except requests.exceptions.RequestException:
                print('Waiting...')
                time.sleep(requestsTimeout)
                continue
            break
