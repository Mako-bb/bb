# -*- coding: utf-8 -*-
import json
import time
import requests
import hashlib
import platform
import sys, os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from common                 import config
from bs4                    import BeautifulSoup
from datetime               import datetime, timedelta
from handle.mongo           import mongo
from updates.upload         import Upload
from handle.datamanager     import Datamanager
from handle.replace         import _replace


class FandangoNOW():
    def __init__(self, ott_site_uid, ott_site_country, type):
        self._config                = config()['ott_sites'][ott_site_uid]
        self._created_at            = time.strftime("%Y-%m-%d")
        self.mongo                  = mongo()
        self._platform_code         = self._config['countries'][ott_site_country]   
        self.titanPreScraping       = config()['mongo']['collections']['prescraping']
        self.titanScraping          = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodes  = config()['mongo']['collections']['episode']
        self.sesion                 = requests.session()
        self.skippedEpis            = 0
        self.skippedTitles          = 0

        if type == 'scraping':
            self._scraping()
        
        elif type == 'testing':
            self._scraping(testing=True)
        
        elif type == 'return':
            '''
            Retorna a la Ultima Fecha
            '''

            params = {"PlatformCode" : self._platform_code}
            lastItem = self.mongo.lastCretedAt(self.titanScraping, params)
            if lastItem.count() > 0:
                for lastContent in lastItem:
                    self._created_at = lastContent['CreatedAt']
            self._scraping()
    
    def _obtain_response(self, url, **kwargs):
        requests_timeout = 5
        method  = kwargs.get("method", "GET")
        headers = kwargs.get("headers", None)
        data    = kwargs.get("data", None)
        while True:
            try:
                timeout = requests_timeout if method == "GET" else None
                response = self.sesion.request(method, url, headers=headers, data=data, timeout=timeout)
                return response
            except requests.exceptions.ConnectionError:
                print("Connection Error, Retrying")
                time.sleep(requests_timeout)
                continue
            except requests.exceptions.RequestException:
                print('Waiting...', url)
                time.sleep(requests_timeout)
                continue

    def _async_requests(self, list_urls, max_workers=3, headers=None):
        list_responses = []
        len_urls = len(list_urls)
        # ["url1","url2","url3","url4","url5","url6"] --> [["url1","url2","url3"], ["url4","url5","url6"]]
        list_urls = [list(list_urls[i:i+max_workers]) for i in range(0, len_urls, max_workers)]
        for sublist_urls in list_urls:
            list_threads = []
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                for url in sublist_urls:
                    # time.sleep(0.1)
                    print("ASYNC request:", url)
                    list_threads.append(executor.submit(self._obtain_response, url, headers=headers))
                for task in as_completed(list_threads):
                    list_responses.append(task.result())
            del list_threads
        
        return list_responses
    
    def _query_field(self, collection, field, extra_filter=None):
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
    
    def _scraping(self, testing=False):
        initial_time = time.perf_counter()
        categories = [
            'Sci-Fi',
            'Family',
            'Kids',
            'Biography',
            'Drama',
            'Comedy',
            'Romantic-Comedy',
            'Comedy-Drama',
            'Action-Comedy',
            'Action',
            'Superhero',
            'Thriller',
            'Horror',
            'Fantasy',
            'Documentary',
            'Independent',
            'Foreign',
            #SERIES
            'Action',
            'Health-and-Wellness',
            'Reality-TV',
            'Competition',
            'Comedy',
            'Crime',
            'Drama',
            'Sci-Fi',
            'Kids'
        ]

        self.scraped_movies = self._query_field(self.titanScraping, 'Id')
        self.scraped_episodes = self._query_field(self.titanScrapingEpisodes, 'Id')
        print('En titanScraping: {}'.format(len(self.scraped_movies)))
        print('En titanScrapingEpisodes: {}'.format(len(self.scraped_episodes)))
        
        typeOfContent = 'movie'
        contentType = 'movies'
        for category in categories:
            payloads_movies = []
            
            headers = {
                'Content-Type': 'application/json',
            }
            
            data = {
                "id": category,
                "listQuery":"http://prd-eurekavip.mgo.lax:8080/eureka/v2/list/genre?q="+category+"&contentTypes="+contentType,
                "pageNumber":0,
                "pageCount":1,
                "listType":"genre",
                "template":"template/list/paginate",
                "_csrf":"gbrXYbmS-onW4HsgzyORQGahHQIGAYNYMvNA"
            }
            
            URL = "https://www.fandangonow.com/responder/list/paginate"    
            jsonLista = Datamanager._getJSON(self,URL,usePOST=True,json=data,headers=headers)
            totalItems = jsonLista['result_data']['totalCount']
            
            data['pageCount'] = totalItems

            jsonLista = Datamanager._getJSON(self,URL,usePOST=True,json=data,headers=headers) 
            soup = BeautifulSoup(jsonLista['result_html'], features="html.parser")

            _list_urls = []
            for content in soup.findAll('li',{'class':'media-list__item js-list-item'}):
                URLContenido = content.find('a',{'class':'item__link js-flyout-item plus-card'}).get('href')
                contenidoId = content.find('a',{'class':'item__link js-flyout-item plus-card'}).get('data-id')
                if contenidoId in self.scraped_movies and contentType == "movies":
                    continue
                _list_urls.append(URLContenido)
            _list_responses = self._async_requests(_list_urls, max_workers=3)
            _dict_responses = {res.url: res for res in _list_responses if res}
            
            for content in soup.findAll('li',{'class':'media-list__item js-list-item'}):
                URLContenido = content.find('a',{'class':'item__link js-flyout-item plus-card'}).get('href')
                contenidoId = content.find('a',{'class':'item__link js-flyout-item plus-card'}).get('data-id')

                if contenidoId in self.scraped_movies and contentType == "movies":
                    print(f"\033[33mExiste contenido {contenidoId}\033[0m")
                    continue
                else:
                    if URLContenido in _dict_responses:
                        res = _dict_responses[URLContenido]
                        soupContenido = BeautifulSoup(res.text, features="html.parser")
                    else:
                        try:
                            soupContenido = Datamanager._getSoup(self, URLContenido)
                        except Exception:
                            continue

                    print(f"\033[31m{res.url}\033[0m")
                    try:
                        rating = soupContenido.find('li',{'id':'media-details-meta-info-rating-value'}).text
                    except:
                        rating = None

                    packages = self._get_precios(soupContenido)
                    if not packages:
                        continue
                    
                    genres = []
                    soupGenre = soupContenido.find('li',{'class':'dil-block genres genre-info'})
                    for genre in soupGenre.findAll('a'):
                        genres.append(genre.text)
                    
                    images = []
                    image = soupContenido.find('img',{'id':'media-details-image'}).get('src')
                    images.append(image)
                    
                    descripcion = str(soupContenido.find('div',{'class':'data-description js-collapse--description'}).text).strip()
                    
                    try:
                        provider = str(soupContenido.find('div',{'class':'md-copyright'}).text)
                        provider = provider[6:len(provider)].replace("All Rights Reserved.","",-1)
                        providers = [provider]
                    except:
                        providers = None
                    
                    json_content = json.loads(soupContenido.find('script',{'type':'application/ld+json'}).text)
                    
                    try:
                        expiration = json_content['potentialAction']['expectsAcceptanceOf']['availabilityEnds']
                    except:
                        expiration = None
                    
                    has_free_episodes = False
                    list_payloads_seasons = []
                    
                    if contentType == 'movies':
                        title = soupContenido.find('h1',{'id':'media-details-title'}).text
                        print(f"\033[32m{title}\033[0m")
                        year = int(soupContenido.find('li',{'class':'dil-block release-date'}).text)
                        if year > datetime.now().year:
                            year = None
                        runtime = str(soupContenido.find('li',{'id':'media-details-meta-info-runtime'}).text).replace(" min.","")
                        runtime = int(runtime)
                        
                        director = set()
                        for dir in range(0, len(json_content['director'])):
                            director.add(json_content['director'][dir]['name'])
                        director = list(director)  # set -> list
                        
                        cast = set()
                        for actor in range(0, len(json_content['actor'])):
                            cast.add(json_content['actor'][actor]['name'])
                        cast = list(cast) # set -> list
                    elif contentType == 'tvshows':
                        title = soupContenido.find('h1',{'id':'media-details-tv-title'}).text
                        print(f"\033[32m{title}\033[0m")
                        year = None
                        runtime = None
                        director = None

                        #TEMPORADAS
                        print("\n\n\033[33;1;40mTITULO SERIE ACTUAL:", title, "\033[0m")
                        seasons = soupContenido.find('ul',{'id':'media-details-tv-season-numbers'})
                        list_urls = []
                        for season in seasons.findAll('li',{'class':'dil-block'}):
                            if season.find('a'):
                                linkSeason = 'https://www.fandangonow.com' + season.find('a').get('href')
                                list_urls.append(linkSeason)
                        list_responses = self._async_requests(list_urls)
                        dict_responses = {res.url: res for res in list_responses if res}
                        for season in seasons.findAll('li', {'class':'dil-block'}):
                            seasonNumberLink = season.find('a')
                            if seasonNumberLink != None:
                                linkSeason = 'https://www.fandangonow.com' + season.find('a').get('href')
                                for res in list_responses:
                                    if linkSeason == res.url:
                                        soupSeason = BeautifulSoup(res.text, features="html.parser")
                                        break
                                if linkSeason in dict_responses:
                                    res_season = dict_responses[linkSeason]
                                    soupContenido = BeautifulSoup(res_season.text, features="html.parser")
                                else:
                                    soupContenido = Datamanager._getSoup(self, linkSeason)
                                
                                print("DEEPLINK SEASON:", linkSeason)
                                json_content_season = self._load_json_content_season(soup=soupContenido)
                                # Armamos los precios de los episodios en base al JSON que se obtuvo del HTML
                                dict_packages_epi = self._obtain_packages_episodes_from_json(json_content_season)

                                try:
                                    jsonEpisodios = json.loads(soupSeason.find('script',{'type':'application/ld+json'}).text)
                                except:
                                    try:
                                        jsonEpisodios = json.loads(soupContenido.find('script',{'type':'application/ld+json'}).text)
                                    except:
                                        continue
                                
                                packages_season = self._get_precios(soupContenido=soupContenido)
                                print("\033[34mPACKAGES SEASON:", packages_season, "\033[0m")

                                seasonNumber = int(jsonEpisodios['seasonNumber'])
                                print(f"\033[32mEpisodes of season: {seasonNumber}\033[0m")
                                try:
                                    expirationEpi = jsonEpisodios['potentialAction']['expectsAcceptanceOf']['availabilityEnds']
                                except:
                                    expirationEpi = None
                                
                                cast = []
                                for actor in soupSeason.findAll('a',{'class':'cast-crew-member'}):
                                    cast.append(actor.text)

                                #Excepcion para la temp 1 de Seinfeld, que separa el S1 y el S2
                                if 'MSEE1159A4009B1C71D32F6CD8DB99A3117A' in linkSeason:
                                    epiSeinNum = 13
                                
                                # EPISODIOS
                                list_urls_epi = []
                                dict_responses_epi = {}
                                for epi in jsonEpisodios['episode']:
                                    id_epi = epi['@id']
                                    if id_epi in dict_packages_epi:
                                        continue
                                    url_obtain_prices = f"https://www.fandangonow.com/responder/offers/{id_epi}"
                                    list_urls_epi.append(url_obtain_prices)
                                if list_urls_epi:
                                    list_responses_epi = self._async_requests(list_urls_epi)
                                    dict_responses_epi = {res.url: res for res in list_responses_epi if res}
                                payloads_epi = []

                                for epi in jsonEpisodios['episode']:
                                    id_epi = epi['@id']
                                    if id_epi in self.scraped_episodes:
                                        print(f"\033[33mExiste episodio {id_epi}\033[0m")
                                        continue
                                    # #### #### #### #### #### ####
                                    self.scraped_episodes.add(id_epi)
                                    # #### #### #### #### #### ####
                                    titleEpi = epi['name']
                                    descriptionEpi = epi['description']
                                    
                                    try:
                                        epiNumber = int(epi['episodeNumber'])
                                        if epiNumber == 0:
                                            epiNumber = None
                                    except:
                                        epiNumber = None

                                    try:
                                        seasonNumber = int(epi['partOfSeason']['seasonNumber'])
                                        if epiNumber and epiNumber > 100:
                                            seasonNumber = 0
                                    except Exception:
                                        seasonNumber = None

                                    #Excepcion para la temp 1 de Seinfeld, que separa el S1 y el S2
                                    if 'MSEE1159A4009B1C71D32F6CD8DB99A3117A' in linkSeason:
                                        if int(epiNumber) >= 6:
                                            seasonNumber = 2
                                            epiNumber = epiSeinNum
                                            epiSeinNum -= 1
                                        else:
                                            seasonNumber = 1
                                    
                                    linkEpi = epi['potentialAction']['target'][0]['urlTemplate']
                                    year_epi = epi["releasedEvent"].get("startDate", None)
                                    if year_epi:
                                        year_epi = int(year_epi.split("-")[0])
                                    
                                    packages_epi = []
                                    if id_epi in dict_packages_epi:
                                        tmp_packages = dict_packages_epi[id_epi]
                                        if not tmp_packages:  # Para los episodios sin precio.
                                            packages_epi = self._build_season_price_pkg(packages_season)
                                        elif tmp_packages and not packages_season:  # Los episodios tienen precio pero no la temporada
                                            packages_epi = tmp_packages.copy()
                                        else:
                                            for pkg in tmp_packages:
                                                if pkg["Type"] == "transaction-vod":
                                                    buy_price = pkg["BuyPrice"]
                                                    definition = pkg["Definition"]
                                                    # Para los episodios con precio mayor o igual a la season
                                                    if any([pkg["BuyPrice"] <= buy_price for pkg in packages_season]):
                                                        # Rearmamos el package con el precio de la serie
                                                        packages_epi = self._build_season_price_pkg(packages_season)
                                                        break
                                                    else:
                                                        current_pkgs = self._build_pkgs_with_season_price(buy_price, definition, parent_packages=packages_season)
                                                        packages_epi.extend(current_pkgs)
                                                else:
                                                    packages_epi.append(pkg)
                                    
                                    if not packages_epi and packages_season:
                                        url_obtain_prices = f"https://www.fandangonow.com/responder/offers/{id_epi}"
                                        if url_obtain_prices in dict_responses_epi:
                                            response_epi = dict_responses_epi[url_obtain_prices]  # Ofrece un JSON que contiene una parte de código HTML para obtener el precio
                                        else:
                                            response_epi = self._obtain_response(url_obtain_prices)
                                        
                                        try:
                                            json_epi = response_epi.json()
                                            packages_epi = self._get_precios_episodio(json_epi, packages_season, id_epi=id_epi, deeplink_epi=linkEpi)
                                            if not packages_epi:
                                                raise Exception("Packages de episodio vacío; verificando nuevamente.")
                                        except Exception as e:
                                            # print("EXCEPCION EPI:", e)
                                            try:
                                                packages_epi = []
                                                acceptance_offer = epi["potentialAction"]["expectsAcceptanceOf"]
                                                buy_price = acceptance_offer["price"]
                                                if acceptance_offer["@type"] == "" or buy_price == "":
                                                    packages_epi = self._build_season_price_pkg(packages_season)
                                                    # DEBUG: usado para testing
                                                    test_payload = {
                                                        "Id": id_epi,
                                                        "PlatformCode": self._platform_code + "-testing",
                                                        "CreatedAt": self._created_at,
                                                        "Deeplink": linkEpi,
                                                        "Error": "Episodio sin precio",
                                                    }
                                                    self.mongo.insert(self.titanPreScraping, test_payload)
                                                elif float(buy_price) == 0:
                                                    packages_epi.append({"Type": "free-vod"})
                                                else:
                                                    buy_price = float(buy_price)
                                                    if any([pkg["BuyPrice"] <= buy_price for pkg in packages_season]):
                                                        # Rearmamos el package con el precio de la temporada
                                                        packages_epi = self._build_season_price_pkg(packages_season)
                                                    else:
                                                        packages_epi = self._build_pkgs_with_season_price(buy_price, definition="HD", parent_packages=packages_season)
                                            except Exception:
                                                continue

                                    if not packages_epi:
                                        continue

                                    if any([_pkg["Type"] == "free-vod" for _pkg in packages_epi]):
                                        has_free_episodes = True
                                    
                                    new_packages_epi = []
                                    for pkg in new_packages_epi:
                                        current_pkg = pkg.copy()
                                        if "Definition" in current_pkg:
                                            current_pkg["Definition"] = self.__validate_definition(current_pkg["Definition"])
                                        new_packages_epi.append(current_pkg.copy())
                                    
                                    payloadEpi = {
                                        'PlatformCode'  : self._platform_code,
                                        'ParentId'      : contenidoId,
                                        'ParentTitle'   : title,
                                        'Id'            : id_epi,
                                        'Title'         : titleEpi,
                                        'Episode'       : epiNumber,
                                        'Season'        : seasonNumber,
                                        'Year'          : year_epi,
                                        'Duration'      : None,
                                        'Deeplinks'     : {
                                            'Web': linkEpi,
                                            'Android': None,
                                            'iOS': None
                                        },
                                        'Synopsis'      : descriptionEpi,
                                        'Rating'        : rating,
                                        'Provider'      : None,
                                        'Genres'        : genres,
                                        'Cast'          : cast,
                                        'Directors'     : None,
                                        'Availability'  : expirationEpi,
                                        'Download'      : None,
                                        'IsOriginal'    : None,
                                        'IsAdult'       : None,
                                        'Country'       : None,
                                        'Packages'      : new_packages_epi,
                                        'Timestamp'     : datetime.now().isoformat(),
                                        'CreatedAt'     : self._created_at
                                    }
                                    payloads_epi.append(payloadEpi)
                                
                                payload_season = self._build_payload_season(json_content_season, soupContenido)
                                print("PAYLOAD SEASON:\n", payload_season)
                                list_payloads_seasons.append(payload_season)

                                if payloads_epi:
                                    self.mongo.insertMany(self.titanScrapingEpisodes, payloads_epi)
                                    print('Insert many episodes: {}'.format(len(payloads_epi)))

                    # Para los casos de las series que pasaron el filtro previo.
                    if contenidoId in self.scraped_movies:
                        print(f"\033[33mExiste contenido {contenidoId}\033[0m")
                        continue
                    # #### #### #### #### #### #### ####
                    self.scraped_movies.add(contenidoId)
                    # #### #### #### #### #### #### ####

                    clean_title = _replace(title)
                    if clean_title == "":
                        clean_title = title
                    
                    if typeOfContent == "serie":  # No hay precio de series, solamente de las temporadas
                        packages = [{"Type": "transaction-vod"}]
                        if has_free_episodes:
                            packages.append({"Type": "free-vod"})

                    payload = {
                        'PlatformCode':  self._platform_code,
                        'Id':            contenidoId,
                        'Title':         title,
                        'CleanTitle':    clean_title,
                        'OriginalTitle': None,
                        'Type':          typeOfContent, # 'movie' o 'serie'
                        'Seasons':       list_payloads_seasons if list_payloads_seasons else None,
                        'Year':          year,
                        'Duration':      runtime, # duracion en minutos
                        'Deeplinks': {
                            'Web':       URLContenido,
                            'Android':   None,
                            'iOS':       None,
                        },
                        'Playback':      None,
                        'Synopsis':      descripcion,
                        'Image':         images, # [str, str, str...] # []
                        'Rating':        rating,
                        'Provider':      providers,
                        'Genres':        genres, # [str, str, str...]
                        'Cast':          cast, # [str, str, str...]
                        'Directors':     director, # [str, str, str...]
                        'Availability':  expiration,
                        'Download':      None,
                        'IsOriginal':    None,
                        'IsAdult':       None,
                        'Packages':      packages,
                        'Country':       None, # [str, str, str...]
                        'Timestamp':     datetime.now().isoformat(),
                        'CreatedAt':     self._created_at
                    }
                    if typeOfContent == "movie":
                        del payload["Seasons"]
                    
                    print(packages)
                    payloads_movies.append(payload)
            
            if category == 'Foreign':
                contentType = 'tvshows'
            
            if contentType == 'tvshows':
                typeOfContent = 'serie'
            else:
                typeOfContent = 'movie'
            
            if payloads_movies:
                self.mongo.insertMany(self.titanScraping, payloads_movies)
                print('Insert many: {}'.format(len(payloads_movies)))

        current_time = time.perf_counter()
        diff_seconds = int(current_time - initial_time)
        elapsed_time = str(timedelta(seconds=diff_seconds))
        print(f'\n\033[32mTiempo transcurrido: {elapsed_time}.\033[0m')
        self.sesion.close()
        '''
        Upload
        '''
        if not testing:
            Upload(self._platform_code, self._created_at, testing=False) 

    def _load_json_content_season(self, soup):
        json_content_season = dict()
        list_script_content = soup.find_all('script')
        term_search1 = "window.mgo.data = "
        term_search2 = "window.mgo.origin = "
        for script in list_script_content:
            if term_search2 in script.text:
                start_index = script.text.index(term_search1) + len(term_search1)
                end_index = script.text.index(term_search2)
                content = script.text[start_index:end_index].strip()
                last_semicolon = content.rfind(";")
                content = content[:last_semicolon]
                try:
                    json_content_season = json.loads(content)
                except:
                    pass
                break
        
        return json_content_season

    def _get_precios(self, soupContenido):
        packages = []
        rentUHD = soupContenido.find('a',{'id':'RENTAL_UHD_OfferLink'})
        rentHD = soupContenido.find('a',{'id':'RENTAL_HD_OfferLink'})
        rentSD = soupContenido.find('a',{'id':'RENTAL_SD_OfferLink'})
        
        precioUHD = soupContenido.find('a',{'id':'PURCHASE_UHD_OfferLink'})
        precioHD = soupContenido.find('a',{'id':'PURCHASE_HD_OfferLink'})
        precioSD = soupContenido.find('a',{'id':'PURCHASE_SD_OfferLink'})

        try:
            if precioUHD:
                precioUHD = str(precioUHD.find('span',{'class':'transact__button__price'}).text).strip()
                precioUHD = float(precioUHD.replace("4K UHD $",""))

            if rentUHD:
                rentUHD = str(rentUHD.find('span',{'class':'transact__button__price'}).text).strip()
                rentUHD = float(rentUHD.replace("4K UHD $",""))

            if precioHD:
                precioHD = str(precioHD.find('span',{'class':'transact__button__price'}).text).strip()
                precioHD = float(precioHD.replace("HD $",""))

            if rentHD:
                rentHD = str(rentHD.find('span',{'class':'transact__button__price'}).text).strip()
                rentHD = float(rentHD.replace("HD $",""))

            if precioSD:
                precioSD = str(precioSD.find('span',{'class':'transact__button__price'}).text).strip()
                precioSD = float(precioSD.replace("SD $",""))

            if rentSD:
                rentSD = str(rentSD.find('span',{'class':'transact__button__price'}).text).strip()
                rentSD = float(rentSD.replace("SD $",""))
        except:
            precioHD = None
            precioSD = None
            precioUHD = None
            rentHD = None
            rentSD = None
            rentUHD = None

        if precioUHD or rentUHD:
            packages.append(
                {
                    'Type': 'transaction-vod',
                    'BuyPrice': precioUHD,
                    'RentPrice' : rentUHD,
                    'Definition': '4K',
                    'Currency': 'USD',
                }
            )

        if precioHD or rentHD:
            packages.append(
                {
                    'Type': 'transaction-vod',
                    'BuyPrice': precioHD,
                    'RentPrice' : rentHD,
                    'Definition': 'HD',
                    'Currency': 'USD',
                }
            )

        if precioSD or rentSD:
            packages.append(
                {
                    'Type': 'transaction-vod',
                    'BuyPrice': precioSD,
                    'RentPrice' : rentSD,
                    'Definition': 'SD',
                    'Currency': 'USD',
                }
            )

        if packages == []:
            packages = None  # Si no tienen ningún precio no estan disponibles para comprar ni son gratuitos!

        return packages

    def _get_precios_episodio(self, json_epi, parent_packages, **kwargs):  # kwargs = contiene campos para testing
        packages = []
        raw_html_price = json_epi["result_html"]
        prices_epi = BeautifulSoup(raw_html_price, "html.parser").find_all("li")
        for p in prices_epi:
            text_price = p.text.upper()
            if "BUY HD" in text_price or "BUY SD" in text_price:
                if "BUY HD" in text_price:
                    buy_price = float(text_price.replace("BUY HD", "").replace("$", "").strip())
                    definition = "HD"
                elif "BUY SD":
                    buy_price = float(text_price.replace("BUY SD", "").replace("$", "").strip())
                    definition = "SD"
                if buy_price:
                    # Para los episodios con precio mayor o igual a la season
                    if any([pkg["BuyPrice"] <= buy_price for pkg in parent_packages]):
                        # Rearmamos el package con el precio de la serie
                        packages = self._build_season_price_pkg(parent_packages)
                        break
                    else:
                        current_pkgs = self._build_pkgs_with_season_price(buy_price, definition, parent_packages=parent_packages)
                        packages.extend(current_pkgs)
            elif "FREE HD" in text_price or "FREE SD" in text_price:
                if any([pkg["Type"] == "free-vod" for pkg in packages]):
                    continue
                packages.append({"Type": "free-vod"})
            else:  # Los episodios sin precio
                packages = self._build_season_price_pkg(parent_packages)
                break

        if packages:
            if any([pkg["Type"] == "free-vod" for pkg in parent_packages]):
                packages.append({"Type": "free-vod"})
            elif any([pkg["Type"] == "subscription-vod" for pkg in parent_packages]):
                packages.append({"Type": "subscription-vod"})

        print(packages)
        return packages

    # Solo para los episodios cuyo valor supera al de la temporada
    def _build_season_price_pkg(self, parent_packages):
        packages = []
        if not parent_packages:  # Temporadas sin precios
            return packages
        else:
            for pkg in parent_packages:
                if pkg["Type"] == "transaction-vod":
                    buy_price = float(pkg["BuyPrice"])
                    definition = pkg["Definition"]
                    packages.append({
                        "Type": "transaction-vod",
                        "SeasonPrice": buy_price,
                        "Definition": definition,
                        "Currency": "USD",
                    })
        return packages

    # Para todos los episodios y agregando el SeasonPrice acorde a la definición
    def _build_pkgs_with_season_price(self, buy_price, definition, parent_packages):
        packages = []
        exists_definition = any([_pkg["Definition"] == definition for _pkg in parent_packages if _pkg["Type"] == "transaction-vod"])
        if exists_definition:
            for pkg in parent_packages:
                if pkg["Type"] == "transaction-vod" and pkg["Definition"] == definition:
                    season_price = float(pkg["BuyPrice"])
        else:  # Obtenemos el SeasonPrice de cualquier otra definición
            for pkg in parent_packages:
                if pkg["Type"] == "transaction-vod":
                    season_price = float(pkg["BuyPrice"])
                    break
        packages.append({
            "Type": "transaction-vod",
            "BuyPrice": buy_price,
            "RentPrice": None,
            "SeasonPrice": season_price,
            "Definition": definition,
            "Currency": "USD",
        })
        return packages

    def _build_payload_season(self, json_content_season, soup):
        season_id = json_content_season["seasonId"]
        try:
            season_number = int(json_content_season.get("seasonNumber"))
        except:
            season_number = None
        deeplink_season = f"https://www.fandangonow.com/details/{season_id}"
        season_title = json_content_season.get("seasonTitle")
        if not season_title:
            try:
                h2 = soup.find("h2", {"class": "children-header"})
                for child in h2.find_all('span'):
                    child.decompose()
                season_title = h2.text.strip()
            except:
                season_title = None
        year = json_content_season.get("origReleaseDate")
        season_synopsis = json_content_season.get("description")
        cast = []
        directors = []
        for artist in json_content_season.get("actor", []):
            cast.append(artist["name"])
        for artist in json_content_season.get("crew", []):
            directors.append(artist["name"])
        
        if not season_synopsis:
            try:
                section = soup.find("section", {"class": "media-details media-details-upper"})
                season_synopsis = section.find("div", {"class": "collapse-container description-container group"})
                season_synopsis = season_synopsis.find("div", {"id": "media-details-description"})
                if season_synopsis:
                    season_synopsis = season_synopsis.text.strip().replace("\n", "")
            except:
                season_synopsis = None
        if not cast:
            try:
                section = soup.find("section", {"class": "media-details media-details-upper"})
                soup_season_cast = section.find("div", {"class": "collapse-container cast-crew-container"})
                list_season_cast = soup_season_cast.find_all("a", {"class": "cast-crew-member "})
                for actor in list_season_cast:
                    cast.append(actor.text.strip())
            except:
                cast = None
        
        images = []
        for img in json_content_season.get("images", []):
            if img["url"] not in images:
                images.append(img["url"])
        
        payload_season = {
            "Id": season_id,
            "Synopsis": season_synopsis,
            "Title": season_title,
            "Deeplink":  deeplink_season,
            "Number": season_number,
            "Year": year,
            "Image": images if images else None,
            "Directors": directors if directors else None,
            "Cast": cast if cast else None,
        }

        return payload_season

    def __validate_definition(self, definition):
        return "4K" if definition == "UHD" else definition

    def _obtain_packages_episodes_from_json(self, json_content_season):
        dict_packages_epi = dict()
        for item in json_content_season.get("season", {}).get("episodes", []):
            try:
                id_epi = item["id"]
                tmp_packages = []
                for price in item["offers"].get("PURCHASE", []):
                    definition = price["definition"]
                    buy_price = price["purchasePrice"]["price"]
                    buy_price = float(buy_price)
                    if buy_price == 0:
                        tmp_packages.append({"Type": "free-vod", "Definition": definition})
                        continue
                    tmp_packages.append({
                        "Type": "transaction-vod",
                        "Definition": definition,
                        "BuyPrice": buy_price,
                        "Currency": "USD",
                    })
                print("PRE-PACKAGES EPISODES:", tmp_packages)
                dict_packages_epi[id_epi] = tmp_packages
            except:
                pass
        
        return dict_packages_epi
