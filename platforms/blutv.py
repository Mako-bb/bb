# -*- coding: utf-8 -*-
import json
import time
import requests
import random
import hashlib
import platform
import sys, os
import re
import string
from urllib.parse                       import unquote
from concurrent.futures                 import ThreadPoolExecutor, as_completed
from seleniumwire                       import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.common.exceptions         import NoSuchElementException
from selenium.common.exceptions         import TimeoutException
from selenium.webdriver.common.by       import By
from selenium.webdriver.common.keys     import Keys
from selenium.webdriver.support         import expected_conditions as EC
from selenium.webdriver.support.ui      import WebDriverWait
from pyvirtualdisplay                   import Display
from bs4                                import BeautifulSoup
from handle.replace                     import _replace
from handle.payload                     import Payload
from common                             import config
from bs4                                import BeautifulSoup
from datetime                           import datetime
from handle.mongo                       import mongo
from handle.season_helper               import SeasonHelper
from updates.upload                     import Upload


class BluTV:
    """
    BluTV es una OTT de Turquía.

    DATOS IMPORTANTES:
    - VPN: Sí. ExpressVPN
    - ¿Usa Selenium?: Sí, solo para obtener subcategorías e IDs de algunos contenidos. Dura unos minutos.
    - ¿Tiene API?: Sí.
    - ¿Usa BS4?: Sí.
    - ¿Se relaciona con scripts TP? No.
    - ¿Instanacia otro archivo de la carpeta "platforms"?: No.
    - ¿Cuánto demoró la ultima vez? 9 minutos(2021-04-24)
    - ¿Cuánto contenidos trajo la ultima vez? 788 movies/series - 7829 episodios(2021-04-24)

    OTROS COMENTARIOS:
    - Modelo de negocio: subscription-vod, free-vod y transaction-vod(Renta y free-vod solo en algunos)
    - Aunque la plataforma ofrece sus servicios a cualquier país ocurre que, si no está conectado por VPN a TR, al deeplink se le agrega /int/ al final de la URL y no muestra la categoría de KIDS.
    - Hay deeplinks que solo funciona si uno está logueado. Algunos se pueden ver sin problemas desde otro navegador sin la sesión iniciada.
    - En el caso de los contenidos transaccionales son solamente de renta. Solo hacen diferencia por el medio de pago(celular, tarj. crédito, cuenta, etc)
    - Los contenidos SD y HD valen lo mismo en los Packages de tipo transaction-vod.
    - Hay contenidos que son free-vod, el cual incluso pueden verse sin crearse una cuenta.
    - Algunos contenidos no están disponibles si no estás conectado a un VPN.
    - Muchas series tienen su primer episodio gratuito.
    """
    def __init__(self, ott_site_uid, ott_site_country, operation_type):
        self._config                = config()['ott_sites'][ott_site_uid]
        self._created_at            = time.strftime("%Y-%m-%d")
        self.mongo                  = mongo()
        self._platform_code         = self._config['countries'][ott_site_country]
        self.currency               = config()['currency'][ott_site_country]
        self.titanPreScraping       = config()['mongo']['collections']['prescraping']
        self.titanScraping          = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodes  = config()['mongo']['collections']['episode']
        self.session                = requests.session()

        #####################################################################################
        # REGEX
        #####################################################################################
        self.regex_season = re.compile(r'\b(sezona)\b\s*(?P<season>\d{1,3})', re.I)
        self.regex_idimdb = re.compile(r'\b(title/)\b(?P<idimdb>tt\d+)', re.I)
        self.regex_date   = re.compile(r'\b(\d{1,2}\.\s?\d{1,2}\.\s?\d{4})\b')
        self.regex_number = re.compile(r"\D+")

        self.series_with_epis = set()
        self.set_extra_ids = set()  # IDs obtenidos mediante Selenium que no pudieron obtenerse de otra manera. También contiene IDs de subcategorías.
        self.dict_person_ids = dict()

        try:
            if platform.system() == 'Linux':
                Display(visible=0, size=(1366, 768)).start()
        except Exception:
            pass
        
        self.options = Options()
        self.options.add_argument('--headless')
        self.driver = webdriver.Firefox(options=self.options)

        if operation_type == 'return':
            params = {"PlatformCode" : self._platform_code}
            lastItem = self.mongo.lastCretedAt(self.titanScraping, params)
            if lastItem.count() > 0:
                for lastContent in lastItem:
                    self._created_at = lastContent['CreatedAt']
            self._prescraping()
            self._scraping()
        elif operation_type == 'scraping':
            delete = self.mongo.delete(self.titanPreScraping, {'PlatformCode': self._platform_code})
            print('Deleted {} Items PreScraping'.format(delete))
            self._prescraping()
            self._scraping()
        elif operation_type == 'testing':
            delete = self.mongo.delete(self.titanPreScraping, {'PlatformCode': self._platform_code})
            print('Deleted {} Items PreScraping'.format(delete))
            self._prescraping()
            self._scraping(testing=True)


    def _obtain_response(self, url, **kwargs):
        requests_timeout = 15
        method  = kwargs.get("method", "GET")
        headers = kwargs.get("headers", None)
        data    = kwargs.get("data", None)
        params  = kwargs.get("params", None)
        while True:
            try:
                timeout = requests_timeout if method == "GET" else None
                response = self.session.request(method, url, headers=headers, data=data, params=params, timeout=timeout)
                return response
            except requests.exceptions.ConnectionError:
                print("Connection Error, Retrying")
                time.sleep(requests_timeout)
                continue
            except requests.exceptions.RequestException:
                print(f'Waiting... {url}')
                time.sleep(requests_timeout)
                continue

    def _async_requests(self, list_urls, max_workers=3, **kwargs):
        list_responses = []
        len_urls = len(list_urls)
        list_urls = [list(list_urls[i:i+max_workers]) for i in range(0, len_urls, max_workers)]
        for sublist_urls in list_urls:
            list_threads = []
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                for url in sublist_urls:
                    print(f'ASYNC request: {url}')
                    list_threads.append(executor.submit(self._obtain_response, url, **kwargs))
                for task in as_completed(list_threads):
                    list_responses.append(task.result())
            del list_threads
        
        return list_responses

    def _query_field(self, collection, field=None, extra_filter=None):
        find_filter = {'PlatformCode': self._platform_code, 'CreatedAt': self._created_at}

        if extra_filter:
            find_filter.update(extra_filter)

        find_projection = {'_id': 0, field: 1,} if field else None

        query = self.mongo.db[collection].find(
            filter=find_filter,
            projection=find_projection,
            no_cursor_timeout=False
        )

        if field:
            query = [item[field] for item in query]
        else:
            query = list(query)

        return query

    def _prescraping(self):
        host = 'https://www.blutv.com/'
        list_errors_msg = ["502 Bad Gateway", "Error code: 502", "error response"]

        def obtain_ids(soup):
            tmp_ids = set()
            k_data = ['data-containerid', 'data-container']
            for k in k_data:
                all_elems = soup.find_all('div', {k: True})
                tmp_ids.update({e.get('data-container') for e in all_elems if e.get('data-container')})
            return tmp_ids

        self.login(host)

        # Obtener IDs de otros contenidos y subcategorías.
        for subcat in ('', 'cocuk', 'kirala',):
            current_url = host + subcat
            while True:
                try:
                    print("CATEGORÍA ACTUAL -->", current_url)
                    self.driver.get(current_url)
                    time.sleep(5)
                    content_html = self.driver.page_source
                    if any([msg in content_html for msg in list_errors_msg]):
                        print("ERROR. Reloading")
                        continue
                    height = counter = 0
                    scroll_pause = 0.5
                    incr_height = 200
                    large_height = self.driver.execute_script("return document.body.scrollHeight")
                    while height <= large_height:
                        content_html = self.driver.page_source
                        if any([msg in content_html for msg in list_errors_msg]):
                            print("ERROR. Reloading")
                            self.driver.refresh()
                            time.sleep(2)
                            height = counter = 0
                            continue
                        self.driver.execute_script(f"window.scrollTo(0, {height});")
                        time.sleep(scroll_pause)
                        counter += 1
                        
                        large_height = self.driver.execute_script("return document.body.scrollHeight")
                        height += incr_height
                        # print("CURRENT HEIGHT:", height)
                        # print("MAX HEIGHT:", large_height)
                    
                    content_html = self.driver.page_source
                    soup = BeautifulSoup(content_html, 'html.parser')
                    self.set_extra_ids.update(obtain_ids(soup))
                    break
                except Exception as e:
                    time.sleep(0.5)
                    pass
        
        self.driver.quit()
        print("\033[32;1;40mPRESCRAPING FINISHED\033[0m")

    def _scrape_content(self, item, is_epi=False, already_requested=False, **kwargs):
        id = item['id']
        if is_epi:
            is_not_scraped_epi = True  # Para evitar retornar si existe ya el payload en la DB y reutilizarlo para el campo 'seasons' de la serie
            parent_id    = kwargs['parent_id']
            parent_title = kwargs['parent_title']
            self.series_with_epis.add(parent_id)
            if id in self.scraped_epis:
                is_not_scraped_epi = False
            self.scraped_epis.append(id)
            season_number = item['SeasonNumber'] or None
            episode_number = item['EpisodeNumber'] or None
        else:
            if id in self.scraped:
                return
            self.scraped.append(id)

        def split_str(raw_str, sep=',', extra_sep=None):
            try:
                if not raw_str:
                    splited_str = None
                else:
                    splited_str = [i.strip() for i in raw_str.split(sep) if i]
            except:
                splited_str = None
            if splited_str and extra_sep:
                for e in extra_sep:
                    tmp_list = splited_str.copy()
                    splited_str.clear()
                    for s in tmp_list:
                        splited_str.extend([_ for _ in s.split(e) if _ != ""])

            return splited_str

        json_item = {}
        if not already_requested and not is_epi:
            new_url = self.api_items.format(content_id=id)  # Se obtiene más información al ingresar a cada contenido.
            res = self._obtain_response(new_url, headers=self.headers_items)
            json_item = res.json()

        try:
            title = item['Title']
        except:
            title = ''
        title = title.strip()
        if not title:
            print(f"\033[31mCONTENIDO SIN TÍTULO: {id}\033[0m")
            if is_epi:
                return None, is_not_scraped_epi
            else:
                return
        clean_title = _replace(title) or title
        try:
            original_title = item['OriginalName'].strip()
        except:
            original_title = None

        availability = None
        if is_epi:
            content_type = 'episode'
            print("\033[1;32;40m     INSERT EPISODE \033[0m {}x{} --> {} : {} : {}".format(season_number, episode_number, parent_id, id, title))
        else:
            if item["ContentType"] == "MovieContainer":
                content_type = "movie"
            elif item["ContentType"] == "SerieContainer":
                content_type = "serie"
            elif item["ContentType"] == "Page":
                content_type = "subcategory"
                return
            else:
                print(f"\033[31;1;40mTIPO INDEFINIDO: {id}\033[0m")
                content_type = "undefined"
                return

            try:
                date_string = item['EndDate']  # Ej: "2021-04-30T20:59:00"
                end_date = datetime.fromisoformat(date_string)
                availability = end_date.date().isoformat()
                if datetime.now() > end_date:
                    print(f"\033[31;1;40mCONTENIDO EXPIRADO. ID: {id} - {date_string}\033[0m")
                    return
            except:
                pass
            print("\x1b[1;32;40m INSERT {} \x1b[0m {} : {}".format(content_type.upper(), id, clean_title))

        description = item['Description']
        try:
            genres = [g.strip() for g in item['Genres'] if g.strip() != '']
            if not genres:
                genres = None
        except:
            genres = None
        try:
            duration = item['Duration'] // 60
            if duration == 0:
                duration = None
        except:
            duration = None

        endpoint = item['Url']
        deeplink = "https://www.blutv.com" + endpoint

        # https://blutv-images.mncdn.com/q/t/i/bluv2/80/1920x1080/601b9c52866ac30ed4174d9f
        images = set()
        template_img = "https://blutv-images.mncdn.com/q/t/i/bluv2/80/1920x1080/"
        for k_item in ("Posters", "Files",):
            for p in item.get(k_item, []):
                k = None
                content_type_obj = p.get('ContentType') or p.get('contentType') or ''
                if not content_type_obj in ("image/jpeg", "image/png",):
                    continue
                images.update([template_img + p[k] for k in ('_id', '_Id', 'id', 'Id',) if k in p])
        if not images:
            images = None
        else:
            images = list(images)

        country = split_str(item.get('Origin', ''), sep=',', extra_sep=['|'])

        external_ids = None
        url_imdb = item.get('ImdbUrl') or ''
        if url_imdb and 'www.imdb.com' in url_imdb:
            # print("EXTERNAL ID ->", url_imdb)
            match_imdb = self.regex_idimdb.search(url_imdb)
            if match_imdb:
                id_imdb = match_imdb.group("idimdb")
                external_ids = [{
                    "Provider": "IMDb",
                    "Id": id_imdb,
                }]
        try:
            year = item['MadeYear']
            if type(year) == str:
                year = year[:4]
            year = int(year)
            if year <= 1870 or year >= datetime.now().year:
                year = None
        except:
            year = None

        persons_dict = {}
        for role in ('Cast', 'Directors',):
            try:
                for person in json_item.get(role, []):
                    if type(person) == str:
                        person_id = person
                        if not person_id in self.dict_person_ids:
                            print("PERSON ID NOT IN DICT:", person_id)
                            continue
                        full_name = self.dict_person_ids[person_id]
                    else:
                        person_id = person['id']
                        full_name = person['Fullname']
                    full_name = full_name.strip()
                    self.dict_person_ids[person_id] = full_name

                    list_persons = persons_dict.setdefault(role, [])
                    list_persons.append(full_name)
                    persons_dict[role] = list_persons
            except AttributeError:
                pass
        cast = persons_dict.get('Cast') or None
        directors = persons_dict.get('Directors') or None
        provider = None

        is_branded = False
        properties = item.get('Properties') or []
        for prop in properties:
            if prop['IxName'] == 'Exclusive' and prop.get('SelectValues'):
                for s in prop['SelectValues']:
                    if s['IxName'] == "no" and s['Selected'] == False:  # 'IxName' == 'no' => Contenidos normales
                        is_branded = True
                        break

        packages = []
        is_subsctiption_vod = is_free = is_transactionable = False
        for pkg in item['Packages']:
            if pkg['Name'] == 'SVOD':
                if is_subsctiption_vod:  # Si ya se agregó el mismo Package
                    continue
                packages.append({"Type": "subscription-vod"})
                is_subsctiption_vod = True
            elif pkg['Name'] == 'TVOD':
                if is_transactionable:
                    continue
                currency = self.currency
                try:
                    variant_code = pkg['VariantCode']
                    raw_number = self.regex_number.sub('', variant_code)  # TVOD_590_CC => 590
                    price_f = int(raw_number) / 100.0  # 590 => 5.9
                except Exception as e:
                    print("EXCEPTION PRICE:", id, "-->", e)
                    continue
                for definition in ('SD', 'HD',):
                    packages.append({
                        "Type": "transaction-vod",
                        "BuyPrice": None,
                        "RentPrice": price_f,
                        "Definition": definition,
                        "Currency": self.currency,
                    })
                is_transactionable = True
            elif pkg['Name'] in ('FREE', 'FREE_BLUTV',):
                if is_free:
                    continue
                packages.append({"Type": "free-vod"})
                is_free = True

        if not packages and is_epi:
            for pkg in kwargs['parent_packages']:
                if pkg['Type'] == 'transaction-vod':
                    packages.append({
                        "Type": "transaction-vod",
                        "BuyPrice": None,
                        "SeasonPrice": pkg['RentPrice'],
                        "Definition": pkg['Definition'],
                        "Currency": self.currency,
                    })
                else:
                    packages.append(pkg)
        elif not packages:
            print(f"\033[31;1;40mSIN PACKAGES. ID: {id} - {content_type}\033[0m")
            return

        obj_payload = Payload(id_=id,
                              platform_code=self._platform_code,
                              title=title,
                              original_title=original_title,
                              duration=duration,
                              synopsis=description,
                              deeplink_web=deeplink,
                              provider=provider,
                              external_ids=external_ids,
                              year=year,
                              genres=genres,
                              country=country,
                              cast=cast,
                              directors=directors,
                              image=images,
                              packages=packages,
                              createdAt=self._created_at)

        if content_type == "episode":
            obj_payload.parent_id = parent_id
            obj_payload.parent_title = parent_title
            obj_payload.season = season_number
            obj_payload.episode = episode_number
            payload = obj_payload.payload_episode()
        else:
            obj_payload.clean_title = clean_title
            obj_payload.is_branded = is_branded
            obj_payload.availability = availability
            if content_type == "movie":
                payload = obj_payload.payload_movie()
            else:
                api_get_episodes = self.api_episodes.replace('__REPLACE_ENDPOINT_SERIE', endpoint + '/')
                res = self._obtain_response(api_get_episodes, headers=self.headers_items)
                content_epis = res.json()

                epi_payloads = []
                for item_epi in content_epis:
                    if item_epi['ContentType'] != 'Episode':  # SeasonContainer y Clip descartados
                        continue

                    payload_epi, _is_not_scraped_epi = self._scrape_content(item_epi, is_epi=True, parent_id=id, parent_title=obj_payload.title, parent_packages=obj_payload.packages)
                    if payload_epi:
                        if _is_not_scraped_epi:
                            self.payloads_epis.append(payload_epi)
                        epi_payloads.append(payload_epi.copy())

                if len(self.payloads_epis) > 300:
                    self.mongo.insertMany(self.titanScrapingEpisodes, self.payloads_epis)
                    print('Insertados {} en {}'.format(len(self.payloads_epis), self.titanScrapingEpisodes))
                    self.payloads_epis.clear()

                # Si no tiene episodios no se crea el payload
                if id in self.series_with_epis:
                    season_payloads = SeasonHelper().get_seasons_complete(epi_payloads)
                    obj_payload.seasons = season_payloads
                    payload = obj_payload.payload_serie()
                else:
                    print("SERIE SIN EPISODIOS:", id, "- DEEPLINK:", deeplink)
                    payload = None

                similars = item.get("Similars") or []  # DEBUG: Almacenar en lista temporal

        if content_type == 'episode':
            return payload, is_not_scraped_epi
        else:
            return payload

    def login(self, host):
        url_login = "https://www.blutv.com/giris"
        xpath_log_email = '//*[@id="username"]'
        xpath_log_pass = '//*[@id="password"]'
        xpath_log_btn = '//button[@type="submit"]'
        saved_email = self._config['auth']['email']
        saved_pass = self._config['auth']['pass']

        driver = self.driver
        driver.get(url_login)
        email_input = driver.find_element_by_xpath(xpath_log_email)
        email_input.send_keys(saved_email)
        pass_input = driver.find_element_by_xpath(xpath_log_pass)
        pass_input.send_keys(saved_pass)
        login_btn = driver.find_element_by_xpath(xpath_log_btn)
        login_btn.click()
        time.sleep(10)

        driver.get(host)
        time.sleep(1.5)

    def _scraping(self, testing=False):
        MAX_ITEMS = 25
        DEFAULT_TOKEN = "Basic 53f4c89cef14f5147495c3bf:nA9gt/ca3/apPYDdmHYGy3GoSBDhVYbpHKM3HWqeI+2bry3eFW2gjx6KTTdDRIScvl5Sh/LrAmPi7cvtJCU6lg=="

        self.api_episodes = "https://apicache.blutv.com/api/supercontents/active()?q={ContentType:{$in:['SerieContainer']}&    format=json&$filter=Ancestors/any(a:a/SelfPath eq '__REPLACE_ENDPOINT_SERIE')&$orderby=SeasonNumber asc, EpisodeNumber asc"
        self.api_contents = "https://apicache.blutv.com/api/search/template/quark_blu_ranking_cocktail_template_v10"
        self.api_items = "https://apicache.blutv.com/api/supercontents/{content_id}?format=json"
        self.api_items2 = "https://apicache.blutv.com/api/contents/getbyurl()?url={endpoint_content}&format=json"

        self.scraped = self._query_field(self.titanScraping, field='Id')
        self.scraped_epis = self._query_field(self.titanScrapingEpisodes, field='Id')

        self.payloads = []
        self.payloads_epis = []

        categories = {
            "SerieContainer": "/diziler",
            "MovieContainer": "/filmler",
        }

        self.headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:87.0) Gecko/20100101 Firefox/87.0',
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.5',
            'Referer': 'https://www.blutv.com/',
            "Authorization": DEFAULT_TOKEN,
            'Content-Type': 'application/json; charset=UTF-8',
            'Origin': 'https://www.blutv.com',
            'DNT': '1',
            'Connection': 'keep-alive',
            'TE': 'Trailers'
        }
        self.headers_items = self.headers.copy()
        del self.headers_items["Content-Type"]

        self.template_data = "{\"application\":\"com.blu\",\"privilege_name\":\"Application:com.blu\",\"package_name\":\"SVOD\",\"from\":__FROM_INDEX,\"size\":__SIZE,\"path\":\"__ENDPOINT\",\"content_type\":\"__CATEGORY\",\"sort_field\":\"_score\"}"

        for cat in categories:
            endpoint = categories[cat]
            num_lap = from_index = total_items = 0

            while True:
                if from_index > total_items:
                    break
                data_payload = self.template_data.replace('__FROM_INDEX', str(from_index)).replace('__SIZE', str(MAX_ITEMS)).replace('__ENDPOINT', endpoint).replace('__CATEGORY', cat)
                res = self._obtain_response(self.api_contents, method='POST', headers=self.headers, data=data_payload)
                # print("URL:", self.api_contents, "STATUS:", res.status_code)
                content = res.json()
                total_items = content['hits']['total']

                for item in content['hits']['hits']:
                    id = item['_id']
                    if id in self.scraped:
                        continue
                    source_item = item['_source']
                    payload = self._scrape_content(item=source_item)
                    if payload:
                        self.payloads.append(payload)

                    if len(self.payloads) > 300:
                        self.mongo.insertMany(self.titanScraping, self.payloads)
                        print('Insertados {} en {}'.format(len(self.payloads), self.titanScraping))
                        self.payloads.clear()

                from_index += 25

        print("EXTRA IDS:")
        for id in self.set_extra_ids:
            if id in self.scraped:
                print("EXTRA ID SKIPPED:", id)
                continue
            new_url = self.api_items.format(content_id=id)
            res = self._obtain_response(new_url, headers=self.headers_items)
            json_item = res.json()

            if json_item["ContentType"] == "Page":  # Subcategoría
                contents_items = json_item['Template']['Regions'][0]['Controls'][0]['ContentViews']
                for item in contents_items:
                    content = item['Content']
                    payload = self._scrape_content(item=content, already_requested=False)
                    if payload:
                        self.payloads.append(payload)
            else:
                payload = self._scrape_content(item=json_item, already_requested=True)
                if payload:
                    self.payloads.append(payload)

        if self.payloads:
            self.mongo.insertMany(self.titanScraping, self.payloads)
            print('Insertados {} en {}'.format(len(self.payloads), self.titanScraping))
            self.payloads.clear()
        if self.payloads_epis:
            self.mongo.insertMany(self.titanScrapingEpisodes, self.payloads_epis)
            print('Insertados {} en {}'.format(len(self.payloads_epis), self.titanScrapingEpisodes))
            self.payloads_epis.clear()

        Upload(self._platform_code, self._created_at, testing=testing)
        print("\033[32mSCRAPING FINISHED.\033[0m")
