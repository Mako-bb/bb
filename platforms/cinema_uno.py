# -*- coding: utf-8 -*-
import requests
import time
from datetime                import datetime
from common                  import config
from handle.mongo            import mongo
from bs4                     import BeautifulSoup
from updates.upload          import Upload
from handle.replace          import _replace

class CinemaUno():
    def __init__(self, ott_site_uid, ott_site_country, operation):
        self._config = config()['ott_sites'][ott_site_uid]

        self._platform_code = self._config['countries'][ott_site_country]
        self._created_at = time.strftime('%Y-%m-%d')

        self._start_url = self._config['start_url']

        self._currency = config()['currency'][ott_site_country]

        self._mongo = mongo()
        self._titanScraping = config()['mongo']['collections']['scraping']
        self._titanScrapingEpisodes = config()['mongo']['collections']['episode']

        self._http_session = self._get_session()

        if operation == 'return':
            params = {'PlatformCode': self._platform_code}
            last_item = self._mongo.lastCretedAt(self._titanScraping, params)
            if last_item.count() > 0:
                for last_content in last_item:
                    self._created_at = last_content['CreatedAt']

            self._scraping()

        elif operation == 'scraping':
            self._scraping()

        elif operation == 'testing':
            self._scraping(testing=True)

        else:
            print('Operacion no valida.')
            return

    def _get_session(self):
        s = requests.session()
        a = requests.adapters.HTTPAdapter(max_retries=3, pool_maxsize=40)
        s.mount('https://cinemauno.com/', a)

        return s

    def _query_field(self, collection, field, extra_filter=None):
        if not extra_filter:
            extra_filter = {}

        query = self._mongo.db[collection].find(
            filter={
                'PlatformCode': self._platform_code,
                'CreatedAt': self._created_at,
            }.update(extra_filter),
            projection={
                '_id': 0,
                field: 1,
            },
            no_cursor_timeout=False
        )

        query = {item[field] for item in query}

        return query

    def _scraping(self, testing=False):
        scraped = self._query_field(self._titanScraping, 'Id')

        categories = [
            'estrenos',
            'mujeresalpoder',
            'cinedelmundo',
            'documentalerxs',
            'exclusivas-en-renta',
            'grandesdirectores',
            'nuevasmiradas',
            'horror',
            'queer'
        ]
        movie_list = []

        for category in categories:
            r = self._http_session.get(self._start_url.format(category=category))
            html_soup = BeautifulSoup(r.text, 'lxml')

            titles = html_soup.find('div', {'class': 'row grid-movie'})
            if titles:
                findDeeplink = titles.find_all('div', {'class': 'small-2 columns mix'})
                for d in findDeeplink:
                    s_deeplink = d.a['href']
                    deeplink = 'https://cinemauno.com' + s_deeplink
                    movie_list.append(deeplink)

        for movie in movie_list:
            content_id = movie.split('/')[-1]

            # if content_id in scraped:
            #     print('Existe {}'.format(content_id))
            #     continue

            r = self._http_session.get(movie)
            html_soup = BeautifulSoup(r.text, 'lxml')

            find_deeplink = html_soup.find('meta', {'property': 'og:url'})
            if find_deeplink:
                deeplink = find_deeplink['content']

            box1 = html_soup.find('div', {'class': 'small-9 columns movie-details'})
            if box1:
                info1 = box1.find('div', {'class': 'small-8 columns description'})

                fTitle = info1.find('div', {'class': 'movie-title'})
                title = fTitle.h1.text
                year = fTitle.span.text
                year = year.split(', ')
                year = year[1]
                year = int(year.strip())
                origin_country = fTitle.span.a.text
                countries = [origin_country]

                fSynopsis = info1.find('div', {'class': 'synopsis'})
                synopsis = fSynopsis.p.text
                synopsis = synopsis.replace('\n', '')
                synopsis = synopsis.replace('\r', '')
                synopsis = synopsis.replace('\xa0', '')

                director = info1.find('div', {'class': 'small-7 columns direction'}).text
                director = director.replace('\n', '')
                directors = [director]

                cast = []
                find_info = html_soup.find('div', {'id': 'more-info'})
                if find_info:
                    table = find_info.table
                    if table:
                        body = table.find('tbody')
                        if body:
                            tr = body.find_all('tr')
                            if len(tr):
                                for td in tr:
                                    tdd = td.find_all('td')
                                    i = 0
                                    descripcion = None
                                    for dt in tdd:
                                        if i == 0:
                                            descripcion = dt.text
                                            descripcion = descripcion.strip()

                                        if i > 0:
                                            if descripcion == 'Elenco':
                                                persons = dt.find_all('a')
                                                for person in persons:
                                                    person = person.text.strip()
                                                    cast.append(person)
                                        i += 1

                quality = None
                runtime = None
                info2 = box1.find('ul', {'class': 'inline-list formats'})
                if info2:
                    formats = info2.find_all('li')
                    if len(formats) > 0:
                        for info in formats:
                            info = info.text
                            if info.find('HD') != -1:
                                quality = 'HD'
                            elif info.find('SD') != -1:
                                quality = 'SD'
                            elif info.find('MIN') != -1:
                                runtime = info.replace(' MIN', '')
                                runtime = int(runtime)

            rent = None
            box2 = html_soup.find('div', {'class': 'buy-actions'})
            if box2:
                find_button_sec = box2.find_all('a')
                if len(find_button_sec) > 0:
                    for buy in find_button_sec:
                        comp = buy['href']
                        if comp.find('rent=true') != -1:
                            rent = buy.text
                            rent = rent.split('$')
                            rent = rent[1]
                            rent = rent.strip()

            packages = []
            packages.append(
                {
                    'Type': 'subscription-vod',
                    'Definition': quality,
                }
            )

            if rent and float(rent) != 0:
                packages.append(
                    {
                        'Type'      : 'transaction-vod',
                        'Definition': quality,
                        'RentPrice' : float(rent),
                    }
                )

            payload = {
                'PlatformCode'    : self._platform_code,
                'Id'              : content_id,
                'Type'            : 'movie',
                'Title'           : title,
                'CleanTitle'      : _replace(title),
                'OriginalTitle'   : None,
                'Year'            : year,
                'Duration'        : runtime,
                'Deeplinks'       : {
                    'Web'         : deeplink,
                    'Android'     : None,
                    'iOS'         : None
                },
                'Synopsis'        : synopsis,
                'Rating'          : None,
                'Provider'        : None,
                'Genres'          : None,
                'Cast'            : cast,
                'Directors'       : directors,
                'Availability'    : None,
                'Download'        : None,
                'IsOriginal'      : None,
                'IsAdult'         : None,
                'Packages'        : packages,
                'Country'         : countries,
                'Timestamp'       : datetime.now().isoformat(),
                'CreatedAt'       : self._created_at
            }

            if payload['Id'] not in scraped:
                scraped.add(payload['Id'])
                self._mongo.insert(self._titanScraping, payload)
                print('Insert {}'.format(content_id))

        '''
        Upload
        '''
        if not testing:
            Upload(self._platform_code, self._created_at, False)
