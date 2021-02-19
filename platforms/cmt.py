from os import replace
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
from handle.payload_testing import Payload

class Cmt():
    def __init__(self, ott_site_uid, ott_site_country, type):
        self._config                = config()['ott_sites'][ott_site_uid]
        self._platform_code         = self._config['countries'][ott_site_country]
        #self._start_url             = self._config['start_url']
        self._created_at            = time.strftime("%Y-%m-%d")
        self.mongo                  = mongo()
        self.titanPreScraping       = config()['mongo']['collections']['prescraping']
        self.titanScraping          = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodes  = config()['mongo']['collections']['episode']
        self.titanScrapingEpisodios  = config()['mongo']['collections']['episode']
        self.skippedTitles = 0
        self.skippedEpis = 0

        self.sesion = requests.session()
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
            self._scraping(testing = True)

    def get_number(self, stringList, index):
        """ Obtiene el numero de temporada/episodio si es que tiene """
        number = None
        try:
            if not stringList.text == '':
                stringList = stringList.text.split(', ')
                if len(stringList) > index:
                    number = int(stringList[index].split(' ')[1])
        finally:
            return number

    def scroll_down_and_click(self, browser):
        """ Funcion que utiliza selenium para hacer un scroll down hasta llegar lo más
            abajo posible de la pagina, esperar y clickear el boton de "Load More".
        """
        browser.execute_script("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(2)
        more_buttons = browser.find_elements_by_class_name("L001_line_list_load-more")
        if more_buttons and not more_buttons[0].value_of_css_property('display') == 'none':
            while True:
                more_buttons[0].click()
                more_buttons = []
                time.sleep(2)
                browser.execute_script("window.scrollTo(0, document.body.scrollHeight)")
                more_buttons = browser.find_elements_by_class_name("L001_line_list_load-more")
                display = more_buttons[0].value_of_css_property('display')
                if display == 'none':
                    break

    def load_payload(self, payloadEpisodes, ids_guardados, parentTitle, parentId, link_wrappers):
        for link in link_wrappers:
            try:
                potentialYear = link.contents[-1].text.replace('aired','').strip().split('/')[2].split(' · ')[0]
            except:
                potentialYear = None
                
            if len(link.contents[2].contents) > 1:
                title = link.contents[2].contents[1]
            else:
                title = link.contents[2].text
            duration = link.find('div',class_='s_layouts_lineListDurationBadge')
            if duration:
                duration = int(duration.text.split(':')[0])
            else:
                duration = None
            synopsis = link.contents[3].text
            deepLinkWeb = link.get('href')

            season = self.get_number(link.find('h4'),0)
            if season:
                if len(str(season)) > 2:
                    season = None

            episodeNumber = self.get_number(link.find('h4'),1)

            payload = Payload(platformCode=self._platform_code,
                            id = hashlib.md5(title.encode('utf-8')+str(season).encode('utf-8')+str(potentialYear).encode('utf-8')).hexdigest(),
                            title=title,cleanTitle=_replace(title),duration=duration,synopsis=synopsis,episode=episodeNumber,season=season,
                            parentId=parentId,parentTitle=parentTitle,deeplinksWeb=deepLinkWeb,packages=[{'Type' : 'tv-everywhere'}],
                            timestamp=datetime.now().isoformat(),createdAt=self._created_at)
            
            if potentialYear:
                payload.year = '20' + potentialYear

            Datamanager._checkDBandAppend(self,payload.payloadEpisodeJson(),ids_guardados,payloadEpisodes,isEpi=True)

    def extract_episodes(self, show, payloadEpisodes, ids_guardados):
        """ Funcion que extrae todos los episodios y los almacena en
            el Datamanager y retorna el año del primer episodio, osea 
            la fecha de estreno del show. Si es que tiene.
            
            Returns:
                Integer or None 
        """

        parentTitle = show['title'] 
        parentId = show['itemId']

        soup = Datamanager._getSoup(self,show['url'] + '/episode-guide')

        if soup.find('span',class_='s_episodeAirDate'):
            ### Lo comentado aca es para hacer con soup sin selenium en el caso de que no tenga un boton de 'Load-More'
            #------------------------------------------------------------------------------------------------#
            # link_wrappers = soup.find_all('a',class_='link_wrapper')
            # load_more_display = soup.find('div',class_='L001_line_list_load-more custom_button_hover s_buttons_button').attrs['style']
            # print(load_more_display)
            # if 'none' in load_more_display:
            #     self.load_payload(payloadEpisodes, ids_guardados, parentTitle, parentId, link_wrappers)
            # else:
            #------------------------------------------------------------------------------------------------#

            browser = webdriver.Firefox()
            browser.get(show['url'] + '/episode-guide')
            time.sleep(3)
            
            self.scroll_down_and_click(browser)

            page_source = browser.page_source

            soup = BeautifulSoup(page_source,features='html.parser')
            link_wrappers = soup.find_all('a',class_='link_wrapper')

            self.load_payload(payloadEpisodes, ids_guardados, parentTitle, parentId, link_wrappers)

            browser.close()

            airDate = soup.find('span',class_='s_episodeAirDate').text
            
            return airDate.split(' ')[1]

        else:  
            browser = webdriver.Firefox()
            browser.get(show['url'] + '/video-guide')
            time.sleep(3)
            
            self.scroll_down_and_click(browser)

            page_source = browser.page_source

            soup = BeautifulSoup(page_source,features='html.parser')
            link_wrappers = soup.find_all('a',class_='link_wrapper')
                        
            self.load_payload(payloadEpisodes, ids_guardados, parentTitle, parentId, link_wrappers)

            browser.close()

            if link_wrappers:
                return link_wrappers[0].contents[-1].text.split(' ')[0]
            else:
                return None
        
    def _scraping(self, testing = False):
        """ Datos importantes:
                Necesita VPN: NO Al correr el script en Argentina o USA, trae el mismo contenido.
                Tiempo de ejecucion: Depende del internet ya que se utiliza Selenium. Aprox: 10Mins.
        """
        payloadsShows = []
        payloadsEpisodes = []
        ids_guardados_shows = Datamanager._getListDB(self,self.titanScraping)
        ids_guardados_episodes = Datamanager._getListDB(self,self.titanScrapingEpisodios)

        shows_api_url = 'http://www.cmt.com/feeds/ent_m150/36add037-353a-4e73-aea7-39b16dc5b3fb?hash=d82a6358eceaaa5ff0bc870e11d1af81ce191f22'

        shows_json = Datamanager._getJSON(self,shows_api_url)
        
        showList = shows_json['result']['data']['items']

        for show in showList:
            shows = show['sortedItems']
            if shows:
                for each in shows:
                    soup = Datamanager._getSoup(self,each['url'])
                    synopsis = soup.find('meta',{"itemprop": "description", "content" : True})['content']
                    title=each['title']

                    payload = Payload(id=each['itemId'],type='serie',cleanTitle=_replace(title),platformCode=self._platform_code,
                                      synopsis=synopsis,createdAt=self._created_at,timestamp=datetime.now().isoformat(),
                                      title=title,deeplinksWeb=each['url'],packages=[{'Type' : 'tv-everywhere'}])

                    print(payload.title)

                    #Episodes
                    potentialYear = self.extract_episodes(each,payloadsEpisodes,ids_guardados_episodes)

                    if potentialYear:
                        payload.year = '20' + potentialYear.split('/')[2]

                    payload_json = payload.payloadJson()

                    Datamanager._checkDBandAppend(self,payload_json,ids_guardados_shows,payloadsShows)

        Datamanager._insertIntoDB(self,payloadsShows,self.titanScraping)
        Datamanager._insertIntoDB(self,payloadsEpisodes,self.titanScrapingEpisodes)
        
        self.sesion.close()

        if not testing:
            Upload(self._platform_code, self._created_at, testing=True)