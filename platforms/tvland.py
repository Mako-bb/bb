# -*- coding: utf-8 -*-
import time
import requests
import hashlib   
import pymongo 
import re
import json
import platform
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from handle.replace         import _replace
from common                 import config
from datetime               import datetime
from handle.mongo           import mongo
from slugify                import slugify
from handle.datamanager     import Datamanager
from updates.upload         import Upload
from bs4                     import BeautifulSoup
from selenium.webdriver import ActionChains
import sys
class TvLand():
    def __init__(self, ott_site_uid, ott_site_country, type):
        self._config                = config()['ott_sites'][ott_site_uid]
        self._platform_code         = self._config['countries'][ott_site_country]
        #self._start_url             = self._config['start_url']
        self._created_at            = time.strftime("%Y-%m-%d")
        self.mongo                  = mongo()
        self.titanPreScraping       = config()['mongo']['collections']['prescraping']
        self.titanScraping          = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodios  = config()['mongo']['collections']['episode']
        self.driver                 = webdriver.Firefox()
        self.sesion = requests.session()
        self.skippedTitles=0
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

    def _scraping(self, testing = False):
       

        scraped = Datamanager._getListDB(self,self.titanScraping)
        scrapedEpisodes = Datamanager._getListDB(self,self.titanScrapingEpisodios)
        payloads = []
        payloadsEpisodios = []
        packages = [
                        {
                            'Type': 'tv-everywhere'
                        }
                    ]
        URL = 'https://www.tvland.com/shows'

        soup = Datamanager._getSoup(self,URL)

        shows = soup.findAll('span')#,{'class':'header'})
        links = soup.findAll('li',{'class':'item poster css-q2f74n-Wrapper e19yuxbf0'})
        # .a['href']
    
        nameShows = []
        urlShows = []
        seasonsShow = []
        seasonsUrl = []   
        descriptionsShow = []   
        for show in shows:
            nameShows.append(show.text)
        del nameShows[11]
        for link in links:
            season = []
            seasonUrl=[]
            urlShow = URL+'/'+link.a['href'].split('/')[2]
            urlShows.append(urlShow)
            soup = Datamanager._getSoup(self,urlShow)
            descriptionsShow.append(soup.find('div',{'class':"deck"}).text)
            try:
                aux=soup.find('button',{'data-display-name':"Button"})
                season.append(aux.text.split('.')[0])
            except:
                season.append(soup.find('span',{'data-display-name':"Button"}).text if soup.find('span',{'data-display-name':"Button"}) else "Season 1")
            seasonUrl.append(urlShow)
            seasonAux= soup.findAll('a',{'class':'css-1wkgy79-StyledTypography e1wje7qk0','tabindex':'-1'})
            for seasonaux in seasonAux:
                season.append(seasonaux.text)
                aux =seasonaux['href'].split('/')[3]+'/'+seasonaux['href'].split('/')[4]
                seasonUrl.append(urlShow+'/'+aux)
            seasonsShow.append(season)
            seasonsUrl.append(seasonUrl)

        

        episodesName=[]
        episodesDate=[]
        episodesSeason=[]
        episodesDescription = []
        episodesUrl = []
        Url = 'www.tvland.com'
        for seasonUrl in seasonsUrl:
            episodeTitle=[]
            episodeUrl = []
            episodeDate=[]
            episodeSeason=[]
            episodeDescription = []
            for url in seasonUrl:
                try:                                                                       
                    soup = Datamanager._clickAndGetSoupSelenium(self,url,"expand-wrap",waitTime=5,showURL=True)
                except:
                    soup = Datamanager._getSoup(self,url,showURL=False)
                sectionClass = soup.findAll('section',{'class':'module-container video-guide-container'})[0]
                episodes =  sectionClass.findAll('div',{'class':'meta-wrap css-1u1rran-Wrapper e1u7s1dj0'}) if sectionClass.findAll('div',{'class':'meta-wrap css-1u1rran-Wrapper e1u7s1dj0'}) else sectionClass.findAll('div',{'class':'css-qc960f-Box-Flex-Meta e1oi4lqz1'})
                for episode in episodes:
                    try: 
                        episodeSeason.append(episode.find('div',{'class':'header'}).text.split('•'))
                        episodeTitle.append(episode.find('div',{'class':'sub-header'}).text)
                        episodeDescription.append(episode.find('div',{'class':'deck'}).text)
                        episodeDate.append(episode.find('div',{'class':'meta'}).text)
                        episodeUrl.append(Url+sectionClass.find('li',{'class':'item full-ep css-q2f74n-Wrapper e19yuxbf0'}).a['href'])
                    except:
                        episodeSeason.append(episode.find('p',{'class':'ev0yupn0 e1r0v7z20 css-se0neo-StyledTypography-StyledElement-StyledSuperHeader e1wje7qk0'}).text.split('•'))
                        episodeTitle.append(episode.find('h3',{'class':'ev0yupn1 e15zpijj0 css-1mcwcud-StyledTypography-StyledTypography-StyledHeader e1wje7qk0'}).text)
                        episodeDescription.append(episode.find('p',{'class':'ev0yupn2 e1815zq20 css-9rag0y-StyledTypography-StyledTypography-StyledDeck e1wje7qk0'}).text)
                        episodeUrl.append(Url+sectionClass.find('li',{'class':'css-1yucgj6-Box-Flex-Layout-StyledWrapper ev0yupn4'}).a['href'])
            episodesDate.append(episodeDate)
            episodesDescription.append(episodeDescription)
            episodesName.append(episodeTitle)
            episodesSeason.append(episodeSeason)
            episodesUrl.append(episodeUrl)

        for i in range(0,len(nameShows)):
            
            title = nameShows[i]
            _id = hashlib.md5(title.encode('utf-8')).hexdigest()
            _type = 'serie'
            seasons = len(seasonsShow[i])
            URLContenido = urlShows[i]
            description = descriptionsShow[i]
            payload = {
                'PlatformCode':  self._platform_code,
                'Id':            _id,
                'Title':         title,
                'CleanTitle':    _replace(title),
                'OriginalTitle': None,
                'Type':          _type, # 'movie' o 'serie'
                'Seasons':       seasons,
                'Year':          None,
                'Duration':      None, # duracion en minutos
                'Deeplinks': {
                    'Web':       URLContenido,
                    'Android':   None,
                    'iOS':       None,
                },
                'Playback':      None,
                'Synopsis':      description,
                'Image':         None, # [str, str, str...] # []
                'Rating':        None,
                'Provider':      None,
                'Genres':        None, # [str, str, str...]
                'Cast':          None, # [str, str, str...]
                'Directors':     None, # [str, str, str...]
                'Availability':  None,
                'Download':      None,
                'IsOriginal':    None,
                'IsAdult':       None,
                'Packages':      packages,
                'Country':       None, # [str, str, str...]
                'Timestamp':     datetime.now().isoformat(),
                'CreatedAt':     self._created_at
            }
            Datamanager._checkDBandAppend(self, payload,scraped,payloads)
            Datamanager._insertIntoDB(self,payloads,self.titanScraping)

        for i in range(0,len(episodesDate)):
            for j in range(0,len(episodesDate[0])):
                title = episodesName[i][j]
                _id = hashlib.md5(title.encode('utf-8')).hexdigest()
                seasons = episodesSeason[i][j][0]
                try:
                    episode =  episodesSeason[i][j][1]
                    if episode[0] !='E':
                        episode = None

                except:
                    episode=None
                URLContenido = episodesUrl[i][j]
                description = episodesDescription[i][j]
                # year =
                payload = {
                            "PlatformCode":  self._platform_code, #Obligatorio      
                            "Id":            _id, #Obligatorio
                            "ParentId":      None, #Obligatorio #Unicamente en Episodios
                            "ParentTitle":   None, #Unicamente en Episodios 
                            "Episode":       episode, #Obligatorio #Unicamente en Episodios  
                            "Season":        seasons, #Obligatorio #Unicamente en Episodios
                            "Title":         title, #Obligatorio      
                            "CleanTitle":    _replace(title), #Obligatorio      
                            "OriginalTitle": None,                          
                            "Type":          'serie',     #Obligatorio      
                            "Year":          None,     #Important!     
                            "Duration":      None,      
                            "ExternalIds":   None,      
                            "Deeplinks": {          
                                "Web":       URLContenido,       #Obligatorio          
                                "Android":   None,          
                                "iOS":       None,      
                            },      
                            "Synopsis":      description,      
                            "Image":         None,      
                            "Rating":        None,     #Important!      
                            "Provider":      None,      
                            "Genres":        None,    #Important!      
                            "Cast":          None,      
                            "Directors":     None,    #Important!      
                            "Availability":  None,     #Important!      
                            "Download":      None,      
                            "IsOriginal":    None,    #Important!      
                            "IsAdult":       None,    #Important!   
                            "IsBranded":     None,    #Important!   
                            "Packages":      packages,    #Obligatorio      
                            "Country":       None,      
                            "Timestamp":     datetime.now().isoformat(), #Obligatorio      
                            "CreatedAt":     self._created_at, #Obli
                }
                Datamanager._checkDBandAppend(self, payload,scrapedEpisodes,payloadsEpisodios)
                Datamanager._insertIntoDB(self,payloads,self.titanScrapingEpisodios)

        self.sesion.close()


        if not testing:
            Upload(self._platform_code, self._created_at, testing=True)