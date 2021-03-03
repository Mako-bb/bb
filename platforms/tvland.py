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
import platform
from pyvirtualdisplay       import Display
 
import sys

def validacionDatos(datoAValidar,datoAuxiliar):
    """
    Este metodo recibe un dato y revisa si se encuentra ese dato, si lo encuentra lo guarda sino lo guarda 
    por el dato auxiliar
    """
    if datoAValidar:
        return datoAValidar
    else:
        return datoAuxiliar

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
        path = 'C:\geckodriver.exe'
        if platform.system() == 'Linux':
            Display(visible=0, size=(1366, 768)).start()
        self.driver                 = webdriver.Firefox()
        self.sesion = requests.session()
        self.skippedTitles=0
        self.skippedEpis = 0
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
       
        """
        ¿VPN? NO
        ¿API,HTML o SELENIUM? Selenium
        
        TvLand es una plataforma de estados unidos con solo series, algunas presentan todas las temporadas mientras que otras
        presente algunas. Algunos episodios no tienen año, 
        """
        start_time = time.time()
        scraped = Datamanager._getListDB(self,self.titanScraping)
        scrapedEpisodes = Datamanager._getListDB(self,self.titanScrapingEpisodios)
        payloads = []
        payloadsEpisodios = []
        packages = [
                        {   
                            'Type': 'subscription-vod'
                        }
                    ]
        URL = self._config['urls']['url_principal'] +'/shows'

        soup = Datamanager._getSoup(self,URL)

        shows = soup.findAll(self._config['queries']['span_shows'])#,{'class':'header'})
        links = soup.findAll('li',{'class':self._config['queries']['links_shows_li_class']})
        #images = links.findAll('source',{'media':'screen'})
        # .a['href']
    
        nameShows = []
        urlShows = []
        seasonsShow = []
        seasonsUrl = []   
        descriptionsShow = []
        imgShow =[]
        #estraigo los nomrbes de los shows
        for show in shows:
            nameShows.append(show.text)
        #busco la url de cada show, depaso me quedo con las temporadas y otra informacion util
        for link in links:
            season = []
            seasonUrl=[]
            urlShow = URL+'/'+link.a['href'].split('/')[2]
            urlShows.append(urlShow)
            imgShow.append(link.find('div',{'class':self._config['queries']['img_show_div_class']}).noscript.img['srcset'])
            soup = Datamanager._getSoup(self,urlShow)
            descriptionsShow.append(soup.find('div',{'class':self._config['queries']['description_show_div_class']}).text)
            try:
                aux=soup.find('button',{'data-display-name':self._config['queries']['season_show_button_data_display_name']})
                season.append(aux.text.split('.')[0])
            except:
                season.append(soup.find('span',{'data-display-name':self._config['queries']['season_show_button_data_display_name']}).text if soup.find('span',{'data-display-name':"Button"}) else "Season 1")
            seasonUrl.append(urlShow)
                
            seasonAux= soup.findAll('a',{'class':self._config['queries']['season_show_a_class'],'tabindex':self._config['queries']['season_show_a_tabindex']})
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
        imgEpisodes = []
        Url = self._config['urls']['url_principal']
        for seasonUrl in seasonsUrl:
            episodeTitle=[]
            episodeUrl = []
            episodeDate=[]
            episodeSeason=[]
            episodeDescription = []
            imgEpisode =[]
            #recorro cada url de la temporada para sacar info de las episodios
            for url in seasonUrl:
                soup = Datamanager._getSoup(self,url,showURL=True)
                #El siguiente if busca todos los episodios, ahora si lo encuentra significa que no tiene un boton de load more
                #Pero si no lo encuentra entonces tiene un boton de load more y para eso uso selenium
                if soup.findAll('section',{'class':self._config['queries']['episodes_soup_section_class_type_1']}):
                    sectionClass = soup.find('section',{'class':self._config['queries']['episodes_soup_section_class_type_1']})
                    episodes =sectionClass.findAll('div',{'class':self._config['queries']['episodes_div_class']})
                    #recorro los episodios y extraigo la informacion de los mismos.
                    for episode in episodes:
                        # Url:
                        episodeUrl.append(Url+validacionDatos(sectionClass.find('li',{'class':self._config['queries']['url_episodes_li_class_type1']}),
                                                sectionClass.find('li',{'class':self._config['queries']['url_episodes_li_class_type2']})).a['href'])
                        # Season:
                        episodeSeason.append(validacionDatos(episode.find('div',{'class':self._config['queries']['season_episode_div_class_type1']}),
                                             episode.find('div',{'class':self._config['queries']['season_episode_div_class_type2']})).text.split('•'))
                        # Title:
                        episodeTitle.append(validacionDatos(episode.find('div',{'class':self._config['queries']['title_episode_div_class_type1']}),
                                            episode.find('div',{'class':self._config['queries']['title_episode_div_class_type2']})).text)
                        #Los siguientes try son para controlor que validacionDatos si devuelve None, no haga el .text
                        # Description:
                        try:
                            episodeDescription.append(validacionDatos(episode.find('div',{'class':self._config['queries']['description_episode_div_class']}),
                                                                      None).text)
                        except:
                            episodeDescription.append(None)
                        # Date:
                        try:
                            episodeDate.append(validacionDatos(episode.find('div',{'class':self._config['queries']['date_episode_div_class']}),
                                                   None).text)
                        except:
                            episodeDate.append(None)
                            
                    # Imagen
                    imagenes = sectionClass.findAll('div',{'class':self._config['queries']['img_episode_dic_class']})
                    for imagene in imagenes:
                        try:
                            imgEpisode.append(imagene.noscript.img['srcset'])
                        except:
                            imgEpisode.append(None)
                      
                else:  
                    soup = Datamanager._clickAndGetSoupSelenium(self,url,self._config['queries']['botton_episode_load_more'],waitTime=5,showURL=False)
                    sectionClass = soup.find('section',{'class':self._config['queries']['episodes_soup_section_class_type_1']})
                    #hay dos tipos de paginas de epìsodes con diferente busqueda, una es con un tipo de class y la otra es con otro por esto el siguiente if.
                    episodes =sectionClass.findAll('div',{'class':self._config['queries']['episodes_div_class']})
                    #recorro los episodios y extraigo la informacion de los mismos.
                    for episode in episodes:
                        # Url:
                        episodeUrl.append(Url+validacionDatos(sectionClass.find('li',{'class':self._config['queries']['url_episodes_li_class_type1']}),
                                                sectionClass.find('li',{'class':self._config['queries']['url_episodes_li_class_type2']})).a['href'])
                        # Season:
                        episodeSeason.append(validacionDatos(episode.find('div',{'class':self._config['queries']['season_episode_div_class_type1']}),
                                             episode.find('div',{'class':self._config['queries']['season_episode_div_class_type2']})).text.split('•'))
                        # Title:
                        episodeTitle.append(validacionDatos(episode.find('div',{'class':self._config['queries']['title_episode_div_class_type1']}),
                                            episode.find('div',{'class':self._config['queries']['title_episode_div_class_type2']})).text)
                        #Los siguientes try son para controlor que validacionDatos si devuelve None, no haga el .text
                        # Description:
                        try:
                            episodeDescription.append(validacionDatos(episode.find('div',{'class':self._config['queries']['description_episode_div_class']}),
                                                                      None).text)
                        except:
                            episodeDescription.append(None)
                        # Date:
                        try:
                            episodeDate.append(validacionDatos(episode.find('div',{'class':self._config['queries']['date_episode_div_class']}),
                                                   None).text)
                        except:
                            episodeDate.append(None)
                    # Imagen
                    imagenes = sectionClass.findAll('div',{'class':self._config['queries']['img_episode_dic_class']})
                    for imagene in imagenes:
                        try:
                            imgEpisode.append(imagene.noscript.img['srcset'])
                        except:
                            imgEpisode.append(None)
               
            episodesDate.append(episodeDate)
            episodesDescription.append(episodeDescription)
            episodesName.append(episodeTitle)
            episodesSeason.append(episodeSeason)
            episodesUrl.append(episodeUrl)
            imgEpisodes.append(imgEpisode)
        for i in range(0,len(nameShows)):
            
            title = nameShows[i]
            _id = hashlib.md5(title.encode('utf-8')).hexdigest()
            _type = 'serie'
            img=[imgShow[i]]
            URLContenido = urlShows[i]
            description = descriptionsShow[i]
            if description[-1]=='\n':
                description=description[:-1]
            payload = {
                'PlatformCode':  self._platform_code,
                'Id':            _id,
                'Title':         title,
                'CleanTitle':    _replace(title),
                'OriginalTitle': None,
                'Type':          _type, # 'movie' o 'serie'
                'Seasons':       None,
                'Year':          None,
                'Duration':      None, # duracion en minutos
                'Deeplinks': {
                    'Web':       URLContenido,
                    'Android':   None,
                    'iOS':       None,
                },
                'Playback':      None,
                'Synopsis':      description,
                'Image':         img, # [str, str, str...] # []
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

        for i in range(0,len(episodesDescription)):
            nameShow = nameShows[i]
            parrentId = hashlib.md5(nameShow.encode('utf-8')).hexdigest()
            for j in range(0,len(episodesDescription[i])):
                
                title = episodesName[i][j]
                if imgEpisodes[i][j]==None:
                    img=None
                else:
                    img=[imgEpisodes[i][j]]
                try: #algunos episodios no tienen el año, por lo que lo puse en None, por ende este try es para evitar el error de hacer none object tiene split.
                    date = int(episodesDate[i][j].split('/')[-1]) 
                except:
                    date = episodesDate[i][j]
                _id = hashlib.md5(title.encode('utf-8')+nameShow.encode('utf-8')).hexdigest()
                try:
                    aux = episodesSeason[i][j].remove("Highlight")[0].split(" ")
                except:
                    aux = 0
                try:
                    seasons = int(episodesSeason[i][j][0][1::])
                except:
                    seasons = None
                try:
                    episode =  int(episodesSeason[i][j][1][2::])
                except:
                    episode=None
                URLContenido = episodesUrl[i][j]
                description = episodesDescription[i][j]
                if description[-1]=='\n':
                    description=description[::-1]
                # year =
                payload = {
                            "PlatformCode":  self._platform_code, #Obligatorio      
                            "Id":            _id, #Obligatorio
                            "ParentId":      parrentId, #Obligatorio #Unicamente en Episodios
                            "ParentTitle":   nameShow, #Unicamente en Episodios 
                            "Episode":       episode, #Obligatorio #Unicamente en Episodios  
                            "Season":        seasons, #Obligatorio #Unicamente en Episodios
                            "Title":         title, #Obligatorio           
                            "OriginalTitle": None,                              
                            "Year":          date,     #Important!     
                            "Duration":      None,           
                            "Deeplinks": {          
                                "Web":       URLContenido,       #Obligatorio          
                                "Android":   None,          
                                "iOS":       None,      
                            },      
                            "Synopsis":      description,      
                            "Image":         img,      
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
                Datamanager._checkDBandAppend(self, payload,scrapedEpisodes,payloadsEpisodios,isEpi=True)
                Datamanager._insertIntoDB(self,payloadsEpisodios,self.titanScrapingEpisodios)

        self.sesion.close()
        Upload(self._platform_code, self._created_at, testing=testing)
        finish_time = time.time()
        print("Tiempo de ejecucion: "+str((finish_time-start_time)/60)+" min")