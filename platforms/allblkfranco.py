# -*- coding: utf-8 -*-
import requests # Si el script usa requests/api o requests/bs4
import time
import json
import re
import hashlib
from bs4                import BeautifulSoup # Si el script usa bs4
from handle.datamanager import Datamanager # Opcional si el script usa Datamanager
from common             import config
from handle.mongo       import mongo
from updates.upload     import Upload
from handle.replace     import _replace
from handle.payload     import Payload
from pyvirtualdisplay   import Display

class AllblkFranco():
    """
    - Status: En progreso
    - VPN: US - ExpressVPNju
    - Método: BS4
    - Runtime: 20min~
    """
    def __init__(self, ott_platforms, ott_site_country, ott_operation):
        self.test = ott_operation in ("testing", "return") #
        config_ = config()['ott_sites'][ott_platforms] # Obligatorio
        self.country = ott_site_country # Opcional, puede ser útil dependiendo de la lógica del script.
        self._created_at = time.strftime('%Y-%m-%d')
        self._platform_code = config_['countries'][ott_site_country]
        self.mongo = mongo()
        self.sesion                 = requests.session() # Requerido si se va a usar Datamanager
        self.titanPreScraping       = config()['mongo']['collections']['prescraping'] # Opcional
        self.titanScraping          = config()['mongo']['collections']['scraping'] # Obligatorio. También lo usa Datamanager
        self.titanScrapingEpisodios = config()['mongo']['collections']['episode'] # Obligatorio. También lo usa Datamanager
        self.skippedTitles          = 0 # Requerido si se va a usar Datamanager
        self.skippedEpis            = 0 # Requerido si se va a usar Datamanager
        self.url                    = config_['url']
        self.payloads               = list()
        self.payloads_episodes      = list()
        self.ids_scrapeados         = Datamanager._getListDB(self,self.titanScraping)
        self.ids_scrapeados_episodios = Datamanager._getListDB(self,self.titanScrapingEpisodios)
        """
        La operación 'return' la usamos en caso que se nos corte el script a mitad de camino cuando
        testeamos, sea por un error de conexión u otra cosa. Nos crea una lista de ids ya insertados en
        nuestro Mongo local, la cual podemos usar para saltar los contenidos scrapeados y volver rápidamente
        a donde había cortado el script.
        """
        if ott_operation == 'return':
            return_params = {'PlatformCode' : self._platform_code}
            last_item = self.mongo.lastCretedAt('titanPreScraping', return_params)
            if last_item.count() > 0:
                for last_content in last_item:
                    self._created_at = last_content['CreatedAt']
            self.prescraped_ids = [pay["Id"] for pay in Datamanager._getListDB(self, self.titanPreScraping)]

            self.scraping()
        else:
            self.prescraped_ids = list()

        if ott_operation in ('testing', 'scraping'):
            self.scraping()

    def getSoup(self, url):
        req_    = self.sesion.get(url)
        soup    = BeautifulSoup(req_.text, 'html.parser')
        return soup

    def getAllLinks(self, url):
        links   = list()
        soup    = self.getSoup(url)
        a_tags  = soup.find_all('a', itemprop=True, href=True)
        for a in a_tags:
            links.append(a['href'])
        return links

    def scraping(self):
        links_list      = self.getAllLinks(self.url)
        ### Obtener una lista con getAllLinks para cada género, y cuando
        ### se haga getGenre en un contenido, que compare el deeplink con
        ### esas listas. Si sale match, entonces se le appendea ese género
        ### al contenido.
        for url in links_list:
            content_metadata    = self.getSoup(url)
            payload             = self.buildPayload(content_metadata)
            if payload:
                Datamanager._checkDBandAppend(self,payload,self.ids_scrapeados,self.payloads)

        Datamanager._insertIntoDB(self,self.payloads,self.titanScraping)
        Datamanager._insertIntoDB(self,self.payloads_episodes,self.titanScrapingEpisodios)
        Upload(self._platform_code,self._created_at,testing=False)

    def buildPayload(self, content_metadata):        
        script_metadata = self.getScriptMetadata(content_metadata)
        _type = script_metadata['@type']
        
        if _type == 'Movie':
            payload = self.buildPayloadMovie(content_metadata, script_metadata, _type)
        elif _type == 'TVSeries':
            payload = self.buildPayloadSeries(content_metadata, script_metadata, _type)
        return payload

    def getScriptMetadata(self, content_metadata) -> dict:
        """
        Varios metadatos vienen de un <script>, en formato javascript, la
        cual es parseada con json.loads.
        """
        script_metadata = content_metadata.find('script', type='application/ld+json')
        if script_metadata:
            return json.loads(script_metadata.next)
        else:
            return dict()

    def buildPayloadMovie(self, content_metadata, script_metadata, _type)  -> dict:
        payload = Payload()
        payload.platform_code   = self._platform_code
        payload.title           = self.getTitle(content_metadata)  
        payload.clean_title     = _replace(payload.title) 
        payload.synopsis        = self.getSynopsis(script_metadata)
        payload.duration        = self.getDuration(script_metadata)
        payload.deeplink_web    = self.getDeeplink(script_metadata)
        payload.cast            = self.getCast(content_metadata, 
                                               script_metadata, 
                                               payload.synopsis)
        payload.directors       = self.getDirectors(content_metadata,
                                                    script_metadata)
        payload.image           = self.getImage(content_metadata, 
                                                script_metadata)
        payload.rating          = self.getRatingMovie(content_metadata)
        payload.packages        = [{"Type":"subscription-vod"}]
        payload.createdAt       = self._created_at
        payload.id              = self.hashId(payload.title, 
                                              payload.synopsis, 
                                              _type)
        return payload.payload_movie()

    def buildPayloadSeries(self, content_metadata, script_metadata, _type) -> dict:
        payload                 = Payload()
        payload.platform_code   = self._platform_code
        payload.title           = self.getTitle(content_metadata)  
        payload.clean_title     = _replace(payload.title) 
        payload.synopsis        = self.getSynopsis(script_metadata)
        payload.deeplink_web    = self.getDeeplink(script_metadata)
        payload.cast            = self.getCast(content_metadata, 
                                               script_metadata, 
                                               payload.synopsis)
        payload.directors       = self.getDirectors(content_metadata,
                                                    script_metadata)
        payload.id              = self.hashId(payload.title, 
                                              payload.synopsis, 
                                              _type)
        seasons                 = self.buildPayloadSeasons(content_metadata,
                                                           payload.id,
                                                           payload.title)
        payload.seasons         = seasons
        payload.packages        = [{"Type":"subscription-vod"}]
        payload.createdAt       = self._created_at
        return payload.payload_serie()

    def buildPayloadSeasons(self, content_metadata, parent_id, parent_title) -> list:
        payload_seasons     = list()
        seasons_container   = content_metadata.find('div', class_='container episode')
        seasons             = seasons_container.findAll('span', class_='episode-content-strip')
        for season_metadata in seasons:
            payload                 = Payload()
            payload.title           = self.getTitleSeason(season_metadata)
            payload.number          = self.getNumberSeason(season_metadata)
            payload.episodes        = self.getEpisodesSeason(season_metadata)
            payload.deeplink_web    = self.getDeeplinkSeason(season_metadata)
            payload.id              = self.hashIdSeason(payload.title, 
                                                        payload.number,
                                                        season_metadata)
            payload_season          = payload.payload_season()
            payload_seasons.append(payload_season)
            
            self.buildPayloadEpisodes(season_metadata,
                                      parent_id,
                                      parent_title,
                                      payload.number)
        return payload_seasons

    def buildPayloadEpisodes(self, season_metadata, parent_id, parent_title, season_number) -> None:  
        episodes = season_metadata.findAll('span', itemprop='episode')
        for episode_metadata in episodes:
            # if Chequear que no tenga 'bonus' o 'trailer' en el nombre
            payload = Payload()
            payload.platform_code   = self._platform_code
            payload.parent_id       = parent_id
            payload.parent_title    = parent_title
            payload.season          = season_number
            payload.episode         = self.getEpisodeNumber(episode_metadata)
            payload.deeplink_web    = self.getDeeplinkEpisode(episode_metadata)
            
            more_metadata           = self.getSoup(payload.deeplink_web)
            script_metadata         = self.getScriptMetadata(more_metadata)
            payload.title           = self.getTitleEpisode(episode_metadata,
                                                           script_metadata)
            payload.synopsis        = self.getSynopsis(script_metadata)
            payload.duration        = self.getDuration(script_metadata)
            payload.cast            = self.getCastEpisode(episode_metadata,
                                                          script_metadata)
            # ¿Tienen siquiera los episodios "director"?
            payload.directors       = self.getDirectors(episode_metadata,
                                                        script_metadata)
            payload.id              = self.hashIdEpisode(parent_title,
                                                         payload.title,
                                                         season_number,
                                                         payload.number)
            payload.packages        = [{"Type":"subscription-vod"}]
            payload.createdAt       = self._created_at
            
            payload_episode    = payload.payload_episode()
            Datamanager._checkDBandAppend(self,
                                          payload_episode,
                                          self.ids_scrapeados_episodios,
                                          self.payloads_episodes, 
                                          isEpi=True)

    def getEpisodeNumber(self, episode_metadata) -> int:
        episode_number = episode_metadata.find('span', itemprop='episodeNumber').text
        return int(episode_number)

    def getDeeplinkEpisode(self, episode_metadata) -> str:
        return episode_metadata.find('a', itemprop='url')['href']

    def hashIdEpisode(self, parent_title, title, season_number, number):
        _id = parent_title          \
              + title               \
              + str(season_number)  \
              + str(number)
        return hashlib.md5(_id.encode("utf-8")).hexdigest()
    
    def getTitleSeason(self, season_metadata) -> str:
        season_title = season_metadata.find('meta', itemprop='name')
        if season_title:
            return season_title['content']

    def getNumberSeason(self, season_metadata) -> int:
        season_number = season_metadata.find('meta', itemprop='seasonNumber')
        if season_number:
            return int(season_number['content'])

    def getEpisodesSeason(self, season_metadata) -> int:
        episodes_in_season = season_metadata.find('meta', itemprop='numberOfEpisodes')
        if episodes_in_season:
            return int(episodes_in_season['content'])

    def getDeeplinkSeason(self, season_metadata) -> str:
        deeplink_season = season_metadata.find('a', class_='btn btn-link')
        return deeplink_season.get('href')

    def hashIdSeason(self, title, number, season_metadata) -> str:
        parent_title    = season_metadata.find('meta', itemprop='partOfSeries')['content']
        _id             = title         \
                          + str(number) \
                          + parent_title
        return hashlib.md5(_id.encode("utf-8")).hexdigest()

    def hashId(self, title, synopsis, _type) -> str:
        _id = title         \
              + synopsis    \
              + _type
        return hashlib.md5(_id.encode("utf-8")).hexdigest()

    def getTitle(self, content_metadata) -> str:
        title = content_metadata.find('span', itemprop='name')
        if title:
            return title.text

    def getTitleEpisode(self, episode_metadata, script_metadata) -> str:
        script_title = script_metadata.get('name')
        if script_title:
            return script_title
        else:
            html_title = episode_metadata.find('h5', itemprop='name')
            if html_title:
                return html_title.text

    def getSynopsis(self, script_metadata) -> str:
        return script_metadata.get('description')

    def getDuration(self, script_metadata) -> int:
        """
        Duración viene en formato 'T1H19M19S'.
        A veces no trae horas, pero siempre minutos.
        """        
        duration = script_metadata.get('duration')
        if not duration:
            duration = script_metadata.get('timeRequired')
            if not duration:
                print('<<<<<< No se encontró la duración >>>>>>')
                return
        minutes = re.search(r'(\d*)M', duration)
        if minutes:
            minutes = minutes.group(1)
            minutes = int(minutes) if minutes else 0
            hours = re.search(r'(\d*)H', duration)
            if hours:
                hours = hours.group(1)
                hours = int(hours) if hours else 0
                return hours * 60 + minutes
            return minutes
        else:
            print('>>>>>> REGEX falló en encontrar duración <<<<<<')
            return

    def getDeeplink(self, script_metadata) -> str:
        deeplink = script_metadata.get('url')
        if deeplink:
            return deeplink.replace("\\", "")

    def getImage(self, content_metadata, script_metadata) -> list:
        image       = list()
        
        poster_link = content_metadata.find('meta', property='og:image')
        if poster_link:
            poster_link = poster_link['content'].split('?')[0]
            image.append(poster_link)
            
        preview_link  = script_metadata.get('image')
        if preview_link:
            preview_link = preview_link.replace("\\", "")
            if preview_link != poster_link:
                image.append(preview_link)

        if image != []:
            return image

    def getCast(self, content_metadata, script_metadata, synopsis) -> list:
        actors = script_metadata.get('actor')
        if actors:
            cast = list()
            for actor in actors:
                cast.append(actor['name'])
            return cast

        actors = content_metadata.find('p', id='franchise-actor')
        if actors: # Esto podría limpiarse con regex también
            actors = actors.text
            actors = actors.replace("Cast:", "")
            actors = actors.replace('\n', "")
            actors = actors.replace('\t', "")
            actors = actors.replace("Common, ", "")
            actors = actors.split(", ")
            return actors

        # Hay que buscarlo en la descripción mediante regex
        # Empieza con "Directed" o "Starred"
        #
        # search in synopsis
        # return ...

    def getCastEpisode(self, episode_metadata, script_metadata) -> list:
        actors = script_metadata.get('actor')
        if actors:
            cast = list()
            for actor in actors:
                cast.append(actor['name'])
            return cast

        actors = episode_metadata.find('div', class_='episode_starring')
        if actors:
            cast = list()
            actors = findAll('span', itemprop='actor')
            for actor in actors:
                actor_metadata = actor.text
                actor_metadata = actor_metadata.replace(", ", "")
                ########################################################
                # Debería ser con Regex para sacar solo el punto final #
                ########################################################
                actor_metadata = actor_metadata.replace(".", "") 
                cast.append(actor_metadata)
            return cast

    def getDirectors(self, content_metadata, script_metadata) -> list:
        directors = script_metadata.get('director')
        if directors:
            directors_payload = list()
            for director in directors:
                directors_payload.append(director['name'])
            return directors_payload
        
        directors = content_metadata.find('p', id='franchise-director')
        if directors: # Esto podría limpiarse con regex también
            directors = directors.text
            directors = directors.replace("Director:", "")
            directors = directors.replace("Directors:", "")
            directors = directors.replace('\n', "")
            directors = directors.replace('\t', "")
            directors = directors.split(", ")
            return directors

    def getRatingMovie(self, content_metadata) -> str:
        """
        Acceder al rating requiere una request adicional.
        """
        play_url   = content_metadata.find('a', class_='inline btn btn-primary')
        if play_url:
            play_url    = play_url.get('href')
            soup        = self.getSoup(play_url)
            div_rating  = soup.find('div', id="eps-tags")
            if div_rating:
                rating = div_rating.find('div', class_='').text.replace('Array RATING: ', '')
                return rating

    def getRatingEpisode(self, soup) -> str:
        """
        Aprovechamos que necesitamos una request por cada episodio
        para sacar el rating de una.
        """
        div_rating = soup.find('div', id="eps-tags")
        if div_rating:
            rating = div_rating.find('div', class_='').text.replace('Array TV RATING: ', '')
            return rating

# Package: 7-day Free Trial, Suscription Afterwards (4.99$ + taxes per month) or (49.99$ + taxes per year)
# Gift promo 49.99$ for 12 months (+ taxes)

# On occasion, we may make select full episodes available for free for
# limited promotional periods. Video extras may also be made available
# for free, at our discretion.