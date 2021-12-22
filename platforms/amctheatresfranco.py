# -*- coding: utf-8 -*-
import requests # Si el script usa requests/api o requests/bs4
import time
import platform
import re
import json
from bs4                            import BeautifulSoup # Si el script usa bs4
from selenium                       import webdriver # Si el script usa selenium
from selenium.webdriver.common.by   import By
from selenium.webdriver.support.ui  import WebDriverWait
from selenium.webdriver.support     import expected_conditions as EC
from handle.datamanager             import Datamanager # Opcional si el script usa Datamanager
from common                         import config
from handle.mongo                   import mongo
from updates.upload                 import Upload
from handle.replace                 import _replace
from handle.payload                 import Payload
from pyvirtualdisplay               import Display

class AmctheatresFranco():

    def __init__(self, ott_platforms, ott_site_country, ott_operation):
        self.test = ott_operation in ("testing", "return") #
        config_ = config()['ott_sites'][ott_platforms] # Obligatorio
        self.country = ott_site_country # Opcional, puede ser útil dependiendo de la lógica del script.
        self._created_at = time.strftime('%Y-%m-%d')
        self._platform_code = config_['countries'][ott_site_country]
        self.mongo                  = mongo()
        self.sesion                 = requests.session() # Requerido si se va a usar Datamanager
        self.titanPreScraping       = config()['mongo']['collections']['prescraping'] # Opcional
        self.titanScraping          = config()['mongo']['collections']['scraping'] # Obligatorio. También lo usa Datamanager
        self.titanScrapingEpisodios = config()['mongo']['collections']['episode'] # Obligatorio. También lo usa Datamanager
        self.skippedTitles          = 0 # Requerido si se va a usar Datamanager
        self.skippedEpis            = 0 # Requerido si se va a usar Datamanager}
        self.url                    = config_['url']
        self.query                  = ['?availability=NOW_PLAYING', '?availability=COMING_SOON','?availability=ON_DEMAND']
        self.payloads               = list()
        self.ids_scrapeados         = Datamanager._getListDB(self,self.titanScraping)
        self.titles_scrapeados      = list()
        self.all_links              = list()
        self.patterns               = { 'movie_title': re.compile(r'(?:Double|Triple) Feature?$|[\d]+\SMovie[\s]?|[\d]+\SPack|Bundle$|Collection$|Trilogy$|Pack$|Best (?:Picture|Movie[s]?|Actor|Actress)|.+[\s]+[\/\+][\s]+.+|[\d]+[\s]+and[\s]+[\d]+|[\d]+[\s]+&[\s]+[\d]+|[\(]?Extended Cut[\)]?$|[\(]?Unrated[\)]?$|Iconic Films of the'),
                                        'id_deeplink' : re.compile(r'[.\d]+$')
                                    }

        # si se usa selenium, el browser debe declararse como un atributo de clase para que nos sea más fácil cerrarlo con el destructor
        self.browser = webdriver.Chrome()

        #### este bloque de abajo es OBLIGATORIO si se usa Selenium para que pueda correr en los servers
        #### se debe copiar y pegar tal cual está para que funcione
        # primero declarando la variable self.display en una línea
        # y luego llamando al método start de esa variable en la línea que le sigue
        try:
            if platform.system() == 'Linux':
                self.display = Display(visible=0, size=(1366, 768))
                self.display.start()
        except Exception:
            pass
        ####

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

    def __del__(self):
        # este es el destructor de clase, realmente solo es necesario añadirlo en scripts donde se use selenium
        # ya que lo usamos para asegurarnos que se cierren todas las instancias de selenium y los virtual displays
        # el destructor de clase se encarga de cerrarlos independientemente de si el script corrió hasta el final o
        # si cortó por un error
        try:
            self.browser.quit()
        except Exception:
            pass

        try:
            self.display.stop()
        except Exception:
            pass

    def scraping(self):
        #### The Hardcode Coven ####
        #### CHEQUEAR SIEMPRE INSTERTINTODB ####
        self.explorePages()
        #all_links = ['https://www.amctheatres.com/movies/mulan-49548']
        #self.browser.close()
        #with open('all_links.json') as f:
        #    self.all_links = json.load(f)
        #print('Cantidad de links: ' + str(len(self.all_links)))
        #pass
        ############################
        for link in self.all_links:
            content_metadata = Datamanager._getSoup(self, URL=link)
            if content_metadata.find('ul', class_='dropdown-btn__list'): # ¿Hay botón de Buy / Rent?
                script_metadata = self.getScriptMetadata(content_metadata)
                payload = self.buildPayloadMovie(content_metadata, script_metadata)
                if payload:
                    Datamanager._checkDBandAppend(self,payload,self.ids_scrapeados,self.payloads)

        Datamanager._insertIntoDB(self,self.payloads,self.titanScraping)
        Upload(self._platform_code,self._created_at,testing=self.test)

    def explorePages(self):
        ### Featured Movies / Now playing ###
        self.browser.get(self.url + self.query[0])
        self.scrapePage()

        ### Featured Movies / Coming soon ###
        self.browser.get(self.url + self.query[1])
        self.scrapePage()

        ### Featured Movies / On demand ###
        self.browser.get(self.url + self.query[2])
        self.waitForElementByClass('Slide', 10)

        # Closing the popup
        popup_button_xpath = '/html/body/div[2]/div[2]/div/div/div/div[2]/div/div/div[2]/div[1]/button'
        self.clickByXpath(popup_button_xpath) 

        # Declaring the 'Next' button
        next_button_xpath = '/html/body/div[1]/div/main/div/div[2]/section/div/div[2]/div[2]/button[2]'
        next_button = self.loadElementByXpath(next_button_xpath)

        # Iterating through all 'On demand' pages
        while next_button.is_enabled():
            self.scrapePage()
            self.clickByXpath(next_button_xpath)
            time.sleep(2) 
            ### Acá tiene que haber sí o sí un "time.sleep" o algo así, es de vital importancia. 
            ### A lo mejor un waitHastaQueTal elemento pueda ser identificable con
            ### el .is_enabled(), cosa que el loop while pueda funcionar

        self.browser.close()

    def scrapePage(self):
        all_movies = self.loadAllElementsByClass('Slide')
        for movie in all_movies:
            try:
                title = movie.find_element(By.TAG_NAME, 'h3')
                if title:
                    title_str = title.text
                    if not self.checkForBundle(title_str) and not self.checkForDuplicate(title_str):
                        link        = self.findLink(movie)
                        duration    = self.findDuration(movie)
                        deeplink    = self.createDeeplink(link)
                        if deeplink not in self.all_links:
                            if duration:
                                print(title_str + ', ' + str(duration) + ', ' + deeplink)
                            else:
                                print(title_str + ', ND, ' + deeplink)
                            self.all_links.append(link)
            except:
                print('NO SE ENCONTRÓ MOVIE. SIGUIENTE...')

    def findLink(self, movie):
        link = movie.find_element(By.TAG_NAME, 'a')
        return link.get_attribute('href')

    def findDuration(self, movie):
        try:
            duration = movie.find_element(By.CLASS_NAME, 'u-separator.js-runtimeConvert.u-inlineFlexCenter').text
            minutes  = re.search(r'(\d+)[\s]?M', duration)
            if minutes:
                minutes = minutes.group(1)
                minutes = int(minutes) if minutes else 0
                hours = re.search(r'(\d+)[\s]?H', duration)
                if hours:
                    hours = hours.group(1)
                    hours = int(hours) if hours else 0
                    return hours * 60 + minutes
                return minutes
        except:
            pass

    def createDeeplink(self, link) -> str:
        try:
            id_ = self.patterns['id_deeplink'].search(link).group()
            return self.url + '/' + id_
        except:
            pass

    def waitForElementByClass(self, name: 'str', timer: 'int'):
        WebDriverWait(self.browser, timer).until(
                EC.presence_of_element_located((By.CLASS_NAME, name))
            )

    def waitForElementByXpath(self, xpath: 'str', timer: 'int'):
        WebDriverWait(self.browser, timer).until(
                EC.presence_of_element_located((By.XPATH, xpath))
            )

    def clickByXpath(self, xpath: 'str'):
        WebDriverWait(self.browser, 5).until(
                EC.element_to_be_clickable((By.XPATH, xpath))
            ).click()

    def loadElementByXpath(self, xpath: 'str') -> 'SeleniumObject':    
        self.waitForElementByXpath(xpath, 5)
        return self.browser.find_element(By.XPATH, xpath)

    def loadAllElementsByClass(self, name: 'str') -> 'SeleniumObject':    
        self.waitForElementByClass(name, 10)
        return self.browser.find_elements(By.CLASS_NAME, name)

    def writeData (self, nombre_archivo: 'str', list_preparado: 'list') -> None: # Obsoleto
        """
        Función encargada de escribir el diccionario recibido en un archivo json formateado.
        nombre_archivo = Nombre que tendrá el archivo .json.
        list_preparado = Diccionario ya armado para ser convertido a json.
        
        """
        editor = open(nombre_archivo, 'w')
        to_json = json.dumps(list_preparado, indent=4)
        editor.write(to_json)
        editor.close()

    def getScriptMetadata(self, content_metadata):
        apollo = content_metadata.find('script', {'id' : 'apollo-data'})
        return json.loads(apollo.next)

    def getId(self, content_json):
        id_ = content_json.get('movieId')
        if id_:
            return str(id_)
        else:
            raise print('MOVIE DESCONOCIDA NO TRAJO ID.')

    def getTitle(self, content_json):
        title = content_json.get('name')
        if title:
            return title
        else:
            id_ = getId(content_json)
            raise print('ID ' + id_ + ': NO TRAJO TITLE.')

    def getSynopsis(self, content_json):
        return content_json.get('synopsis')

    def getRating(self, content_json):
        rating = content_json.get('mpaaRating')
        if rating != 'NR':
            return rating

    def getGenres(self, content_json):
        genres_str = content_json.get('genre')
        if genres_str:
            return genres_str.title().split(", ")

    def getDuration(self, content_json):
        duration = content_json.get('runTime')
        if duration:
            if duration < 300:
                return duration

    def getDeeplink(self, content_json):
        deeplink = content_json.get('absoluteWebsiteUrl')
        if deeplink:
            return deeplink
        else:
            id_ = getId(content_json)
            raise print('ID ' + id_ + ': NO TRAJO DEEPLINK.')

    def getDirectors(self, content_json):
        directors = list()
        directors_str = content_json.get('directors')
        if directors_str:
            directors = directors_str.title().split(", ")
            for member in directors:
                if '?' in member:
                    directors.remove(member)
        return directors

    def getCast(self, content_json):
        cast = list()
        cast_str = content_json.get('starringActors')
        if cast_str:
            cast = cast_str.title().split(", ")
            for member in cast:
                if '?' in member:
                    cast.remove(member)
        return cast

    def checkForBundle(self, title) -> bool:
        match = self.patterns['movie_title'].search(title)
        if match:
            return True

    def checkForDuplicate(self, title) -> bool:
        clean_title = _replace(title)
        if clean_title not in self.titles_scrapeados:
            self.titles_scrapeados.append(clean_title)
        else:
            return True

    def buildPayloadMovie(self, content_metadata, script_metadata):
        payload             = Payload()
        payload.packages    = self.getPackages(content_metadata)
        if not payload.packages:
            print('NO TRAJO PACKAGE. SIGUIENTE...')
            return None # Es un título en pre-Order

        content_json        = None
        for key in script_metadata:
            if not payload.title:
                if script_metadata[key].get('__typename') == 'Movie':
                    content_json            = script_metadata[key]
                    payload.duration        = self.getDuration(content_json)
                    payload.synopsis        = self.getSynopsis(content_json)
                    if not payload.duration and self.checkSynopsis(payload.synopsis):
                        print('BUNDLE DETECTADO. SIGUIENTE...')
                        return None # Es un bundle que pasó el filtro
                    payload.title           = self.getTitle(content_json)
                    payload.id              = self.getId(content_json)
                    payload.rating          = self.getRating(content_json)
                    payload.genres          = self.getGenres(content_json)
                    payload.deeplink_web    = self.getDeeplink(content_json)
                    payload.cast            = self.getCast(content_json)
                    payload.directors       = self.getDirectors(content_json)
                    payload.crew            = list()
            
            if script_metadata[key].get('__typename') == 'CastAndCrew':
                content_json    = script_metadata[key]
                payload         = self.orderCastAndCrew(content_json, payload)

        payload.platform_code   = self._platform_code        
        payload.image           = self.getImage(content_metadata)
        payload.createdAt       = self._created_at
        return self.cleanPayload(payload)

    def checkSynopsis(self, synopsis) -> bool:
        if synopsis:
            if 'bundle' in synopsis or 'includes' in synopsis or 'including' in synopsis:
                return synopsis

    def cleanPayload(self, payload):
        if not payload.cast:
            payload.cast        = None
        if not payload.directors:
            payload.directors   = None
        if not payload.crew:
            payload.crew        = None
        return payload.payload_movie()

    def orderCastAndCrew(self, content_json, payload):
        role = content_json['role']
        name = content_json['name']
        if '?' not in name:
            if role == 'Actor' and name not in payload.cast:
                payload.cast.append(name)
            elif role == 'Director' and name not in payload.directors:
                payload.directors.append(name)
            elif role == 'Writer' or role == 'Producer':
                person = {
                    "Role" : role,
                    "Name" : name
                }
                if person not in payload.crew:
                    payload.crew.append(person)
        return payload

    def getImage(self, content_metadata):
        ### Hay una imagen de poster que se podría encontrar también, tal vez
        image = list()
        images_carousel = content_metadata.find('ul', class_='Carousel-Slides')
        if images_carousel:
            images_img = images_carousel.find_all('img')
            for image_img in images_img:
                image_queries   = image_img['src'].split('/')[5] + '/'
                image_url       = image_img['src'].replace(image_queries, '')
                image.append(image_url)
            if len(image):
                return image

    def getPackages(self, content_metadata):
        """
        Cortesía de CharlieGOD
        """
        packages = list()
        prices = dict()
        div_container = content_metadata.find('div', class_='LoadingContainer-inline')
        options       = div_container.find_all('li', role='option')

        for option in options:
            ### PROBAR POR QUE EN EL CASO DE Grinch TRAE 3.99 EN VEZ DE 3, PERO EL OTRO PRECIO LO TRAE BIEN
            quality     = option.contents[0]
            price       = option.contents[-1].replace('$', '')
            price_type  = option['aria-labelledby']
            if prices.get(quality):
                var = {
                    'Price' : price,
                    'Price Type' : price_type
                }
                prices[quality].append(var)                       
            else:
                prices[quality] = [{
                    'Price' : price,
                    'Price Type' : price_type
                }]

        #### SI TIENE PRECIO DENTRO DE <del>, TRAER EL MÁS ALTO
        for quality in prices:
            package = {
                'Type' : 'transaction-vod',
                'Currency' : 'USD',
                'Definition' : quality
            }
            for offer in prices[quality]:
                if 'Pre-Order' in offer['Price Type']:
                    return None
                elif 'Rent' in offer['Price Type']:
                    package['RentPrice'] = float(offer['Price'])
                elif 'Buy' in offer['Price Type']:
                    package['BuyPrice'] = float(offer['Price'])
            if package not in packages:
                packages.append(package)

        if packages:
            return packages