# -*- coding: utf-8 -*-
import requests # Si el script usa requests/api o requests/bs4
import time
import platform
from bs4                import BeautifulSoup # Si el script usa bs4
from selenium           import webdriver # Si el script usa selenium
from handle.datamanager import Datamanager # Opcional si el script usa Datamanager
from common             import config
from handle.mongo       import mongo
from updates.upload     import Upload
from handle.replace     import _replace
from handle.payload     import Payload
from pyvirtualdisplay   import Display
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import re

class amcLE():

    """
    Plantilla de muestra para la definición de la clase de una plataforma nueva.

    Los imports van arriba de todo, de acuerdo a las convenciones de buenas prácticas.

    Esta es la descripción de la clase, donde se indica toda la información pertinente al script.
    Siempre debe ir, como mínimo, la siguiente información:
    - Status: A revision
    - VPN: VPN express Miami 1
    - Método: BS4 Y selenium
    - Runtime: 3 hs
    El __init__ de la clase define los atributos de la clase al instanciar un objeto de la misma.
    Los parámetros que se le pasan al momento de instanciar una clase son los que se insertan desde la terminal
    y siempre son los mismos:
    - ott_platforms: El nombre de la clase, debe coincidir con el nombre que se encuentra en el config.yaml
    - ott_site_country: El ISO code de 2 dígitos del país a scrapear. ejm: AR (Argentina), US (United States)
    - ott_operation: El tipo de operación a realizar. Cuando estamos desarrollando usamos 'testing', cuando
    se corre en el server usa 'scraping'
    Al insertar el comando en la terminal, se vería algo así:
    python main.py --o [ott_operation] --c [ott_site_country] [ott_platforms]

    Los atributos de la clase que use Datamanager siempre deben mantener el nombre, ya que Datamanager
    accede a ellos por nombre. Por ejemplo, si el script usa Datamanager, entonces self.titanScrapingEpisodios
    debe llamarse tal cual, no se le puede cambiar el nombre a self.titanScrapingEpisodes o algo así, porque
    Datamanager no lo va a reconocer y va a lanzar una excepción.
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
        self.payloads = list()
        self.ids_scrapeados = list()
        # si se usa selenium, el browser debe declararse como un atributo de clase para que nos sea más fácil cerrarlo con el destructor
        self.browser = webdriver.Firefox()
        self.url_all_movies = config_["all_movies_url"]
        self.all_dict_movies = list()
        self.test = ott_operation in ("testing", "return")
        self.list_movies_compare = list()
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
        self.open_link()
        self.call_payload_movie()
        for movies_url in self.all_dict_movies:
            url = "https://www.amctheatres.com/" + str(movies_url["url"])
            #res = requests.get("https://www.amctheatres.com/" + str(movies_url["url"]))
            res = self.handle_requets(url)
            if res.status_code != 200:
                continue
            soup = BeautifulSoup(res.text,"html.parser")
            exists = self.page_exist(soup)
            if exists == True:
                continue
            payload = self.build_payload_movies(soup,movies_url)
            Datamanager._checkDBandAppend(self,payload,self.ids_scrapeados,self.payloads)
        Datamanager._insertIntoDB(self,self.payloads,self.titanScraping)
        Upload(self._platform_code,self._created_at,testing = self.test)

    def open_link(self):
        self.browser.get(self.url_all_movies)
        pop_up_close = self.browser.find_elements_by_xpath("/html/body/div[2]/div[2]/div/div/div/div[2]/div/div/div[2]/div[1]/button")
        pop_up_close[0].click()
        self.browser.get(self.url_all_movies)

    def call_payload_movie(self):
        button_next_page = self.browser.find_element_by_xpath("/html/body/div[1]/div/main/div/div[2]/section/div/div[2]/div[2]/button[2]") #funcion is_enabled() devuelvo true or false, si hay false llego al final
        while button_next_page.is_enabled():
            button_next_page = self.browser.find_element_by_xpath("/html/body/div[1]/div/main/div/div[2]/section/div/div[2]/div[2]/button[2]")  
            divs_movies = self.browser.find_elements_by_class_name("Slide")
            soup = BeautifulSoup(self.browser.page_source,features="html.parser")
            divs_movies = soup.find_all("div", {"class" : "Slide"})
            for divs in divs_movies:
                movies_json_data = dict()
                a_movies = divs.find("a")
                movies_json_data["url"] = a_movies["href"]
                movies_json_data["title"] = self.get_title(divs)
                movies_json_data["image"] = self.get_image(divs)
                movies_json_data["time"] = self.get_time(divs)
                if movies_json_data["url"] not in self.all_dict_movies and movies_json_data["time"] != None:# si no tiene tiempo es una collection, bundle, etc
                    self.all_dict_movies.append(movies_json_data)
            time.sleep(2)
            if(button_next_page.is_enabled()):
                element = WebDriverWait(self.browser, 10).until(
                EC.presence_of_element_located((By.XPATH, "/html/body/div[1]/div/main/div/div[2]/section/div/div[2]/div[2]/button[2]"))).click()

    def build_payload_movies(self,soup,all_dict_movies):
        payload = Payload()
        payload.platform_code = self._platform_code
        payload.id = self.get_id(soup,all_dict_movies["url"])
        payload.title = all_dict_movies["title"] 
        payload.clean_title = _replace(all_dict_movies["title"])
        payload.duration = all_dict_movies["time"] if all_dict_movies["time"] <= 300 else None
        payload.deeplink_web = ("https://www.amctheatres.com" + str(all_dict_movies["url"]))
        payload.synopsis = self.get_synopsis(soup)
        payload.image = self.get_images_soup_data(soup,all_dict_movies)
        payload.genre = self.get_genre(soup)
        payload.rating = self.get_rating(soup)
        crews = self.get_all_crew(soup)
        payload.cast = crews["actores"]#self.get_cast(soup)# 
        payload.directors =crews["directores"]#self.get_director(soup)
        payload.Crew = crews["crew"]#self.get_crew(soup)#
        payload.packages = self.get_package(soup)
        payload.createdAt = self._created_at
        return payload.payload_movie()

    def handle_requets(self,url):
        res = requests.get(url)
        count_tries = 0
        while res.status_code != 200 and count_tries < 5:
            count_tries += 1
            res = requests.get(url)

        if res.status_code != 200 and count_tries >= 5:
            print("se supero la cantidad de intentos,error de estado de la solicitud = " + str(res.status_code))

        return res

    """def check_title(self,title,regex_keys,regex_signs):
        res = re.search(regex_keys, title)
        if res == None:
            res = re.search(regex_signs,title)
            if res == None:
                res = 0 # es una movie
            else:
                res = 1 # posible movie
        else:
            res = 2 # es coleccion
       
        return res """

    def get_id(self,soup,url):
        try:
            _id = soup.find("meta", {"name":"amc:title-id"})
            _id = _id.get("content")
        except:
            _id = url.split("-")
            _id = _id[-1]
        return _id or None

    def get_title(self, soup):
        title = soup.find("h3").text
        return  title or None

    def get_image(self, soup):
        image = soup.find("img").get("src")
        return image or None

    def get_time(self, soup):
        try:
            time = soup.find("span", {"class" : "js-runtimeConvert"}).text
            regex = re.compile(r'(\d)\s(hr)\s(\d\d)')
            hours = re.search(regex, time).group(1)
            minutes = re.search(regex, time).group(3)
            total = int(hours)*60 + int(minutes)
        except:
            total = None
        return total

    def get_synopsis(self,soup):
        synopsis = soup.find("p", {"itemprop" : "description"})
        synopsis = synopsis.text
        return synopsis or None

    def get_genre(self,soup):
        try:
            genre = soup.find("li", {"itemprop" : "genre"})
            genre = genre.text
        except:
            genre = None
        return genre

    def get_images_soup_data(self,soup,json_data):
        images = list()
        images.append(json_data["image"])
        images_url = soup.find_all("img", {"itemprop" : "image"})
        picture_source = soup.find("picture", {"class" : "Carousel-Slide-Image"})
        source = picture_source.find("source")
        images_url.append(source)
        for posters in images_url:
            posters = posters.get("srcset")
            posters = posters.split(" ")
            if posters[0] == "https://amc-theatres-res.cloudinary.com/image/upload/c_fill,f_auto,fl_lossy,g_auto,h_410,q_auto,w_240/v1/amc-cdn/static/images/hero/no-fi--lg.jpg":
                continue
            query = posters[0].split("/")
            query_replace = query[5]
            imagen = posters[0].replace(query_replace + "/", "")
            images.append(imagen)
        return images    

    def get_cast(self,soup):
        actores = list()
        crew = ["Producer","Director","Writer","Executive producer"]
        try:
            div_crew = soup.find("div",{"class" : "MovieCastCrewList"})
            crew_list = div_crew.find_all("h3")
            for actor in crew_list:
                if (actor.find("span", {"class" : "MovieCastCrewList-role"}).text not in crew):
                     actores.append(actor.find("span", {"itemprop" : "name"}).text)
        except:
            actores = None
        return actores

    def get_director(self,soup):
        director = list()
        try:
            div_crew = soup.find("div",{"class" : "MovieCastCrewList"})
            crew_list = div_crew.find_all("h3")
            for actor in crew_list:
                rol = actor.find("span", {"class" : "MovieCastCrewList-role"}).text
                if (rol == "Director"):
                    director.append(actor.find("span", {"itemprop" : "name"}).text)
        except:
            director = None
        return director

    def get_crew(self,soup):
        crew = list()
        try:
            div_crew = soup.find("div",{"class" : "MovieCastCrewList"})
            crew_list = div_crew.find_all("h3")
            for actor in crew_list:
                rol = actor.find("span", {"class" : "MovieCastCrewList-role"}).text
                if (rol != "Director" and rol != "Actor"):
                    name = actor.find("span", {"itemprop" : "name"}).text
                    crew_rest = {
                        "Role" : rol, 
                        "Name" : name
                    }
                    crew.append(crew_rest)
        except:
            crew = None
        return crew

    def get_all_crew(self,soup):
        crew_members = ["Producer","Director","Writer","Executive producer"]
        actores = list()
        directores = list()
        crew = list()
        crew_completa = dict()
        try:
            div_crew = soup.find("div",{"class" : "MovieCastCrewList"})
            crew_list = div_crew.find_all("h3")
            for crew_member in crew_list:
                rol = crew_member.find("span", {"class" : "MovieCastCrewList-role"}).text
                if(rol not in crew_members):
                        #Es actor
                    actores.append(crew_member.find("span", {"itemprop" : "name"}).text)
                elif(rol == "Director"):
                        #Director
                    directores.append(crew_member.find("span", {"itemprop" : "name"}).text)
                else:
                        #crew member
                    member = {"Role" : rol,"Name" : crew_member.find("span", {"itemprop" : "name"}).text}
                    crew.append(member)
            
            if len(actores) == 0:
                actores = None
            if len(directores) == 0:
                directores = None
            if len(crew) == 0:
                crew = None
            crew_completa["actores"] = actores
            crew_completa["directores"] = directores
            crew_completa["crew"] = crew
        except:
            crew_completa["actores"] = None
            crew_completa["directores"] = None
            crew_completa["crew"] = None
        return crew_completa
    
    def get_package(self,soup):
        packages = list()
        prices = dict()
        div = soup.find("div", {"class": "LoadingContainer-inline"})
        options = div.find_all("li", {"role":"option"})
        for option in options:
            quality = option.contents[0]
            price = option.contents[-1].replace("$", "")
            price_type = option["aria-labelledby"]
            if prices.get(quality):
                var = {"price" : price,"price Type" : price_type}
                prices[quality].append(var)
            else:
                prices[quality] = [{"price" : price,"price Type" : price_type}]

        for quality in prices:
            package = {
                "Type"      : "transaction-vod",
                "Currency"  : "USD",
                "Definition": quality
            }
            for offer in prices[quality]:
                if "vod-cta-Rent" in offer["price Type"]:
                    package["RentPrice"] = offer["price"]
                elif "vod-cta-Buy" in offer["price Type"]:
                    package["BuyPrice"] = offer["price"]
            if package not in packages:
                packages.append(package)

        return packages

    def page_exist(self,soup):
        try:
            error = soup.find("h1", {"class" : "headline-paused", "itemprop" : "name"}).text
            if error == "You’ve gone off script":
                error = True
            else:
                error = False
        except:
            error = False
        return error

    def get_rating(self, soup):
        rating = soup.find("span",{"itemprop" : "contentRating"}).text
        return rating or None