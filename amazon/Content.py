import time
from bs4 import BeautifulSoup
from src.Token import Token
from src.Api import Api
from datamanager import Datamanager
from src.Dictionary import Dictionary
from src.globals import domain


class Content:
    @staticmethod
    def get_full_url(url): return f'{domain()}{url}'

    @staticmethod
    def get_amount_content(soup):
        dictionary = Dictionary.get_dictionary(soup, 'opts')
        return dictionary['opts']['totalItems']

    @classmethod
    def get_collections(cls, amazon, token, collection_type):
        if collection_type == 'channels':
            url_api = Api.api_channels(token)
        else:
            url_api = Api.api_collection(token)
        contenidos = Datamanager._getJSON(amazon, url_api, headers=amazon.headers)

        return contenidos['collections']

    @classmethod
    def get_channels(cls, amazon):
        channels = []
        url = "{}/gp/video/storefront/ref=atv_hm_hom_c_rf1gk1_15_smr?" \
              "contentType=subscription&contentId=default&benefitId=default#nav-top".format(domain())
        token = Token.get_from_html(url, headers=amazon.headers)
        collections = cls.get_collections(amazon, token, 'channels')
        for collection in collections:
            for item in collection['items']:
                url = "{}{}".format(domain(), item['link']['url'])
                channels.append(url)

        return channels

    @classmethod
    def itera_catalogo(cls, amazon, cantidad_contenido, token, lista_contenidos):
        for index in range(0, cantidad_contenido + 20):
            if index % 20 == 0:
                items = Api.request_api_catalog(amazon, token, index, cantidad_contenido)
                for item in items:
                    lista_contenidos.append(cls.get_full_url(item['href']))

    @classmethod
    def get_from_channels(cls, amazon, lista_contenidos):
        lista_canales = cls.get_channels(amazon)
        for canal in lista_canales:
            soup = Datamanager._getSoup(amazon, canal, headers=amazon.headers)
            colecciones = soup.find_all('a', {'class': '_1NNx6V _3xQCHA tst-see-more'})
            for coleccion in colecciones:
                url = cls.get_full_url(coleccion['href'])
                try:
                    token = Token.get_from_url(url)
                except AttributeError:
                    print("error al conseguir el token")
                    continue
                soup_collection = Datamanager._getSoup(amazon, url, headers=amazon.headers)
                cantidad_contenido = cls.get_amount_content(soup_collection)
                cls.itera_catalogo(amazon, cantidad_contenido, token, lista_contenidos)

        return lista_contenidos

    @staticmethod
    def get_url(collection): return collection['seeMoreLink']['url']

    @classmethod
    def get_from_catalog(cls, amazon, collection, lista_contenidos):
        url = cls.get_url(collection)
        token = Token.get_from_url(url)
        link = cls.get_full_url(url)
        soup = Datamanager._getSoupSelenium(amazon, link)
        cantidad_contenido = cls.get_amount_content(soup)
        cls.itera_catalogo(amazon, cantidad_contenido, token, lista_contenidos)

    @classmethod
    def get_from_home(cls, amazon, lista_contenidos, url):
        token = Token.get_from_html(url, headers=amazon.headers)
        collections = cls.get_collections(amazon, token, 'home')
        for collection in collections:
            if 'seeMoreLink' in collection:
                if 'queryToken' in cls.get_url(collection):
                    cls.get_from_catalog(amazon, collection, lista_contenidos)
        return lista_contenidos

    @classmethod
    def get_all_content(cls, amazon):
        lista_contenidos = []
        cls.get_from_channels(amazon, lista_contenidos)
        if not lista_contenidos:
            raise Exception("Hubo un cambio en el html. Revisar la funcion get_from_channels")
        for url in amazon.links:
            cls.get_from_home(amazon, lista_contenidos, url)
        return list(set(lista_contenidos))
    
    # India style
    @classmethod
    def get_all_content_pv(cls, amazon):
        lista_contenidos = []
        for url in amazon.links:
            amazon.driver.get(url)
            time.sleep(10)
            cls.go_to_bottom(amazon)
            soup = BeautifulSoup(amazon.driver.page_source, features="html.parser")
            ver_mas = soup.find_all('a', {'class': '_1NNx6V _3xQCHA tst-see-more'})
            for link in ver_mas:
                cls.scroll_collection(amazon, link, lista_contenidos)
        return list(set(lista_contenidos))

    @classmethod
    def get_collections_pv(cls, amazon, token, collection_type):
        url_api = Api.api_collection_pv(token)
        contenidos = Datamanager._getJSON(amazon, url_api, headers=amazon.headers)
        return contenidos['collections']

    @staticmethod
    def go_to_bottom(primevideo):
        primevideo.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(10)

    @classmethod
    def scroll_collection(cls, primevideo, url, lista_contenidos):
        soup = Datamanager._getSoupSelenium(primevideo, "https://primevideo.com" + url['href'])
        try:
            amount = cls.get_amount_content(soup)
        except TypeError:
            amount = 20
        for i in range(1, amount // 20):
            cls.go_to_bottom(primevideo)
        soup = BeautifulSoup(primevideo.driver.page_source, features="html.parser")
        contents = soup.find_all('div', {'class': '_1Opa2_ dvui-packshot av-grid-packshot'})
        for content in contents:
            lista_contenidos.append(f"{domain()}{content.a['href']}")
