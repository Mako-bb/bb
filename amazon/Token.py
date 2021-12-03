import requests
import re
from bs4 import BeautifulSoup
from amazon.Dictionary import Dictionary
from handle.datamanager import Datamanager
 

class Token:
    @staticmethod
    def get_from_html(url, headers):
        print(url)
        amazon_home = requests.get(url, headers=headers)
        soup = BeautifulSoup(amazon_home.content, 'lxml')
        diccionario = Dictionary.get_dictionary(soup, 'pagination')
        return diccionario['pagination']['token']

    @staticmethod
    def get_from_url(url): return re.search(r"ey.*&", url).group(0).split('&')[0]

    @classmethod
    def get_token_pv(cls, primevideo, url):
        soup = Datamanager._getSoupSelenium(primevideo, url, showURL=False)
        colecciones = soup.find_all('a', {'class': '_1NNx6V _3xQCHA tst-see-more'})
        for coleccion in colecciones:
            return cls.get_from_url(coleccion['href'])
