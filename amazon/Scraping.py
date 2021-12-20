import itertools
import time
import os
from handle.mongo import mongo
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from handle.datamanager import Datamanager
from amazon.Pelicula import Pelicula
from amazon.Serie import Serie
from amazon.Data import Data


class Scraping:
    @staticmethod
    def get_driver():
        option = Options()
        option.add_argument('--headless')
        return webdriver.Firefox(options=option)

    @staticmethod
    def insert_conect_into_db(plataforma):
        lista_contenidos = list(itertools.chain.from_iterable(plataforma.lista_contenidos))
        print(f'Insertando {len(lista_contenidos)} series/peliculas')
        plataforma.mongo.insertMany(plataforma.titanScraping, lista_contenidos)
        plataforma.lista_contenidos.clear()

    @staticmethod
    def insert_episodes_into_db(plataforma):
        lista_episodios = list(itertools.chain.from_iterable(plataforma.lista_episodios))
        print(f'Insertando {len(lista_episodios)} episodios')
        plataforma.mongo.insertMany(plataforma.titanScrapingEpisodes, lista_episodios)
        plataforma.lista_episodios.clear()

    @staticmethod
    def generar_espera(tiempo):
        n = tiempo
        while n >= 0:
            print('\rTiempo para volver a scrapear: {} segundos'.format(n), end=' ')
            n -= 1
            time.sleep(1)
        print(' ')

    @classmethod
    def reiniciar_mongo(cls, plataforma):
        with open('/tmp/reset-mongo-db.lock', 'w') as f:
            pass
        cls.generar_espera(70)
        if os.path.exists('/tmp/reset-mongo-db.lock'):
            os.path.remove('/tmp/reset-mongo-db.lock')
        plataforma.mongo = mongo()

    @staticmethod
    def is_movie(dictionary):
        return not dictionary['detail']['detail']

    @staticmethod
    def guardar_log(contenido, exception):
        dia = time.strftime("%d/%m/%y")
        hora = time.strftime("%H:%M:%S")
        with open("logs.txt", mode='a') as file:
            file.writelines(
                f'{contenido}\n'
                f'{type(exception)}\n'
                f'{repr(exception)}'
                f'\n{dia} - {hora}\n')

    @classmethod
    def scrapear_contenido(cls, plataforma, contenido, iteraciones):
        soup = Datamanager._getSoupSelenium(plataforma, contenido)
        data = Data.get_data(soup)
        try:
            if cls.is_movie(data):
                Pelicula.insertar_peliculas(plataforma, soup, contenido)
            else:
                Serie.insertar_series(plataforma, data, contenido)
        except Exception as e:
            cls.guardar_log(contenido, e)
        if iteraciones % 250 == 0:
            cls.insert_conect_into_db(plataforma)
            cls.insert_episodes_into_db(plataforma)
            cls.reiniciar_mongo(plataforma)

    @classmethod
    def end_scraping(cls, plataforma):
        plataforma.driver.close()
        cls.insert_conect_into_db(plataforma)
        cls.insert_episodes_into_db(plataforma)
        cls.reiniciar_mongo(plataforma)
