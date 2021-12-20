from amazon.Data import Data
from amazon.Payload import Payload
from amazon.Dictionary import Dictionary
from amazon.Platform_dispatcher import PlatformDispatcher
import copy


class Pelicula(Data):
    @classmethod
    def insertar_peliculas(cls, amazon, soup, url,):
        dicionary = Data.get_data(soup)
        payload = cls.get_payload(dicionary, url, amazon._created_at)
        lista_payloads = cls.get_payloads(soup, payload)
        print(f'Agregada/s {len(lista_payloads)} pelicula/s')
        amazon.lista_contenidos.append(lista_payloads)

    @staticmethod
    def get_platforms_and_packages(soup):
        dict_common = Dictionary.get_dictionary(soup, 'state')
        return PlatformDispatcher.dispatch_movie(dict_common)

    @classmethod
    def get_payload(cls, dictionary, url, created_at):
        id_contenido = cls.get_id(url)
        title_id = dictionary['pageTitleId']
        detail = cls.get_header_detail(dictionary)[title_id]
        duration = cls.get_duration(detail)
        year = cls.get_year(detail)
        synopsis = cls.get_synopsis(detail)
        title = cls.get_title(detail)
        image = cls.get_images(detail)
        genre = cls.get_genre(detail)
        rating = cls.get_rating(detail)
        is_original = cls.is_original(detail)
        is_branded = cls.is_branded(detail)
        crew = detail['contributors']
        director = cls.get_director(crew)
        cast = cls.get_cast(crew)
        payload = Payload(title=title, id_=id_contenido, duration=duration, year=year, synopsis=synopsis,
                          image=image, genres=genre, rating=rating, directors=director, cast=cast, clean_title=title,
                          deeplink_web=url, is_original=is_original, is_branded=is_branded, createdAt=created_at)
        return payload.payload_movie()  # hasta aca llegan todos los datos basicos que se pueden sacar

    @classmethod
    def get_payloads(cls, soup, payload_pelicula):
        # obtiene todos los platformcodes y packages en un diccionario de diccionarios
        # y los itera con el payload de arriba
        lista_payloads = []
        platform_and_packages = cls.get_platforms_and_packages(soup)
        for platform in platform_and_packages.keys():
            payload = copy.deepcopy(payload_pelicula)
            payload['PlatformCode'] = cls.get_platform_clean(platform)
            payload['Packages'] = platform_and_packages[platform]
            payload['Provider'] = cls.get_provider(payload['PlatformCode'])
            payload['IsBranded'] = cls.is_prime(payload['PlatformCode'])
            lista_payloads.append(payload)
        return lista_payloads
