from src.Dictionary import Dictionary
import html
import re
from src.globals import domain


class Data:
    @staticmethod
    def get_data(soup, tipo_contenido=None):
        data = Dictionary.get_info(soup, tipo_contenido=tipo_contenido)
        return data['props']['state']

    @staticmethod
    def get_header_detail(dictionary): return dictionary['detail']['headerDetail']

    @staticmethod
    def get_id(url): return re.search(r'[A-Z0-9]+(?=\/|\?)', url).group(0)

    @staticmethod
    def get_director(dictionary):
        try:
            directors = []
            for director in dictionary['contributors']['directors']:
                directors.append(director['name'])
        except KeyError:
            directors = None
        return directors

    @staticmethod
    def get_cast(dictionary):
        try:
            cast = []
            for actor in dictionary['contributors']['starringActors']:
                cast.append(actor['name'])
            for actor in dictionary['contributors']['supportingActors']:
                cast.append(actor['name'])
        except KeyError:
            cast = None
        return cast

    @staticmethod
    def get_duration(dictionary):
        try:
            return dictionary['duration'] // 60
        except KeyError:
            return None

    @staticmethod
    def get_year(dictionary):
        try:
            return dictionary['releaseYear']
        except KeyError:
            return None

    @staticmethod
    def get_synopsis(dictionary):
        try:
            synopsis = html.unescape(dictionary['synopsis'])
        except KeyError:
            synopsis = None
        return synopsis

    @staticmethod
    def get_title(dictionary): return html.unescape(dictionary['title'])

    @staticmethod
    def get_genre(dictionary):
        try:
            generos = []
            for genero in dictionary['genres']:
                generos.append(genero['text'])
        except KeyError:
            generos = []
        return generos

    @staticmethod
    def get_images(dictionary):
        images = []
        for image in dictionary['images'].values():
            if image != '':
                images.append(image)
        return images

    @staticmethod
    def is_original(dictionary):
        try:
            is_original = dictionary['studios'][0] == "Amazon Studios"
        except (IndexError, KeyError):
            is_original = None
        return is_original

    @staticmethod
    def is_branded(dictionary):
        try:
            return dictionary['isPrime']
        except KeyError:
            return None

    @staticmethod
    def get_rating(dictionary):
        try:
            return dictionary['ratingBadge']['displayText']
        except KeyError:
            return None

    @staticmethod
    def get_provider(platform_code):
        if "-" in platform_code:
            provider = re.sub(r"\w{2}\.amazon-", '', platform_code).capitalize()
            if provider != "Prime":
                return [provider]
        else:
            return None
    
    @staticmethod
    def is_prime(string):
        return "amazon-prime" in string

    @staticmethod
    def get_platform_clean(string): return html.unescape(string)
