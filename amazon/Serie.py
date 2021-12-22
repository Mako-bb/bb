from amazon.Data import Data
from amazon.Payload import Payload
import re
import copy
import html
from handle.datamanager import Datamanager
from amazon.Platform_dispatcher import PlatformDispatcher
from amazon.Dictionary import Dictionary
from amazon.globals import domain


class Serie(Data):
    @classmethod
    def insertar_series(cls, amazon, data, contenido):
        payload_serie, episodes = cls.get_payloads_series(amazon, data, contenido)
        serie_plataformas, packages_platforms = PlatformDispatcher.get_serie_platforms(episodes)
        lista_payloads = cls.get_complete_series(payload_serie, episodes, serie_plataformas, packages_platforms)
        print(f'Agregada/s {len(lista_payloads)} series/s')
        amazon.lista_contenidos.append(lista_payloads)
        print(f'Agregado/s {len(episodes)} capitulos/s')
        amazon.lista_episodios.append(episodes)

    @staticmethod
    def get_season_number(dictionary): return dictionary['seasonNumber']

    @staticmethod
    def get_season_deeplink(dictionary, id_season):
        serie = list(dictionary['seasons'].keys())[0]
        for season in dictionary['seasons'][serie]:
            if season['seasonId'] == id_season:
                return f"{domain()}{season['seasonLink']}"

    @staticmethod
    def get_parent_title(dictionary):
        parent_title = dictionary['parentTitle']
        titulo = re.sub(r"-?\sSeason\s+$", '', parent_title).strip()
        return html.unescape(titulo)

    @staticmethod
    def get_episode_season(deeplink_temporada):
        try:
            season = re.search(r"\d{1,3}$", deeplink_temporada).group(0)
            return int(season)
        except AttributeError:
            link_nuevo = re.sub(r"\?.*$", '', deeplink_temporada)
            season = re.search(r"\d{1,3}$", link_nuevo).group(0)
            return int(season)

    @classmethod
    def get_payload_serie(cls, dictionary, url, created_at):
        id_serie = cls.get_id(url)
        title_id = dictionary['pageTitleId']
        detail = cls.get_header_detail(dictionary)[title_id]
        year = cls.get_year(detail)
        synopsis = cls.get_synopsis(detail)
        image = cls.get_images(detail)
        genre = cls.get_genre(detail)
        rating = cls.get_rating(detail)
        is_original = cls.is_original(detail)
        is_branded = cls.is_branded(detail)
        director = cls.get_director(detail)
        cast = cls.get_cast(detail)
        seasons = cls.get_data_seasons(dictionary)
        title = cls.get_parent_title(detail)
        payload = Payload(title=title, id_=id_serie, year=year, synopsis=synopsis, seasons=seasons,
                          clean_title=title, image=image, genres=genre, rating=rating, directors=director,
                          cast=cast, deeplink_web=url, is_original=is_original, is_branded=is_branded,
                          createdAt=created_at)
        return payload.payload_serie()

    @classmethod
    def get_data_seasons(cls, dictionary):
        data_seasons = []
        seasons = dictionary['detail']['detail']
        for season in seasons:
            id_season = season
            temporada = seasons[season]
            synopsis = cls.get_synopsis(temporada)
            title = cls.get_title(temporada) 
            number = Serie.get_season_number(temporada)
            deeplink = cls.get_season_deeplink(dictionary, id_season)
            image = cls.get_images(temporada)
            director = cls.get_director(temporada)
            cast = cls.get_cast(temporada)
            is_original = cls.is_original(temporada)
            year = cls.get_year(temporada)
            payload = Payload(id_=id_season, synopsis=synopsis, title=title, deeplink_web=deeplink, number=number,
                              image=image, directors=director, cast=cast, is_original=is_original, year=year)
            data_seasons.append(payload.payload_season())
        return data_seasons

    @classmethod
    def get_episode_number(cls, dictionary): return dictionary['episodeNumber']

    @classmethod
    def get_payload_episode(cls, payload, createdAt, amazon):
        lista_capitulos = []
        soups_temporadas= {}
        for temporada in payload['Seasons']:
            deeplink_temporada = temporada['Deeplink']
            soup = Datamanager._getSoupSelenium(amazon, deeplink_temporada)
            soups_temporadas[temporada['Id']] = soup
            episodes = cls.get_data(soup, tipo_contenido='episode')
            parent_id = payload['Id']
            parent_title = payload['Title']
            capitulos = episodes['detail']['detail']
            cant_ep = cls.payload_episode(capitulos, parent_id, parent_title, deeplink_temporada, lista_capitulos, createdAt)
            temporada['Episodes'] = cant_ep
        return lista_capitulos, soups_temporadas

    @classmethod
    def payload_episode(cls, capitulos, parent_id, parent_title, deeplink_temporada, lista_capitulos, createdAt):
        cant_ep = 0
        for capitulo in capitulos:
            episode = capitulos[capitulo]
            if episode['titleType'] == 'episode':
                id_contenido = capitulo
                year = cls.get_year(episode)
                synopsis = cls.get_synopsis(episode)
                genre = cls.get_genre(episode)
                rating = cls.get_rating(episode)
                is_original = cls.is_original(episode)
                episode_number = cls.get_episode_number(episode)
                season = cls.get_episode_season(deeplink_temporada)
                # parent_title = cls.get_parent_title(episode)
                title = cls.get_title(episode)
                duration = cls.get_duration(episode)
                director = cls.get_director(episode)
                cast = cls.get_cast(episode)
                payload = Payload(parent_title=parent_title, parent_id=parent_id, id_=id_contenido, title=title,
                                  episode=episode_number, season=season, year=year, duration=duration, rating=rating,
                                  deeplink_web=deeplink_temporada, synopsis=synopsis, is_original=is_original,
                                  directors=director, cast=cast,
                                  genres=genre, createdAt= createdAt)
                if episode_number > 0:
                    cant_ep += 1
                    lista_capitulos.append(Payload.payload_episode(payload))
        return cant_ep
 
    @classmethod
    def completar_season(cls, serie_copy, episodios):
        for season in serie_copy['Seasons']:
            season['Episodes'] = cls.cant_ep_season(episodios, season['Number'], serie_copy['PlatformCode'])

    @staticmethod
    def cant_ep_season(episodes, season_num, platform_code):
        cant = 0
        for episode in episodes:
            if episode['PlatformCode'] == platform_code and season_num == episode['Season']:
                cant += 1
        return cant

    @classmethod
    def get_complete_series(cls,serie, episodios, serie_plataformas, packages_platforms):
        series = []
        for plataforma in serie_plataformas:
            serie_copy = copy.deepcopy(serie)
            serie_copy['PlatformCode'] = cls.get_platform_clean(plataforma)
            serie_copy['Packages'] = packages_platforms[plataforma]
            serie_copy['IsBranded'] = cls.is_prime(serie_copy['PlatformCode'])
            serie_copy['Provider'] = cls.get_provider(serie_copy['PlatformCode'])
            cls.completar_season(serie_copy, episodios)
            serie_copy['Seasons'] = cls.clean_seasons(serie_copy['Seasons'])
            series.append(serie_copy)
        return series

    @staticmethod
    def clean_seasons(seasons):
        correct_season = []
        for season in seasons:
            if  season['Episodes'] > 0:
                correct_season.append(season)
        
        return correct_season if correct_season else None

    @classmethod
    def get_payloads_series(cls, amazon, data, contenido):
        payload_serie = cls.get_payload_serie(data, contenido, amazon._created_at)
        payload_episodes, soups_seasons = cls.get_payload_episode(payload_serie, amazon._created_at, amazon)
        platforms_and_packages = cls.get_platforms_and_packages(soups_seasons, amazon.dispatcher)
        episodes = cls.get_episodes_with_packages(payload_episodes, platforms_and_packages)
        return payload_serie, episodes
    
    @classmethod
    def get_platforms_and_packages(cls, soups, dispatcher):
        platforms = []
        for soup in soups:
            dict_serie = Dictionary.get_dictionary_serie(soups[soup], 'state')
            dict_common = Dictionary.get_dictionary(soups[soup], 'state')
            platformas = dispatcher.dispatch_episodes(dict_serie, dict_common)
            if platformas:
                platforms += platformas
        return platforms

    @classmethod
    def get_episodes_with_packages(cls, payloads, platforms):
        list_ = []
        for payload in payloads:
            platforms_code = cls.get_platforms_epi(payload['Id'], platforms)
            for platform in platforms_code:
                pay = copy.deepcopy(payload)
                pay['PlatformCode'] = cls.get_platform_clean(platform)
                pay['Packages'] = platforms_code[platform]
                pay['Provider'] = cls.get_provider(pay['PlatformCode'])
                list_.append(pay)
        return list_
    
    @staticmethod
    def get_platforms_epi(id_, platforms):
        for element in platforms:
            key = element.keys()
            if id_ in key:
                return element[id_]