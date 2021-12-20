import re
import unicodedata
import japanese_numbers as jn
import time
class Replace():
    """
    Recopilación de metodos para limpiar titulos de plataformas japonesas
    Se utlizan patrones encontrados en varias plataformas para encontrar 
    numeros de episodio y temporada y limpiar los titulos.

    Modo de uso basico en scripts:
    
        from handle.replace_kanji import Replace

    Luego, sin necesidad de instanciar la clase llamar al metodo, por ej:

        clean_title = Replace._replace_kanji(title)

    Las plataformas japonesas son la muerte, buena suerte
    """
    @staticmethod
    def _replace_kanji(title):
        """
        Limpia los titulos para evitar que aparezcan datos indeseados (Subtitulado/Doblado, Años, etc...)
        En caso de que este metodo no logre limpiar tu contenido, ir a la tarea: https://app.clickup.com/t/4v9pgy
        y comentar el caso para que sea añadido.
        Args:
            title (str): Titulo que quieras limpiar
        Returns:
            str: Titulo limpio
        """
        title = unicodedata.normalize('NFKC', title)

        regex_movie_sub = re.compile(r'(\(|【|\[)(吹替|字幕|字幕版|日本語吹替版|吹替版|.+・.+)(\)|】|\])')
        regex_year_sub = re.compile(r'\(\d{4}(年|版)?\)')
        regex_corchetes = re.compile(r'【.+?】')
        title = title.replace('PV','')
        title = regex_movie_sub.sub('',title)
        title = regex_corchetes.sub('',title)
        title = regex_year_sub.sub('',title)
        return title.strip()

    @staticmethod
    def _get_epi_number(title):
        """
        Obtener el numero de episodio a partir del titulo del contenido
        Se recopilaron las diferentes maneras en las que las plataformas japonesas traen los numeros de episodios
        En caso de no matchear con ninguno devuelve None
        Args:
            title (str): Titulo del contenido
        Returns:
            epi_number (int): numero del episodio
        """
        title = unicodedata.normalize('NFKC', title)
        title = Replace.formal_to_japanese(title)
        regex_epis_1 = re.compile(r'(\-)?(第|スカウト|人)(\d+|\D+)(番|殺|号|局|夜|話|章|回|波|羽|人目|骨|時限目|弾|集|霊)')
        regex_epis_2 = re.compile(r'(village(\.)?|vol\.|Target|ROGUE|その|track-|事変|SP|TURN|Line(\.)?|DAY|trap:|trip|STAGE(\.)?|File|Station|meets|#|刑事|Lesson|＃|PAGE\.|episode|episode-|episode_|ページ|Lv\.|ep|EP\.|ope\.|Mission|CASE|STORY|VOL|Track\.|PHASE-|Op\.\d+|予告)\s?\d+', re.IGNORECASE)
        regex_epis_3 = re.compile(r'シーズン\d+\s\d+')
        regex_epis_4 = re.compile(r'\d+(時間目|夜|話|章|回|波|羽|骨|時限目|限目)')
        clean_number = None
        if regex_epis_1.search(title): # r'第(\d+|\D+)(夜|話)'
            recieve = regex_epis_1.search(title).group()
            clean_number = re.findall(r'[0-9０-９]+', recieve)
            clean_number = clean_number[0] if clean_number else None
            if not clean_number:
                clean_number = jn.to_arabic_numbers(recieve)
                clean_number = clean_number[-1] if clean_number != () else None
        if regex_epis_2.search(title): # r'(#|＃|PAGE\.|episode|episode-)\s?\d+'
            recieve = regex_epis_2.search(title).group()
            clean_number = re.findall(r'[0-9０-９]+', recieve)
            clean_number = clean_number[0] if clean_number else None
        if regex_epis_3.search(title):
            recieve = regex_epis_3.search(title).group()
            clean_number = re.findall(r'[0-9０-９]+', recieve)
            clean_number = clean_number[-1] if clean_number else None
        if regex_epis_4.search(title):
            recieve = regex_epis_4.search(title).group()
            clean_number = re.findall(r'[0-9０-９]+', recieve)
            clean_number = clean_number[-1] if clean_number else None
        return int(clean_number) if clean_number else None

    @staticmethod
    def _get_serie_title(title):
        """
        Obtener el titulo de una serie para los casos que viene como "nombre de serie season X epi X, etc"
        Se recopilaron todos los casos que encontramos para episodios y temporadas
        Args:
            title (str): Titulo del contenido
        Returns:
            serie_title (str): Idealmente el nombre de la serie a la que corresponde ese contenido
        """
        title_ = unicodedata.normalize('NFKC', title)
        regex_epis_1 = re.compile(r'(\-)?(全|第|スカウト|人)(\d+|\D+)(番|殺|号|局|夜|話|章|回|波|羽|人目|骨|時限目|弾|集|霊)')
        regex_epis_2 = re.compile(r'(village(\.)?|vol\.|Target|ROGUE|その|track-|事変|SP|TURN|Line(\.)?|DAY|trap:|trip|STAGE(\.)?|File|Station|meets|#|刑事|Lesson|＃|PAGE\.|episode|episode-|episode_|ページ|Lv\.|ep|EP\.|ope\.|Mission|CASE|STORY|VOL|Track\.|PHASE-|Op\.\d+|予告)\s?\d+', re.IGNORECASE)
        regex_epis_3 = re.compile(r'シーズン\d+\s\d+')
        regex_epis_4 = re.compile(r'\d+(時間目|夜|話|章|回|波|羽|骨|時限目|限目)')
        regex_season_1 = re.compile(r'\d+(st|nd|rd|th)\s?(シーズン|Season)', re.IGNORECASE)
        regex_season_2 = re.compile(r'第\d+(シリーズ|シーズン)')
        regex_season_3 = re.compile(r'第(\d+|\D+)(期|番|週)') 
        regex_season_4 = re.compile(r'(season|シーズン| Part)(\.|\s)?\d+',re.IGNORECASE)
        title = regex_epis_1.sub('',title_).strip()
        title = regex_epis_2.sub('',title).strip()
        title = regex_epis_3.sub('',title).strip()
        title = regex_epis_4.sub('',title).strip()
        title = regex_season_1.sub('',title).strip()
        title = regex_season_2.sub('',title).strip()
        title = regex_season_3.sub('',title).strip()
        title = regex_season_4.sub('',title).strip()
        return title.strip() if title != '' else title_

    @staticmethod
    def _get_season_number(title):
        """
        Obtener el numero de temporada a partir del titulo del contenido
        Se recopilaron las diferentes maneras en las que las plataformas japonesas traen los numeros de temporadas
        En caso de no matchear con ninguno devuelve None
        Args:
            title (str): Titulo del contenido
        Returns:
            season_number (int): numero de temporada
        """
        title = unicodedata.normalize('NFKC', title)
        title = Replace.formal_to_japanese(title)
        regex_season_1 = re.compile(r'\d+(st|nd|rd|th)\s?(シーズン|Season)', re.IGNORECASE)
        regex_season_2 = re.compile(r'第\d+(シリーズ|シーズン)')
        regex_season_3 = re.compile(r'第(\d+|\D+)(期|番|週)') 
        regex_season_4 = re.compile(r'(season|シーズン| Part)(\.|\s)?\d+',re.IGNORECASE)
        if "season" in title.lower():
            if "first" in title.lower():
                clean_number = 1
                return int(clean_number)
            if "second" in  title:
                clean_number = 2
                return int(clean_number)
            if "third"in title:
                clean_number = 3
                return int(clean_number)
            if "fourth"in title:
                clean_number = 4
                return int(clean_number)
            if "fifth"in title:
                clean_number = 5
                return int(clean_number)
        if 'The Final Season' in title:
            clean_number = None
            return int(clean_number)
        if '前編・後編' in title:
            clean_number = None
            return int(clean_number)
        regex_search_1 = regex_season_1.search(title) # r'\d+(st|nd|rd|th)\s?(シーズン|Season)'
        if regex_search_1:
            recieve = regex_search_1.group()
            clean_number = re.findall(r'[0-9０-９]+', recieve)[0]
            return int(clean_number)

        regex_search_2 = regex_season_2.search(title) # r'第\d+(シリーズ|シーズン)'
        if regex_search_2:
            recieve = regex_search_2.group()
            clean_number = re.findall(r'[0-9０-９]+', recieve)[0]
            return int(clean_number)

        regex_search_3 = regex_season_3.search(title) # r'第(\d+|\D+)期'
        if regex_search_3:
            recieve = regex_search_3.group()
            clean_number = re.findall(r'[0-9０-９]+', recieve)
            clean_number = clean_number[0] if clean_number else jn.to_arabic_numbers(recieve)[0]
            return int(clean_number)

        regex_search_4 = regex_season_4.search(title) # r'season(\.|\s)?\d+'
        if regex_search_4:
            recieve = regex_search_4.group()
            if " Part" in title:
                clean_number = re.findall(r'[0-9０-９]+', recieve)[0]
                return int(clean_number)
            clean_number = re.findall(r'[0-9０-９]+', recieve)[0]
            return int(clean_number)

        return None

    @staticmethod
    def formal_to_japanese(title):
        # Fuente : https://es.wikipedia.org/wiki/N%C3%BAmeros_japoneses wikipedia <3

        title = title.replace('壱',"一")    # 1
        title = title.replace('弐',"二")    # 2
        title = title.replace('参',"三")    # 3
        title = title.replace('肆','四')    # 4
        title = title.replace('伍','五')    # 5
        title = title.replace('陸','六')    # 6
        title = title.replace('漆','七')    # 7
        title = title.replace('捌','八')    # 8
        title = title.replace('玖','九')    # 9
        title = title.replace('拾','十')    # 10
        title = title.replace('廿','二十')  # 20 
        title = title.replace('佰','百')    # 100 
        title = title.replace('仟','千')    # 1000
        title = title.replace('萬','万')    # 10000

        return title

    @staticmethod
    def _get_year(title):
        year = int(time.strftime('%Y')) + 1
        for year_ in range(1887,year):
            if str(year_) in title:
                return year_
        return None