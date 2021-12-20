from .dataframe import GetDataFrame

class PayloadAnalysis():
    @staticmethod
    def first_check(df):
        """Intro al análisis.
        TODO: Validar si faltan columnas, valida si no existe una columna
        llamda por ejemplo Crew, pero no valida si falta.
        """
        print(f" ***** Keys del Payload ***** \n")
        list_columns = df.columns.values
        # Campos obligatorios en titanScraping:
        titanScraping = ['_id', 'PlatformCode', 'Id', 'Title',
            'OriginalTitle', 'CleanTitle','Type', 'ExternalIds',
            'Year', 'Duration', 'Deeplinks', 'Playback',
            'Synopsis', 'Image', 'Rating', 'Provider',
            'Genres', 'Cast', 'Directors', 'Crew', 'Availability', 'Download',
            'IsOriginal', 'Seasons', 'IsBranded', 'IsAdult',
            'Packages', 'Country', 'Timestamp', 'CreatedAt'
        ]
        error = False
        for col in list_columns:
            if not col in  titanScraping:
                print(f"¡{col} no va!")
                error = True
        if not error:
            print("¡Trae todos los valores (keys) del Payload okay!")

    @staticmethod
    def check_title(df):
        """Método para analizar Title.

        - Revisa Titles duplicados.

        Args:
            df (objetct): DataFrame de pandas.
        """
        campo = 'Title'
        print(f" ***** Análisis {campo} ***** ")

        print("\n ##### DUPLICADOS: ##### \n")
        duplicates = df[campo].value_counts().sort_values(ascending=False).head(5)

        if duplicates[0] > 1:
            duplicated_titles = []
            duplicates_dict = duplicates.to_dict()
            for title, n in duplicates_dict.items():
                duplicated_titles.append(title)
                print(f"{title} -> {n} veces.")
            print('\nCHEQUEAR CONTENIDO!!!\n')
            for title in duplicated_titles:
                column = df[df[campo] == title]['Deeplinks']
                web_link = [col.get('Web') for col in column]
                print(f'{title}:\n', '\n'.join(web_link), '\n')
        else:
            print("DUPLICADOS: NO HAY DUPLICADOS \n")
        print("\n ####################### \n")            

    @staticmethod
    def check_clean_title(df):
        """Método para analizar Title.

        - Revisa CleanTitles duplicados.
        - Revisa si hay caracteres especiales para mejorar la limpieza del título.

        Args:
            df (objetct): DataFrame de pandas.
        """
        campo = 'CleanTitle'
        print(f" ***** Análisis {campo} ***** ")

        print("\n ##### DUPLICADOS: ##### \n")
        duplicates = df[campo].value_counts().sort_values(ascending=False).head(5)

        if duplicates[0] > 1:
            duplicated_titles = []
            duplicates_dict = duplicates.to_dict()
            for title, n in duplicates_dict.items():
                duplicated_titles.append(title)
                print(f"{title} -> {n} veces.")
            print('\nCHEQUEAR CONTENIDO!!!\n')
            for title in duplicated_titles:
                column = df[df[campo] == title]['Deeplinks']
                web_link = [col.get('Web') for col in column]
                print(f'{title}:\n', '\n'.join(web_link), '\n')
        else:
            print("DUPLICADOS: NO HAY DUPLICADOS \n")
        print("\n ####################### ")            

        # TODO: Mejorar este algoritmo, filtrar más cosas.
        print("\n ##### CARACTERES: ##### \n Indica si hay \\n, \\t, \\r, paréntesis o cosas raris.\n")
        titles_dict = df[campo].to_dict()
        errors = 0
        for key, value in titles_dict.items():
            if ('\n' in value) or ('\t' in value) or ('\r' in value):
                print(f"{key} -> {value.strip()}-> Tiene \\n o \\t")
                errors += 1
            elif ('(' in value) or (')' in value):
                print(f"{key} -> {value.strip()}-> Tiene ()")
                errors += 1
            elif ('[' in value) or (']' in value):
                print(f"{key} -> {value.strip()}-> Tiene []")
                errors += 1
            
        if not errors:
            print("CARACTERES: SIN ERRORES \n")
        print("\n ####################### \n")             

    @staticmethod
    def check_original_title(df):
        """Método para analizar OriginalTitles.

        - Revisa si trajo todos los OriginalTitles.

        Args:
            df (objetct): DataFrame de pandas.
        """
        campo = 'OriginalTitle'
        print(f" ***** Análisis {campo} ***** ")

        total_contenidos = int(df.shape[0])
        trajo = ~df[campo].isnull()
        total_trajo = int(df[trajo].shape[0])

        if not total_trajo:
            print(f"\nNo trajo {campo}, ¿Se podrán traer?")
        elif total_contenidos == total_trajo:
            print(f"\n¡OMG, trajo todos los {campo}!")
        else:
            difference = int(total_contenidos - total_trajo)
            print(f"\n¿¿¿Se podrán traer los {difference} {campo} faltantes???")

    @staticmethod
    def check_platform_code(df):
        pass
    @staticmethod
    def check_id(df):
        print("EN DESARROLLO...")

    @staticmethod
    def check_type(df):
        pass
    @staticmethod
    def check_year(df):
        """TODO: No valido si es un año válido. entre 1500 y el actual.
        """
        campo = 'Year'
        print(f" ***** Análisis {campo} ***** ")

        total_contenidos = int(df.shape[0])
        trajo = ~df[campo].isnull()
        total_trajo = int(df[trajo].shape[0])

        if not total_trajo:
            print(f"\nNo trajo {campo}, ¿Se podrán traer?")
        elif total_contenidos == total_trajo:
            print(f"\n¡OMG, trajo todos los {campo}!")
        else:
            difference = int(total_contenidos - total_trajo)
            print(f"\n¿¿¿Se podrán traer los {difference} {campo} faltantes???")

    @staticmethod
    def check_logic_year(df):
        '''Analiza si hay años que son mayores al actual, 
        o menores al 1930'''

        campo = 'Year'
        if df[campo].isnull().all() == True:
            print(f'No hay ningun dato en el campo {campo}.')
        else:
            print('ANALISIS LOGICA YEAR.')
            low_year = df[df[campo] < 1930]
            if low_year.empty:
                print('No hay Year muy antiguos.')
            else:
                print('ESTOS AÑOS SON MUY ANTIGUOS, CHEQUEAR EN WEB E IMDB!!!')
                for index, content in low_year.iterrows():
                    print(content['Title'], ' Año: ', content[campo], ' ', content['Deeplinks'].get('Web'))
                
            high_year = df[df[campo] > 2021]
            if high_year.empty:
                print('No hay Year mayores a 2021.')
            else:
                print('ESTOS AÑOS SON MAYORES A 2021!!! CHEQUEAR')
                for index, content in high_year.iterrows():
                    print(content['Title'], ' Año: ', content[campo], ' ', content['Deeplinks'].get('Web'))

    @staticmethod
    def check_duration(df):
        # TODO: Darle un poco más de amor.
        campo = 'Duration'
        print(f" ***** Análisis {campo} ***** ")

        total_contenidos = int(df.shape[0])
        trajo = ~df[campo].isnull()
        total_trajo = int(df[trajo].shape[0])

        if not total_trajo:
            print(f"\nNo trajo {campo}, ¿Se podrán traer?")
        elif total_contenidos == total_trajo:
            print(f"\n¡OMG, trajo todos los {campo}!")
        else:
            difference = int(total_contenidos - total_trajo)
            print(f"\n¿¿¿Se podrán traer los {difference} {campo} faltantes???")

        print("\nMayor duración:\n")
        print(df['Duration'].sort_values(ascending=False).head())

        print("\nMenor duración:\n")
        print(df['Duration'].sort_values(ascending=True).head())

        print("\nIMPORTANTE: Dudar de los contenidos que duran poco, pueden ser trailes.")
    
    @staticmethod
    def check_logic_duration(df):
        '''Analiza si las duraciones son muy cortas o largas para
        cierto contenido'''
        # TODO: Que avise si está todo ok al finalizar.

        print('ANALIZANDO DURACIÓN SERIES.')
        campo = 'Duration'
        series = df[df['Type'] == 'serie']
        for index, serie in series.iterrows():
            if serie[campo] < 5 or serie[campo] > 150:
                print('CHEQUEAR DURACIÓN ILOGICA SERIE, CHEQUEAR WEB E IMDB:\n', serie['Title'], 
                    ' Duracion: ', serie[campo], ' ', serie['Deeplinks'].get('Web'))
        print('ANALISIS FINALIZADO SERIES.\n')

        print('ANALIZANDO DURACIÓN MOVIES.')
        movies = df[df['Type'] == 'movie']
        for index, movie in movies.iterrows():
            if movie[campo] < 20 or movie[campo] > 300:
                print('CHEQUEAR DURACIÓN ILOGICA MOVIE, CHEQUEAR WEB E IMDB:\n', 
                    movie['Title'], ' Duracion: ', movie[campo], ' ', movie['Deeplinks'].get('Web'))
        print('ANALISIS FINALIZADO MOVIES.')

    @staticmethod
    def check_external_ids(df):
        print("EN DESARROLLO...") 
    @staticmethod
    def check_deeplinks(df):
        """TODO: Agregar columna con Title.
        """
        campo = 'Deeplinks'
        print(f" ***** Análisis {campo} ***** ")

        print("\n ##### MUESTRA ALEATORIA: ##### \nRevisar que los deeplinks funcionen correctamente\n")
        df_ok = df.loc[:,["Id","Title","Deeplinks"]].sample(10)
        dic = df_ok.T.to_dict()
        for i in dic.values():
            print(f"Id {i['Id']}: {i['Deeplinks']['Web']}")
        print("\n ####################### \n")

    @staticmethod
    def check_playback(df):
        campo = 'Playback'
        print(f" ***** Análisis {campo} ***** ")

        total_contenidos = int(df.shape[0])
        trajo = ~df[campo].isnull()
        total_trajo = int(df[trajo].shape[0])

        if not total_trajo:
            print(f"\nNo trajo {campo}, ¿Se podrán traer?")
        elif total_contenidos == total_trajo:
            print(f"\n¡OMG, trajo todos los {campo}!")
        else:
            difference = int(total_contenidos - total_trajo)
            print(f"\n¿¿¿Se podrán traer los {difference} {campo} faltantes???")

    @staticmethod
    def check_synopsis(df):
        campo = 'Synopsis'
        print(f" ***** Análisis {campo} ***** ")

        total_contenidos = int(df.shape[0])
        trajo = ~df[campo].isnull()
        total_trajo = int(df[trajo].shape[0])

        if not total_trajo:
            print(f"\nNo trajo {campo}, ¿Se podrán traer?")
        elif total_contenidos == total_trajo:
            print(f"\n¡OMG, trajo todos los {campo}!")
        else:
            difference = int(total_contenidos - total_trajo)
            print(f"\n¿¿¿Se podrán traer los {difference} {campo} faltantes???")

    @staticmethod
    def check_images(df):
        """
        TODO: A futuro el Jpg pasarlo a string y matchearlo con regex con Title.
        """
        campo = 'Image'
        print(f" ***** Análisis {campo} ***** ")

        total_contenidos = int(df.shape[0])
        trajo = ~df[campo].isnull()
        total_trajo = int(df[trajo].shape[0])

        if not total_trajo:
            print(f"\nNo trajo {campo}, ¿Se podrán traer?")
        elif total_contenidos == total_trajo:
            print(f"\n¡OMG, trajo todos los {campo}!")
        else:
            difference = int(total_contenidos - total_trajo)
            print(f"\n¿¿¿Se podrán traer los {difference} {campo} faltantes???")

    @staticmethod
    def check_rating(df):
        """
        TODO: Validar que efectivamente sean ratings y que por ejemplo,
        no sea un year o un actor.
        """
        # df['Rating'].unique()
        print("EN DESARROLLO...")
    @staticmethod
    def check_provider(df):
        print("EN DESARROLLO...")
    @staticmethod
    def check_genres(df):
        '''En desarrollo'''
        campo = 'Genres'
        if df[campo].isnull().all() == True:
            print(f'No hay ningun dato en el campo {campo}, verificar si se pueden traer.')
        else:
            genres = df[campo].explode().unique()
            clean_genres = ', '.join(genres)
            print('Tenemos: ', clean_genres, '.', '\n GENRES SON LOGICOS???')

    @staticmethod
    def check_cast(df):
        print("EN DESARROLLO...")
    @staticmethod
    def check_directors(df):
        print("EN DESARROLLO...")
    @staticmethod
    def check_availability(df):
        print("EN DESARROLLO...")
    @staticmethod
    def check_download(df):
        print("EN DESARROLLO...")
    @staticmethod
    def check_is_original(df):
        print("EN DESARROLLO...")
    @staticmethod
    def check_is_branded(df):
        print("EN DESARROLLO...")
    @staticmethod
    def check_is_adult(df):
        print("EN DESARROLLO...")
    @staticmethod
    def check_packages(df):
        print("EN DESARROLLO...")
    @staticmethod
    def check_country(df):
        print("EN DESARROLLO...")
    @staticmethod
    def check_seasons(df):
        '''Metodo que sirve unicamente
        para visualizar cada season, numero y capitulos, y comparar
        con la pagina'''

        print('--- METODO UNICAMENTE PARA CHEQUEAR A OJO LAS SEASONS --- \n')
        campo = 'Seasons'
        if campo not in df.columns or df[campo].isnull().all() == True:
            print('--- CAMPO SEASONS SIN DATA O INEXISTENTE ---')
        else:
            seasons = df[df[campo].notnull()]
            seasons.reset_index(inplace=True)
            print('SEASON TITLE --- DEEPLINK ---          NUMBER SEASON - EPISODE')
            for i in range(len(seasons)):
                for data in seasons[campo][i]:
                    print(data['Title'], data['Deeplink'], data['Number'], data['Episodes'])

class EpisodesAnalysis():
    """
    Hacer lo mismo que Payload análisis, pero que sirva para validar
    la metadata de Episodios.
    """
    @staticmethod
    def check_episodes(df):

        print(f" ***** Keys del Payload ***** \n")
        list_columns = df.columns.values
        # Campos obligatorios en titanScraping:
        titanScrapingEpisodes = ['_id', 'PlatformCode', 'Id', 'Title',
            'ParentTitle', 'ParentId', 'Season', 'Episode',
            'Year', 'Duration', 'Deeplinks', 'Playback',
            'Synopsis', 'Image', 'Rating', 'Provider', 'Genres',
            'Cast', 'Crew', 'Directors', 'Availability', 'Download',
            'IsOriginal', 'IsAdult', 'Packages', 'Country',
            'Timestamp', 'CreatedAt'
        ]
        error = False
        for col in list_columns:
            if not col in  titanScrapingEpisodes:
                print(f"¡{col} no va!")
                error = True
        if not error:
            print("¡Trae todos los valores (keys) del Payload okay!")

    @staticmethod
    def check_id(df):
        print("EN DESARROLLO...")

    @staticmethod
    def check_title(df):
        print("EN DESARROLLO...")

    @staticmethod
    def check_series(df):
        print("EN DESARROLLO...")

    @staticmethod 
    def check_logic_year(df):
        '''Analiza si hay años que son mayores al actual, 
        o menores al 1930'''

        campo = 'Year'
        if df[campo].isnull().all() == True:
            print(f'No hay ningun dato en el campo {campo}.')
        else:
            print('ANALISIS LOGICA YEAR.')
            low_year = df[df[campo] < 1930]
            if low_year.empty:
                print('No hay Year muy antiguos.')
            else:
                print('ESTOS AÑOS SON MUY ANTIGUOS, CHEQUEAR EN WEB E IMDB!!!')
                for index, content in low_year.iterrows():
                    print(content['Title'], ' Año: ', content[campo], ' ', content['Deeplinks'].get('Web'))
                
            high_year = df[df[campo] > 2021]
            if high_year.empty:
                print('No hay Year mayores a 2021.')
            else:
                print('ESTOS AÑOS SON MAYORES A 2021!!! CHEQUEAR')
                for index, content in high_year.iterrows():
                    print(content['Title'], ' Año: ', content[campo], ' ', content['Deeplinks'].get('Web'))
    @staticmethod
    def check_episode(df):
        campo = 'Episode'
        print(f" ***** Análisis {campo} ***** ")

        total_contenidos = int(df.shape[0])
        trajo = ~df[campo].isnull()
        total_trajo = int(df[trajo].shape[0])

        if not total_trajo:
            print(f"\nNo trajo {campo}, ¿Se podrán traer?")
        elif total_contenidos == total_trajo:
            print(f"\n¡OMG, trajo todos los {campo}!")
        else:
            difference = int(total_contenidos - total_trajo)
            print(f"\n¿¿¿Se podrán traer los {difference} {campo} faltantes???")

    @staticmethod
    def check_logic_episode(df):
        '''Chequea que los numero de episodes sean logicos'''

        print('ANALIZANDO NUMERO EPISODES.')
        campo = 'Episode'
        epis = df[['Title', 'Episode', 'Season', 'Deeplinks']]
        for index, epi in epis.iterrows():
            if epi[campo] <= 0:
                print('CHEQUEAR EPISODIO <= a CERO:\n', epi['Title'], 
                    ' Numero: ', epi[campo], 'Season: ', epi['Season'], ' ', 
                    epi['Deeplinks'].get('Web'))
            elif epi[campo] > 200:
                print('CHEQUEAR EPISODIO MUY GRANDE:\n', epi['Title'], 
                    ' Numero: ', epi[campo], 'Season: ', epi['Season'], ' ', 
                    epi['Deeplinks'].get('Web'))
        print('ANALISIS FINALIZADO EPISODES.\n')

    @staticmethod
    def check_season(df):
        campo = 'Season'
        print(f" ***** Análisis {campo} ***** ")

        total_contenidos = int(df.shape[0])
        trajo = ~df[campo].isnull()
        total_trajo = int(df[trajo].shape[0])

        if not total_trajo:
            print(f"\nNo trajo {campo}, ¿Se podrán traer?")
        elif total_contenidos == total_trajo:
            print(f"\n¡OMG, trajo todos los {campo}!")
        else:
            difference = int(total_contenidos - total_trajo)
            print(f"\n¿¿¿Se podrán traer los {difference} {campo} faltantes???")

    @staticmethod
    def check_logic_season(df):
        '''Chequea que los numero de season sean logicos'''

        print('ANALIZANDO NUMERO SEASONS.')
        campo = 'Season'
        epis = df[['Title', 'Season', 'Deeplinks']]
        for index, epi in epis.iterrows():
            if epi[campo] <= 0:
                print('CHEQUEAR SEASON <= a CERO:\n', epi['Title'], 
                    ' Numero: ', epi[campo], 'Season: ', epi['Season'], ' ', 
                    epi['Deeplinks'].get('Web'))
            elif epi[campo] > 80:
                print('CHEQUEAR SEASON MUY GRANDE:\n', epi['Title'], 
                    ' Numero: ', epi[campo], 'Season: ', epi['Season'], ' ', 
                    epi['Deeplinks'].get('Web'))
        print('ANALISIS FINALIZADO SEASONS.\n')

    @staticmethod
    def check_logic_duration(df):
        '''Analiza si las duraciones son muy cortas o largas para
        cierto contenido'''
        # TODO: Que avise si está todo ok al finalizar.

        print('ANALIZANDO DURACIÓN EPISODIOS.')
        campo = 'Duration'
        if df[campo].isnull().all() == True:
            print(f'No hay ningun dato en el campo {campo}.')
        else:
            epis = df[df[campo].notnull()]
            for index, epi in epis.iterrows():
                if epi[campo] < 5 or epi[campo] > 120:
                    print('CHEQUEAR DURACIÓN ILOGICA EPISODIO, CHEQUEAR WEB E IMDB:\n', epi['Title'], 
                        ' Duracion: ', epi[campo], ' Season: ', epi['Season'], ' Episodio: ', epi['Episode'], 
                            epi['Deeplinks'].get('Web'))
            print('\nANALISIS FINALIZADO EPISODIOS.\n')

    @staticmethod
    def check_deeplinks(df):
        print("EN DESARROLLO...")

    @staticmethod
    def check_image(df):
        print("EN DESARROLLO...") 
    @staticmethod
    def check_synopsis(df):
        print("EN DESARROLLO...")
    @staticmethod
    def check_genres(df):
        campo = 'Genres'
        print(df[campo].explode().unique(), '\n GENRES SON LOGICOS???')         