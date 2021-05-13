from .dataframe import GetDataFrame

class PayloadAnalysis():
    @staticmethod
    def first_check(df):
        """Intro al análisis.
        """
        print(f" ***** Keys del Payload ***** \n")
        list_columns = df.columns.values
        # Campos obligatorios en titanScraping:
        titanScraping = ['_id', 'PlatformCode', 'Id', 'Title',
            'OriginalTitle', 'CleanTitle','Type', 'ExternalIds',
            'Year', 'Duration', 'Deeplinks', 'Playback',
            'Synopsis', 'Image', 'Rating', 'Provider',
            'Genres', 'Cast', 'Directors', 'Availability', 'Download',
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
            duplicates_dict = duplicates.to_dict()
            for title, n in duplicates_dict.items():
                print(f"{title} -> {n} veces.")
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
            duplicates_dict = duplicates.to_dict()
            for title, n in duplicates_dict.items():
                print(f"{title} -> {n} veces.")
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
    def check_duration(df):
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
    def check_external_ids(df):
        print("EN DESARROLLO...") 
    @staticmethod
    def check_deeplinks(df):
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
        print("EN DESARROLLO...")
    @staticmethod
    def check_provider(df):
        print("EN DESARROLLO...")
    @staticmethod
    def check_genres(df):
        print("EN DESARROLLO...")
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
        print("EN DESARROLLO...")

class EpisodesAnalysis():
    @staticmethod
    def check_episodes(df):

        print(f" ***** Keys del Payload ***** \n")
        list_columns = df.columns.values
        # Campos obligatorios en titanScraping:
        titanScrapingEpisodes = ['_id', 'PlatformCode', 'Id', 'Title',
            'ParentTitle', 'ParentId', 'Season', 'Episode',
            'Year', 'Duration', 'Deeplinks', 'Playback',
            'Synopsis', 'Image', 'Rating', 'Provider', 'Genres',
            'Cast', 'Directors', 'Availability', 'Download',
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
    def check_episode(df):
        print("EN DESARROLLO...")

    @staticmethod
    def check_season(df):
        print("EN DESARROLLO...")

    @staticmethod
    def check_deeplinks(df):
        print("EN DESARROLLO...")

    @staticmethod
    def check_image(df):
        print("EN DESARROLLO...") 
    @staticmethod
    def check_synopsis(df):
        print("EN DESARROLLO...")         