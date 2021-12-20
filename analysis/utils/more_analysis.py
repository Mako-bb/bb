# from .dataframe import GetDataFrame
import pandas as pd
from IPython.display import display, Markdown, Latex
class MoreAnalysis():
    """La idea de esta clase es agregar métodos que puedan ser
    relevantes para el análisis de una plataforma.

    IMPORTANTE: Documentar lo que hace cada método.
    """
    @staticmethod
    def check_something(df):
        """Este método compara...

        Args:
            df (DataFrame): Recibe un DataFrame.
        """
        print("PRUEBA")
    
    @staticmethod
    def series(df):
        """
        Este método devuelve un dataframe de las series existentes en la plataforma
        Imprime ademas la cantidad de series

        Args:
            df(DataFrame): Recibe DataFrame.
        Returns:
            df_series: DataFrame de series
        """
        series = df['Type'] == 'serie'
        df_series = df[series]

        print(f'La plataforma tiene {len(df_series)} series')

        return df_series

    @staticmethod
    def movies(df):
        """
        Este método devuelve un dataframe de las movies existentes en la plataforma
        Imprime ademas la cantidad de movies

        Args:
            df(DataFrame): Recibe DataFrame.
        Returns:
            df_movies: DataFrame de movies
        """
        movies = df['Type'] == 'movie'
        df_movies = df[movies]

        print(f'La plataforma tiene {len(df_movies)} movies')

        return df_movies
    
    @staticmethod
    def check_dups(df):
        '''Metodo que recibe dataframe y hace
        un conteo de duplicados por title e id. Que falta? printear
        solamente el link en deeplinks'''
        campo = 'Id'
        campo2 = 'Title'
        id_not_null = df[df[campo].notnull()]
        title_not_null = df[df[campo2].notnull()]
        by_id = id_not_null[campo].duplicated().sum()
        by_title = title_not_null[campo2].duplicated().sum()
        pd.options.display.max_colwidth = 240
        print(f'--- REPETIDOS POR ID ({by_id}), REPETIDOS POR TITLE ({by_title}) ---')
        if by_id > 0 and by_title > 0:
            dup_titles = df[df[campo2].duplicated()]['Title'].tolist()
            dup_ids = df[df[campo].duplicated()]['Id'].tolist()
            print('\n --- ANALISIS IDS DUPLICADOS --- \n')
            for id_ in dup_ids:
                print(df[df['Id'] == id_][['Id', 'Title', 'Type', 'Deeplinks']])
            print('\n --- ANALISIS TITLE DUPLICADOS --- \n')
            for title in dup_titles:
                print(df[df['Title'] == title][['Id', 'Title', 'Type', 'Deeplinks']])
        elif by_title > 0 and by_id == 0:
            dup_titles = df[df[campo2].duplicated()]['Title'].tolist()
            print('\n --- ANALISIS TITLE DUPLICADOS --- \n')
            for title in dup_titles:
                print(df[df['Title'] == title][['Id', 'Title', 'Type', 'Deeplinks']])
        elif by_title == 0 and by_id > 0:
            dup_ids = df[df[campo].duplicated()]['Id'].tolist()
            print('\n --- ANALISIS IDS DUPLICADOS --- \n')
            for id_ in dup_ids:
                print(df[df['Id'] == id_][['Id', 'Title', 'Type', 'Deeplinks']])
        print('\n--- ANALISIS REPETIDOS FINALIZADO ---')

    @staticmethod
    def check_title(df, head=5):
        """Método para analizar Title.

        - Revisa Titles duplicados.

        Args:
            df (objetct): DataFrame de pandas.
            head(int): cantidad de registros que queremos que muestre 
        """
        campo = 'Title'
        print(f" ***** Análisis {campo} ***** ")

        print("\n ##### DUPLICADOS: ##### \n")
        duplicates = df[campo].value_counts().sort_values(ascending=False).head(head)

        if duplicates[0] > 1:
            duplicates_dict = duplicates.to_dict()
            for title, n in duplicates_dict.items():
                print(f"{title} -> {n} veces.")
        else:
            print("DUPLICADOS: NO HAY DUPLICADOS \n")
        print("\n ####################### \n")   

    @staticmethod
    def check_clean_title(df, *caracteres, head=5):
        """Método para analizar Title.

        - Revisa CleanTitles duplicados.
        - Revisa si hay caracteres especiales para mejorar la limpieza del título.

        Args:
            df (objetct): DataFrame de pandas.
            head(int): cantidad de registros que queremos que muestre
            *caracteres: caraceteres extra para buscar en clean_title
        Returns:
            title_dups(list): lista de titulos duplicados
        """
        campo = 'CleanTitle'
        print(f" ***** Análisis {campo} ***** ")

        print("\n ##### DUPLICADOS: ##### \n")
        duplicates = df[campo].value_counts().sort_values(ascending=False)
        duplicates_show = duplicates.head(head)

        titles_dup = []
        if duplicates[0] > 1:
            duplicates_dict = duplicates.to_dict()
            for title, n in duplicates_dict.items():
                if title in duplicates_show:
                    print(f"{title} -> {n} veces.")
                if n > 1:
                    titles_dup.append(title)
        else:
            print("DUPLICADOS: NO HAY DUPLICADOS \n")
        print("\n ####################### ")            

        # TODO: Mejorar este algoritmo, filtrar más cosas. 
        print("\n ##### CARACTERES: ##### \n Indica si hay \\n, \\t, \\r, paréntesis o cosas raris.\n")
        titles_dict = df[campo].to_dict()
        errors = 0
        caracteres_filtro = ['\n', '\t', '\r', '©', 'Ã', '£', '¥', 'â', "[", "\\", "]", "^", "_",
              "`",'【','】']
        caracteres = list(caracteres) if caracteres else None
        caracteres_filtro = caracteres_filtro + caracteres if caracteres else caracteres_filtro
        for caracter in caracteres_filtro:
            for key, value in titles_dict.items():
                if caracter in value:
                    print_caracter = caracter if '\\' not in caracter else '\\' + caracter
                    print(f"{key} -> {value.strip()} -> Tiene {print_caracter}")
                    errors += 1

            
        if not errors:
            print("CARACTERES: SIN ERRORES \n")
        print("\n ####################### \n")    
        
        return titles_dup if titles_dup != [] else None

    @staticmethod
    def check_deeplinks_images(df):
        campo = 'Deeplinks e Images'
        print(f" ***** Análisis {campo} ***** ")

        print("\n ##### MUESTRA ALEATORIA: ##### \nRevisar que los deeplinks funcionen correctamente\n")
        df_ok = df.loc[:,["Id","Title","Deeplinks","Image"]].sample(10)
        dic = df_ok.T.to_dict()
        for i in dic.values():
            print(f"Id {i['Id']}: {i['Deeplinks']['Web']}")
            for img in i['Image']:
                print(img)
            print('*'*30)
        print("\n ####################### \n")

    @staticmethod
    def check_synopsis(df, *caracteres):
        """Método para analizar Synopsis.

        - Revisa si hay caracteres especiales para mejorar la limpieza de la synopsis.

        Args:
            df (objetct): DataFrame de pandas.
            *caracteres: caraceteres extra para buscar en clean_title
        """
        campo = 'Synopsis'
        print(f" ***** Análisis {campo} ***** ")

        # TODO: Mejorar este algoritmo, filtrar más cosas. 
        print("\n ##### CARACTERES: ##### \n Indica si hay \\n, \\t, \\r\n")
        synopsis_dict = df[campo].to_dict()
        errors = 0
        caracteres_filtro = ['\n', '\t', '\r']
        caracteres = list(caracteres) if caracteres else None
        caracteres_filtro = caracteres_filtro + caracteres if caracteres else caracteres_filtro
        for caracter in caracteres_filtro:
            for key, value in synopsis_dict.items():
                if value and caracter in value:
                    print_caracter = caracter if '\\' not in caracter else '\\' + caracter
                    print(f"{key} -> {value.strip()} -> Tiene {print_caracter}")
                    errors += 1

            
        if not errors:
            print("CARACTERES: SIN ERRORES \n")
        print("\n ####################### \n")   

    @staticmethod
    def check_cast(df, *caracteres):
        """Método para analizar Cast.

        - Revisa si hay caracteres especiales para mejorar la limpieza del Cast.

        Args:
            df (objetct): DataFrame de pandas.
            *caracteres: caraceteres extra para buscar en clean_title
        """
        campo = 'Cast'
        print(f" ***** Análisis {campo} ***** ")

        # TODO: Mejorar este algoritmo, filtrar más cosas. 
        print("\n ##### CARACTERES: ##### \n Indica si hay \\n, \\t, \\r\n")
        cast_dict = df[campo].to_dict()
        errors = 0
        caracteres_filtro = ['\n', '\t', '\r']
        caracteres = list(caracteres) if caracteres else None
        caracteres_filtro = caracteres_filtro + caracteres if caracteres else caracteres_filtro
        for caracter in caracteres_filtro:
            for key, value in cast_dict.items():
                if value:
                    for actor in value:
                        if actor and caracter in actor:
                            print_caracter = caracter if '\\' not in caracter else '\\' + caracter
                            print(f"{key} -> {actor.strip()} -> Tiene {print_caracter}")
                            errors += 1

            
        if not errors:
            print("CARACTERES: SIN ERRORES \n")
        print("\n ####################### \n")
    
    @staticmethod
    def check_directors(df, *caracteres):
        """Método para analizar Directors.

        - Revisa si hay caracteres especiales para mejorar la limpieza del Directors.

        Args:
            df (objetct): DataFrame de pandas.
            *caracteres: caraceteres extra para buscar en clean_title
        """
        campo = 'Directors'
        print(f" ***** Análisis {campo} ***** ")

        # TODO: Mejorar este algoritmo, filtrar más cosas. 
        print("\n ##### CARACTERES: ##### \n Indica si hay \\n, \\t, \\r\n")
        cast_dict = df[campo].to_dict()
        errors = 0
        caracteres_filtro = ['\n', '\t', '\r']
        caracteres = list(caracteres) if caracteres else None
        caracteres_filtro = caracteres_filtro + caracteres if caracteres else caracteres_filtro
        for caracter in caracteres_filtro:
            for key, value in cast_dict.items():
                if value:
                    for director in value:
                        if director and caracter in director:
                            print_caracter = caracter if '\\' not in caracter else '\\' + caracter
                            print(f"{key} -> {director.strip()} -> Tiene {print_caracter}")
                            errors += 1
                            
    @staticmethod
    def check_crew(df, *caracteres):
        # TODO Extender analisis al campo Role
        """Método para analizar Crew.

        - Revisa si hay caracteres especiales para mejorar la limpieza de los nombres del Crew.

        Args:
            df (objetct): DataFrame de pandas.
            *caracteres: caraceteres extra para buscar en los nombres
        """
        campo = 'Crew'
        print(f" ***** Análisis {campo} ***** ")

        print("\n ##### CARACTERES: ##### \n Indica si hay \\n, \\t, \\r\n")
        crew_dict = df[campo].to_dict()
        #print(crew_dict)
        errors = 0
        caracteres_filtro = ['\n', '\t', '\r']
        caracteres = list(caracteres) if caracteres else None
        caracteres_filtro = caracteres_filtro + caracteres if caracteres else caracteres_filtro
        for caracter in caracteres_filtro:
            for key, value in crew_dict.items():
                if value:
                    for crewmember in value:
                        if crewmember['Name'] and caracter in crewmember['Name']:
                            print_caracter = caracter if '\\' not in caracter else '\\' + caracter
                            print(f"{key} -> {crewmember['Name'].strip()} -> Tiene {print_caracter}")
                            errors += 1

            
        if not errors:
            print("CARACTERES: SIN ERRORES \n")
        print("\n ####################### \n")

    @staticmethod
    def check_duplicates(df, field):
        """Método para chequear duplicados, en base al
            campo que recibe por parámetro (idealmente
            Id, Title o CleanTitle).

        Args:
            df (objetct): DataFrame de pandas.
            field (str): Campo a revisar
        """
        # Hola Soy Facu, Estoy re manija ya fue pongo todo como markdown, hay que testear fijate si se adaptan a lo que querías lo que añadi en comentarios.
        print(f" ***** Análisis {field} ***** ")
        # display(Markdown(f'***** Análisis {field} *****'))   
        print("\n ##### DUPLICADOS: ##### \n")
        # display(Markdown(f' ##### DUPLICADOS: #####'))   
        duplicates = df[field].value_counts().sort_values(ascending=False).head(5)

        if duplicates[0] > 1:
            duplicates_dict = duplicates.to_dict()
            for title, n in duplicates_dict.items():
                print(f"{title} -> {n} veces.")
        else:
            print("DUPLICADOS: NO HAY DUPLICADOS \n")
            # display(Markdown(f' DUPLICADOS: NO HAY DUPLICADOS'))
        print("\n ####################### \n")
        # display(Markdown(f' #######################'))
    @staticmethod
    def check_links(df, percentage=20):
        """
        Método para chequear un porcentaje deseado de las urls que figuren
        en el dataframe (deeplinks e imagenes).

        - Args:
            - df (object): DataFrame de pandas
            - percentage (int): porcentaje de contenidos a traer, por default 20%    
        """
        campo = 'Deeplinks e Images'
        print(f" ***** Análisis {campo} ***** ")
        sample = int(len(df) * (percentage / 100)  )

        print(f"\n ##### MUESTRA ALEATORIA DE {percentage}%: ##### \nRevisar que los deeplinks funcionen correctamente\n")
        df_ok = df.loc[:,["Id","Title","Deeplinks","Image"]].sample(sample)
        dic = df_ok.T.to_dict()
        for i in dic.values():
            print(f"Id {i['Id']}: {i['Deeplinks']['Web']}")
            for img in i['Image']:
                print(img)
            print('*'*30)
        print(f"\n ##### SE MUESTRAN {sample} LINKS EN TOTAL #####")
        print("\n ####################### \n")
    
    @staticmethod
    def momo_sampler(df,amount=10):
        # Momo Sampler es el ultimo disco de lo redo aguante lo redo vieja
        # https://www.youtube.com/watch?v=zLdu4Qnymyo&ab_channel=santiagoescobar
        """
        Metodo para traer ejemplos de Deeplinks e Imagenes en Markdown.
        - Args:
            - df (object): DataFrame de pandas
            - amount (int): Cantidad de Ejemplos
        """
        sample = df.sample(amount)
        for content in range(sample.shape[0]):
            #<img class="pre" src="'+sample['Image'].iloc[content][0]+r'" >'+sample['Title'].iloc[content]+'</a>')
            display(Markdown(' <a href="'+sample['Deeplinks'].iloc[content]['Web']+'" style="font-size:21px;">'
                             + sample['Title'].iloc[content]
                             + '</a>'
                             + '<img src="'
                             + sample['Image'].iloc[content][0]
                             + '" alt="Slide 5" style="height: 5%">'
                            ))
    def check_id(df):
        if df['Id'].nunique() == df.shape[0]:
            display(Markdown('Todos los Ids son unicos, vo si que so cra'))
        else:
            display(Markdown('Hay Ids que no son unicos, revisar lo que sucede.'))
    @staticmethod
    def check_genres(df):
        genre_list = []
        for content in df['Genres']:
            if content[0] not in genre_list:
                genre_list.append(content[0])
        display(Markdown('<h1 align="center"> Los generos son: </h1>'))
        for i in genre_list:
            display(Markdown('<h3 align="center">'+i+'</h2>'))
    @staticmethod
    def df_duplicados(column_name, df, value=None, list_=None):
        """
        Devuelve un Dataframe con todas las filas del valor de la columna
        que se indique.
        Args:
        - column_name: Columna del Dataframe. Ejemplo: 'Title'.
        - df: El Dataframe en cuestión.
        - value: Valor de la columna del Dataframe. Ejemplo: 'Pokémon'. Defaults None.
        - list_: Lista a recorrer. Defaults None.
        Return:
        df: Dataframe filtrado por value o lista.
        """
        if value and list_:
            print("Debe elegir solo un value o lista.")
        if value:
            duplicados = df[column_name] == value
            duplicados #Lista boleana
            get_duplicates = df[duplicados]
            get_duplicates = get_duplicates
            return get_duplicates
        elif list_:
            list_of_df = []
            for i in list_:
                duplicados = df[column_name] == i
                duplicados
                get_duplicates = df[duplicados]
                list_of_df.append(get_duplicates)
            return pd.concat(list_of_df)
        else:
            print("Debe elegir value o lista.")
    @staticmethod
    def duplicate_display(dups):
        """ Hace display de los duplicados, viendo su descripción, deeplink,titulo y duración. 
            Recibe como parametro el return de df_duplicados, siempre y cuando este reciba un titulo como valor. 
        Args:
            dups ([get_duplicates]): lo que retorna df_duplicados en caso de que uses un titulo como value. 
        """
        # Verguenza es robar y no traer nada a casa(?
        deeplink = dups['Deeplinks']
        title = dups['Title']
        Cleantitle = dups['CleanTitle']
        Type = dups['Type']
        image = dups['Image']
        year = dups['Year']
        duration = dups['Duration']
        synopsis = dups['Synopsis']

        for i in range(len(dups)):
            display(Markdown('<a style="font-size:25px;" href="'+ deeplink.iloc[i]['Web']+'">'+title.iloc[i]+'</a>'))
            display(Markdown('<p style="font-family:Aleo;color:#0c2243;font-size:25px">'+Cleantitle.iloc[i]+'</p>'))
            display(Markdown('<p style="font-family:Aleo;color:#0c2243;font-size:25px">'+Type.iloc[i]+'</p>'))
            display(Markdown('<p style="font-family:Aleo;color:#0c2243;font-size:25px">'+str(year.iloc[i])+'</p>'))
            display(Markdown('<p style="font-family:Aleo;color:#0c2243;font-size:25px">'+str(duration.iloc[i])+'</a>'))
            display(Markdown('<b style="color:#0c2243;font-size:25px;">'+str(synopsis.iloc[i])+'</a>'))
            display(Markdown('<img src="'
                            + image.iloc[i][0]
                            + '" alt="Slide 5" style="height:300px;width:300px;">'))
    @staticmethod
    def check_genres(df):
        """
        Devuelve un markdown con todos los generos que hay en el DF
        Args:
            df ([DataFrame]): Dataframe capo que no entende
        """
        genre_list = []
        for genre in df['Genres']:
            if genre[0] not in genre_list:
                genre_list.append(genre[0])
        display(Markdown('<h1  style="font-family:Gotham Bold;font-size:50px;color:#0c2243;"> Los generos son: </h1>'))
        display(Markdown('<div style="align:center;">'))
        for i in genre_list:
            display(Markdown('<h1 style="font-family:Aleo;color:#0c2243;margin-left:350px;margin-top:-10px;font-size:30px;">'+i+'</h2>'))
    @staticmethod
    def check_rating(df):
        """
        Devuelve un markdown con todos los ratings que hay en el DF
        Args:
            df ([DataFrame]): Dataframe capo que no entende
        """
        rating_list = []
        for rating in df['Rating']:
            if rating not in rating_list:
                rating_list.append(rating)
        display(Markdown('<h1  style="font-family:Gotham Bold;font-size:50px;color:#0c2243;"> Los tipos de Rating son: </h1>'))
        display(Markdown('<div style="align:center;">'))
        for i in rating_list:
            display(Markdown('<h1 style="font-family:Aleo;color:#0c2243;margin-left:350px;margin-top:-10px;font-size:30px;"> - '+i+'</h2>'))
    @staticmethod
    def check_packages(df):
        """Devuelve un markdown con todos los packages que haya en la plataforma.

        Args:
            df ([DataFrame]): Te lo tengo que volver a repetir? un dataframe noma flaquito 
        """
        package_list = []
        for package in df['Packages']:
            if package[0]['Type'] not in package_list:
                package_list.append(package[0]['Type'])
        display(Markdown('<h1  style="font-family:Gotham Bold;font-size:50px;color:#0c2243;"> Los packages son: </h1>'))
        display(Markdown('<div style="align:center;">'))
        for package in package_list:
            display(Markdown('<h1 style="font-family:Aleo;color:#0c2243;margin-left:350px;margin-top:-10px;font-size:30px;">'+package+'</h2>'))
    @staticmethod
    def check_rating(df):
        """
        Devuelve un markdown con todos los generos que hay en el DF
        Args:
            df ([DataFrame]): Dataframe capo que no entende
        """
        rating_list = []
        for rating in df['Rating']:
            if rating[0] not in rating_list:
                rating_list.append(rating[0])
        display(Markdown('<h1  style="font-family:Gotham Bold;font-size:50px;color:#0c2243;"> Los generos son: </h1>'))
        display(Markdown('<div style="align:center;">'))
        for i in rating_list:
            display(Markdown('<h1 style="font-family:Aleo;color:#0c2243;margin-left:350px;margin-top:-10px;font-size:30px;">'+i+'</h2>'))