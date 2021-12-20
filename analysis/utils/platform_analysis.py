from .dataframe import GetDataFrame
from .dataframe import ConsultsDB

class PlatformAnalysis():
    @staticmethod
    def compare_apiPresence(query, kaji=False):
        """Método para comparar un DataFrame local o de kaji
        con apiPresence.
        """
        host_name = 'localhost'
        platform_code = query.get('PlatformCode') # Validar PlatformCode
        if not platform_code:
            return 'No hay PlatformCode en la consulta'
        
        df = None
        if kaji:
            host_name = 'kaji'
            df = GetDataFrame.kaji(query)
        else:
            df = GetDataFrame.local(query)

        query_misato = {"PlatformCode": platform_code}

        # DF apiPresence.
        df_apiPresence = GetDataFrame.misato(query_misato, collection='apiPresence')

        print("##############################################\n")
        print(f"Comparando apiPresence con {host_name}:\n")

        print(" --- Match con Id ---")

        # No Match entre apiPresences vs local o kaji
        no_match = ~df_apiPresence["ContentId"].isin(df['Id']) # Tabla booleana.
        # DF de contenidos donde no hubo match.
        df_no_match = df_apiPresence[no_match]

        print(f"Total contenidos en apiPresence: {df_apiPresence.shape[0]}")
        print(f"Match con id: {(df_apiPresence.shape[0] - df_no_match.shape[0])}")
        print(f"No Match {df_no_match.shape[0]}\n")

        # De los que no matchearon...
        if df_no_match.shape[0]:
            no_match = ~df_no_match["Title"].isin(df['CleanTitle']) # Tabla booleana.
            # DF de contenidos donde no hubo match.
            df_no_match_title = df_no_match[no_match]

            print(" --- Match con Title ---")
            print(f"De los {df_no_match.shape[0]} contenidos que no matchearon con Id:")
            print(f"Match con Title: {(df_no_match.shape[0] - df_no_match_title.shape[0])}")
            print(f"No Match {df_no_match_title.shape[0]}\n")

            print(" --- Conclusiones ---")            
            print(f"De esos {df_no_match_title.shape[0]} contenidos que no matchearon...")

            status_list = list(df_no_match_title['Status'].unique())
            for status in status_list:
                match = df_no_match_title['Status'] == status
                df2 = df_no_match_title[match]
                print(f"{df2.shape[0]} ---> {status}")
        else:
            print("¡¡¡ Matcheó todo !!!")

    def compare_apiPresenceEpisodes(query, kaji=False):
        pass

    @staticmethod
    def export_all(query, kaji=False, file_name=None):
        conexion_local = None
        if kaji:
            # Exporto de kaji.
            conexion_local = ConsultsDB('kaji')
        else:
            # Exporto de localhost.
            conexion_local = ConsultsDB()  
        df1 = conexion_local.find_mongo(query, collection='titanScraping')
        df2 = conexion_local.find_mongo(query, collection='titanScrapingEpisodes')
        df3 = conexion_local.find_mongo(query, collection='titanPreScraping')

        print("")
        conexion_misato = ConsultsDB('misato')
        df4 = conexion_misato.find_mongo(query, collection='apiPresence')

        if not file_name:
            file_name = 'export_all.xlsx'
        else:
            file_name += '.xlsx'

        import os
        try:
            os.mkdir("excel_exports")
        except FileExistsError:
            pass
        del os
        import pandas as pd
        if df1.empty and df2.empty and df4.empty:
            print("No se encontró ninguna colección")
        else:
            with pd.ExcelWriter("excel_exports/" + file_name) as writer:
                # titanScraping
                if df1.empty:
                    print("No se encontró titanScraping")
                else:
                    df1.to_excel(writer, sheet_name='titanScraping', encoding="utf-8", index=False)
                # titanScrapingEpisodes
                if df2.empty:
                    print("No se encontró titanScrapingEpisodes")
                else:                
                    df2.to_excel(writer, sheet_name='titanScrapingEpisodes', encoding="utf-8", index=False)
                # apiPresence
                if df4.empty:
                    print("No se encontró apiPresence")
                else:
                    df4.to_excel(writer, sheet_name='apiPresence', encoding="utf-8", index=False)
                
                print("\n¡Excel exportado!")
    
    @staticmethod
    def check_all(query, kaji=False, file_name=None):
        """ TODO: Automatizar todo lo que se puede"""
        pass