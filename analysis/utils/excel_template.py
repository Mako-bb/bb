import time
import pandas as pd
from .dataframe import ConsultsDB

class ExcelTemplate():
    """
    Esta clase permite exportar a excel una plataforma que se esté
    desarrollando desde la terminal.
    """
    @staticmethod
    def export_excel(platform_code, date):
        """Exportar ExcelTemplate para analizar plataforma. 
        """
        if not date:
            date = time.strftime("%Y-%m-%d")
        print(f"\nExportando excel-> {platform_code} {date}")

        # Obtener DataFrames de localhost:
        query = { "PlatformCode" : platform_code, "CreatedAt" : date }
        conexion_local = ConsultsDB()

        # Obtener DataFrames locales:
        df_p = conexion_local.find_mongo(query, collection='titanPreScraping')
        df = conexion_local.find_mongo(query, collection='titanScraping')
        df_episodes = conexion_local.find_mongo(query, collection='titanScrapingEpisodes')

        if df_p.empty and df.empty and df_episodes.empty:
            print(f"\n¡No se encontró {platform_code} al {date} en mongo local!")

        file_name = ("Analisis " + platform_code + " " + str(date) + ".xlsx" )

        import os
        try:
            # Creo Carpeta:
            os.mkdir("excel_exports")
        except FileExistsError:
            # Si ya existe excel_exports avanzo:
            pass
        del os

        with pd.ExcelWriter("excel_exports/" + file_name) as writer:
            if df_p.empty:
                pass
            else:
                df_p.to_excel(writer, sheet_name='titanPreScraping', encoding="utf-8", index=False)

            df.to_excel(writer, sheet_name='titanScraping', encoding="utf-8", index=False)
            df_episodes.to_excel(writer, sheet_name='titanScrapingEpisodes', encoding="utf-8", index=False)

        print("\n¡Excel Exportado!\tBuscar en la carpeta \"excel_exports\"\n")