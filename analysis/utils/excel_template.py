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

        if df.empty and df_episodes.empty:
            print(f"\n¡No se encontró {platform_code} al {date} en mongo local!")

        # Obtener DataFrame de apiPresence:
        misato_query = { "PlatformCode" : platform_code }

        try:
            conexion_misato = ConsultsDB('misato')
            df_apiPresence = conexion_misato.find_mongo(misato_query, collection='apiPresence')
        except Exception as e:
            print(f"No esta bien la llave: {e}")

        # TODO: Estaría bueno validar el "file name" para la próxima.
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
            try:
                if not df_p.empty:
                    df_p.to_excel(writer, sheet_name='titanPreScraping', encoding="utf-8", index=False)
            except:
                print("No hay preScraping")

            df.to_excel(writer, sheet_name='titanScraping', encoding="utf-8", index=False)
            df_episodes.to_excel(writer, sheet_name='titanScrapingEpisodes', encoding="utf-8", index=False)

            try:
                if df_apiPresence.empty:
                    print("\nNo trajo apiPresence. El PlatformCode debe estar en apiPlatforms")
                else:
                    df_apiPresence.to_excel(writer, sheet_name='apiPresence', encoding="utf-8", index=False)
            except:
                pass

        print("\n¡Excel Exportado!\tBuscar en la carpeta \"excel_exports\"\n")