# Netflix - Scraping Manuales

A continuación, detallo los pasos a seguir con las particularidades que deben verificarse para realizar un scraping a Netflix desde las plataformas que ejecutamos en Manuales.

  

Antes de iniciar:

  

1.  Validar que las colecciones tengan los index configurados:
    *   **titanPreScraping**:
        *   _\_id\__
        *   _Id_
        *   _PlatformCode_
        *   _CreatedAt_
        *   _ContentId_
    *   **titanScraping**:
        *   _\_id\__
        *   _Id_
        *   _PlatformCode_
        *   _CreatedAt_
    *   **titanScrapingEpisodes**:
        *   _\_id\__
        *   _Id_
        *   _PlatformCode_
        *   _CreatedAt_
        *   _ParentId_
2.  Eliminar el preScraping de netflix que tengan en sus locales

  

Para iniciar el Scraping:

  

1.  **_Ejecutar por única vez_** el siguiente comando, SIN conexión a ninguna VPN:
    *   _python_ [_netflix.py_](http://netflix.py) _--o pre-scraping --m unogs --c XX --p 50000_
    *   **XX:** iso code country
2.  Conectar al VPN del pais a scrapear, validar ingresando a [fast.com](http://fast.com) si el pais es correcto.
3.  PreScraping: Ejecutar en cada scraping conectado con VPN al país a scrapear:
    *   _python_ _netflix.py_ _--o pre-scraping --m unogs --c XX --p 50000 --i yes_
4.  Scraping: Ejecutar el siguiente comando con VPN al país a scrapear:
    *   _python_ _netflix.py_ _--o scraping --m no-login --c XX --d xxxx-xx-xx_
    *   **XX:** iso code country
    *   **XXXX-XX-XX:** date (Ej: 2021-06-24)
5.  Validar si la cantidad de títulos obtenidos en titanScraping (local) es similar a la ultima actualización en Misato, lo cual es posible verificar ingresando en la colección updateStats, ordenar por fecha (descending) y filtrar por el platformCode correspondiente. En caso de ser similar, realizar el upload; caso contrario repetir el paso 2 y 4, conectando a otro cliente VPN y reintentando el scraping hasta obtener la cantidad similar.
6.  Realizar el upload desde el repo de agentes, ya que el repo de Netflix no tiene esa funcionalidad.