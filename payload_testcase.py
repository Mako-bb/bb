from handle.payload import Payload
from handle.replace import _replace

"""
Este archivo sirve para demostrar los campos existentes de la clase payload
para películas, series, episodios y temporadas.

Para la descripción detallada de cada campo, revisar el "Payload.pdf" en la raíz del repositorio
"""

def build_payload_movie(content=dict()):

    payload = Payload()

    payload.platform_code = "mi.plataforma"  # (str) directamente lo asignamos al self._platform_code que definimos en el init de la clase de la plataforma 
    payload.id = "12345" # (str) debe ser único para este contenido de esta plataforma
    payload.title = "Mi película" # (str)
    payload.original_title = "My movie" # (str)
    payload.clean_title = _replace(payload.title) # (str)
    payload.year = 2021 # (int)
    payload.duration = 60 # (int) en minutos
    payload.deeplink_web = "https://bb.vision/mi-pelicula-12345" # (str)
    payload.playback = True # (bool)
    payload.synopsis = "Mi sinopsis" # (str)
    payload.image = [ # lista de strings.
        "https://bb.vision/img/imagen1.jpg",  # (str)
        "https://bb.vision/img/imagen2.jpg"  # (str)
    ]
    payload.rating = "18+" # (str)
    payload.provider = [ # lista de strings
        "Netflix", # (str)
        "NBC" # (str)
    ]
    payload.external_ids = [ # lista de diccionarios. Muy raro, por lo general no está y se deja en None
        {
            "Provider": "IMDb", # (str)
            "Id": "tt12345678" # (str)
        },
        {
            "Provider": "tvdb", # (str)
            "Id": "12345678" # (str)
        }
    ]
    payload.genres = [ # lista de strings
        "Acción", # (str)
        "Suspenso" # (str)
    ]
    payload.crew = [ # lista de diccionarios
        {
            "Role":"writer", # (str)
            "Name":"Charlie Kaufman" # (str)
        },
        {
            "Role":"compositor", # (str)
            "Name":"Jon Brion" # (str)
        }
    ]
    payload.cast = [ # lista de strings
        "Bill Murray", # (str)
        "Scarlett Johansson" # (str)
    ]
    payload.directors = [ # lista de strings
        "Sofia Coppola", # (str)
        "Gaspar Noé" # (str)
    ]
    payload.availability = "2022-08-20" # (str)
    payload.download = True # (bool)
    payload.is_original = True # (bool)
    payload.is_adult = True # (bool)
    payload.is_branded = True # (bool)
    payload.packages = [ # lista de diccionarios. Para más info sobre los posibles packages, chequear el pdf
        {
            "Type":"subscription-vod" # (str)
        },
        {
            "Type":"transaction-vod", # (str)
            "BuyPrice":9.99, # (float)
            "RentPrice":4.99, # (float)
            "Definition":"HD" # (str)
        }
    ]
    payload.country = [ # lista de strings
        "USA", # (str)
        "Argentina" # (str)
    ]
    payload.createdAt = "2021-08-20" # (str) directamente lo asignamos al self._created_at que definimos en el init de la clase de la plataforma
    
    #payload_movie() construye un payload en formato diccionario usando los atributos anteriormente seteados
    return payload.payload_movie()

def build_payload_serie(content=dict()):

    seasons = list()
    episodes = list()
    for season in content.get("seasons", []):

        for episode in season.get("episodes", []):
            episodes.append(build_payload_episode(episode))

        seasons.append(build_payload_season(season))

    payload = Payload()

    payload.platform_code = "mi.plataforma"  # (str) directamente lo asignamos al self._platform_code que definimos en el init de la clase de la plataforma 
    payload.id = "12345" # (str) debe ser único para este contenido de esta plataforma
    payload.seasons = seasons
    payload.title = "Mi serie" # (str)
    payload.original_title = "My series" # (str)
    payload.clean_title = _replace(payload.title) # (str)
    payload.year = 2021 # (int)
    payload.deeplink_web = "https://bb.vision/mi-serie-12345" # (str)
    payload.playback = True # (bool)
    payload.synopsis = "Mi sinopsis" # (str)
    payload.image = [ # lista de strings.
        "https://bb.vision/img/imagen1.jpg",  # (str)
        "https://bb.vision/img/imagen2.jpg"  # (str)
    ]
    payload.rating = "18+" # (str)
    payload.provider = [ # lista de strings
        "Netflix", # (str)
        "NBC" # (str)
    ]
    payload.external_ids = [ # lista de diccionarios. Muy raro, por lo general no está y se deja en None
        {
            "Provider": "IMDb", # (str)
            "Id": "tt12345678" # (str)
        },
        {
            "Provider": "tvdb", # (str)
            "Id": "12345678" # (str)
        }
    ]
    payload.genres = [ # lista de strings
        "Acción", # (str)
        "Suspenso" # (str)
    ]
    payload.crew = [ # lista de diccionarios
        {
            "Role":"writer", # (str)
            "Name":"Charlie Kaufman" # (str)
        },
        {
            "Role":"compositor", # (str)
            "Name":"Jon Brion" # (str)
        }
    ]
    payload.cast = [ # lista de strings
        "Bill Murray", # (str)
        "Scarlett Johansson" # (str)
    ]
    payload.directors = [ # lista de strings
        "Sofia Coppola", # (str)
        "Gaspar Noé" # (str)
    ]
    payload.availability = "2022-08-20" # (str)
    payload.download = True # (bool)
    payload.is_original = True # (bool)
    payload.is_adult = True # (bool)
    payload.is_branded = True # (bool)
    payload.packages = [ # lista de diccionarios. Para más info sobre los posibles packages, chequear el pdf
        {
            "Type":"subscription-vod" # (str)
        },
        {
            "Type":"transaction-vod", # (str)
            "BuyPrice":9.99, # (float)
            "RentPrice":4.99, # (float)
            "Definition":"HD" # (str)
        }
    ]
    payload.country = [ # lista de strings
        "USA", # (str)
        "Argentina" # (str)
    ]
    payload.createdAt = "2021-08-20" # (str) directamente lo asignamos al self._created_at que definimos en el init de la clase de la plataforma
    
    #payload_serie() construye un payload en formato diccionario usando los atributos anteriormente seteados
    return payload.payload_serie()

def build_payload_season(season):
    
    payload = Payload()
    payload.id = "S123"
    payload.synopsis = "La sinopsis de una temporada"
    payload.title = "Mi serie Temporada X"
    payload.deeplink_web = "https://bb.vision/mi-serie-12345/season-1"
    payload.number = 1
    payload.image = []
    payload.directors = []
    payload.cast = []
    payload.episodes = 24
    payload.is_original = True
    payload.year = 2021

    #payload_season() construye un payload en formato diccionario usando los atributos anteriormente seteados
    return payload.payload_season()

def build_payload_episode(episode):

    payload = Payload()

    payload.platform_code = "mi.plataforma"  # (str) directamente lo asignamos al self._platform_code que definimos en el init de la clase de la plataforma 
    payload.parent_id = "12345" # (str) el id de la serie al que pertenece
    payload.parent_title = "Mi serie" # (str) el título de la serie al que pertenece
    payload.id = "54321" # (str) debe ser único para este contenido de esta plataforma
    payload.title = "Mi episodio" # (str)
    payload.original_title = "My episode" # (str)
    payload.season = 1 # (int) el número de la temporada a la que pertenece
    payload.number = 1 # (int) el número de episodio
    payload.year = 2021 # (int)
    payload.deeplink_web = "https://bb.vision/mi-serie-12345/temporada-1/mi-episodio-54321" # (str)
    payload.playback = True # (bool)
    payload.synopsis = "Mi sinopsis" # (str)
    payload.image = [ # lista de strings.
        "https://bb.vision/img/imagen1.jpg",  # (str)
        "https://bb.vision/img/imagen2.jpg"  # (str)
    ]
    payload.rating = "18+" # (str)
    payload.provider = [ # lista de strings
        "Netflix", # (str)
        "NBC" # (str)
    ]
    payload.genres = [ # lista de strings
        "Acción", # (str)
        "Suspenso" # (str)
    ]
    payload.crew = [ # lista de diccionarios
        {
            "Role":"writer", # (str)
            "Name":"Charlie Kaufman" # (str)
        },
        {
            "Role":"compositor", # (str)
            "Name":"Jon Brion" # (str)
        }
    ]
    payload.cast = [ # lista de strings
        "Bill Murray", # (str)
        "Scarlett Johansson" # (str)
    ]
    payload.directors = [ # lista de strings
        "Sofia Coppola", # (str)
        "Gaspar Noé" # (str)
    ]
    payload.availability = "2022-08-20" # (str)
    payload.download = True # (bool)
    payload.is_original = True # (bool)
    payload.is_adult = True # (bool)
    payload.is_branded = True # (bool)
    payload.packages = [ # lista de diccionarios. Para más info sobre los posibles packages, chequear el pdf
        {
            "Type":"subscription-vod" # (str)
        },
        {
            "Type":"transaction-vod", # (str)
            "BuyPrice":9.99, # (float)
            "RentPrice":4.99, # (float)
            "Definition":"HD" # (str)
        }
    ]
    payload.country = [ # lista de strings
        "USA", # (str)
        "Argentina" # (str)
    ]
    payload.createdAt = "2021-08-20" # (str) directamente lo asignamos al self._created_at que definimos en el init de la clase de la plataforma
    
    #payload_episode() construye un payload en formato diccionario usando los atributos anteriormente seteados
    return payload.payload_episode()

print(build_payload_movie())