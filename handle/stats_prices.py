# -*- coding: utf-8 -*-
import argparse
from bson.regex import Regex
from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017")
database = client["titan"]

""" Obtiene estadísticas de los precios de una plataforma especificada e
    imprime una tabla de los datos obtenidos. \n
    Los datos lo obtiene de la DB local. Habría que implementar métodos para obtener precios de los servers.

    - Puede especificarse solo el nombre de la plataforma sin el país para obtener una tabla de datos para comparar con el resto de países de la plataforma:
        $ python handle/stats_prices.py --p microsoft --date 2020-12-16
    - Para obtener los datos del país en específico ingresar:
        $ python handle/stats_prices.py --p us.microsoft --date 2020-12-16
    - Se puede especificar que traiga los datos de una colección en específico(por defecto obtiene los datos de titanScraping y titanScrapingEpisodes):
        $ python handle/stats_prices.py --p microsoft --date 2020-12-16 --c titanScraping
"""


def __obtain_docs(match, collection_name):
    collection = database[collection_name]
    field_buy = "$Packages.BuyPrice"
    field_rent = "$Packages.RentPrice"
    pipeline = [
        {
            "$match": match,
        }, {
            "$unwind": "$Packages"
        }, {
            "$match": match,
        }, {
            "$group": {
                "_id": "$PlatformCode",
                "currency": {
                    "$first": "$Packages.Currency"
                },
                "count": {
                    "$sum": 1
                },
                "minBuy": {
                    "$min": field_buy
                },
                "minRent": {
                    "$min": field_rent
                },
                "maxBuy": {
                    "$max": field_buy
                },
                "maxRent": {
                    "$max": field_rent
                },
                "avgBuy": {
                    "$avg": field_buy
                },
                "avgRent": {
                    "$avg": field_rent
                },
                "createdAt": {
                    "$first": created_at
                }
            }
        }
    ]
    cursor = collection.aggregate(pipeline)
    docs = []
    try:
        for doc in cursor:
            doc = {k: v if v else 0 for k, v in doc.items()}
            doc["currency"] = doc["currency"] if doc["currency"] else "null"
            docs.append(doc)
    except Exception as e:
        print(e)
    finally:
        cursor.close()
    
    return docs


def print_table(platform_code, created_at, collection_name=None):
    default_collections = ["titanScraping", "titanScrapingEpisodes"]
    match = {
        "PlatformCode": Regex(f".*{platform_code}.*", "i"),
    }
    if created_at:
        match.update({"CreatedAt": created_at})
    if collection_name and collection_name not in default_collections:
        return
    collections = [collection_name] if collection_name else default_collections

    for collection_name in collections:
        docs = __obtain_docs(match, collection_name)
        if docs:
            header = "+    PlatformCode   | Currency |   COUNT    |  MIN BUY   |  MAX BUY   |  MIN RENT  |  MAX RENT  |  AVG BUY   |  AVG RENT  | CreatedAt  +"
            columns = len(header)
            print("\n\n{collection:^{columns}}".format(collection=collection_name.upper(), columns=columns))
            print("="*columns)
            print(header)
            print("="*columns)
            for doc in docs:
                print("|{_id:^18} | {currency:^8} | {count:^10} | {minBuy:^10.2f} | {maxBuy:^10.2f} | {minRent:^10.2f} | {maxRent:^10.2f} | {avgBuy:^10.2f} | {avgRent:^10.2f} | {createdAt:^8} |".format(**doc))
                print("¯"*columns)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--p',      help='PlatformCode. Ej: us.microsoft ó microsoft.', type=str)
    parser.add_argument('--date',   help='Fecha de la plataforma.', type=str)
    parser.add_argument('--c',      help='Colección.', type=str)
    args = parser.parse_args()

    platform_code = args.p
    created_at = args.date
    collection = args.c

    print_table(platform_code, created_at, collection)
