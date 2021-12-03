# -*- coding: utf-8 -*-
import argparse
import time
from pymongo import MongoClient

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--p', help='PlatformCode', type=str, required=True)
    parser.add_argument('--at', help='CreatedAt', type=str, required=False)
    parser.add_argument('--all', help='Todas las fechas', action='store_true')
    parser.add_argument('--prescraping', help='Eliminar Prescraping', nargs='?', default=False, const = True)
    args = parser.parse_args()

    platform_code = args.p
    created_at = args.at or time.strftime('%Y-%m-%d')

    with MongoClient() as client:
        local_mongo = client.titan

        if args.all:
            delete_filter = {
                'PlatformCode': platform_code,
            }
        else:
            delete_filter = {
                'PlatformCode': platform_code,
                'CreatedAt': created_at
            }

        collections = ['titanScraping', 'titanScrapingEpisodes']
        if args.prescraping:
            collections.append('titanPreScraping')
        
        for collection in collections:
            result = local_mongo[collection].delete_many(
                filter=delete_filter
            )

            print('Plataforma: {platform} - Colleccion: {coll} - {count} docs eliminados.'.format(
                    platform=platform_code,
                    coll=collection,
                    count=result.deleted_count
                )
            )
