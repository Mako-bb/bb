# -*- coding: utf-8 -*-
import argparse
import json
import pymongo
import socket
import time
from sshtunnel          import SSHTunnelForwarder
from common             import config
from pathlib            import Path
from datetime           import datetime

if __name__ == '__main__':
    base_path = Path(__file__).parent.parent
    file_path = (base_path / 'id_rsa')
    path_str = str(file_path)

    server = SSHTunnelForwarder(
        '67.205.166.244',
        ssh_username             = 'root',
        ssh_pkey                 = path_str,
        ssh_private_key_password = 'KLM2012a',
        remote_bind_address      = ('127.0.0.1', 27017)
    )

    server.start()
    remote_client = pymongo.MongoClient('127.0.0.1', server.local_bind_port)
    remote_mongo = remote_client.business

    # print('Eliminando errores no criticos....')
    # result = remote_mongo['titanLog'].delete_many(
    #     filter={
    #         'Critical': False,
    #     }
    # )
    # print('{} docs eliminados'.format(result.deleted_count))

    print('Buscando Qua logs....')
    result = remote_mongo['titanLog'].find(
        filter={
            'QuaPresence': {
                '$exists': True
            }
        },
        sort=[('CreatedAt', -1)]
    )

    platforms = set()
    delete_list = []
    for item in result:
        if item['PlatformCode'] not in platforms:
            platforms.add(item['PlatformCode'])
        else:
            delete_list.append(item['_id'])

    print('Para eliminar: {}'.format(len(delete_list)))
    result = remote_mongo['titanLog'].delete_many(
        filter={
            '_id': {
                '$in': delete_list
            }
        }
    )
    print('{} docs eliminados'.format(result.deleted_count))

    print('Buscando errores de upload....')
    result = remote_mongo['titanLog'].find(
        filter={
            'Critical': True,
            'Collection': {
                '$exists': True
            }
        }
    )

    result = list(result)
    print('{} docs encontrados'.format(len(result)))

    log_dict = {}
    delete_list = []

    print('Filtrando duplicados...')
    for item in result:
        plat_code = item['PlatformCode']
        coll = item['Collection']

        log_dict.setdefault(plat_code, {})
        log_dict[plat_code].setdefault(coll, [])

        if item['Message'] in log_dict[plat_code][coll]:
            delete_list.append(item['_id'])
        else:
            log_dict[plat_code][coll].append(item['Message'])

    result = remote_mongo['titanLog'].delete_many(
        filter={
            '_id': {
                '$in': delete_list
            }
        }
    )
    print('{} docs eliminados'.format(result.deleted_count))

    remote_client.close()
    server.close()
