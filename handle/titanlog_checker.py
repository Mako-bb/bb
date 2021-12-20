# -*- coding: utf-8 -*-
import os
import sys
import time
import argparse
from datetime import datetime
from bson.regex import Regex
from pymongo import MongoClient
try:
    from root import servers
except ModuleNotFoundError:
    path = os.path.abspath('.')
    sys.path.insert(1, path)
    from root import servers


MAX_COLS_MSG = 70
DEFAULT_COLLECTIONS = ["titanLog", "updateLog"]


def __obtain_docs(query, collection_name):
    docs = []
    column_names = ['_id', 'PlatformCode', 'Error', 'Message', 'Collection', 'Source', 'CreatedAt', ]

    ssh_connection = servers.MisatoConnection()
    with ssh_connection.connect() as server:
        business = MongoClient(port=server.local_bind_port).business
        collection_log = business[collection_name]
        cursor = collection_log.find(query, no_cursor_timeout=True)
        try:
            for doc in cursor:
                doc = {k: v if v else '' for k, v in doc.items() if k in column_names}
                doc['_id'] = str(doc['_id'])
                for col in column_names:
                    if not col in doc:
                        doc.setdefault(col, '')
                if doc.get('CreatedAt'):
                    created_at = doc['CreatedAt']
                    if isinstance(created_at, (str, )):
                        created_at = created_at.split(' ')[0]
                    elif isinstance(created_at, (datetime, time, )):
                        created_at = created_at.strftime("%Y-%m-%d")
                    doc['CreatedAt'] = created_at
                docs.append(doc)
        except Exception as e:
            print(e)
        finally:
            cursor.close()
    
    return docs


def print_table(platform_code, created_at=None, collection_name=None):
    query = {
        'PlatformCode': Regex(f".*{platform_code}.*", "i"),
    }
    if created_at:
        query.update({
            'CreatedAt': created_at,
        })
    if collection_name and collection_name not in DEFAULT_COLLECTIONS:
        return
    collections = [collection_name] if collection_name else DEFAULT_COLLECTIONS

    for collection_name in collections:
        docs = __obtain_docs(query=query, collection_name=collection_name)
        if docs:
            header = "+           _id              |    PlatformCode   |     Error      | CreatedAt  |       Collection       |   Source   |                                 Message                                +"
            columns = len(header)
            print("\n\n\033[1;40m{collection:^{columns}}\033[0m".format(collection=collection_name.upper(), columns=columns))
            print('='*columns)
            print(header)
            print('='*columns)
            for doc in docs:
                msg = doc['Message']
                parts_msgs = [msg[i:i+MAX_COLS_MSG] for i in range(0, len(msg), MAX_COLS_MSG)]
                for counter, msg in enumerate(parts_msgs):
                    if counter != 0:
                        doc = {k: '' for k in doc.keys()}
                    doc['Message'] = msg
                    print("|{_id:^27} | {PlatformCode:^17} | {Error:^14} | {CreatedAt:^10} | {Collection:^22} | {Source:^10} | {Message:^70} |".format(**doc))
                print('¯'*columns)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--p',      help='PlatformCode. Ej: us.microsoft ó microsoft.', type=str)
    parser.add_argument('--date',   help='Fecha de la plataforma. Opcional.', type=str)
    parser.add_argument('--c',      help='Colección.', type=str)
    args = parser.parse_args()

    platform_code   = args.p
    created_at      = args.date
    collection      = args.c

    print_table(platform_code, created_at, collection)
