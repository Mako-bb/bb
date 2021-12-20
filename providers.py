# -*- coding: utf-8 -*-
import pymongo
import requests
from common import config
from pathlib import Path
from sshtunnel import SSHTunnelForwarder

config = config()['ott_sites']['JustWatch']
locales = config['locales']

url = 'https://apis.justwatch.com/content/providers/locale/{locale}'
s = requests.session()

platform_code = '{}.{}'
payload = []

for country, locale in locales.items():
    print(country, locale)

    r = s.get(url.format(locale=locale))
    j = r.json()

    country_code = country.lower()

    for item in j:
        jw_code = item['technical_name']
        code = config['codes'].get(jw_code, jw_code)

        payload.append({
            'Country': country,
            'Provider': jw_code,
            'PlatformCode': platform_code.format(country_code, code)
        })

base_path = Path(__file__).resolve().parent
# file_path = (base_path / 'id_rsa')
file_path = (base_path / 'misato')
file_str = str(file_path)

server = SSHTunnelForwarder(
    ('168.61.73.89', 31415),
    ssh_username = 'bb',
    ssh_pkey = file_str,
    ssh_private_key_password = 'KLM2012a',
    remote_bind_address = ('127.0.0.1', 27017)
)

try:
    server.start()
    client = pymongo.MongoClient('127.0.0.1', server.local_bind_port)
    db = client['business']

    cursor = list(db['titanProviders'].find())
    jwplatforms = [doc['PlatformCode'] for doc in cursor]

    print('{} Platforms in JustWatch'.format(len(payload)))
    print('{} Platforms in titanProviders'.format(len(jwplatforms)))

    addlist = []
    for platf in payload:        
        if platf['PlatformCode'] not in jwplatforms:
            addlist.append(platf)
    
    if len(addlist) > 0:
        print('{} Platforms added'.format(len(addlist)))
        db['titanProviders'].insert_many(addlist)

except Exception as e:
    print(e)
finally:
    client.close()
    server.stop()
