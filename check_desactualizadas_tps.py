import time
import pymongo
import sshtunnel
from pathlib      import Path
from datetime     import datetime, timedelta
'''
Script para revisar que plataformas de tp (justwatch - reelgood) estan desactualizadas (mas de una semana desde la ultima actualizacion)
Hace una query a last update buscando todas las plataformas que estan en la lista platform_codes
y despues para cada una de las desactualizadas busca en titanLog y updateLog
IMPORTANTE! si alguna plataforma pasa de tp a script bb sacarla de la list
'''
def sshConnect():
    base_path = Path(__file__).parent
    file_path = (base_path / "misato")
    __file = str(file_path)

    sshtunnel.SSH_TIMEOUT = sshtunnel.TUNNEL_TIMEOUT = 10.0

    server = sshtunnel.open_tunnel(
        ('168.61.73.89', 31415),
        ssh_username='bb',
        ssh_pkey=__file,
        ssh_private_key_password ='KLM2012a',
        remote_bind_address=('127.0.0.1', 27017)
    )
    return server


platform_codes = [
    'se.netflix',
    'de.amazonbbcplayer',
    'in.netflix',
    'ee.google-play',
    # 'us.wwenetwork',
    'it.amazonprimevideo',
    've.netflix',
    'nl.amazonprimevideo',
    'gr.amazonprimevideo',
    'tr.amazonprimevideo',
    'lt.netflix',
    'lt.amazonprimevideo',
    'fi.amazonprimevideo',
    'us.google-play',
    'us.imdb',
    'au.netflix',
    'at.amazon',
    'ch.swisscom',
    'ru.google-play',
    'in.amazonprimevideo',
    'ru.netflix',
    'fr.netflix',
    'us.umc',
    'th.netflix',
    #  'us.youtube',
    'ca.google-play',
    'ro.amazonprimevideo',
    'us.amazonprime',
    'jp.google-play',
    'lv.netflix',
    'it.netflix',
    'at.netflix',
    'ie.amazonprimevideo',
    'ch.amazonprimevideo',
    'ee.netflix',
    'dk.google-play',
    # 'hu.google-play',
    'us.nbc-universo',
    'es.netflix',
    'de.amazonmubi',
    #'us.magnoliaselects',
    'us.netflix',
    # 'us.smithsonianchannel',
    'de.amazonstarzplay',
    'de.amazonzdfherzkino',
    'hu.netflix',
    'gb.amazonstarzplay',
    # 'gb.skygo',
    'ro.netflix',
    'google-play',
    'ru.ivi',
    'nz.netflix',
    'sg.netflix',
    'pl.google-play',
    'hu.hbogoeu',
    'ru.okko',
    'de.google-play',
    'in.youtube',
    # 'ie.skygo',
    #'pt.meo',
    # 'pl.vod-poland',
    'ar.netflix',
    'ee.amazonprimevideo',
    'ph.google-play',
    'jp.netflix',
    'us.pbs',
    'us.disney-now',
    'za.amazonprimevideo',
    #'us.amazon',
    'be.google-play',
    'gr.netflix',
    'ch.google-play',
    'dk.netflix',
    'nl.netflix',
    'no.google-play',
    'my.netflix',
    'pt.hbopt',
    'us.foxusa',
    'at.amazonprime',
    'gb.amazonhayu',
    'cz.google-play',
    'id.google-play',
    'br.google-play',
    'pt.amazonprimevideo',
    'tr.netflix',
    'tr.google-play',
    'ro.hbogoeu',
    'fr.google-play',
    'cl.netflix',
    'jp.amazon',
    'it.google-play',
    'se.amazonprimevideo',
    # 'sa.google-play',
    'be.amazonprimevideo',
    'de.amazonanimaxplus',
    'no.amazonprimevideo',
    'kr.netflix',
    # 'us.usanetwork',
    'fi.google-play',
    # 'us.national-geographic',
    'at.skygo',
    'cz.o2tvcz',
    'se.google-play',
    'sg.google-play',
    # 'it.skygo',
    'my.google-play',
    # 'us.motortrend',
    'cz.hbogoeu',
    'in.google-play',
    'de.amazonshudder',
    'us.darkmatter',
    # 'us.fxnow',
    'ph.netflix',
    'gb.amazonprime',
    'ie.netflix',
    'pl.amazonprimevideo',
    'de.amazon',
    'kr.google-play',
    # 'us.rokuchannel',
    'es.google-play',
    'nz.google-play',
    # 'fr.bbox',
    'nz.amazonprimevideo',
    'be.netflix',
    'ca.amazonprimevideo',
    'lv.google-play',
    'de.netflix',
    'br.netflix',
    'ie.google-play',
    'gb.amazonbfiplayer',
    'fr.amazonprimevideo',
    # 'us.shout-factory-tv',
    'th.google-play',
    'za.netflix',
    'nl.google-play',
    'us.sprout-now',
    'co.netflix',
    'de.amazonprime',
    'jp.amazonprime',
    'us.nbc',
    'lv.amazonprimevideo',
    'de.amazonfilmtastic',
    'at.google-play',
    'us.hopster',
    # 'us.historyvault',
    'gb.amazon',
    'cz.netflix',
    'gr.google-play',
    'cz.amazonprimevideo',
    'ru.amazonprimevideo',
    'mx.netflix',
    'us.hbo-now',
    'pl.hbogoeu',
    #'pt.google-play',
    'us.laughoutloud',
    'pt.netflix',
    'id.netflix',
    'fi.netflix',
    'au.google-play',
    'lt.google-play',
    'ec.netflix',
    'pl.netflix',
    # 'mx.amazonprimevideo',
    'gb.google-play',
    # 'cr.netflix',
    'no.netflix',
    'au.amazonprimevideo',
    'gb.netflix',
    'hu.amazonprimevideo',
    'ch.netflix',
    # 'jp.dtv',
    'mx.google-play',
    'kr.naverstore',
    'es.amazonprimevideo',
    'be.betvgo',
    'ca.netflix',
    'pe.netflix',
    'sg.amazonprimevideo',
    'kr.amazonprimevideo',
    'dk.amazonprimevideo',
    'us.viceland'
]

server = sshConnect()
server.start()
time.sleep(11)
client   = pymongo.MongoClient('127.0.0.1', server.local_bind_port)
business = client['business']

# se considera desactualizado aquellos que tienen mas de una semana sin actualizar
semana = datetime.now() - timedelta(days=6)
semana = semana.strftime("%Y-%m-%d")

last_update = business['last_update']
query = {'PlatformCode':{'$in':platform_codes}, 'LastUpdate':{'$lt':semana}}
desactualizadas = last_update.find(query, no_cursor_timeout=True, projection=['PlatformCode', 'LastUpdate']).batch_size(10)

contador = 0
for desactualizada in desactualizadas:
    contador += 1
    print(f'{"*"*50}\n')
    print('PlatformCode:', desactualizada['PlatformCode'], desactualizada['LastUpdate'])
    desactualizada = desactualizada['PlatformCode']
    query = {'PlatformCode':desactualizada}
    for logger in ['titanLog','updateLog']:
        print('\nErrores en', logger, '\n')
        logger = business[logger]
        errores = list(logger.find(query, no_cursor_timeout=True).sort('Timestamp',-1))
        if errores == []:
            print('Sin errores')
        for error in errores:
            print(f"{error['CreatedAt']}\n{error.get('Source') if error.get('Source') else '-'}\n{error.get('Collection')}\n{error['Message']}\n")
        print(f'{"-"*50}\n')

if contador == 0:
    print('Sin plataformas desactualizadas')
server.stop()


