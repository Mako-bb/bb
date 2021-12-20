# -*- coding: utf-8 -*-

import time
import socket
import pymongo
import pymsteams
import sshtunnel
from pathlib      import Path
from datetime     import datetime, timedelta

def sshConnect():
    base_path = Path(__file__).parent.parent
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

if __name__ == '__main__':
    server = sshConnect()
    server.start()
    time.sleep(11)
    client   = pymongo.MongoClient('127.0.0.1', server.local_bind_port)
    business = client['business']

    today = time.strftime("%Y-%m-%d")
    yesterday = str(datetime.now() - timedelta(days=1)).split(" ")[0]
    weekAgo = str(datetime.now() - timedelta(weeks=1)).split(" ")[0]
    
    dailyList = [
        'us.itunes',
        'us.abcgo',
        'us.adultswim',
        'us.amc',
        'all.appletv',
        'us.bet',
        'us.cbs',
        'us.comedycentral',
        'us.cwseed',
        'us.foxusa',
        'us.fxnow',
        'us.google-play', 
        'us.hbo-now',
        'us.ifc',
        'max-go',
        'us.nbc',
        'us.showtime',
        'us.starz',
        'us.syfy',
        'us.youtube',
        'us.youtubered',
        'us.thecw',
        'us.tubitv',
        'us.plutotv',
        'hulu',
        'us.hbomax',
        'us.peacock',
        'cl.netflix',
        'us.netflix',
        'br.netflix',
        'co.netflix',
        'ar.netflix',
        'mx.netflix',
        'br.hbo-go',
        'hbo-go',
        'ro.hbogoeu', 
        'hu.hbogoeu', 
        'cz.hbogoeu', 
        'pl.hbogoeu',
        'es.hbo',
        'dk.hbo',
        'se.hbo',
        'fi.hbo',
        'no.hbo',
        'pt.hbopt',
        'fr.ocs' 
    ]

    broken = False

    for platform in dailyList:
        broken = False
        cursor = business['apiWave'].find({'PlatformCode': platform}, no_cursor_timeout=True).sort("_id", pymongo.DESCENDING).limit(1)
        if cursor:
            for doc in cursor:                    
                isodate = datetime.fromtimestamp(doc['CreatedAt'])
                isodate = isodate.strftime('%Y-%m-%d')

                if platform == doc['PlatformCode']:
                    if isodate == today:
                        print("\x1b[1;32;40m {} se actualizo hoy!\x1b[0m".format(platform))
                    elif isodate == yesterday:
                        print("\x1b[1;33;40m {} se actualizo por ultima vez ayer!\x1b[0m".format(platform))
                    elif isodate <= weekAgo:
                        print("\x1b[1;31;40m {} se actualizo hace mas de una semana! ({})\x1b[0m".format(platform,isodate))
                        broken = True
                    elif isodate >= weekAgo:
                        print("\x1b[1;33;40m {} se actualizo esta semana! ({})\x1b[0m".format(platform,isodate))
                        broken = True
                    else:
                        print("\x1b[1;31;40m {} se actualizo por ultima vez el {}\x1b[0m".format(platform,isodate))
                        broken = True

        if broken: 
            cursorTitanLog = business['titanLog'].find({'PlatformCode': platform}, no_cursor_timeout=True)
            if cursorTitanLog:
                for error in cursorTitanLog:
                    print("ERROR {}".format(error.get('Error')), error['Message'])

            cursorUpdateLog = business['updateLog'].find({'PlatformCode': platform}, no_cursor_timeout=True)
            if cursorUpdateLog:
                for error in cursorUpdateLog:
                    print("ERROR UPDATE {}".format(error.get('Error')), error['Message'])

    print()