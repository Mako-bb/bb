# -*- coding: utf-8 -*-
import csv
import hashlib
import itertools
import json
import time
from common import config
from handle.mongo import mongo

# TODO
#  Type - revisar
#  Runtime - convertir a minutos
#  Normalizar payloads ReporTV y Flow
# borrar primera columna sagai.csv
# reportv channel -> channelname

def delta_minutes(st1, st2, max_delta=6):
    if st1[:-4] != st2[:-4]:
        return False

    # datetime es leeeento
    delta = abs(int(st1[-5:-3]) - int(st2[-5:-3]))

    return delta <= max_delta

def match():
    sagai = list()

    with open('sagai.csv', newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        keys = ['senal', 'genero', 'fecha_inicio', 'hora_inicio', 'hora_fin', 'duracion', 'titulo', 'episodio', 'rating', 'share']

        for row in reader:
            emision = dict(zip(keys, row))

            if emision['senal'] == 'Señal' or not emision['fecha_inicio']:
                continue

            sagai.append(emision)

    for canal_sagai in canales:
        canal_flow = canales[canal_sagai]['flow']

        match_results = {
            'ChannelName': canal_flow,
            'Total': 0,
            'NoMatch': 0,
            'ReporTV': 0,
            'Flow': 0,
            'CreatedAt': created_at,
        }

        cursor = mongo_client.search(titanReport, {'ChannelName': canal_flow}) or list()
        emisiones_reportv = [i for i in cursor]

        cursor = mongo_client.search(titanGuia, {'ChannelName': canal_flow}) or list()
        emisiones_flow = [i for i in cursor]

        payloads = list()
        for emision in sagai:
            match_sagai = canales.get(emision['senal'])

            if not match_sagai or match_sagai['flow'] != canal_flow:
                continue

            match_results['Total'] += 1

            dia, mes, anio = emision['fecha_inicio'].split('/')
            start_time = '{}-{}-{}T{}'.format(anio, mes, dia, emision['hora_inicio'])

            print('{} {} {}'.format(canal_flow, start_time, emision['titulo']))

            emision_rep = emision_flow = None

            for e in emisiones_reportv:
                if e['ChannelName'] == canal_flow and delta_minutes(e['StartTime'], start_time):
                    emision_rep = e
                    match_results['ReporTV'] += 1
                    break

            for e in emisiones_flow:
                if e['ChannelName'] == canal_flow and delta_minutes(e['StartTime'], start_time):
                    emision_flow = e
                    match_results['Flow'] += 1
                    break

            if emision_rep or emision_flow:
                title = emision_rep['Title'] if emision_rep else emision_flow['Title']

                genres = emision_flow['Genre'] if emision_flow else emision_rep['Genre']
                genres = genres.split(',')

                cast = emision_rep['Cast'] if emision_rep else emision_flow['Cast']['actors']
                directors = emision_rep['Directors'] if emision_rep else emision_flow['Cast']['directors']
                year = emision_rep['Year'] if emision_rep else None
                country = emision_rep['Country'] if emision_rep else None
                stime = emision_rep['StartTime'] if emision_rep else emision_flow['StartTime']
                etime = emision_rep['EndTime'] if emision_rep else emision_flow['EndTime']

                # dif formatos
                runtime = emision_rep['Duration'] if emision_rep else emision_flow['Duration']

                synopsis = emision_rep['Synopsis'] if emision_rep else emision_flow['Description']
                season = emision_rep['Season'] if emision_rep else None
                ep_title = emision_rep['EpisodeTitle'] if emision_rep else emision_flow.get('EpisodeTitle')
                ep_synopsis = emision_rep['SynopsisEpisode'] if emision_rep else None

                content_type = emision_flow['Type'] if emision_flow else None
                content_type = 'TV' if content_type == 'SE' else 'Movie' if content_type == 'MO' else None

                payload = {
                    'ChannelId': canales[canal_sagai]['id'],
                    'ChannelNumber': canales[canal_sagai]['number'],
                    'ChannelName': canal_flow,
                    'ChannelHD': canales[canal_sagai]['is_hd'],
                    'Title': title,
                    'Type': content_type,
                    'Year': year,
                    'Runtime': runtime,
                    'StartTime': stime,
                    'EndTime': etime,
                    'Country': country,
                    'Genres': genres,
                    'Rating': emision['rating'],
                    'Share': emision['share'],
                    'Synopsis': synopsis,
                    'Season': season,
                    'EpisodeTitle': ep_title,
                    'EpisodeSynopsis': ep_synopsis,
                    'Directors': directors,
                    'Cast': cast,
                    'Source': 'match',
                    'CreatedAt': created_at,
                }
                payloads.append(payload)

            else:
                match_results['NoMatch'] += 1

        if payloads:
            mongo_client.insertMany(titanMatch, payloads)
        print('Insertados {} matches'.format(len(payloads)))
        mongo_client.insert('titanGuiaMatchResults', match_results)

def fill_reportv():
    for canal in canales.values():
        cursor = mongo_client.search(titanMatch, {'ChannelName': canal['flow']}) or list()
        emisiones_match = [i['StartTime'] for i in cursor]
        emisiones = mongo_client.search(titanReport, {'ChannelName': canal['flow']}) or list()

        payloads = list()
        for emision in emisiones:
            if emision['StartTime'] not in emisiones_match:
                payload = {
                    'ChannelId': canal['id'],
                    'ChannelNumber': canal['number'],
                    'ChannelName': canal['flow'],
                    'ChannelHD': canal['is_hd'],
                    'Title': emision['Title'],
                    'Type': None,
                    'Year': emision['Year'],
                    'Runtime': emision['Duration'],
                    'StartTime': emision['StartTime'],
                    'EndTime': emision['EndTime'],
                    'Country': emision['Country'],
                    'Genres': emision['Genre'],
                    'Rating': None,
                    'Share': None,
                    'Synopsis': emision['Synopsis'],
                    'Season': emision['Season'],
                    'EpisodeTitle': emision['EpisodeTitle'],
                    'EpisodeSynopsis': emision['SynopsisEpisode'],
                    'Directors': emision['Directors'],
                    'Cast': emision['Cast'],
                    'Source': 'reportv',
                    'CreatedAt': created_at,
                }

                emisiones_match.append(emision['StartTime'])
                payloads.append(payload)

        if payloads:
            mongo_client.insertMany(titanMatch, payloads)
        print('No Matcheados ReporTV - Canal {} - Insertados {}'.format(canal['flow'], len(payloads)))

def fill_flow():
    for canal in canales.values():
        cursor = mongo_client.search(titanMatch, {'ChannelName': canal['flow']}) or list()
        emisiones_match = [i['StartTime'] for i in cursor]
        emisiones = mongo_client.search(titanGuia, {'ChannelName': canal['flow']}) or list()

        payloads = list()
        for emision in emisiones:
            if emision['StartTime'] not in emisiones_match:
                payload = {
                    'ChannelId': canal['id'],
                    'ChannelNumber': canal['number'],
                    'ChannelName': canal['flow'],
                    'ChannelHD': canal['is_hd'],
                    'Title': emision['Title'],
                    'Type': emision['Type'],
                    'Year': None,
                    'Runtime': emision['Duration'],
                    'StartTime': emision['StartTime'],
                    'EndTime': emision['EndTime'],
                    'Country': None,
                    'Genres': emision['Genre'],
                    'Rating': None,
                    'Share': None,
                    'Synopsis': emision['Description'],
                    'Season': None,
                    'EpisodeTitle': emision.get('EpisodeTitle'),
                    'EpisodeSynopsis': emision.get('SynopsisEpisode'),
                    'Directors': emision['Cast']['directors'],
                    'Cast': emision['Cast']['actors'],
                    'Source': 'flow',
                    'CreatedAt': created_at,
                }

                emisiones_match.append(emision['StartTime'])
                payloads.append(payload)

        if payloads:
            mongo_client.insertMany(titanMatch, payloads)
        print('No Matcheados Flow - Canal {} - Insertados {}'.format(canal['flow'], len(payloads)))

if __name__ == "__main__":
    mongo_client = mongo()
    titanGuia = config()['mongo']['collections']['guia']
    titanReport = config()['mongo']['collections']['guiaReport']
    titanChannel = config()['mongo']['collections']['guiaChannel']
    titanMatch = config()['mongo']['collections']['guiaMatch']
    created_at = time.strftime('%Y-%m-%d')

    canales = dict()
    with open('canales.csv', newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='"')

        for row in itertools.islice(reader, 1, None):
            if row[0] and row[1]:
                canales[row[0]] = {
                    'flow': row[1],
                    'reportv': 'no esta en reportv' not in row[2],
                }

    with mongo_client.search(titanChannel, {}) as cursor:
        data_channels = dict()
        for item in cursor:
            data_channels[item['Name']] = {'number': item['ChannelNumber'], 'is_hd': item['IsHD']}

    for c in canales:
        canal_id = str(int(hashlib.md5(canales[c]['flow'].encode('utf-8')).hexdigest(), 16))[:12]
        canal_id = int(canal_id)

        canales[c].update(
            {
                'id': canal_id,
                'number': data_channels[canales[c]['flow']]['number'],
                'is_hd': data_channels[canales[c]['flow']]['is_hd'],
            }
        )

    match()
    fill_reportv()
    fill_flow()
