import hashlib
from argparse import ArgumentParser
from datetime import datetime
from socket import gethostname

import sshtunnel
from pymongo import MongoClient, UpdateOne

try:
    from updates.validator import Validator
except ModuleNotFoundError:
    from validator import Validator


# Mover a config cuando tengamos un archivo limpio

class Upload:
    def __init__(self,
                 platform_code,
                 created_at,
                 testing=True,
                 has_episodes=True,
                 bypass=False,
                 server=1):
        self.platform_code = platform_code
        self.created_at = created_at
        self.has_episodes = has_episodes
        self.upload = not testing
        self.db_local = MongoClient().titan

        self._run()

    def _run(self):
        print(f'\n{"-" * 80}')

        status = {}
        validator = Validator()

        if self.has_episodes and self._has_series():
            collections = ('titanScraping', 'titanScrapingEpisodes')
        else:
            collections = ('titanScraping',)

        print(f'PlatformCode: {self.platform_code}, CreatedAt: {self.created_at}')

        for col in collections:
            status[col] = validator.run_checks(col, self._find_items(col))
            print(f'{col}: total {status[col]["total"]}')
            if status[col]['ok']:
                print(f'{col}: OK')

        if not self.upload:
            return

    def _has_series(self):
        return self.db_local['titanScraping'].find_one(
            {'PlatformCode' : self.platform_code,
             'CreatedAt'    : self.created_at,
             'Type'         : 'serie'},
            projection={'_id': 1})

    def _find_items(self, collection):
        cur = self.db_local[collection].find(
            {'PlatformCode': self.platform_code, 'CreatedAt': self.created_at},
            projection={'_id': 0})

        for item in cur:
            yield item


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('-p', '--platformcode', type=str, required=True)
    parser.add_argument('-c', '--createdat', type=str, required=False)
    parser.add_argument('-u', '--upload', action='store_const', const=True, default=False)
    parser.add_argument('-s', '--server', type=int, required=False, default=1)
    parser.add_argument('--bypass', action='store_const', const=True, default=False)
    parser.add_argument('--noepisodes', action='store_const', const=True, default=False)
    args = parser.parse_args()

    Upload(args.platformcode,
           args.createdat or datetime.now().strftime('%Y-%m-%d'),
           testing=not args.upload,
           bypass=args.bypass,
           has_episodes=not args.noepisodes,
           server=args.server)
