import os
from argparse import ArgumentParser
from datetime import datetime
from socket import gethostname
from pymongo import MongoClient, UpdateOne

try:
    from root import servers
except ModuleNotFoundError:
    import sys

    path = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
    sys.path.insert(1, path)

from root import servers
from settings import settings
from updates.validator import Validator
from updates.deeplinks import DeeplinkChecker


servers_info = {
    1: {
        'name': settings.MISATO_SERVER_NAME,
        'ssh_connection': servers.MisatoConnection(),
        'production': True
    },
    2: {
        'name': settings.KAJI_SERVER_NAME,
        'ssh_connection': servers.KajiConnection(),
        'production': False
    },
}


class Upload:
    def __init__(
        self,
        platform_code,
        created_at,
        testing=False,
        has_episodes=True,
        bypass=False,
        server=1,
        titan=False,
        no_deeplink_check=False
    ):
        self.platform_code = platform_code
        self.created_at = created_at
        self.hostname = gethostname()
        self.bypass = bypass
        self.has_episodes = has_episodes
        self.server = servers_info[server]
        self.db_local = MongoClient(settings.MONGODB_DATABASE_URI).titan
        self.db_api = None
        self.upload_number = None
        self.use_titan_db = titan  # bool para hacer upload a titan en vez de bussiness
        self.no_deeplink_check = no_deeplink_check
        self.collections = self._get_collections()
        self.validator = Validator()
        self.deeplink_errors = []
        self.has_errors = False

        self._run_checks()

        if not testing:
            self._connect_and_upload()

    def _run_checks(self):
        print(f'\n{"-" * 80}')
        print(f'PlatformCode: {self.platform_code}, CreatedAt: {self.created_at}')

        self._validate_scraping()
        self._check_deeplinks()
        self._root_error_lock()

    def _validate_scraping(self):
        for col in self.collections:
            self.collections[col] = self.validator.run_checks(col, self._find_items(col))

            print(f'{col}: total {self.collections[col]["total"]}')

            if self.collections[col]['ok']:
                print(f'{col}: OK')
            elif not self.has_episodes and self.collections[col]['no_epis']:
                print(f'{col}: OK')
            else:
                self.has_errors = True

    def _has_series(self):
        return self.db_local['titanScraping'].find_one(
            {
                'PlatformCode': self.platform_code,
                'CreatedAt': self.created_at,
                'Type': 'serie'
            },
            projection={'_id': 1}
        )

    def _get_collections(self):
        if self.has_episodes and self._has_series():
            return {'titanScraping': {}, 'titanScrapingEpisodes': {}}
        else:
            return {'titanScraping': {}}

    def _find_items(self, collection):
        cur = self.db_local[collection].find(
            {
                'PlatformCode': self.platform_code,
                'CreatedAt': self.created_at
            },
            projection={'_id': 0}
        )

        for item in cur:
            yield item

    def _upload_logs(self):
        errors = self.validator.unique_errors()

        if not errors:
            return

        dt_now = datetime.now()
        date_iso = dt_now.date().isoformat()
        bulk_logs = []
        bulk_logs_non_critical = []
        
        for error in errors:
            if error.critical:
                bulk_logs.append(
                    UpdateOne(
                        {
                            'PlatformCode': self.platform_code,
                            'Collection': error.collection,
                            'Error': error.error
                        },
                        {
                            '$set': {
                                'Message': error.message,
                                'Source': self.hostname,
                                'UpdatedAt': date_iso,
                                'Timestamp': dt_now,
                            },
                            '$setOnInsert': {
                                'CreatedAt': date_iso,
                            }
                        },
                        upsert=True,
                    )
                )
            else:
                bulk_logs_non_critical.append(
                    UpdateOne(
                        {
                            'PlatformCode': self.platform_code,
                            'Collection': error.collection,
                            'Error': error.error
                        },
                        {
                            '$set': {
                                'Message': error.message,
                                'Source': self.hostname,
                                'UpdatedAt': date_iso,
                                'Timestamp': dt_now,
                            },
                            '$setOnInsert': {
                                'CreatedAt': date_iso,
                            }
                        },
                        upsert=True,
                    )
                )
        
        if bulk_logs != []:
            result = self.db_api['titanLog'].bulk_write(bulk_logs)
            print(f'titanLog: {result.upserted_count} logs nuevos, {result.matched_count} existentes')
        if bulk_logs_non_critical != []:
            result = self.db_api['titanLogNonCritical'].bulk_write(bulk_logs_non_critical)
            print(f'titanLogNonCritical: {result.upserted_count} logs nuevos, {result.matched_count} existentes')
    
    def _upload_deeplink_errors(self):
        if not self.deeplink_errors:
            return

        self.db_api['titanDeeplinksQA'].delete_many({'PlatformCode': self.platform_code})
        self.db_api['titanDeeplinksQA'].insert_many(self.deeplink_errors)

    def _get_upload_number(self):
        prev_upload = self.db_api['titanStats'].find_one(
            {'PlatformCode': self.platform_code},
            sort=[('UploadNumber', -1)],
            projection={'_id': 0, 'UploadNumber': 1}
        )

        if prev_upload:
            return prev_upload['UploadNumber'] + 1
        else:
            return 1

    def _upload(self, collection):
        payload = list(self._find_items(collection))

        print(f'\nUpload {collection}')
        print(f'{len(payload)} para subir a {self.server["name"]}')

        res = self.db_api[collection].delete_many({'PlatformCode': self.platform_code})
        print(f'{res.deleted_count} eliminados en {self.server["name"]}')

        res = self.db_api[collection].insert_many(payload)
        print(f'{len(res.inserted_ids)} insertados en {self.server["name"]}')

    def _insert_titan_stats(self):
        self.db_api['titanStats'].insert_one(
            {
                'PlatformCode': self.platform_code,
                'UploadNumber': self.upload_number,
                'Collection': 'titanScraping',  # Necesario para que funcione el update
                'Count': self.collections['titanScraping']['total'],
                'CountEpisodes': self.collections.get('titanScrapingEpisodes', {}).get('total', 0),
                'Bypass': self.bypass,
                'Source': self.hostname,
                'CreatedAt': self.created_at,  # No queda claro que es el CreatedAt de la plataforma
                'Timestamp': datetime.now(),
            }
        )

    def _check_deeplinks(self):
        if not self.has_errors and not self.no_deeplink_check:
            print('Chequeando deeplinks....')
            dc = DeeplinkChecker()
            self.deeplink_errors = dc.check(self.platform_code, self.created_at)

    def _root_error_lock(self):
        if self.has_errors and not settings.DEBUG:
            parent_dir = os.path.join('/', 'tmp', 'platforms')

            if not os.path.exists(parent_dir):
                os.makedirs(parent_dir)

            file_lock = os.path.join(parent_dir, self.platform_code + '-upload.pid')
            with open(file_lock, 'w') as _:
                pass

    def _connect_mongo_db(self, ssh_conn):
        print('Conectando a Mongo DB....')

        if self.use_titan_db and not self.server['production']:  # hacer upload a titan en vez de bussiness
            self.db_api = MongoClient(port=ssh_conn.local_bind_port).titan
        else:
            self.db_api = MongoClient(port=ssh_conn.local_bind_port).business

    def _connect_and_upload(self):
        print(f'Conectando a {self.server["name"]}....')

        with self.server['ssh_connection'].connect() as server:
            self._connect_mongo_db(server)

            self._upload_logs()
            self._upload_deeplink_errors()

            if self.collections['titanScraping']['ok']:
                self.upload_number = self._get_upload_number()

                if not self.collections.get('titanScrapingEpisodes'):
                    self._upload('titanScraping')
                    self._insert_titan_stats()
                elif self.collections['titanScrapingEpisodes']['ok']:
                    self._upload('titanScrapingEpisodes')
                    self._upload('titanScraping')
                    self._insert_titan_stats()


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('-p', '--platformcode', type=str, required=True)
    parser.add_argument('-c', '--createdat', type=str, default=datetime.now().date().isoformat())
    parser.add_argument('-u', '--upload', action='store_true')
    parser.add_argument('-s', '--server', type=int, default=1)
    parser.add_argument('--bypass', action='store_true')
    parser.add_argument('--noepisodes', action='store_true')
    parser.add_argument('--titan', action='store_true')
    parser.add_argument('--nolinkcheck', action='store_true')
    args = parser.parse_args()

    Upload(
        args.platformcode,
        args.createdat,
        testing=not args.upload,
        bypass=args.bypass,
        has_episodes=not args.noepisodes,
        server=args.server,
        titan=args.titan,
        no_deeplink_check=args.nolinkcheck,
    )
