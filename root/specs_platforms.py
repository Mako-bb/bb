# -*- coding: utf-8 -*-
import os
import sys
import glob
import math
import time
import shutil
import pymongo
import subprocess
try:
    from . import utils
    from . import servers
except (ImportError, ModuleNotFoundError):
    path = os.path.abspath('.')
    sys.path.insert(1, path)
    from root import utils
    from root import servers
try:
    import settings
except ModuleNotFoundError:
    path = os.path.abspath('.')
    sys.path.insert(1, path)
from common import config
from settings import settings


class Platform():

    def __init__(self, platform):
        self.platform_name = platform['PlatformName']
        self.country = platform['Country']
        self.provider = platform.get('Provider') or ''
        self.process = platform['Process']
        self.enable_searches = platform['EnableSearches']
        self.list_platform_codes = platform['ListPlatformCodes']
        self._check_status = None
        self.__filename_searches = None

        extra_countries = platform.get('Countries') or []
        self.extra_countries = '--m ' + ' '.join(extra_countries) if extra_countries else ''

    def __del__(self):
        try:
            os.remove(self.__filename_searches)
        except (OSError, TypeError):
            pass

    def run(self, testing=False) -> None:
        payload_platform = {
            "PlatformName": self.platform_name,
            "Country": self.country,
            "Provider": self.provider,
            "Countries": self.extra_countries,  # Para plataformas con regiones, como Itunes
        }
        if self.enable_searches:
            self.setting_searches(payload_platform)

        if self.platform_name == "JustWatch":
            try:
                payload_platform.update({
                    "Process": 'jwtesting' if testing else self.process,
                })
                command = "python main.py {PlatformName} --o {Process} --c {Country} --provider {Provider}".format(**payload_platform)
                self._run_command(command, testing=testing)
            except:
                self._check_status = 4
        else:
            payload_platform.update({
                "Process": 'testing' if testing else self.process,
            })
            command = "python main.py {PlatformName} --o {Process} --c {Country} {Countries}".format(**payload_platform)
            self._run_command(command, testing=testing)

    def _run_command(self, command, testing=False):
        current_date = time.strftime(settings.FORMAT_DATE).replace("-", "")
        if self.platform_name == "JustWatch":
            path_file_log = "log/{}_{}-{}_{}.log".format(self.country, self.platform_name, self.provider, current_date)  # TODO: Usar platformCode para generar el log
        else:
            path_file_log = "log/{}_{}_{}.log".format(self.country, self.platform_name, current_date)  # TODO: Usar platformCode para generar el log
        log_file = open(path_file_log, "a+")

        command_proc = subprocess.run(command, shell=True, stdout=log_file, stderr=subprocess.STDOUT)

        if len(self.list_platform_codes) == 1:
            platform_code = self.list_platform_codes[0]
        elif self.platform_name.startswith("JustWatch"):
            platform_code = None
            for _ in range(2):
                try:
                    ssh_connection = servers.MisatoConnection()
                    with ssh_connection.connect() as server:
                        business = pymongo.MongoClient(port=server.local_bind_port).business
                        cursor = business['titanProviders'].find_one({'Country': self.country, 'Provider': self.provider}, no_cursor_timeout=True)
                        platform_code = cursor["PlatformCode"]
                except:
                    continue
                else:
                    break
            if not platform_code:
                platform_code = self.provider or self.platform_name
        elif self.platform_name in config()['ott_sites']:
            platform_for_country = config()['ott_sites'][self.platform_name].get('countries', {}).get(self.country)
            if not platform_for_country:
                platform_code = self.platform_name
            elif type(platform_for_country) == str:
                platform_code = platform_for_country
            else:
                if type(platform_for_country) == dict and 'platform_code' in platform_for_country:
                    platform_code = platform_for_country['platform_code']
                else:
                    platform_code = platform_for_country or self.platform_name
        else:
            platform_code = self.platform_name

        status_code = command_proc.returncode
        if status_code in (1, 2, 3, ):
            if not testing:
                utils.generate_titanlog(status_code=status_code, platform_code=platform_code, path_log=path_file_log)
            self._check_status = 3 if status_code == 3 else 1
        else:
            time.sleep(1)
            parent_dir = settings.PATH_PLATFORMS_TMP_PID
            if not os.path.exists(parent_dir):
                os.makedirs(parent_dir)
            has_upload_error = self.comprobe_upload_error(platform_code=platform_code, list_platform_codes=self.list_platform_codes)
            if has_upload_error:
                self._check_status = 2
            else:
                self._check_status = 0
        log_file.close()

    def check_status(self):
        return self._check_status

    def setting_searches(self, payload_platform: dict) -> None:
        payload_platform["Provider"] = payload_platform["Provider"] or ""
        parent_dir = "/tmp/platform-searches"
        try:
            os.mkdir(parent_dir)
        except FileExistsError:
            pass
        filename = "{PlatformName}-{Country}--{Provider}".format(**payload_platform)
        self.__filename_searches = os.path.join(parent_dir, filename)
        with open(self.__filename_searches, "w") as _:
            pass

    @staticmethod
    def comprobe_upload_error(platform_code: str, list_platform_codes: list) -> bool:
        has_error = False
        list_files_to_remove = []
        file_lock_ptn = os.path.join(settings.PATH_PLATFORMS_TMP_PID, '*-upload.pid')
        files_match = glob.glob(file_lock_ptn)
        for path_file in files_match:
            tmp_platform_code = path_file.split('/')[-1].replace('-upload.pid', '')
            if platform_code == tmp_platform_code:
                list_files_to_remove.append(path_file)
                break
            elif tmp_platform_code in list_platform_codes:  # Casos como Itunes ALL
                list_files_to_remove.append(path_file)
        for path_file in list_files_to_remove:
            os.remove(path_file)
            has_error = True
        return has_error


class TableMonitoring():
    HEADER = "       PLATFORM       | COUNTRY |      PROVIDER       |      SART TIME      |      END TIME       |         STATUS         |   ELAPSED TIME     "
    template_row = "{platform_name:^21} | {country:^7} | {provider:^19} | {start_time:^19} | {end_time:^19} | {status_color}{status:^22}\033[0m | {elapsed_time:^18} "
    columns_table = len(HEADER)

    def __init__(self, dict_platforms, country_code):
        self._dict_platforms = dict_platforms
        self.created_at = time.strftime(settings.FORMAT_DATE)
        self.country_code = country_code.replace(' ', '_').upper()
        self.start_timestamp = utils.get_datetime()
        self.end_timestamp = None

    def update(self, first_print=False, save_log=False):
        rows = []
        for code_name in self._dict_platforms:
            platform = self._dict_platforms[code_name]
            start_time = platform['start_timestamp'].strftime(settings.FORMAT_TIMESTAMP) if 'start_timestamp' in platform else ''
            end_time = platform['end_timestamp'].strftime(settings.FORMAT_TIMESTAMP) if 'end_timestamp' in platform else ''
            elapsed_time = platform.get('duration', '')
            if end_time:
                _status_alias = platform['status']
                status = '[ ' + _status_alias.replace('ok', 'success').replace('error', 'failure').upper() + ' ]'
                if _status_alias == 'ok':
                    status_color = "\033[32;1m"
                else:
                    status_color = "\033[31;1m"
            elif start_time:
                status = "[ RUNNING ]"
                status_color = "\033[33;1m"
            else:
                status = ""
                status_color = "\033[0m"
            
            rows.append({
                'platform_name': platform['PlatformName'],
                'country': platform['Country'],
                'provider': platform.get('Provider') or '',
                'start_time': start_time,
                'end_time': end_time,
                'status': status,
                'status_color': status_color,
                'elapsed_time': elapsed_time,
            })

        columns_tty, lines_tty = self.__get_size_tty()
        incr_lines = 1
        if not first_print:
            if columns_tty < self.columns_table:
                 incr_lines = math.ceil(self.columns_table/columns_tty)
            
            for _ in range((len(rows) + 4)*incr_lines):  # +4 por el header y el bottom
                sys.stdout.write("\033[F\033[K")

        sys.stdout.write(self.HEADER + "\n")
        sys.stdout.write("="*self.columns_table + "\n")
        for row in rows:
            sys.stdout.write(self.template_row.format(**row) + "\n")
        start_str = self.start_timestamp.strftime(settings.FORMAT_TIMESTAMP)
        end_str = self.end_timestamp.strftime(settings.FORMAT_TIMESTAMP) if self.end_timestamp else ''
        if self.end_timestamp:
            duration = str(self.end_timestamp - self.start_timestamp)
        else:
            now = utils.get_datetime()
            duration = str(now - self.start_timestamp)
        format_bottom = {
            "country": self.country_code.replace("_", " "),
            "start": start_str,
            "end": end_str,
            "duration": duration,
        }
        sys.stdout.write("\n\033[33;1;40m [ {country:^7} ] START: {start:<30}    END: {end:<30}    DURATION: {duration:<25}\033[0m\n".format(**format_bottom))

        if save_log:
            file_name = os.path.join('log', f'Monitoring_{self.country_code}_{self.created_at}.log')
            with open(file_name, 'a') as file:
                table_str = ""
                table_str += self.HEADER + "\n"
                table_str += "="*self.columns_table + "\n"
                for row in rows:
                    current_row = self.template_row.format(**row) + "\n"
                    for ansi_color in ("\033[31;1m", "\033[31;1m", "\033[32;1m", "\033[33;1m", "\033[0m", ):
                        current_row = current_row.replace(ansi_color, '')
                    table_str += current_row
                table_str += "\n [ {country:^7} ] START: {start:<30}    END: {end:<30}    DURATION: {duration:<25}\n".format(**format_bottom)
                file.write(table_str)

    def __get_size_tty(self):
        size_tty = shutil.get_terminal_size((80, 20))
        return size_tty.columns, size_tty.lines


class StatusPlatform:
    OK = 0
    ERROR = 1
    UPLOAD_ERROR = 2
    PLATFORM_NOT_FOUND = 3
    PLATFORM_NOT_RUNNING = 4
    PLATFORM_STOPPED = 5

    def __init__(self) -> None:
        pass

    def __iter__(self):
        iters = dict((x, y) for x, y in StatusPlatform.__dict__.items() if x[:2] != '__')
        iters.update(self.__dict__)

        for x,y in iters.items():
            yield x,y


class InfoStatusPlatform:
    OK = 'ok'
    ERROR = 'error'
    UPLOAD_ERROR = 'upload error'
    PLATFORM_NOT_FOUND = 'not found'
    PLATFORM_NOT_RUNNING = 'not running'
    PLATFORM_STOPPED = 'stopped'

    def __init__(self) -> None:
        pass

    def __iter__(self):
        iters = dict((x, y) for x, y in InfoStatusPlatform.__dict__.items() if x[:2] != '__')
        iters.update(self.__dict__)

        for x,y in iters.items():
            yield x,y
