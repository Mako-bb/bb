import time
import random
import subprocess
import multiprocessing
from datetime import timedelta
from .specs_platforms import Platform, TableMonitoring, StatusPlatform, InfoStatusPlatform
from . import utils
try:
    import settings
except ModuleNotFoundError:
    import os
    import sys
    path = os.path.abspath('.')
    sys.path.insert(1, path)
from settings import get_logger
from common import config


DELAY_LOOP = 3
dict_processes = dict()
logger = get_logger(__file__)


class MultiprocessManager():
    def __init__(self, for_update_list, number_processes, country_code, testing=False):
        manager = multiprocessing.Manager()
        return_dict = manager.dict()

        self._executed_platforms = []

        logger.info('\033[33;1;40mPlatforms to run: {:>20}\033[0m\n'.format(str(len(for_update_list))))
        dict_processes.clear()
        dict_platforms = dict()
        scraped_platforms = []
        finished_processes = []
        some_still_alive = True
        first_print = True

        for platform in for_update_list:
            code_name = self.obtain_code_name(platform=platform)
            dict_platforms[code_name] = {
                'PlatformName': platform['PlatformName'],
                'Country': platform['Country'],
                'Provider': platform.get('Provider'),
                'Process': platform['Process'],
                'EnableSearches': platform['EnableSearches'],
                'Countries': platform['Countries'],
                'ListPlatformCodes': platform['ListPlatformCodes'],
            }
        
        #######################################################
        # Monitorización
        #######################################################
        monitor = TableMonitoring(dict_platforms, country_code)
        #######################################################

        try:
            while some_still_alive:
                if len(finished_processes) == len(dict_platforms):
                    some_still_alive = False
                elif len(dict_processes) < number_processes:
                    for code_name in dict_platforms:
                        platform = dict_platforms[code_name]
                        proc_name = f"process-{code_name}"
                        if proc_name not in scraped_platforms:
                            process = multiprocessing.Process(name=proc_name, target=self.process_worker, args=(platform, return_dict, code_name, testing, ))
                            process.start()
                            dict_processes[proc_name] = process
                            dict_platforms[code_name]['start_timestamp'] = utils.get_datetime()
                            scraped_platforms.append(proc_name)
                            break

                # Procesos actuales
                proc_keys = list(dict_processes.keys())
                for proc_name in proc_keys:
                    number_status = None
                    code_name = proc_name.replace('process-', '')
                    parts = code_name.split("-")[0].split("_")  # Amazon_US-Provider -> ['Amazon', 'US']
                    _platform_name = "_".join(parts[:-1])
                    _country = parts[-1]
                    start_time = dict_platforms[code_name]['start_timestamp']
                    diff_time = utils.get_datetime() - start_time
                    elapsed_hours = diff_time.total_seconds() // 3600
                    max_hours_expected = self.determine_max_hours_expected(_platform_name, _country)
                    if elapsed_hours >= max_hours_expected and len(_country) == 2:  # DEBUG: Mejorar condición
                        stop_process = True
                        if not testing:
                            current_platform = dict_platforms[code_name]
                            try:
                                list_platformcodes = current_platform["ListPlatformCodes"]
                                _platform_code = random.choice(list_platformcodes)  # Casos como Itunes ALL se eligen al azar. La mayoría son listas de un elemento.
                            except:
                                _platform_code = f"{_platform_name}-{_country}"  # DEBUG: Ver razón por la cual no obtiene lista de PlatformaCodes
                            number_status = 5  # stopped
                            utils.generate_titanlog(status_code=number_status, platform_code=_platform_code)
                    elif not dict_processes[proc_name].is_alive() and not proc_name in finished_processes:
                        stop_process = True
                    else:
                        stop_process = False
                    
                    if stop_process:
                        time.sleep(2)
                        self.finish_process(process=dict_processes[proc_name])
                        del dict_processes[proc_name]
                        code_name = proc_name.replace('process-', '')

                        start_time = dict_platforms[code_name]['start_timestamp']
                        end_time = utils.get_datetime()
                        difference = end_time - start_time
                        total_seconds = int(difference.total_seconds())
                        duration = str(timedelta(seconds=total_seconds))
                        status = self.determine_status(number_status, return_dict, code_name)
                        dict_platforms[code_name].update({
                            'end_timestamp': end_time,
                            'total_seconds': total_seconds,
                            'duration': duration,
                            'status': status,
                        })
                        finished_processes.append(proc_name)
                
                time.sleep(DELAY_LOOP)
                monitor.update(first_print=first_print)
                first_print = False

            end_current_country = utils.get_datetime()
            monitor.end_timestamp = end_current_country
            monitor.update(save_log=True)

            for code_name in dict_platforms:
                current_platform = {
                    'Platform': dict_platforms[code_name]['PlatformName'],
                    'Country': dict_platforms[code_name]['Country'],
                }
                if dict_platforms[code_name].get('Provider'):
                    current_platform.update({
                        'Provider': dict_platforms[code_name]['Provider'],
                    })
                current_platform['ElapsedTime'] = dict_platforms[code_name]['duration']
                current_platform['ElapsedSeconds'] = dict_platforms[code_name]['total_seconds']
                current_platform['Status'] = dict_platforms[code_name]['status']
                if country_code != 'Sin VPN' and not country_code.startswith('REGION'):
                    current_platform['CoountryVPN'] = country_code
                self._executed_platforms.append(current_platform)
        except KeyboardInterrupt:
            for proc_name in list(dict_processes.keys()):
                try:
                    self.finish_process(process=dict_processes[proc_name])
                    del dict_processes[proc_name]
                except:
                    pass

    def collect_executed_platforms(self):
        return self._executed_platforms

    def process_worker(self, platform, return_dict, code_name, testing=False):
        instance_platform = Platform(platform)
        instance_platform.run(testing)
        return_dict[code_name] = instance_platform.check_status()

    @classmethod
    def obtain_code_name(cls, platform):
        format = {
            'PlatformName': platform['PlatformName'],
            'Country': platform['Country'],
            'Provider': f"-{platform['Provider']}" if platform.get('Provider') else "",
        }
        return "{PlatformName}_{Country}{Provider}".format(**format)

    @classmethod
    def determine_max_hours_expected(cls, platform_name, country):
        durations = config()['root']['durations']
        max_hours_expected = durations['default']
        long_platforms = durations['long_platforms']
        if platform_name in long_platforms:
            list_countries = long_platforms[platform_name]
            if country in list_countries:
                max_hours_expected = durations['max_duration']
        return max_hours_expected

    @classmethod
    def determine_status(cls, status_code, return_dict, code_name):
        _sp_instance = StatusPlatform()
        dict_statuses = dict(_sp_instance)
        item = {k: v for k, v in dict_statuses.items() if v == status_code}
        if item:
            key_code = list(item.keys())[0]
            _isp_instance = InfoStatusPlatform()
            status = dict(_isp_instance)[key_code]
        elif code_name in return_dict:
            status_code = return_dict[code_name]
            item = {k: v for k, v in dict_statuses.items() if v == status_code}
            key_code = list(item.keys())[0]
            _isp_instance = InfoStatusPlatform()
            status = dict(_isp_instance).get(key_code, "unexpected error")
        else:
            status = 'unexpected error'  # Posiblemente problema de conexión de red
        return status

    @classmethod
    def finish_process(cls, process):
        try:
            process.terminate()
            process.join()
        except:
            pass


# TODO: Manejador de procesos para no generar procesos zombie
def run_command(command, shell=True, background=False):
    subprocess.run(command, shell=shell)
