# -*- coding: utf-8 -*-
import json
import time
import signal
import pymongo
import argparse
from datetime import datetime, timedelta
from common import config
from root.vpn_manager import VPNClient
from root.process_manager import MultiprocessManager
from root.cleaner import Cleaner
from root import servers, utils
from settings import get_logger
from settings import settings


## EJEMPLO comando screen para que no cierre: screen sh -c 'python root.py --l US; exec bash'

## Para revisar la configuracion del vpn en los servers:
# Poner en la consola "expressvpn preferences"
# chequear el estado de "network_lock"
# En caso que figure un resultado diferente a "off"
# escribir el comando: "expressvpn preferences set network_lock off"
# y luego correr el script ubicado aca en agentes llamado "ssh-over-vpn.sh"
# se puede comprobar poniendo "ip rule" y viendo que se haya agregado

DELAY_LOOP = 3
original_sigint = None
dict_processes = dict()
logger = get_logger(__file__)


def signal_handler(signum, frame):
    signal.signal(signal.SIGINT, original_sigint)
    while True:
        try:
            for proc_name in list(dict_processes.keys()):
                try:
                    MultiprocessManager.finish_process(process=dict_processes[proc_name])
                    del dict_processes[proc_name]
                except:
                    pass
        except KeyboardInterrupt:
            continue
        else:
            break
    raise Exception(f"\n\033[31;1;40m{'ROOT CANCELLED':^40}\033[0m")


class CheckPlatforms():
    DIFF_DAYS = 14

    def __init__(self, list_platforms: list, testing=False, check_platforms=True, enable_searches=False):
        self._for_update, self._already_updated = self.comprobe(list_platforms, testing, check_platforms, enable_searches)

    @property
    def for_update(self) -> list:
        return self._for_update

    @property
    def already_updated(self) -> list:
        return self._already_updated

    @classmethod
    def comprobe(cls, list_platforms: list, testing=False, check_platforms=True, enable_searches=False) -> tuple:
        retries = 0
        while retries <= 5:
            try:
                already_updated = []
                to_update = []
                dict_to_update_in_order = {}
                
                dict_executed_platform_names = {}
                dict_platforms_time = {}
                ssh_connection = servers.MisatoConnection()
                with ssh_connection.connect() as server:
                    business = pymongo.MongoClient(port=server.local_bind_port).business
                    # Determine duration
                    if check_platforms:
                        dict_executed_platform_names, dict_platforms_time = cls.get_last_durations(business)

                    for platform in list_platforms:
                        platform_country = platform['Country']
                        platform_name = platform['PlatformName']
                        provider = platform.get('Provider')
                        platform["Countries"] = []  # Para regiones
                        
                        list_platform_code = cls.get_list_platform_code(platform_name, platform_country, provider, business)
                        if not list_platform_code:
                            logger.warning(f"No existe PlatformCode/s: {platform_name} - {platform_country}")
                            continue

                        has_searches_enabled = False
                        if enable_searches and platform_name in config()['root']['searches_next']['platforms']:
                            has_searches_enabled = True

                        platform["EnableSearches"] = has_searches_enabled
                        platform['ListPlatformCodes'] = list_platform_code.copy()
                        if not check_platforms or platform_country == "US":
                            to_update.append(platform)
                            continue

                        platform_is_outdated = False
                        now = utils.get_datetime()
                        week_number = int(now.strftime('%V'))
                        year = int(now.strftime('%Y'))

                        # Buscar el último que actualizó en la semana actual
                        query = {'PlatformCode': {'$in': list_platform_code}, 'Year': year, 'NumberOfWeek': {'$in': [week_number-1, week_number]}}
                        cursor = business['apiWave'].find(query, no_cursor_timeout=True).sort('CreatedAt', pymongo.DESCENDING)
                        apiwave_list = [{'PlatformCode': item['PlatformCode'], 'Wave': item['Wave'], 'CreatedAt': item['CreatedAt']} for item in cursor]
                        
                        query = {"PlatformCode": {"$in": list_platform_code}}
                        cursor = business['apiPlatforms'].find(query, no_cursor_timeout=True)
                        info_platforms = [{'PlatformCode': item['PlatformCode'], 'PlatformCountry': item['PlatformCountry'], 'Period': item['Period']} for item in cursor]
                        if not info_platforms:
                            continue

                        platform['Countries'] = []
                        platform['ListPlatformCodes'] = []
                        # First comprobe platform codes(not all)
                        for platform_code in list_platform_code:
                            current_api_platform = cls.get_api_platform(platform_code=platform_code, platform_name=platform_name, 
                                                                        platform_country=platform_country, info_platforms=info_platforms)
                            is_for_update = cls.check_if_is_outdated(platform_code=platform_code, 
                                                                     platform_name=platform_name, 
                                                                     platform_country=platform_country, 
                                                                     api_platform=current_api_platform, 
                                                                     apiwave_list=apiwave_list)

                            if is_for_update:
                                saved_countries = platform['Countries']
                                saved_platform_codes = platform['ListPlatformCodes']
                                current_country = current_api_platform["PlatformCountry"]
                                if current_country not in saved_countries:
                                    saved_countries.append(current_country)
                                    saved_platform_codes.append(platform_code)
                                platform["Countries"] = saved_countries.copy()
                                platform["ListPlatformCodes"] = saved_platform_codes.copy()
                                platform_is_outdated = True
                            else:
                                already_updated.append(platform_code)

                        # Si es Itunes y no tiene ninguna para actualizar se le pasa el campo 'ListPlatformCodes'
                        # como lista vacía, pero igual se las ejecutan para realziar búsquedas
                        if platform_is_outdated or has_searches_enabled:
                            _custom_platform_name = f"{platform_name}-{platform_country}"
                            if provider:
                                _custom_platform_name += f"{'-'+provider}"
                            if _custom_platform_name in dict_executed_platform_names:
                                dict_to_update_in_order[_custom_platform_name] = platform
                            else:
                                to_update.append(platform)

                    logger.info('\033[33;1;40mALREADY UPDATED: {:>20}\033[0m'.format(str(sorted(already_updated))))

                to_update.extend(cls.sort_and_add_platforms(dict_platforms_time, dict_to_update_in_order))
            except Exception as e:
                logger.warning("Reconnecting SSH... ->", e)
                VPNClient.disconnect()
                time.sleep(3)
                retries += 1
                to_update = []
                already_updated = []
            else:
                break

        return to_update, list(set(already_updated))

    @classmethod
    def get_list_platform_code(cls, platform_name: str, platform_country: str, provider: str or None, remotedb: pymongo) -> list:
        list_platform_code = []
        try:
            _config = config()['ott_sites'][platform_name]
        except:
            number_status = 3  # Clase no existe o nombre de script no coincide con la clase
            utils.generate_titanlog(status_code=number_status, platform_code=f"{platform_name}-{platform_country}")  # Custom PlatformCode
            return
        if platform_name == "JustWatch":
            if not provider:
                logger.error("Falta agregar provider al JSON")
            else:
                cursor = remotedb['titanProviders'].find_one({'Country': platform_country, 'Provider': provider}, no_cursor_timeout=True)
                try:
                    platform_code = cursor.get("PlatformCode")
                    if not platform_code:
                        raise
                    list_platform_code.append(platform_code)
                except Exception:
                    logger.error(f"\033[31;1;40mProvider no existente: {provider}. Generando log...\033[0m")
                    number_status = 3
                    utils.generate_titanlog(status_code=number_status, platform_code=f"{platform_name}-{platform_country}-{provider}")  # Custom PlatformCode
        elif platform_name == 'PlayBrands':
            list_platform_code.append('us.historyplay')
        elif platform_name == 'DiscoveryNetworks':
            list_platform_code.append('us.diynetwork')
        elif platform_name == 'HBOLatam' and platform_country == "LATAM":
            list_platform_code.append('ar.hbomax')
        elif platform_name == 'TuDiscovery':
            list_platform_code.append('ar.tlc')
        elif platform_name == 'Movistar' and platform_country != 'ALL':
            countries = _config['countries']
            try:
                platform_code = countries[platform_country]['platform_code']
            except KeyError:
                platform_code = countries[platform_country]
            list_platform_code.append(platform_code)
        elif platform_country == 'ALL':
            countries = _config['countries']
            for c in countries:
                if type(c) != str:  # Caso Movistar que es un dict
                    platform_code = countries[c]['platform_code']
                else:
                    platform_code = countries[c]
                list_platform_code.append(platform_code)
        else:
            try:
                regions = _config.get("regions") or []
                if platform_country in regions:
                    iso_countries = regions[platform_country]
                    for iso_code in iso_countries:
                        platform_code = _config['countries'].get(iso_code)
                        if not platform_code:
                            continue
                        list_platform_code.append(platform_code)
                else:
                    platform_code = _config['countries'][platform_country]
                    list_platform_code.append(platform_code)
            except:
                logger.error("\033[31;1;40mPaís no existe en config.yaml para plataforma {} - {}!!\033[0m".format(platform_country, platform_name))

        return list_platform_code

    @classmethod
    def get_last_durations(cls, remotedb: pymongo) -> tuple:
        dict_platforms_time = {}
        dict_executed_platform_names = {}
        from_date = (datetime.today() - timedelta(days=cls.DIFF_DAYS)).date().strftime(settings.FORMAT_DATE)
        query = {"Source": settings.HOSTNAME, "CreatedAt": {"$gte": from_date}}
        cursor = remotedb["rootStats"].find(query, no_cursor_timeout=True) or []
        for stats in cursor:
            executed_platforms = stats.get('ToUpdate') or []
            for p in executed_platforms:
                elapsed_seconds = p.get('ElapsedSeconds') or 0
                valid_status = True if p.get('Status', 'error') in ['ok', 'upload error'] else False
                if elapsed_seconds and valid_status:
                    platform_name = p.get('Platform') or p.get('PlatformName')
                    if not platform_name:
                        continue
                    platform_country = p['Country']
                    provider = p.get('Provider') or None
                    _custom_platform_name = f"{platform_name}-{platform_country}"
                    if provider:
                        _custom_platform_name += f"{'-'+provider}"
                    prev_elapsed_seconds = dict_executed_platform_names.get(_custom_platform_name) or 0
                    if elapsed_seconds > prev_elapsed_seconds:
                        dict_executed_platform_names[_custom_platform_name] = elapsed_seconds
        for k, v in dict_executed_platform_names.items():
            k_durations = list(dict_platforms_time.keys())
            while v in k_durations:
                v += 1
            dict_platforms_time[v] = k
        return dict_executed_platform_names, dict_platforms_time

    @classmethod
    def sort_and_add_platforms(cls, dict_platforms_time: dict, dict_to_update_in_order: dict) -> list:
        to_update = []
        sorted_times = list(reversed(sorted(dict_platforms_time.keys())))
        for duration in sorted_times:
            _custom_platform_name = dict_platforms_time[duration]
            if _custom_platform_name not in dict_to_update_in_order:
                continue
            platform = dict_to_update_in_order.pop(_custom_platform_name)
            to_update.append(platform)
        if dict_to_update_in_order:
            for k, v in dict_to_update_in_order.items():
                to_update.append(v)
        return to_update

    @classmethod
    def get_api_platform(cls, **kwargs):
        platform_code = kwargs["platform_code"]
        platform_name = kwargs["platform_name"]
        platform_country = kwargs["platform_country"]
        info_platforms = kwargs["info_platforms"]

        select_platform         = lambda _platform_code: [p for p in info_platforms if p['PlatformCode'] == _platform_code][0] or None
        select_platform_country = lambda _platform_code, _platform_country: [p for p in info_platforms if p['PlatformCode'] == _platform_code and p['PlatformCountry'] == _platform_country][0] or None

        platform_country = 'US' if platform_name == 'AppleTV' else platform_country  # Temporal
        try:
            current_api_platform = select_platform_country(platform_code, platform_country)
        except Exception:
            try:
                current_api_platform = select_platform(platform_code)
            except Exception:
                current_api_platform = info_platforms[0]
        return current_api_platform

    @classmethod
    def check_if_is_outdated(cls, **kwargs) -> bool:
        platform_code = kwargs["platform_code"]
        platform_name = kwargs["platform_name"]
        platform_country = kwargs["platform_country"]
        api_platform = kwargs["api_platform"]
        apiwave_list = kwargs["apiwave_list"]

        check_updates_in_week   = lambda _platform_code: [p["CreatedAt"] for p in apiwave_list if p["PlatformCode"] == _platform_code]

        now = utils.get_datetime()

        updates_in_week = check_updates_in_week(platform_code)
        if updates_in_week:
            max_created_at = max(updates_in_week)
            period = 1 if not api_platform else api_platform.get('Period', 1)
            diff_hours = period * 24 if period < 4 else 5 * 24  # period = 4 => 5 días
            last_update = datetime.fromtimestamp(max_created_at)
            next_update = last_update + timedelta(hours=diff_hours)
            diff_days = (now.date() - last_update.date()).days

            if period == 1 and diff_days >= 1:  # Diarias
                is_for_update = True
            elif platform_name == 'DisneyPlus' and platform_country == 'US' and diff_days >= 2:  # Temporal
                is_for_update = True
            elif platform_name == 'HBOMax':
                is_for_update = True
            elif now >= next_update:  # Si la fecha/horario actual es mayor al tiempo que se espera para actualizar nuevamente
                is_for_update = True
            else:
                is_for_update = False
        else:
            is_for_update = True
        return is_for_update


class Root():

    RESOURCES_JSON = servers.DICT_RESOURCES_JSON

    def __init__(self, location: str, since: str or None, testing=False, check_platforms=True) -> None:
        global original_sigint
        get_number_all_platforms = lambda content: sum([len(c['platforms']) for c in content])
        VPNClient.override_preferences()
        cleaner = Cleaner()

        location = location or settings.DLV_ROOT_NAME
        process_num = settings.CONCURRENT_NUMBER_ROOT
        logger.info(f"\033[32;1;40mLOCATION: {location}\033[0m")
        logger.info(f"\033[32;1;40mCONCURRENT PROCESS NUMBER: {process_num}\033[0m")

        continue_loop = True
        while continue_loop:
            enable_searches = self.enabling_searches()
            with open(self.RESOURCES_JSON[location], 'r') as file:
                data = json.load(file)
            start_root_timestamp = utils.get_datetime()
            logger.info("\033[32;1;40mINICIO: {}\033[0m".format(start_root_timestamp.strftime(settings.FORMAT_TIMESTAMP)))

            platforms_to_update = []
            platforms_already_updated = []

            # Se determina si todas las plataformas en modo testing
            # y solo realiza una vuelta
            testing = testing or self.are_testing_platforms(data)
            continue_loop = not testing

            for country_obj in data:
                country_code = country_obj["location"]
                logger.info("Current country: {:>20}".format(country_code.upper()))

                if since and country_code != since:
                    continue
                else:
                    since = None

                cleaner.prepare_tmp_platforms_dir()

                vpn_client = country_obj.get("client")
                vpn_manager = VPNClient(client=vpn_client)
                vpn_manager.disconnect()
                time.sleep(5)

                _check_platform = CheckPlatforms(list_platforms=country_obj["platforms"], testing=testing, check_platforms=check_platforms, enable_searches=enable_searches)
                for_update_list = _check_platform.for_update
                already_updated = _check_platform.already_updated
                if not for_update_list and not already_updated:
                    logger.error("PROBLEMA AL OBTENER DATOS DE APIWAVE!!!")
                    continue
                elif not for_update_list:
                    continue
                platforms_already_updated.extend(already_updated)

                if vpn_manager.is_valid_location_to_connect(location=country_code):
                    success_connection = False
                    for _ in range(2):
                        time.sleep(1)
                        vpn_manager.connect(location=country_code)
                        is_on_location, country_code_vpn = vpn_manager.comprobe_location(location=country_code, client=vpn_manager.client)
                        if is_on_location:
                            success_connection = True
                            break
                    if not success_connection:
                        current_date = time.strftime(settings.FORMAT_DATE).replace("-", "")
                        msg = f"Problema al conectar al país: {country_code}. Conexión en {country_code_vpn}"
                        logger.warning(msg)
                        file_log = f"log/VPNConnection_{country_code}.log"
                        with open(file_log, "a") as file:
                            msg = f"[ {current_date} ] {msg}"
                            file.write(msg)
                        vpn_manager.disconnect()
                        logger.warning("CONNECTION PROBLEM")
                        continue
                    else:
                        logger.info("SUCCESSFUL CONNECTION")

                original_sigint = signal.getsignal(signal.SIGINT)
                signal.signal(signal.SIGINT, signal_handler)
                if for_update_list:
                    multiprocess_manager = MultiprocessManager(for_update_list, number_processes=process_num, country_code=country_code, testing=testing)
                    executed_platforms = multiprocess_manager.collect_executed_platforms()
                    platforms_to_update.extend(executed_platforms)

                ######################
                cleaner.keep_clean() #
                ######################
                time.sleep(DELAY_LOOP)

            cleaner.keep_clean()

            end_root_timestamp = utils.get_datetime()
            logger.info("\033[32;1;40mFIN: {}\033[0m".format(end_root_timestamp.strftime(settings.FORMAT_TIMESTAMP)))

            elapsed_time = end_root_timestamp - start_root_timestamp
            elapsed_seconds  = int(elapsed_time.total_seconds())
            elapsed_time = str(timedelta(seconds=elapsed_seconds))

            logger.info("\n\033[1mRoot completed in {} hours\033[0m".format(elapsed_time))
            number_runned_platforms = len(platforms_to_update)
            if number_runned_platforms and not testing:
                try:
                    ssh_connection = servers.MisatoConnection()
                    with ssh_connection.connect() as server:
                        stats = {
                            "ToUpdate": platforms_to_update,
                            "AlreadyUpdated": platforms_already_updated,
                            "StartTimestamp": start_root_timestamp.isoformat(),
                            "EndTimestamp": end_root_timestamp.isoformat(),
                            "ElapsedTime": elapsed_time,
                            "ElapsedSeconds": elapsed_seconds,
                            "Source": settings.HOSTNAME,
                            "NumberRunnedPlatforms": number_runned_platforms,  # OLD: NumberOfPlatforms
                            "NumberAllPlatforms": get_number_all_platforms(data),
                            "Message": "Completed root",
                            "CreatedAt": time.strftime("%Y-%m-%d"),
                        }
                        business = pymongo.MongoClient(port=server.local_bind_port).business
                        business['rootStats'].insert_one(stats)
                except Exception as e:
                    logger.error(f"Stats roots not saved. Exception -> {e}")

            logger.info('Starting over in an hour...')
            cleaner.keep_clean(completed_root=True, only_reset_db=True)
            time.sleep(3600) #Para evitar conexiones ssh si no tiene nada que actualizar, espera una hora, se podria poner mas tiempo todavia
            logger.info('\n\n\033[33;1;5;40m  {:^30} \033[0m\n'.format('STARTING OVER'))
            cleaner.remove_reset_file()

    # TODO: Agregar opción para realizar búsquedas para plataformas con un país y no regiones
    @staticmethod
    def enabling_searches() -> bool:
        """Método que habilita a algunas plataformas a realizar búsquedas en la siguiente ejecución del root.
        Comienza creándose un archivo(si no existe). Una vez creado, se descartan los checkeos si están actualizadas
        para plataformas ubicadas en el config: config()["root"]["searches_next"]["platforms"].
        Solo sirve para plataformas que ejecutan varias, como Itunes ALL

        Funcionamiento:
            - si existe el archivo se procede a eliminarlo y se habilitan las búsquedas.
            - si el archivo no existe se lo crea, para que en la siguiente vuelta se habiliten 
                nuevamente las búsquedas. En esta vuelta no se hacen búsquedas.

        Returns:
            - enable :class:`bool`: Indica si se realizan las búsquedas. 
        """
        import os
        try:
            os.remove(settings.PATH_ENABLE_SEARCHES_NEXT)
            enable = True
        except OSError:
            with open(settings.PATH_ENABLE_SEARCHES_NEXT, "w") as _:
                pass
            enable = False
        del os
        return enable

    @staticmethod
    def are_testing_platforms(data: dict) -> bool:
        """Determina si todas las plataformas a ejecutar se realizan en modo testing.
        Args:
            - data :class:`dict`: Contiene todas las plataformas y regiones a ejecutar. 

        Returns:
            - all_testing :class:`bool`: Indica si todas las plataformas se ejecutarán en modo testing
        """
        list_process_status = []
        for country_obj in data:
            list_process_status.extend([p['Process'] == 'testing' for p in country_obj["platforms"]])
        all_testing = all(list_process_status)
        return all_testing


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--l', help="Location [CA/CA2/DE/DE-Test1/DE2/DE3/GB/GB2/SG/NL/US/MX]", type=str)
    parser.add_argument('--s', help="Since [CountryName]", type=str)
    parser.add_argument('--date', help="Indicates the date from the beginning of the root.", type=str)
    parser.add_argument('--testing', help="This option runs all platforms in testing mode and does not generate reports/logs", nargs='?', default=False, const=True)
    parser.add_argument('--no-check', help="This only does not check if the platform is outdate.", nargs='?', default=False, const=True)

    args = parser.parse_args()
    location = args.l
    since = args.s
    testing = args.testing
    check_platforms = not args.no_check

    root = Root(location, since, testing, check_platforms)
