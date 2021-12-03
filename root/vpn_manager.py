#!/usr/bin/env python3
# Script para realizar la conexión de openvpn
# TODO: Agregar para otros clientes VPN
import os
import sys
import time
import requests
import argparse
import threading
import subprocess
import multiprocessing
try:
    import settings
except:
    path = os.path.abspath('.')
    sys.path.insert(1, path)
from root import utils
from settings import get_logger
from settings import settings


DEFAULT_TIMEOUT = 2
logger = get_logger(__file__)


class VPNManagement():
    def __init__(self) -> None:
        self._client_vpn = None
        self._location = None
        self._protocol = None
        self._disconnect = False
        self._create_connection = False
        self._is_alive = False

    def start(self):
        self._t_call_proc = threading.Thread(target=self._call_proc, name="t_call_proc")
        self._t_management = threading.Thread(target=self._management, name="t_management")
        self._t_call_proc.start()
        self._t_management.start()
        self._t_call_proc.join()
        self._t_management.join()

    @staticmethod
    def run_command(command, shell=True):
        sp = subprocess.run(command, shell=shell)

    @classmethod
    def worker(cls, client_vpn, location, protocol):
        command = f"/usr/bin/sudo /usr/sbin/openvpn /etc/openvpn/{client_vpn}/servers/{protocol.lower()}/{location}.{protocol.upper()}.ovpn 1>/dev/null 2>/dev/null"
        cls.run_command(command)

    def _call_proc(self):
        was_created_process = False
        dict_processes = {}
        while True:
            logger.debug("CALL PROCESS. WAITING REQUEST")
            if not was_created_process and self._create_connection:
                process_name = f'process-{self._client_vpn}-{self._location}'
                args = (self._client_vpn, self._location, self._protocol,)
                process = multiprocessing.Process(name=process_name, target=self.worker, args=args)
                process.start()
                dict_processes[process_name] = process
                was_created_process = self._is_alive = True
            for proc_name in list(dict_processes.keys()):
                if not dict_processes[proc_name].is_alive():
                    dict_processes[proc_name].terminate()
                    dict_processes[proc_name].join()
                    time.sleep(DEFAULT_TIMEOUT)
                    del dict_processes[proc_name]
                    was_created_process = self._is_alive = False
            time.sleep(DEFAULT_TIMEOUT)

    def _management(self):
        config = utils.parse_config(filename=settings.PATH_VPN_SETTINGS_INI)
        iterval_timeout = config['DEFAULT'].getint('vpnintervaltimeout') or DEFAULT_TIMEOUT
        while True:
            logger.debug("MAIN PROCESS. WAITING REQUEST")
            if os.path.exists(settings.PATH_REQUIRED_VPN):
                config = utils.parse_config(filename=settings.PATH_VPN_SETTINGS_INI)
                if 'VPN' in config:
                    client = config['VPN'].get('client')
                    if not client:
                        os.remove(settings.PATH_REQUIRED_VPN)
                        time.sleep(DEFAULT_TIMEOUT)
                        continue
                    protocol = config['VPN'].get('protocol', settings.DEFAULT_VPN_PROTOCOL)
                    connect_vpn = config['VPN'].getboolean('connect', False)
                    disconnect_vpn = config['VPN'].getboolean('disconnect', False)
                    if disconnect_vpn and self._is_alive:
                        command = f"/usr/bin/sudo /bin/kill -SIGINT $(pgrep openvpn) 2>/dev/null; /usr/bin/sudo /bin/bash /usr/local/bin/network-routing.sh"
                        self.run_command(command)
                        self._disconnect = True
                        self._create_connection = False
                        logger.info("Disconnecting...")
                    elif connect_vpn and not self._create_connection:
                        location = config['VPN'].get('location')
                        if not self.connect(client, location, protocol):
                            continue
                    config['VPN']["connect"] = "no"
                    config['VPN']["disconnect"] = "no"
                    time.sleep(DEFAULT_TIMEOUT-1)
                    with open(settings.PATH_VPN_SETTINGS_INI, "w") as configfile:
                        config.write(configfile)

                os.remove(settings.PATH_REQUIRED_VPN)
            time.sleep(iterval_timeout)

    def connect(self, client, location, protocol):
        if not location:
            os.remove(settings.PATH_REQUIRED_VPN)
            time.sleep(DEFAULT_TIMEOUT)
            status = False
        else:
            self._create_connection = True
            self._client_vpn = client
            self._location = location
            self._protocol = protocol
            logger.info(f"Connecting to {location}")
            status = True
        return status

    def disconnect(self):
        command = f"/usr/bin/sudo /bin/kill -SIGINT $(pgrep openvpn) 2>/dev/null; /usr/bin/sudo /bin/bash /usr/local/bin/network-routing.sh"
        self.run_command(command)
        self._disconnect = True
        self._create_connection = False
        logger.warning("Disconnecting...")

    def __del__(self):
        pass


class VPNClient():
    CLIENTS = ('express', 'hma', 'le-vpn', 'cyberghost', 'purevpn', 'nordvpn',)
    DEFAULT_VPN_CLIENT = CLIENTS[0]
    PREFERRED_PROTOCOL = settings.DEFAULT_VPN_PROTOCOL

    def __init__(self, client) -> None:
        # logger.info(f"DEFAULT VPN CLIENT: {self.DEFAULT_VPN_CLIENT}")
        self._client = client or self.DEFAULT_VPN_CLIENT
        self._is_active = False

    @property
    def client(self):
        return self._client

    @client.setter
    def client(self, client: str) -> None:
        self._client = client

    @staticmethod
    def run_command(command, shell=True):
        sp = subprocess.run(command, shell=shell)

    @classmethod
    def __emit_signal(cls, config) -> None:
        """Crea un archivo temporal y guarda la configuración del cliente VPN actual\n
        en el archivo de configuración. El archivo temporal es para que el servicio detecte\n
        dicho archivo y proceda a leer el archivo de configuración recientemente guardado.

        Args:
        - config :class:`ConfigParser`: Configuraciones.

        Returns:
        - :class:`None`
        """
        with open(settings.PATH_VPN_SETTINGS_INI, 'w') as configfile:
            config.write(configfile)
        time.sleep(0.1)
        with open(settings.PATH_REQUIRED_VPN, "w") as file:
            pass
        time.sleep(5)

    def connect(self, location) -> None:
        client = self.client
        protocol = self.PREFERRED_PROTOCOL
        if client == self.DEFAULT_VPN_CLIENT:
            command = f"expressvpn connect {location} 2>/dev/null"
        elif client in ('hma', 'le-vpn',):
            config = utils.parse_config(filename=settings.PATH_VPN_SETTINGS_INI)
            if not 'VPN' in config:
                config['VPN'] = {}
            config['VPN']['client'] = client
            config['VPN']['location'] = location
            config['VPN']['connect'] = 'yes'
            config['VPN']['disconnect'] = 'no'
            config['VPN']['protocol'] = protocol
            self.__emit_signal(config=config)
            command = None
        else:
            # TODO: Completar los comandos para otros clientes VPN
            command = f"{client} connect {location} 2>/dev/null"
        if command:
            self.run_command(command=command)

    @classmethod
    def disconnect(cls, client=None) -> None:
        if not client:
            clients = cls.CLIENTS
        else:
            clients = [client]
        for client in clients:
            if client in ('hma', 'le-vpn', cls.DEFAULT_VPN_CLIENT):
                config = utils.parse_config(filename=settings.PATH_VPN_SETTINGS_INI)
                if not 'VPN' in config:
                    config['VPN'] = {}
                config['VPN']['client'] = client
                config['VPN']['connect'] = 'no'
                config['VPN']['disconnect'] = 'yes'
                cls.__emit_signal(config=config)
                command = f"expressvpn disconnect 2>/dev/null"
            else:
                # TODO: Completar los comandos para otros clientes VPN
                command = f"{client} disconnect 2>/dev/null"
            if command:
                cls.run_command(command=command)

    @classmethod
    def is_valid_client(cls, client) -> bool:
        return client in cls.CLIENTS

    @classmethod
    def comprobe_location(cls, location, client) -> tuple:
        url = "https://www.ifconfig.io/all.json"
        headers = {
            "Host": "ifconfig.io",
            "User-Agent": "curl/7.68.0",
            "Accept": "*/*",
        }
        if client == cls.DEFAULT_VPN_CLIENT:
            country_code_vpn = location
        else:
            country_code_vpn = "UNDETERMINED"
            for i in range(3):
                try:
                    res = requests.request('GET', url, headers=headers, timeout=10)
                    content_json = res.json()
                except Exception:
                    time.sleep(3)
                    continue
                else:
                    country_code_vpn = content_json["country_code"]
                    break
        if len(location) > 2:  # Ej: USMI2
            status = location[:2] == country_code_vpn
        else:
            status = location == country_code_vpn
        if not status:
            logger.warning(f"[ ERROR ] No conectado a {location}\n\tVPN en {country_code_vpn}")
        return status, country_code_vpn

    @classmethod
    def is_valid_location_to_connect(cls, location) -> bool:
        return location.upper() != 'SIN VPN' and not location.startswith('REGION')

    @classmethod
    def override_preferences(cls):
        cls.run_command("expressvpn preferences set network_lock off 2>/dev/null")


# TODO: Completar acciones para el modo cliente
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--daemon', help='Start script as a service.', nargs='?', default=False, const=True)
    parser.add_argument('--client', help='Start script as client mode.', nargs='?', default=False, const=True)
    parser.add_argument('--disconnect', help='Disconnect any connection.', nargs='?', default=False, const=True)
    args = parser.parse_args()

    daemon_mode = args.daemon
    client_mode = args.client
    disconnect = args.disconnect

    if daemon_mode:
        vpn_management = VPNManagement()
        vpn_management.start()
    elif disconnect:
        VPNClient.disconnect()
    else:
        pass
