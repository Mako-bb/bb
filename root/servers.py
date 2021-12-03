#!/usr/bin/env python
from pathlib import Path
try:
    import settings
except ModuleNotFoundError:
    import os
    import sys
    path = os.path.abspath('.')
    sys.path.insert(1, path)
from settings.connection import SSHConnection
from settings.security import Security
from settings import settings


DICT_RESOURCES_JSON = {
    "CA": "root/resources/rootCA.json",  # CA, BR, AR, resto de LATAM
    "CA2": "root/resources/rootCA2.json",
    "DE": "root/resources/rootDE.json",
    "DE2": "root/resources/rootDE2.json",  # (google-play, amazon, amazonprime, disneyplus)
    "DE3": "root/resources/rootDE3.json",
    "DE4": "root/resources/rootDE4.json",
    "GB": "root/resources/rootGB.json",  # GB, ES, PT, FR, IE
    "GB2": "root/resources/rootGB2.json",  # iTunes
    "SG": "root/resources/rootSG.json",  # Asia y Oceania (AU, NZ)
    "NL": "root/resources/rootNL.json",  # Europa
    "US": "root/resources/rootUS.json",  # Solo US
    "US2": "root/resources/rootUS2.json",
    "MX": "root/resources/rootMexico.json",  # Solo Mexico
    "DE-Test1": "root/resources/rootDE-Test1.json",
    "Contabo": "root/resources/rootContabo.json",
    "Contabo2": "root/resources/rootContabo2.json",
}


class Connection():

    def __init__(self, server_name) -> None:
        self._server_name = server_name

    def connect(self, username, ip_address=None, port=None, private_key_password=None, remote_bind_address=None, use_paramiko=False):
        default_mode = not use_paramiko
        self._conn_server = SSHConnection(default_mode=default_mode)
        security = Security()
        ip_address = ip_address or security.obtain_ip_address(server_name=self._server_name)
        port = self.select_port(port=port)
        private_key = self.select_private_key()
        private_key_password = private_key_password or self.get_private_key_passwd()
        remote_bind_address = remote_bind_address or ('127.0.0.1', 27017)
        return self._conn_server.connect(ip_address, port=port, ssh_username=username, ssh_private_key=private_key, ssh_private_key_password=private_key_password, remote_bind_address=remote_bind_address)

    def stop(self):
        try:
            self._conn_server.stop()
        except:
            pass

    def select_port(self, port):
        if any(self._server_name == sv_name for sv_name in (settings.MISATO_SERVER_NAME, settings.KAJI_SERVER_NAME,)):
            port = settings.DEFAULT_PORT_PROD
        else:
            port = port or settings.DEFAULT_PORT
        return port

    def select_private_key(self):
        base_path = Path(__file__).parent.parent
        if any(self._server_name == sv_name for sv_name in (settings.MISATO_SERVER_NAME, settings.KAJI_SERVER_NAME,)):
            private_key = str(base_path / self._server_name)
        else:
            private_key = str(base_path / settings.PRIVATE_KEY)
        return private_key

    def get_private_key_passwd(self):
        if any(self._server_name == sv_name for sv_name in (settings.MISATO_SERVER_NAME, settings.KAJI_SERVER_NAME,)):
            pk_passwd = "KLM2012a"  # TODO: Cambiar esto
        else:
            pk_passwd = None
        return pk_passwd

    def __del__(self):
        self.stop()


class MisatoConnection(Connection):

    def __init__(self) -> None:
        super().__init__(server_name=settings.MISATO_SERVER_NAME)

    def connect(self, username=None, remote_bind_address=None, use_paramiko=False):
        security = Security()
        username = username or settings.DEFAULT_USER
        ip_address = security.obtain_ip_address(server_name=settings.MISATO_SERVER_NAME)
        port = self.select_port(port=None)
        return super().connect(username, ip_address=ip_address, port=port, remote_bind_address=remote_bind_address, use_paramiko=use_paramiko)

    def stop(self):
        super().stop()


class KajiConnection(Connection):

    def __init__(self) -> None:
        super().__init__(server_name=settings.KAJI_SERVER_NAME)

    def connect(self, username=None, remote_bind_address=None, use_paramiko=False):
        security = Security()
        username = username or settings.DEFAULT_USER
        ip_address = security.obtain_ip_address(server_name=settings.KAJI_SERVER_NAME)
        port = self.select_port(port=None)
        return super().connect(username, ip_address=ip_address, port=port, remote_bind_address=remote_bind_address, use_paramiko=use_paramiko)

    def stop(self):
        super().stop()
