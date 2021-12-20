import time
import paramiko
import sshtunnel
from settings import settings
from paramiko import MissingHostKeyPolicy


class SSHConnection():

    def __init__(self, default_mode=True) -> None:
        self.server = None
        self._use_paramiko = not default_mode

    def connect(self, ip_address, port, ssh_username, ssh_private_key, ssh_private_key_password=None, remote_bind_address=None):
        if self._use_paramiko:
            self.server = paramiko.SSHClient()
            self.server.set_missing_host_key_policy(MissingHostKeyPolicy())
            self.server.connect(
                ip_address,
                port=port,
                username=ssh_username,
                key_filename=ssh_private_key
            )
            server = self.server
        else:
            sshtunnel.SSH_TIMEOUT = sshtunnel.TUNNEL_TIMEOUT = settings.DEFAULT_SSH_TIMEOUT
            server = sshtunnel.open_tunnel(
                (ip_address, port),
                ssh_username=ssh_username,
                ssh_pkey=ssh_private_key,
                ssh_private_key_password=ssh_private_key_password,
                remote_bind_address=remote_bind_address
            )
            server.start()
            time.sleep(10)
        return server

    def stop(self):
        try:
            self.server.stop()
        except:
            pass
        try:
            self.server.close()
        except:
            pass

    def __del__(self):
        self.stop()


class MongoConnection():
    pass
