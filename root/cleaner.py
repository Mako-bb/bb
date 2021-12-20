import os
import sys
import glob
import time
import subprocess
from .vpn_manager import VPNClient
from settings import get_logger
from settings import settings


logger = get_logger(__file__)


class Cleaner():
    @staticmethod
    def reset_mongo_service(completed_root=False):
        reseted_service = False
        if completed_root or settings.RESET_DB_WHEN_REGION_BLOCK_ENDS:
            reseted_service = True
            # Se crea un archivo para indicar a un servicio que se reinicie la DB
            with open(settings.PATH_RESET_MONGO_DB_LOCK, 'w') as file:
                pass
            time.sleep(settings.DEFAULT_TIMEOUT_MONGO_RESET)
        return reseted_service

    @classmethod
    def keep_clean(cls, completed_root=False, only_reset_db=False):
        status = cls.reset_mongo_service(completed_root)
        logger.debug(f"\033[33;1;40mMongo Service restarted: \033[32;1;40m{status}\033[0m")
        if not only_reset_db:
            VPNClient.disconnect()
            lst_cmds = ["pkill Xvfb", "pkill firefox", "pkill geckodriver"]
            for cmd in lst_cmds:
                cls.run_command(command=cmd)

    @classmethod
    def prepare_tmp_platforms_dir(cls):
        if "linux" in sys.platform:
            parent_dir = settings.PATH_PLATFORMS_TMP_PID
            if not os.path.exists(parent_dir):
                os.makedirs(parent_dir)
            else:
                file_lock_ptn = os.path.join(parent_dir, '*-upload.pid')
                files_match = glob.glob(file_lock_ptn)
                for file in files_match:
                    os.remove(file)

    @classmethod
    def remove_reset_file(cls):
        if os.path.exists(settings.PATH_RESET_MONGO_DB_LOCK):
            os.remove(settings.PATH_RESET_MONGO_DB_LOCK)

    @classmethod
    def run_command(cls, command, shell=True, background=False):
        subprocess.run(command, shell=shell)
