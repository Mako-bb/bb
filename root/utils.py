import os
import re
import time
import pymongo
import subprocess
import configparser
from datetime import datetime, timedelta, timezone
from . import servers
try:
    import settings
except ModuleNotFoundError:
    import sys
    path = os.path.abspath('.')
    sys.path.insert(1, path)
from settings import get_logger
from settings import settings
from root.specs_platforms import StatusPlatform


logger = get_logger(__file__)


def get_datetime(hutimestamp=None, diff_hours=-3):
    return datetime.now(tz=timezone.utc).replace(tzinfo=None) + timedelta(hours=diff_hours, minutes=0)


def parse_config(filename):
    config = configparser.ConfigParser()
    if not os.path.exists(filename):
        config['DEFAULT'] = {}
        config['DEFAULT']['vpnintervaltimeout'] = '2'
        with open(filename, "w") as configfile:
            config.write(configfile)
    else:
        try:
            config.read(filename)
        except configparser.MissingSectionHeaderError:
            os.remove(filename)
            config['DEFAULT'] = {}
            config['DEFAULT']['vpnintervaltimeout'] = '2'
            with open(filename, "w") as configfile:
                config.write(configfile)
    return config


def generate_titanlog(status_code=None, **kwargs):
    platform_code = kwargs.get("platform_code")
    platform_name = kwargs.get("platform_name")
    country       = kwargs.get("country")
    path_log      = kwargs.get("path_log")
    type_error, msg_error = determine_error(path_log=path_log, status_code=status_code)

    payload = {
        "Error"        : type_error,
        "Message"      : msg_error,
        "CreatedAt"    : time.strftime("%Y-%m-%d"),
        "Timestamp"    : get_datetime(),
        "Source"       : settings.HOSTNAME
    }
    filter_query = {"Source": settings.HOSTNAME, "Message": msg_error}
    if platform_code:
        payload.update({"PlatformCode" : platform_code})
        filter_query.update({"PlatformCode" : platform_code})
    else:
        payload.update({"Country": country, "PlatformName": platform_name})
        filter_query.update({"Country": country, "PlatformName": platform_name})
    try:
        ssh_connection = servers.MisatoConnection()
        with ssh_connection.connect() as server:
            business = pymongo.MongoClient(port=server.local_bind_port).business
            business['titanLog'].update_one(filter_query, {u'$set': payload}, upsert=True)
    except Exception as e:
        logger.error(f"Error when creating titanlog. Exception -> {e}")


# TODO: Determinar el error que no permite scrapear correctamente.
def determine_error(path_log, status_code=None):
    if status_code == StatusPlatform.PLATFORM_NOT_FOUND:
        type_error = "exception"
        msg_error = "Exception/Error: ModuleNotFoundError -> Class name does not exist or does not match the script name."
    elif status_code == StatusPlatform.PLATFORM_STOPPED:
        type_error = "elapsed_time"
        msg_error = "Platform removed from root for taking a long time."
    elif path_log:
        type_error = "exception"
        regex_error = re.compile(r'^\w+E(rror|xception):')
        cmd_last_lines_log = f"tail -50 {path_log}"
        out = subprocess.check_output(cmd_last_lines_log.split(" "))
        last_lines_log = str(out).replace(r"\n", "\n")
        parts = last_lines_log.split('\n')
        match = regex_error.search(parts[-1])  # Search last line
        if match:
            first_part  = parts[-1].strip()
            second_part = " -> " + parts[-2].strip() if len(parts) > 1 else "."
            msg_error = "Exception/Error: " + first_part + second_part
        elif "KeyboardInterrupt" in parts[-1]:
            msg_error = "KeyboardInterrupt: Interruption of the script during execution."
        elif "Killed" == parts[-1].strip():
            msg_error = "Process killed by the kernel."
        else:
            msg_error = ""
            reversed_lines = list(reversed(parts))
            for counter, line in enumerate(reversed_lines):
                match = regex_error.search(line)
                if match:
                    first_part  = line.strip()
                    second_part = " -> " + reversed_lines[counter+1].strip() if counter + 1 <= len(reversed_lines) else "."
                    msg_error = "Exception/Error: " + first_part + second_part
                    break
            if not msg_error:
                if "KeyboardInterrupt" in last_lines_log:
                    msg_error = "KeyboardInterrupt: Interruption of the script during execution."
    else:
        msg_error = ""
    if msg_error:
        msg_error = ' '.join(msg_error.split()).replace(r"\'", "\'").strip()
    else:
        msg_error = settings.DEFAULT_LOG_MSG_ERROR
    return type_error, msg_error
