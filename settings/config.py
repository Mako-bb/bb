#!/usr/bin/env python
"""
Deploy settings.
"""
import os
import socket
from common import config as _config

BASEDIR = os.path.abspath(os.path.dirname(__file__))


class Config:
    API_TOKEN = "Meg2VN>ge4Ufk1^8dJ&4<EMr=TN)FIJEIc%%G=sOo~O?o&6$peP&"
    SECRET_KEY = os.getenv('SECRET_KEY', '1637d30261b0980089505af7d38aff42')
    SALT = os.environ.get("SALT", "f54fed89bc3d5a35673f88737679fc02")

    DEBUG = False
    TESTING = False

    # PATHS
    PATH_RESET_MONGO_DB_LOCK = "/tmp/reset-mongo-db.lock"
    PATH_VPN_SETTINGS_INI = "/tmp/vpn-settings.ini"
    PATH_REQUIRED_VPN = "/tmp/required-vpn-connection.tmp"
    PATH_PLATFORMS_TMP_PID = "/tmp/platforms/"
    PATH_RUNNING_PLATFORMS = "/tmp/running-platforms.csv"
    PATH_ENABLE_SEARCHES_NEXT = "/tmp/root-enable-searches-next-loop.lock"

    RESET_DB_WHEN_REGION_BLOCK_ENDS = bool(os.getenv('RESET_MONGO_IN_COUNTRY_LAP', 0))
    MAX_HOURS_EXPECTED_FOR_PLATFORM = 21
    DEFAULT_TIMEOUT_MONGO_RESET = 20
    HEADERS_RUNNING_PLATFORMS_CSV = ('PlatformName', 'Country', 'Provider', 'Status', 'Runtime', 'StartTimeStamp', 'EndTimeStamp',)

    DEFAULT_LOGGER_NAME = "root"
    DEFAULT_VPN_PROTOCOL = "UDP"

    HOSTNAME = socket.gethostname()
    DLV_ROOT_NAME = os.getenv('DLV_ROOT_NAME')
    CONCURRENT_NUMBER_ROOT = int(os.getenv('CONCURRENT_NUMBER_ROOT', 4))
    DEFAULT_SSH_TIMEOUT = 30.0
    PRIVATE_KEY = "bb-private"
    MISATO_KEY = MISATO_SERVER_NAME = "misato"
    KAJI_KEY = KAJI_SERVER_NAME = "kaji"
    DEFAULT_PORT = 22
    DEFAULT_PORT_PROD = 31415
    DEFAULT_USER = "bb"
    DEFAULT_USER_LOG = "bblog"
    DEFAULT_USER_DATA = "bbdata"

    FORMAT_DATE = "%Y-%m-%d"
    FORMAT_TIMESTAMP = "%Y-%m-%d %H:%M:%S"

    DEFAULT_LOG_MSG_ERROR = "Undetermined error."


class DevelopmentConfig(Config):
    DEBUG = True
    MONGODB_DATABASE_URI = _config()['mongo']['host']


class TestingConfig(Config):
    DEBUG = True
    TESTING = True
    MONGODB_DATABASE_URI = _config()['mongo']['host']


class ProductionConfig(Config):
    DEBUG = False
    MONGODB_DATABASE_URI = os.environ.get("MONGODB_DATABASE_URI") or _config()['mongo']['host']


class DockerConfig(ProductionConfig):
    DEBUG = False
    MONGODB_DATABASE_URI = os.environ.get("MONGODB_DOCKER_DATABASE_URI")  or _config()['mongo']['host']


config = {
    'development': DevelopmentConfig,
    'testing': TestingConfig,
    'production': ProductionConfig,
    'docker': DockerConfig,
    'default': DevelopmentConfig
}
