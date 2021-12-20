import os
import logging
from settings.config import config
from settings.logger import CustomLogger

DEFAULT_ENV_NAME = 'AGENTES_ENV'

def determine_env_name():
    return os.getenv(DEFAULT_ENV_NAME) or 'default'


def load_settings():
    config_name = determine_env_name()
    return config[config_name]


def get_logger(name='root', loglevel=logging.INFO) -> logging.Logger:
    config_name = determine_env_name()
    if config_name in ('default', 'development',):
        loglevel = logging.ERROR

    logging.setLoggerClass(CustomLogger)
    logging.basicConfig(level=loglevel)

    CustomLogger.ignore_other_loggers()
    logger = logging.getLogger(name)
    logger.propagate = False
    return logger


settings = load_settings()
