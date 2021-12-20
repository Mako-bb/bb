import logging
import logging.handlers


RESET_SEQ = "\033[0m"
COLOR_SEQ = "\033[1;%dm"
BOLD_SEQ = "\033[1m"
BLACK, RED, GREEN, YELLOW, BLUE, MAGENTA, CYAN, WHITE = range(8)

COLORS = {
    'WARNING': YELLOW,
    'INFO': WHITE,
    'DEBUG': BLUE,
    'CRITICAL': YELLOW,
    'ERROR': RED
}

BLACK_LIST_LOGGING = (
    "urllib3",
    "urllib3.connectionpool",
    "paramiko.transport", 
    "paramiko.transport.sftp",
    "sshtunnel.SSHTunnelForwarder", 
    "seleniumwire.proxy.client", 
    "seleniumwire.proxy.handler",
)


class Formatter(logging.Formatter):
    DATE_FMT = '%Y-%m-%dT%T%Z'

    def __init__(self, msg, use_color=True):
        logging.Formatter.__init__(self, msg)
        logging.Formatter.default_time_format = self.DATE_FMT
        self.use_color = use_color

    def format(self, record):
        levelname = record.levelname
        if self.use_color and levelname in COLORS:
            levelname_color = COLOR_SEQ % (30 + COLORS[levelname]) + levelname + RESET_SEQ
            record.levelname = levelname_color
        return logging.Formatter.format(self, record)


# Custom logger class with multiple destinations
class CustomLogger(logging.Logger):
    # FORMAT = "[$BOLD%(asctime)-20s$RESET][%(levelname)-18s]  %(message)s ($BOLD%(filename)s$RESET:%(lineno)d)"
    FORMAT = '$BOLD%(asctime)-20s$RESET %(filename)-18s %(levelname)-8s: %(message)s'

    def __init__(self, name):
        color_format = self.formatter_message(self.FORMAT, True)
        logging.Logger.__init__(self, name, logging.INFO)

        formatter = Formatter(color_format)
        logging_handler = logging.StreamHandler()
        logging_handler.setFormatter(formatter)

        self.addHandler(logging_handler)

    @classmethod
    def formatter_message(cls, message: str, use_color=True):
        if use_color:
            message = message.replace("$RESET", RESET_SEQ).replace("$BOLD", BOLD_SEQ)
        else:
            message = message.replace("$RESET", "").replace("$BOLD", "")
        return message

    @staticmethod
    def ignore_other_loggers():
        for _ in BLACK_LIST_LOGGING:
            logging.getLogger(_).setLevel(logging.CRITICAL)
