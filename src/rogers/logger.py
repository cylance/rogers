""" Basic logging for roggers
"""
import logging


def init_logging(level=logging.INFO):
    """ Setup default console logging
    :param level:
    :return:
    """
    logger = logging.getLogger('rogers')
    logger.setLevel(level)
    ch = logging.StreamHandler()
    ch.setLevel(level)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)


def get_logger(name):
    """ Get logging handler
    :param name:
    :return:
    """
    return logging.getLogger(name)
