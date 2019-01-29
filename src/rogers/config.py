""" Configuration for Rogers
"""
from .util import get_logger

import os
import configparser
from os import path as path

log = get_logger(__name__)

CWD = os.getcwd()

MODULE_DIR = os.path.dirname(__file__)
DEFAULT_CONF_PATH = os.path.join(MODULE_DIR, 'default.ini')

# path to yara rules
YARA_RULE_PATH = path.abspath(path.join(path.dirname(__file__), 'data/yara/index.yar'))

# location for storing/searching for samples
DEFAULT_SAMPLE_DIR = os.path.join(CWD, 'samples')
DEFAULT_SAMPLE_DIR = os.environ.get('ROGERS_SAMPLE_DIR', DEFAULT_SAMPLE_DIR)

# location to store nn indexes and metadata
DEFAULT_INDEX_DIR = os.path.join(CWD, 'index')
DEFAULT_INDEX_DIR = os.environ.get('ROGERS_INDEX_DIR', DEFAULT_INDEX_DIR)


# global settings
settings = {'SAMPLE_DIR': DEFAULT_SAMPLE_DIR,
            'INDEX_DIR': DEFAULT_INDEX_DIR}


def get(key):
    global settings
    if key in settings:
        return settings[key]
    else:
        raise IndexError("Configuration key doesn't not exist! - %s", key)


def sample_path(path):
    return os.path.join(settings.get('SAMPLE_DIR'), path)


def index_path(path):
    return os.path.join(settings.get('INDEX_DIR'), path)


def configure(cfg_path=None):
    """ Load rogers conf file and set environment
    :param cfg_path:
    :return:
    """
    cfg_path = cfg_path or DEFAULT_CONF_PATH
    log.debug("Loading config: %s", cfg_path)

    config = configparser.ConfigParser()
    config.read(cfg_path)

    settings['SAMPLE_DIR'] = os.path.abspath(config.get('rogers', 'sample_dir', fallback=DEFAULT_SAMPLE_DIR))
    settings['INDEX_DIR'] = os.path.abspath(config.get('rogers', 'index_dir', fallback=DEFAULT_INDEX_DIR))

    # XOR
    settings['XORI_PATH'] = os.path.abspath(config.get('xori', 'bin_path', fallback=''))
    settings['XORI_CONF_PATH'] = os.path.abspath(config.get('xori', 'conf_path', fallback=''))

    settings['VT_API_KEY'] = config.get('virustotal', 'api_key', fallback='')

    sample_dir = settings.get('SAMPLE_DIR')
    index_dir = settings.get('INDEX_DIR')

    os.makedirs(sample_dir, exist_ok=True)
    os.makedirs(index_dir, exist_ok=True)

    log.debug("Sample Directory: %s", sample_dir)
    log.debug("Index Directory: %s", index_dir)
