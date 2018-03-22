import os
from configparser import ConfigParser, SectionProxy

DEFAULT_CONFIG = """
[general]
[redis]
url=redis://localhost:6359
[queue]
prefix=trq:queue:
name=default_queue
[task]
prefix=trq:task:
result_expiry=500
"""

def set_config_from_environ(config, section, key, env):
    if env in os.environ:
        config.setdefault(section, SectionProxy(config, section))[key] = os.environ[env]

def setup_config(config_file=None):

    config = ConfigParser()
    # always read in default config first
    config.read_string(DEFAULT_CONFIG)
    # read in extra config
    if config_file is not None:
        if not os.path.exists(config_file):
            raise FileNotFoundError(config_file)
        config.read(config_file)
    elif 'CONFIG_FILE' in os.environ:
        if not os.path.exists(config_file):
            raise FileNotFoundError(config_file)
        config.read(os.environ['CONFIG_FILE'])

    # override values from environment variables
    set_config_from_environ(config, 'redis', 'url', 'REDIS_URL')
    set_config_from_environ(config, 'task', 'prefix', 'TRQ_TASK_PREFIX')
    set_config_from_environ(config, 'task', 'result_expiry', 'TRQ_TASK_RESULT_EXPIRY')
    set_config_from_environ(config, 'queue', 'prefix', 'TRQ_QUEUE_PREFIX')
    set_config_from_environ(config, 'queue', 'name', 'TRQ_QUEUE_NAME')

    return config

def get_global_config():
    return _global_config

_global_config = setup_config()
