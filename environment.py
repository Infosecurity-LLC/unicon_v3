import logging
import os
import socket
from socutils import get_settings
from modules import tools

logger = logging.getLogger('unicon')

script_location = os.path.dirname(os.path.realpath(__file__))
os.chdir(script_location)

# ## variables
path_to_app_settings = os.getenv("APP_CONFIG_PATH", default='appconfig')
settings_file_path = f'{path_to_app_settings}/settings.yaml'
settings = get_settings(settings_file_path)

tools.prepare_logging(settings)
logger.debug("Loaded config from %s", settings_file_path)

target_config = get_settings(f'{path_to_app_settings}/buffering_targets.yaml')
logger.debug("Loaded buffering configuration %s", target_config)

rule_path: str = f"{path_to_app_settings}/rules"  # scheduler rules

nxlog_config = get_settings(f'{path_to_app_settings}/nxlog.yaml')
logger.debug("Loaded nxlog cofig %s", nxlog_config)
nxlog_client = None

redis = None

app_location = socket.gethostname()
app_version = "3.0.0"
###

# connections
tools.health_check_redis(settings['redis'])
tools.health_check_rmq(settings['rabbitmq']['connection'])
###
