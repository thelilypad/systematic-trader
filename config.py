import os
import typing
from typing import List, Any
import logging
import yaml
import os


ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE_NAME = f'{ROOT_DIR}/config.yaml'

SPECIFIED_PROPERTIES = [
    'GOOGLE_CONSOLE_ACCESS_FILENAME',
    'COINMARKETCAL_API_KEY',
    'SQL_URI',
    'FTX_API_KEY',
    'FTX_API_SECRET',
    'FTX_SUBACCOUNT_NAME',
    'RABBITMQ_SERVER_URI',
]


def specifiable_properties():
    return SPECIFIED_PROPERTIES


class ConfigException(Exception):
    pass


class ConfigValue:
    def __init__(self, value: Any):
        self.value = value

    def is_empty(self):
        return not self.value

    def unwrap(self):
        if not self.value:
            raise ConfigException("Unable to unwrap with no value provided.")
        return self.value

    def map(self, func: typing.Callable) -> Any:
        if not self.value:
            return None
        return func(self.value)


class _Config:
    def __init__(self):
        self.__properties__: dict = {}
        if os.path.exists(CONFIG_FILE_NAME):
            with open(CONFIG_FILE_NAME) as f:
                yaml_data_map = yaml.safe_load(f)
                if type(yaml_data_map) is dict:
                    self.__properties__ = yaml_data_map
                else:
                    raise yaml.YAMLError("Provided yaml file needs to be in tree format.")
        else:
            self._load_from_os()

    def _load_from_os(self):
        for key in SPECIFIED_PROPERTIES:
            prop = os.getenv(key)
            if prop:
                path = key.split(".")
                properties_setter = self.__properties__
                while len(path) > 1:
                    elem = path.pop(0)
                    if elem in properties_setter:
                        if not type(properties_setter[elem]) is dict:
                            raise ConfigException("[" + elem + "] exists in config path already and is not dict.")
                        properties_setter = properties_setter[elem]
                    else:
                        properties_setter[elem] = {}
                properties_setter[path[0]] = prop
            else:
                logging.warning("[" + key + "] not set in OS Env!")

    def get_property(self, property_name: str, if_missing: Any = None) -> ConfigValue:
        if property_name not in specifiable_properties():
            logging.warning("Provided property: [" + property_name + "] not in specifiable properties.")
            return ConfigValue(if_missing)
        path = property_name.split(".")
        properties_accessor = self.__properties__
        for item in path:
            if item not in path:
                logging.warning("Provided property: [" + property_name + "] not found in config.")
                return ConfigValue(if_missing)
            properties_accessor = properties_accessor[item]
        return ConfigValue(properties_accessor)


Config = _Config()
