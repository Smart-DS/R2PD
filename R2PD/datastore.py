"""
This module provides classes for accessing site-level wind and solar data from
internal and external data stores.
"""

from configparser import ConfigParser
import os
import pandas as pds
from .resourcedata import WindResource, SolarResource


class DataStore(object):
    ROOT_PATH = None

    def __init__(self, wind_dir=None, solar_dir=None):
        if self.ROOT_PATH is not None:
            if wind_dir is None:
                self._wind_root = os.path.join(self.ROOT_PATH, 'wind')
            else:
                self._wind_root = os.path.join(self.ROOT_PATH, wind_dir)

            wind_meta_path = os.path.join(self._wind_root,
                                          'wind_site_meta.json')
            self._wind_meta = pds.read_json(wind_meta_path)

            if solar_dir is None:
                self._solar_root = os.path.join(self.ROOT_PATH, 'solar')
            else:
                self._solar_root = os.path.join(self.ROOT_PATH, solar_dir)

            solar_meta_path = os.path.join(self._solar_root,
                                           'solar_site_meta.json')
            self._solar_meta = pds.read_json(solar_meta_path)

    def __repr__(self):
        return '{n} at {i}'.format(n=self.__class__.__name__,
                                   i=self.ROOT_PATH)

    @classmethod
    def repo_size(cls, path):
        repo_size = []
        for (path, dirs, files) in os.walk(path):
            for file in files:
                file_name = os.path.join(path, file)
                repo_size += os.path.getsize(file_name) * 10**-9

        return repo_size

    @classmethod
    def decode_config_entry(cls, entry):
        if entry == 'None' or '':
            return None
        else:
            return entry


class ExternalDataStore(DataStore):
    """
    Abstract class to define interface for accessing stores of resource data.
    """
    def __init__(self, username=None, password=None):
        self._user = username
        self._pass = password
        super(DataStore, self).__init__()

    @classmethod
    def connect(cls, config):
        """
        Reads the configuration, if provided. From configuration and defaults,
        determines location of internal data cache. If the cache is not yet
        there, creates it. Returns an InternalDataStore object open and ready
        for querying / adding data.
        """
        config_parser = ConfigParser()
        config_parser.read(config)
        root_path = cls.decode_config_entry(config_parser.get('repository',
                                                              'root_path'))

        if root_path is not None:
            ExternalDataStore.ROOT_PATH = root_path
        else:
            raise ValueError('root path must be defined!')

        wind_dir = cls.decode_config_entry(config_parser.get('repository',
                                                             'wind_directory'))

        solar_dir = config_parser.get('repository', 'solar_directory')
        solar_dir = cls.decode_config(solar_dir)

        username = cls.decode_config(config_parser.get('repository',
                                                       'username'))

        password = cls.decode_config(config_parser.get('repository',
                                                       'password'))

        return ExternalDataStore(wind_dir=wind_dir, solar_dir=solar_dir,
                                 username=username, password=password)


class BetaStore(ExternalDataStore):
    ROOT_PATH = '/scratch/mrossol/Resource_Repo'
    pass


class DRPower(ExternalDataStore):
    pass


class InternalDataStore(DataStore):
    """
    This class manages an internal cache of already downloaded resource data,
    and other Resource Data Tool information that should persist.

    The default location for the internal cache will be in a place like
    Users/$User/AppData, but the user can set a different location by passing
    in a configuration file.

    A configuration file can also be used to set user library locations, for
    pointing to externally provided shapers and formatters.
    """
    def __init__(self, max_size=None):
        self._max_size = max_size
        super(DataStore, self).__init__()

    @classmethod
    def connect(cls, config=None):
        """
        Reads the configuration, if provided. From configuration and defaults,
        determines location of internal data cache. If the cache is not yet
        there, creates it. Returns an InternalDataStore object open and ready
        for querying / adding data.
        """
        if config is None:
            InternalDataStore.ROOT_PATH = os.getcwd()
        else:
            config_parser = ConfigParser()
            config_parser.read(config)
            root_path = config_parser.get('local_cache', 'root_path')
            root_path = cls.decode_config_entry(root_path)
            if root_path is not None:
                InternalDataStore.ROOT_PATH = root_path

            max_size = cls.decode_config_entry(config_parser.get('local_cache',
                                                                 'max_size'))
            if max_size == 'None' or '':
                max_size = None

        return InternalDataStore(max_size)

    @property
    def cache_size(self):
        """
        Calculate size of local cache in GB
        """
        total_cache = self.repo_size(self.ROOT_PATH)
        wind_cache = self.repo_size(self._wind_root)
        solar_cache = self.repo_size(self._solar_root)

        return total_cache, wind_cache, solar_cache

    def cache_data(self, sites):
        """
        Saves each (ResourceLocation, ResourceData) tuple to disk and logs it
        in the registry / database.
        """

    def get_resource(self, dataset, site_id, frac=None):
        """
        Return resourcedata.Resource object
        If any site_id is not valid or not in the store error is raised
        """
        if dataset == 'wind':
            return WindResource(self._wind_meta.loc[site_id], self._wind_path,
                                frac=frac)
        elif dataset == 'solar':
            return SolarResource(self._solar_meta.loc[site_id],
                                 self._solar_path, frac=frac)
        else:
            raise ValueError("Invalid dataset type, must be 'wind' or 'solar'")
