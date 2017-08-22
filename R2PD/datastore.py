"""
This module provides classes for accessing site-level wind and solar data from
internal and external data stores.
"""

from configparser import ConfigParser
import os
import numpy as np
import pandas as pds
import pexpect
from .powerdata import GeneratorNodeCollection
from .queue import nearest_power_nodes, nearest_met_nodes
from .resourcedata import WindResource, SolarResource


class DataStore(object):
    ROOT_PATH = None

    def __init__(self, wind_dir=None, solar_dir=None):
        if self.ROOT_PATH is not None:
            if wind_dir is None:
                self._wind_root = os.path.join(self.ROOT_PATH, 'wind')
            else:
                self._wind_root = os.path.join(self.ROOT_PATH, wind_dir)

            if solar_dir is None:
                self._solar_root = os.path.join(self.ROOT_PATH, 'solar')
            else:
                self._solar_root = os.path.join(self.ROOT_PATH, solar_dir)
        else:
            raise ValueError('ROOT_PATH must be defined')

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
    def __init__(self, local_cache=None, username=None, password=None,
                 **kwargs):
        if local_cache is None:
            local_cache = InternalDataStore.connect()
        else:
            error_message = "Expecting local_cache to be instance of \
InternalDataStore, but is {:}.".format(type(local_cache))
            assert isinstance(local_cache, InternalDataStore), error_message

        self._local_cache = local_cache
        self._username = username
        self._password = password
        super(ExternalDataStore, self).__init__(**kwargs)

        meta_path = os.path.join(self._wind_root, 'wind_site_meta.json')
        self.download(meta_path, self._local_cache._wind_root,
                      self._username, self._password)
        meta_path = os.path.join(self._local_cache._wind_root,
                                 'wind_site_meta.json')
        self.wind_meta = pds.read_json(meta_path)
        self.wind_meta.index.name = 'site_id'
        self.wind_meta = self.wind_meta.sort_index()

        meta_path = os.path.join(self._solar_root, 'solar_site_meta.json')
        self.download(meta_path, self._local_cache._solar_root,
                      self._username, self._password)
        meta_path = os.path.join(self._local_cache._solar_root,
                                 'solar_site_meta.json')
        self.solar_meta = pds.read_json(meta_path)
        self.solar_meta.index.name = 'site_id'
        self.solar_meta = self.solar_meta.sort_index()

    @classmethod
    def connect(cls, config, repo):
        """
        Reads the configuration. From configuration and defaults,
        determines location of external datastore.
        """
        config_parser = ConfigParser()
        config_parser.read(config)
        wind_dir = cls.decode_config_entry(config_parser.get('repository',
                                                             'wind_directory'))

        solar_dir = config_parser.get('repository', 'solar_directory')
        solar_dir = cls.decode_config(solar_dir)

        username = cls.decode_config(config_parser.get('repository',
                                                       'username'))

        password = cls.decode_config(config_parser.get('repository',
                                                       'password'))

        if config_parser.has_section('local_cache'):
            local_cache = InternalDataStore.connect(config=config)
        else:
            local_cache = None

        error_message = "Expecting repo to be instance of \
ExternalDataStore, but is {:}.".format(type(repo))
        assert isinstance(repo, ExternalDataStore), error_message

        return repo(local_cache=local_cache, wind_dir=wind_dir,
                    solar_dir=solar_dir, username=username, password=password)

    @classmethod
    def download(cls, src, dst, username, password):
        pass

    def nearest_neighbors(self, node_collection, forecasts=True):
        dataset = node_collection._dataset
        if dataset == 'wind':
            resource_meta = self._wind_meta
        else:
            resource_meta = self._solar_meta

        if isinstance(node_collection, GeneratorNodeCollection):
            resource_type = 'power'
            nearest_nodes = nearest_power_nodes(node_collection,
                                                resource_meta)
            site_ids = np.concatenate(nearest_nodes['site_ids'].values)
            site_ids = np.unique(site_ids)
        else:
            resource_type = 'met'
            nearest_nodes = nearest_met_nodes(node_collection,
                                              resource_meta)
            site_ids = nearest_nodes['site_id'].values

            if dataset == 'wind':
                if resource_type == 'power':
                    data_size = len(site_ids) * self.WIND_FILE_SIZES['power']
                    if forecasts:
                        data_size += len(site_ids) * self.WIND_FILE_SIZES['power']

        return nearest_nodes,


class Peregrine(ExternalDataStore):
    ROOT_PATH = '/scratch/mrossol/Resource_Repo'
    # Average File size in MB
    WIND_FILE_SIZES = {'met': 14, 'power': 4, 'fcst': 1}
    SOLAR_FILE_SIZES = {'met': 10, 'irradiance': 50, 'power': 20, 'fcst': 1}}

    @classmethod
    def download(cls, src, dst, username, password, timeout=30):
        if os.path.basename(src) == os.path.basename(dst):
            file_path = dst
        else:
            file_path = file_path = os.path.join(dst, os.path.basename(src))

        if not os.path.isfile(file_path):
            command = 'rsync -avzP {u}@peregrine.nrel.gov:{src} \
{dst}'.format(u=username, src=src, dst=dst)
            try:
                with pexpect.spawn(command, timeout=timeout) as child:
                    expect = "{:}@peregrine.nrel.gov's \
password:".format(username)
                    child.expect(expect)
                    child.sendline(password)
                    child.expect(pexpect.EOF)
            except Exception:
                raise


class DRPower(ExternalDataStore):
    pass


class InternalDataStore(DataStore):
    """
    This class manages an internal cache of already downloaded resource data,
    and other Resource Data Tool information that should persist.

    The default location for the internal cache will be in the current \
    working diretory, but the user can set a different location by passing
    in a configuration file.

    A configuration file can also be used to set user library locations, for
    pointing to externally provided shapers and formatters.
    """
    ROOT_PATH = os.path.join(os.getcwd(), 'R2PD_Cache')

    def __init__(self, max_size=None):
        self._max_size = max_size
        super(InternalDataStore, self).__init__()

        if not os.path.exists(self._wind_root):
            os.makedirs(self._wind_root)

        if not os.path.exists(self._solar_root):
            os.makedirs(self._solar_root)

    @classmethod
    def connect(cls, config=None):
        """
        Reads the configuration, if provided. From configuration and defaults,
        determines location of internal data cache. If the cache is not yet
        there, creates it. Returns an InternalDataStore object open and ready
        for querying / adding data.
        """
        if config is None:
            max_size = None
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

        return InternalDataStore(max_size=max_size)

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
