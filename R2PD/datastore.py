"""
This module provides classes for accessing site-level wind and solar data from
internal and external data stores.
"""

from configparser import ConfigParser
import os
import pandas as pds
import pexpect
from .powerdata import GeneratorNodeCollection
from .queue import nearest_power_nodes, nearest_met_nodes
from .resourcedata import WindResource, SolarResource
import shutil
from .Timeout import Timeout

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
    def decode_config_entry(cls, entry):
        if entry == 'None' or '':
            return None
        else:
            return entry


class ExternalDataStore(DataStore):
    """
    Abstract class to define interface for accessing stores of resource data.
    """
    # Average File size in MB currently estimates
    WIND_FILE_SIZES = {'met': 14, 'power': 4, 'fcst': 1}
    SOLAR_FILE_SIZES = {'met': 10, 'irradiance': 50, 'power': 20, 'fcst': 1}

    def __init__(self, local_cache=None, username=None, password=None,
                 **kwargs):
        if local_cache is None:
            local_cache = InternalDataStore.connect()
        elif not isinstance(local_cache, InternalDataStore):
            raise RuntimeError("Expecting local_cache to be instance of \
InternalDataStore, but is {:}.".format(type(local_cache)))

        self._local_cache = local_cache
        self._username = username
        self._password = password
        super(ExternalDataStore, self).__init__(**kwargs)

        meta_path = os.path.join(self._wind_root, 'wind_site_meta.json')
        self.download(meta_path, self._local_cache._wind_root)
        meta_path = os.path.join(self._local_cache._wind_root,
                                 'wind_site_meta.json')
        self.wind_meta = pds.read_json(meta_path)
        self.wind_meta.index.name = 'site_id'
        self.wind_meta = self.wind_meta.sort_index()

        meta_path = os.path.join(self._solar_root, 'solar_site_meta.json')
        self.download(meta_path, self._local_cache._solar_root)
        meta_path = os.path.join(self._local_cache._solar_root,
                                 'solar_site_meta.json')
        self.solar_meta = pds.read_json(meta_path)
        self.solar_meta.index.name = 'site_id'
        self.solar_meta = self.solar_meta.sort_index()

    @classmethod
    def connect(cls, config):
        """
        Reads the configuration. From configuration and defaults,
        determines location of external datastore.
        """
        config_parser = ConfigParser()
        config_parser.read(config)
        wind_dir = cls.decode_config_entry(config_parser.get('repository',
                                                             'wind_directory'))

        solar_dir = config_parser.get('repository', 'solar_directory')
        solar_dir = cls.decode_config_entry(solar_dir)

        username = cls.decode_config_entry(config_parser.get('repository',
                                                             'username'))

        password = cls.decode_config_entry(config_parser.get('repository',
                                                             'password'))

        if config_parser.has_section('local_cache'):
            local_cache = InternalDataStore.connect(config=config)
        else:
            local_cache = None

        return cls(local_cache=local_cache, wind_dir=wind_dir,
                   solar_dir=solar_dir, username=username, password=password)

    def download(self, src, dst):
        pass

    def nearest_neighbors(self, node_collection):
        dataset = node_collection._dataset
        if dataset == 'wind':
            resource_meta = self._wind_meta
        else:
            resource_meta = self._solar_meta

        if isinstance(node_collection, GeneratorNodeCollection):
            nearest_nodes = nearest_power_nodes(node_collection,
                                                resource_meta)
        else:
            nearest_nodes = nearest_met_nodes(node_collection,
                                              resource_meta)

        return nearest_nodes

    def get_resource(self, dataset, site_id, frac=None):
        """
        Return resourcedata.Resource object
        If any site_id is not valid or not in the store error is raised
        """
        if dataset == 'wind':
            return WindResource(self.wind_meta.loc[site_id],
                                self._local_cache._wind_root, frac=frac)
        elif dataset == 'solar':
            return SolarResource(self.solar_meta.loc[site_id],
                                 self._local_cache._solar_root, frac=frac)
        else:
            raise ValueError("Invalid dataset type, must be 'wind' or 'solar'")


class Peregrine(ExternalDataStore):
    ROOT_PATH = '/scratch/mrossol/Resource_Repo'

    def download(self, src, dst, timeout=30):
        if os.path.basename(src) == os.path.basename(dst):
            file_path = dst
        else:
            file_path = os.path.join(dst, os.path.basename(src))

        if not os.path.isfile(file_path):
            command = 'rsync -avzP {u}@peregrine.nrel.gov:{src} \
{dst}'.format(u=self._username, src=src, dst=dst)
            expect = "{:}@peregrine.nrel.gov's \
password:".format(self._username)
            try:
                with pexpect.spawn(command, timeout=timeout) as child:
                    child.expect(expect)
                    code = child.sendline(self._password)
                    if code == 11:
                        child.expect(pexpect.EOF)

                exit_code = child.exitstatus
                if exit_code != 0:
                    raise RuntimeError('Download failed, check inputs!')
            except Exception:
                raise


class Scratch(ExternalDataStore):
    ROOT_PATH = '/scratch/mrossol/Resource_Repo'

    def download(self, src, dst, timeout=30):
        if os.path.basename(src) == os.path.basename(dst):
            file_path = dst
        else:
            file_path = os.path.join(dst, os.path.basename(src))

        if not os.path.isfile(file_path):
            try:
                with Timeout(timeout):
                    shutil.copy(src, dst)
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

    def __init__(self, size=None):
        self._size = size
        super(InternalDataStore, self).__init__()

        if not os.path.exists(self._wind_root):
            os.makedirs(self._wind_root)

        if not os.path.exists(self._solar_root):
            os.makedirs(self._solar_root)

        self.initialize_cache_metas()
        self.refresh_cache_meta('wind')
        self.refresh_cache_meta('solar')

    @classmethod
    def connect(cls, config=None):
        """
        Reads the configuration, if provided. From configuration and defaults,
        determines location of internal data cache. If the cache is not yet
        there, creates it. Returns an InternalDataStore object open and ready
        for querying / adding data.
        """
        if config is None:
            size = None
        else:
            config_parser = ConfigParser()
            config_parser.read(config)
            root_path = config_parser.get('local_cache', 'root_path')
            root_path = cls.decode_config_entry(root_path)
            if root_path is not None:
                InternalDataStore.ROOT_PATH = root_path

            size = cls.decode_config_entry(config_parser.get('local_cache',
                                                             'size'))
            size = int(size)
            if size == 'None' or '':
                size = None

        return InternalDataStore(size=size)

    @classmethod
    def get_cache_size(cls, path):
        repo_size = []
        for (path, dirs, files) in os.walk(path):
            for file in files:
                file_name = os.path.join(path, file)
                repo_size += os.path.getsize(file_name) * 10**-9

        return repo_size

    def initialize_cache_metas(self):
        for dataset in ['wind', 'solar']:
            if dataset == 'wind':
                cache_path = os.path.join(self._wind_root,
                                          '{:}_cache.csv'.format(dataset))
                self._wind_cache = cache_path
                columns = ['met', 'power', 'fcst', 'fcst-prob',
                           'sub_directory']
            elif dataset == 'solar':
                cache_path = os.path.join(self._solar_root,
                                          '{:}_cache.csv'.format(dataset))
                self._solar_cache = cache_path
                columns = ['met', 'irradiance', 'power', 'fcst', 'fcst-prob',
                           'sub_directory']

            if not os.path.isfile(cache_path):
                cache_meta = pds.DataFrame(columns=columns)
                cache_meta.index.name = 'site_id'

                cache_meta.to_csv(cache_path)

    def refresh_cache_meta(self, dataset):
        if dataset == 'wind':
            cache_path = self._wind_cache
        elif dataset == 'solar':
            cache_path = self._solar_cache
        else:
            raise ValueError("Invalid dataset type, must be 'wind' or \
'solar'")

        root_path = os.path.split(cache_path)[0]
        cache_meta = pds.read_csv(cache_path, index_col=0)
        cache_sites = cache_meta.index

        file_paths = []
        for path, subdirs, files in os.walk(root_path):
            for name in files:
                if name.endswith('.hdf5'):
                    file_paths.append(os.path.join(path, name))

        for file in file_paths:
            sub_dir, name = os.path.split(file)
            name = os.path.splitext(name)[0]
            _, resource, site_id = name.split('_')
            site_id = int(site_id)

            sub_dir = int(os.path.basename(sub_dir))

            if site_id not in cache_sites:
                cache_meta.loc[site_id] = False
                cache_meta.loc[site_id, 'sub_directory'] = sub_dir

            cache_meta.loc[site_id, resource] = True
            cache_sites = cache_meta.index

        cache_meta.to_csv(cache_path)

    def cache_site(self, dataset, site):
        if dataset == 'wind':
            cache_path = self._wind_cache
        elif dataset == 'solar':
            cache_path = self._solar_cache
        else:
            raise ValueError("Invalid dataset type, must be 'wind' or \
'solar'")

        cache_meta = pds.read_csv(cache_path, index_col=0)
        cache_sites = cache_meta.index

        sub_dir, name = os.path.split(site)
        name = os.path.splitext(name)[0]
        _, resource, site_id = name.split('_')
        site_id = int(site_id)

        sub_dir = int(os.path.basename(sub_dir))

        if site_id not in cache_sites:
            cache_meta.loc[site_id] = False
            cache_meta.loc[site_id, 'sub_directory'] = sub_dir

        cache_meta.loc[site_id, resource] = True

        cache_meta.to_csv(cache_path)

    def check_cache(self, dataset, site_id, resource_type):
        if dataset == 'wind':
            cache_path = self._wind_cache
        elif dataset == 'solar':
            cache_path = self._solar_cache
        else:
            raise ValueError("Invalid dataset type, must be 'wind' or \
'solar'")

        cache_meta = pds.read_csv(cache_path, index_col=0)
        cache_sites = cache_meta.index

        if site_id in cache_sites:
            if cache_meta.loc[site_id, resource_type]:
                return cache_meta.loc[site_id, 'sub_directory']
            else:
                return None
        else:
            return None

    def cache_data(self, dataset, sites):
        """
        Saves each (ResourceLocation, ResourceData) tuple to disk and logs it
        in the registry / database.
        """
        for site in sites:
            self.cache_site(dataset, site)

    @property
    def cache_size(self):
        """
        Calculate size of local cache in GB
        """
        total_cache = self.get_cache_size(self.ROOT_PATH)
        wind_cache = self.get_cache_size(self._wind_root)
        solar_cache = self.get_cache_size(self._solar_root)

        return total_cache, wind_cache, solar_cache
