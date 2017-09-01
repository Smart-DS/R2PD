"""
This module provides classes for accessing site-level wind and solar data
from internal and external data stores.
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
    """
    Abstract class to define interface for accessing stores of resource data.
    """
    ROOT_PATH = None

    def __init__(self, wind_dir=None, solar_dir=None):
        """
        Initialize generic DataStore object

        Parameters
        ----------
        wind_dir : 'string'
            Name of directory in which wind data is/will be stored
            if None set to 'wind'
        solar_dir: 'string'
            Name of directory in which solar data is/will be stored
            if None set to 'solar'
        """
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
        """
        Print the type of datastore and its ROOT_PATH

        Returns
        ---------
        'str'
            type of datastore and its ROOT_PATH
        """
        return '{n} at {i}'.format(n=self.__class__.__name__,
                                   i=self.ROOT_PATH)

    @classmethod
    def decode_config_entry(cls, entry):
        """
        Decode config entry converting missing or 'None' entires to None

        Parameters
        ----------
        entry : 'string'
            entry from ConfigParser call

        Returns
        ---------
        entry : 'string' or None
            if config entry is not 'None' or empty the entry is returned
            else None is returned
        """
        if entry == 'None' or '':
            return None
        else:
            return entry


class ExternalDataStore(DataStore):
    """
    Abstract class to define interface for accessing external stores
    of resource data.
    """
    # Average File size in MB currently estimates
    WIND_FILE_SIZES = {'met': 14, 'power': 5, 'fcst': 2}
    SOLAR_FILE_SIZES = {'met': 10, 'irradiance': 20, 'power': 1, 'fcst': 1}

    def __init__(self, local_cache=None, **kwargs):
        """
        Initialize ExternalDataStore object

        Parameters
        ----------
        local_cache : 'InternalDataStore'
            InternalDataStore object represening internal data cache
        **kwargs :
            kwargs for DataStore
        """
        if local_cache is None:
            local_cache = InternalDataStore.connect()
        elif not isinstance(local_cache, InternalDataStore):
            raise RuntimeError("Expecting local_cache to be instance of \
InternalDataStore, but is {:}.".format(type(local_cache)))

        self._local_cache = local_cache
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
        determines initializes ExternalDataStore object.

        Parameters
        ----------
        config : 'string'
            Path to .ini configuration file.
            See library/config.ini for an example

        Returns
        ---------
        'ExternalDataStore'
            Initialized ExternalDataStore object
        """
        config_parser = ConfigParser()
        config_parser.read(config)

        wind_dir = config_parser.get('repository', 'wind_directory')
        wind_dir = cls.decode_config_entry(wind_dir)

        solar_dir = config_parser.get('repository', 'solar_directory')
        solar_dir = cls.decode_config_entry(solar_dir)

        if config_parser.has_section('local_cache'):
            local_cache = InternalDataStore.connect(config=config)
        else:
            local_cache = None

        return cls(wind_dir=wind_dir, solar_dir=solar_dir,
                   local_cache=local_cache)

    def download(self, src, dst):
        """
        Abstract method to download src to dst

        Parameters
        ----------
        src : 'string'
            Path to source file to be downloaded
        dst : 'string'
            Path to destination directory of file path
        """
        pass

    def nearest_neighbors(self, node_collection):
        """
        Find the nearest neighbor resource sites for all nodes in
        Node_collection

        Parameters
        ----------
        node_collection : 'NodeCollection'
            Collection of nodes for which resource sites are to be identified

        Returns
        ---------
        nearest_nodes : 'pandas.DataFrame'
            Dataframe with the nearest neighbor resource sites for each node
        """
        dataset = node_collection._dataset
        if dataset == 'wind':
            resource_meta = self.wind_meta
        else:
            resource_meta = self.solar_meta

        if isinstance(node_collection, GeneratorNodeCollection):
            nearest_nodes = nearest_power_nodes(node_collection,
                                                resource_meta)
        else:
            nearest_nodes = nearest_met_nodes(node_collection,
                                              resource_meta)

        return nearest_nodes

    def get_resource(self, dataset, site_id, frac=None):
        """
        Initialize and return Resource class object for specified resource site

        Parameters
        ----------
        dataset : 'string'
            'wind' or 'solar'
        site_id : int
            Resource site_id
        frac : 'float'
            Fraction of resource to use from resource site

        Returns
        ---------
        'Resource'
            Wind or Solar Resource class instance
        """
        cache = self._local_cache.check_cache(dataset, site_id)
        if cache is not None:
            if dataset == 'wind':
                return WindResource(self.wind_meta.loc[site_id],
                                    self._local_cache._wind_root, frac=frac)
            elif dataset == 'solar':
                return SolarResource(self.solar_meta.loc[site_id],
                                     self._local_cache._solar_root, frac=frac)
            else:
                raise ValueError("Invalid dataset type, must be 'wind' or \
'solar'")
        else:
            raise RuntimeError('{d} site {s} is not in local cache!'
                               .format(d=dataset, s=site_id))


class Peregrine(ExternalDataStore):
    """
    Class object for External DataStore on Peregrine
    """
    ROOT_PATH = '/scratch/mrossol/Resource_Repo'

    def __init__(self, username, password, **kwargs):
        """
        Initialize Peregrine object

        Parameters
        ----------
        username : 'string'
            Peregrine username
        password : 'string'
            Peregrine password
        **kwargs :
            kwargs for ExternalDataStore
        """
        self._username = username
        self._password = password
        super(Peregrine, self).__init__(**kwargs)

    def download(self, src, dst, timeout=30):
        """
        Method to download src file from Peregrine to local dst

        Parameters
        ----------
        src : 'string'
            Path to source file to be downloaded
        dst : 'string'
            Path to destination directory of file path
        timeout : 'int'
            Timeout in seconds
        """
        if os.path.basename(src) == os.path.basename(dst):
            file_path = dst
        else:
            file_path = os.path.join(dst, os.path.basename(src))

        if not os.path.isfile(file_path):
            command = 'rsync -avz {u}@peregrine.nrel.gov:{src} \
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

    @classmethod
    def connect(cls, config):
        """
        Reads the configuration. From configuration and defaults,
        determines initializes Peregine DataStore object.

        Parameters
        ----------
        config : 'string'
            Path to .ini configuration file.

        Returns
        ---------
        'Peregrine'
            Initialized Peregrine object
        """
        config_parser = ConfigParser()
        config_parser.read(config)

        username = cls.decode_config_entry(config_parser.get('repository',
                                                             'username'))

        password = cls.decode_config_entry(config_parser.get('repository',
                                                             'password'))

        if config_parser.has_section('local_cache'):
            local_cache = InternalDataStore.connect(config=config)
        else:
            local_cache = None

        return cls(username, password, local_cache=local_cache)


class Scratch(ExternalDataStore):
    """
    Class object for External DataStore on Peregrine
    to be used within Peregrine
    """
    ROOT_PATH = '/scratch/mrossol/Resource_Repo'

    def download(self, src, dst, timeout=30):
        """
        Method to download src file to dst internally to Peregrine

        Parameters
        ----------
        src : 'string'
            Path to source file to be downloaded
        dst : 'string'
            Path to destination directory of file path
        timeout : 'int'
            Timeout in seconds
        """
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
    """
    Class object for External DataStore at DR Power (egrid.org)
    """
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

    def __init__(self, size=None, **kwargs):
        """
        Initialize InternalDataStore object

        Parameters
        ----------
        size : 'float'
            Maximum local cache size in GB
        **kwargs :
            kwargs for DataStore
        """
        self._size = size
        super(InternalDataStore, self).__init__(**kwargs)

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
        Reads the configuration. From configuration and defaults,
        determines initializes InternalDataStore object.

        Parameters
        ----------
        config : 'string'
            Path to .ini configuration file.
            See library/config.ini for an example

        Returns
        ---------
        'InternalDataStore'
            Initialized InternalDataStore object
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
            if size is not None:
                size = float(size)

            wind_dir = config_parser.get('local_cache', 'wind_directory')
            wind_dir = cls.decode_config_entry(wind_dir)

            solar_dir = config_parser.get('local_cache', 'solar_directory')
            solar_dir = cls.decode_config_entry(solar_dir)

        return cls(wind_dir=wind_dir, solar_dir=solar_dir, size=size)

    @classmethod
    def get_cache_size(cls, path):
        """
        Searches all sub directories in path for .hdf5 files
        computes total size in GB

        Parameters
        ----------
        path : 'string'
            Path to cache directory

        Returns
        ---------
        repo_size : 'float'
            Returns total size of .hdf5 files in cache in GB
        """
        repo_size = 0
        for (path, dirs, files) in os.walk(path):
            for file in files:
                if file.endswith('.hdf5'):
                    file_name = os.path.join(path, file)
                    repo_size += os.path.getsize(file_name) * 10**-9

        return repo_size

    @classmethod
    def get_cache_summary(cls, path):
        """
        Summarize the data available in the local cache

        Parameters
        ----------
        path : 'string'
            Path to cache meta .csv

        Returns
        ---------
        summary : 'pandas.Series'
            Summary table of number of sites, and corresponding resource types
            in local cache
        """
        cache_meta = pds.read_csv(path, index_col=0)
        summary = pds.Series()
        summary['sites'] = len(cache_meta)
        for col in cache_meta.columns:
            if col != 'sub_directory':
                summary[col] = cache_meta[col].sum()

        return summary

    def initialize_cache_metas(self):
        """
        Initialize cache meta .csv files by scanning cache directories
        """
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
        """
        Refresh cache metadata by rescanning cache directory

        Parameters
        ----------
        dataset : 'string'
            'wind' or 'solar'
        """
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

        cache_meta = cache_meta.sort_index()
        cache_meta.to_csv(cache_path)

    def cache_site(self, dataset, site_file):
        """
        Searches all sub directories in path for .hdf5 files
        computes total size in GB

        Parameters
        ----------
        dataset : 'string'
            'wind' or 'solar'
        site_file : 'string'
            Path to resource file for site
        """
        if dataset == 'wind':
            cache_path = self._wind_cache
        elif dataset == 'solar':
            cache_path = self._solar_cache
        else:
            raise ValueError("Invalid dataset type, must be 'wind' or \
'solar'")

        cache_meta = pds.read_csv(cache_path, index_col=0)
        cache_sites = cache_meta.index

        sub_dir, name = os.path.split(site_file)
        name = os.path.splitext(name)[0]
        _, resource, site_id = name.split('_')
        site_id = int(site_id)

        sub_dir = int(os.path.basename(sub_dir))

        if site_id not in cache_sites:
            cache_meta.loc[site_id] = False
            cache_meta.loc[site_id, 'sub_directory'] = sub_dir

        cache_meta.loc[site_id, resource] = True

        cache_meta = cache_meta.sort_index()
        cache_meta.to_csv(cache_path)

    def check_cache(self, dataset, site_id, resource_type=None):
        """
        Check cache for presence of resource.
        If resource_type is None check for any resource_type of site_id
        else check for specific resource_type for site_id

        Parameters
        ----------
        dataset : 'string'
            'wind' or 'solar'
        site_id : 'int'
            Site id number
        resource_type : 'string'
            type of resource
            wind -> ('power', 'fcst', 'met')
            solar -> ('power', 'fcst', 'met', 'irradiance')

        Returns
        ---------
        'int'|None
            Returns subdirectory containing resource site file
            Returns None if resource is not in cache
        """
        self.refresh_cache_meta(dataset)
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
            if resource_type is not None:
                if cache_meta.loc[site_id, resource_type]:
                    return cache_meta.loc[site_id, 'sub_directory']
                else:
                    return None
            else:
                return cache_meta.loc[site_id, 'sub_directory']
        else:
            return None

    def cache_data(self, dataset, site_files):
        """
        Add site to cache meta

        Parameters
        ----------
        dataset : 'string'
            'wind' or 'solar'
        site_files : 'list'
            List of paths to resource files for sites
        """
        for site_file in site_files:
            self.cache_site(dataset, site_file)

    @property
    def cache_size(self):
        """
        Calculate size of local cache and dataset caches in GB

        Returns
        ---------
        'tuple'
            total, wind, and solar cache sizes in GB (floats)
        """
        total_cache = self.get_cache_size(self.ROOT_PATH)
        wind_cache = self.get_cache_size(self._wind_root)
        solar_cache = self.get_cache_size(self._solar_root)

        return total_cache, wind_cache, solar_cache

    @property
    def cache_summary(self):
        """
        Summarize sites and resource types in cache

        Returns
        ---------
        'pandas.DataFrame'
            Summary of Wind and Solar caches
        """
        wind_summary = self.get_cache_summary(self._wind_cache)
        wind_summary.name = 'wind'

        solar_summary = self.get_cache_summary(self._solar_cache)
        solar_summary.name = 'solar'

        return pds.concat((wind_summary, solar_summary), axis=1).T
