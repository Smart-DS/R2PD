"""
This module provides classes for accessing site-level wind and solar data
from internal and external data stores.
"""
import concurrent.futures as cf
import logging
import multiprocessing
import os
from urllib.request import urlretrieve

from configparser import ConfigParser
import numpy as np
import pandas as pds

from R2PD.powerdata import GeneratorNodeCollection
from R2PD.nearestnodes import nearest_power_nodes, nearest_met_nodes
from R2PD.resourcedata import WindResource, SolarResource, ResourceList

logger = logging.getLogger(__name__)


class DataStore(object):
    """
    Abstract class to define interface for accessing stores of resource data.
    """
    META_ROOT = os.path.dirname(os.path.realpath(__file__))
    META_ROOT = os.path.join(META_ROOT, 'library')
    # Average File size in MB currently estimates
    WIND_FILE_SIZES = {'met': 12.1, 'power': 3.7, 'fcst': 1, 'fcst-prob': 1.8}
    SOLAR_FILE_SIZES = {'met': 5.2, 'power': 3.3, 'fcst': 0}

    def __init__(self):
        self._wind_meta = None
        self._solar_meta = None

    def __repr__(self):
        """
        Print the type of datastore and its ROOT_PATH

        Returns
        ---------
        'str'
            type of DataStore
        """
        return self.__class__.__name__

    @staticmethod
    def load_meta(meta_path):
        """
        Load meta data

        Parameters
        ----------
        meta_path : 'str'
            Path to meta data .json

        Returns
        ---------
        meta : 'pandas.DataFrame'
            DataFrame of resource meta data
        """
        meta = pds.read_csv(meta_path).set_index('site_id')
        meta = meta.sort_index()
        return meta

    @property
    def wind_meta(self):
        """
        Return wind meta data

        Returns
        ---------
        self._wind_meta : 'pandas.DataFrame'
            DataFrame of wind resource meta data
        """
        if self._wind_meta is None:
            path = os.path.join(self.META_ROOT, 'wind_site_meta.csv')
            self._wind_meta = self.load_meta(path)

        return self._wind_meta

    @property
    def solar_meta(self):
        """
        Return solar meta data

        Returns
        ---------
        self._wind_meta : 'pandas.DataFrame'
            DataFrame of solar resource meta data
        """
        if self._solar_meta is None:
            path = os.path.join(self.META_ROOT, 'solar_site_meta.csv')
            self._solar_meta = self.load_meta(path)

        return self._solar_meta

    @classmethod
    def decode_config_entry(cls, entry):
        """
        Decode config entry converting missing or 'None' entires to None

        Parameters
        ----------
        entry : 'str'
            entry from ConfigParser call

        Returns
        ---------
        entry : 'str' or None
            if config entry is not 'None' or empty the entry is returned
            else None is returned
        """
        if entry == 'None' or '':
            entry = None

        return entry


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
    PKG_DIR = os.path.dirname(os.path.realpath(__file__))
    PKG_DIR = os.path.dirname(PKG_DIR)

    def __init__(self, cache_root=None, size=1):
        """
        Initialize InternalDataStore object

        Parameters
        ----------
        cache_root : 'str'
            Path to root directory in which local cache should be created
            Default is ./R2PD/R2PD_Cache
        size : 'float'
            Maximum local cache size in GB
        """
        super(InternalDataStore, self).__init__()

        if cache_root is None:
            cache_root = os.path.join(self.PKG_DIR, 'R2PD_Cache')

        self._cache_root = cache_root
        self._wind_root = os.path.join(self._cache_root, 'wind')
        if not os.path.exists(self._wind_root):
            os.makedirs(self._wind_root)

        self._solar_root = os.path.join(self._cache_root, 'solar')
        if not os.path.exists(self._solar_root):
            os.makedirs(self._solar_root)

        self._size = size

        self._wind_cache = None
        self._solar_cache = None
        self.update_cache_meta()

    def __repr__(self):
        """
        Print the type of datastore and its ROOT_PATH

        Returns
        ---------
        'str'
            type of DataStore and its ROOT_PATH
        """
        return '{n} at {i}'.format(n=self.__class__.__name__,
                                   i=self._cache_root)

    @classmethod
    def connect(cls, config=None):
        """
        Reads the configuration. From configuration and defaults,
        determines initializes InternalDataStore object.

        Parameters
        ----------
        config : 'str'
            Path to .ini configuration file.
            See library/config.ini for an example

        Returns
        ---------
        'InternalDataStore'
            Initialized InternalDataStore object
        """
        if config is None:
            size = None
            root_path = os.path.join(cls.PKG_DIR, 'R2PD_Cache')
        else:
            config_parser = ConfigParser()
            config_parser.read(config)
            root_path = config_parser.get('local_cache', 'root_path')
            root_path = cls.decode_config_entry(root_path)
            size = cls.decode_config_entry(config_parser.get('local_cache',
                                                             'size'))
        if size is not None:
            size = float(size)
        else:
            size = 1

        return cls(cache_root=root_path, size=size)

    @staticmethod
    def get_cache_size(cache_meta, file_sizes):
        """
        Searches all sub directories in path for .hdf5 files
        computes total size in GB

        Parameters
        ----------
        cache_path : 'str'
            Path to cache directory

        Returns
        ---------
        repo_size : 'float'
            Returns total size of .hdf5 files in cache in GB
        """
        repo_size = 0
        for resource_type, col in cache_meta.iteritems():
            repo_size += file_sizes[resource_type] * col.sum()

        return repo_size / 1000

    @staticmethod
    def get_cache_summary(cache_meta):
        """
        Summarize the data available in the local cache

        Parameters
        ----------
        path : 'str'
            Path to cache meta .csv

        Returns
        ---------
        summary : 'pandas.Series'
            Summary table of number of sites, and corresponding resource types
            in local cache
        """
        summary = pds.Series()
        summary['sites'] = len(cache_meta)
        for col in cache_meta.columns:
            summary[col] = cache_meta[col].sum()

        return summary

    @property
    def cache_size(self):
        """
        Calculate size of local cache and dataset caches in GB

        Returns
        ---------
        'tuple'
            total, wind, and solar cache sizes in GB (floats)
        """
        wind_cache = self.get_cache_size(self.wind_cache,
                                         self.WIND_FILE_SIZES)
        solar_cache = self.get_cache_size(self.solar_cache,
                                          self.SOLAR_FILE_SIZES)
        total_cache = wind_cache + solar_cache

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
        wind_summary = self.get_cache_summary(self.wind_cache)
        wind_summary.name = 'wind'

        solar_summary = self.get_cache_summary(self.solar_cache)
        solar_summary.name = 'solar'

        return pds.concat((wind_summary, solar_summary), axis=1).T

    @property
    def wind_cache(self):
        """
        Scan wind cache and update cache meta

        Returns
        ---------
        _wind_cache : 'pandas.DataFrame'
            DataFrame of files in wind cache
        """
        if self._wind_cache is None:
            columns = ['met', 'power', 'fcst', 'fcst-prob']
            cache_meta = pds.DataFrame(columns=columns)
            cache_meta.index.name = 'site_id'
            self._wind_cache = self.scan_cache(self._wind_root, cache_meta)

        return self._wind_cache

    @property
    def solar_cache(self):
        """
        Scan solar cache and update cache meta

        Returns
        ---------
        _solar_cache : 'pandas.DataFrame'
            DataFrame of files in solar cache
        """
        if self._solar_cache is None:
            columns = ['met', 'power']
            cache_meta = pds.DataFrame(columns=columns)
            cache_meta.index.name = 'site_id'
            self._solar_cache = self.scan_cache(self._solar_root, cache_meta)

        return self._solar_cache

    @staticmethod
    def scan_cache(cache_path, cache_meta):
        """
        Scan cache_path and update cache_meta

        Parameters
        ----------
        cache_path : 'str'
            Root directory to be scanned for .hdf5 files
        cache_meta : 'pandas.DataFrame'
            DataFrame of resource files in cache

        Returns
        ---------
        cache_meta : 'pandas.DataFrame'
            Updated DataFrame of resource files in cache
        """
        cache_sites = cache_meta.index

        for file in os.listdir(cache_path):
            if file.endswith('.hdf5'):
                name = os.path.splitext(os.path.basename(file))[0]
                _, resource, site_id = name.split('_')
                site_id = int(site_id)

                if site_id not in cache_sites:
                    cache_meta.loc[site_id] = False

                cache_meta.loc[site_id, resource] = True

        cache_sites = cache_meta.index

        return cache_meta

    def update_cache_meta(self, dataset=None):
        """
        Refresh cache metadata by rescanning cache directory

        Parameters
        ----------
        dataset : 'str'
            'wind' or 'solar'
        """
        if dataset is None:
            self._wind_cache = self.scan_cache(self._wind_root,
                                               self.wind_cache)
            self._solar_cache = self.scan_cache(self._solar_root,
                                                self.solar_cache)
        elif dataset == 'wind':
            self._wind_cache = self.scan_cache(self._wind_root,
                                               self.wind_cache)
        elif dataset == 'solar':
            self._solar_cache = self.scan_cache(self._solar_root,
                                                self.solar_cache)
        else:
            msg = "Invalid dataset type, must be 'wind' or 'solar'"
            raise ValueError(msg)

    def check_cache(self, dataset, site_id, resource_type=None):
        """
        Check cache for presence of resource.
        If resource_type is None check for any resource_type of site_id
        else check for specific resource_type for site_id

        Parameters
        ----------
        dataset : 'str'
            'wind' or 'solar'
        site_id : 'int'
            Site id number
        resource_type : 'str'
            type of resource
            wind -> ('power', 'fcst', 'met')
            solar -> ('power', 'fcst', 'met', 'irradiance')

        Returns
        ---------
        'bool'
            Is site/resource present in cache
        """
        # Check for DR Power sites directly
        if resource_type is not None:
            file_name = '{}_{}_{}.hdf5'.format(dataset, resource_type, site_id)
            file_path = os.path.join(self._cache_root, dataset, file_name)
            return os.path.exists(file_path)

        if dataset == 'wind':
            cache_meta = self.wind_cache
        elif dataset == 'solar':
            cache_meta = self.solar_cache
        else:
            msg = "Invalid dataset type, must be 'wind' or 'solar'"
            raise ValueError(msg)
        cache_sites = cache_meta.index

        if site_id in cache_sites:
            if resource_type is not None:
                cached = bool(cache_meta.loc[site_id, resource_type])
            else:
                cached = True
        else:
            cached = False

        return cached

    def test_cache_size(self, download_size):
        """
        Test to see if download will fit in cache

        Parameters
        ----------
        download_size : 'float'
            Size of requested download in GB
        """
        if self._size is not None:
            cache_size, wind_size, solar_size = self.cache_size
            open_cache = self._size - cache_size
            if open_cache < download_size:
                msg = ('Not enough space available in local cache:',
                       '\nDownload size = {:.2f}GB'.format(download_size),
                       '\nLocal cache = {:.2f}GB of'.format(cache_size),
                       ' {:.2f}GB in use'.format(self._size),
                       '\n\tCached wind data = {:.2f}GB'.format(wind_size),
                       '\n\tCached solar data = {:.2f}GB'.format(solar_size))
                raise RuntimeError(''.join(msg))


class ExternalDataStore(DataStore):
    """
    Abstract class to define interface for accessing external stores
    of resource data.
    """
    def __init__(self, local_cache=None, threads=None):
        """
        Initialize ExternalDataStore object

        Parameters
        ----------
        local_cache : 'InternalDataStore'
            InternalDataStore object represening internal data cache
        threads : 'int'
            Number of threads to use during downloads
        """
        super(ExternalDataStore, self).__init__()

        if local_cache is None:
            local_cache = InternalDataStore.connect()
        elif not isinstance(local_cache, InternalDataStore):
            msg = ("Expecting local_cache to be instance of",
                   "InternalDataStore,",
                   "but is {:}.".format(type(local_cache)))
            raise RuntimeError(' '.join(msg))

        self._local_cache = local_cache

        if threads:
            if not isinstance(threads, int):
                threads = multiprocessing.cpu_count() // 2
        else:
            threads = None

        self._threads = threads

    @classmethod
    def connect(cls, config=None):
        """
        Reads the configuration. From configuration and defaults,
        determines initializes ExternalDataStore object.

        Parameters
        ----------
        config : 'str'
            Path to .ini configuration file.
            See library/config.ini for an example

        Returns
        ---------
        'ExternalDataStore'
            Initialized ExternalDataStore object
        """
        if config is None:
            threads = None
            local_cache = None
        else:
            config_parser = ConfigParser()
            config_parser.read(config)

            if config_parser.has_section('local_cache'):
                local_cache = InternalDataStore.connect(config=config)
            else:
                local_cache = None

            threads = config_parser.get('local_cache', 'threads',
                                        fallback=None)

        return cls(local_cache=local_cache, threads=threads)

    def get_meta(self, dataset):
        """
        Get meta associated with given dataset

        Parameters
        ----------
        dataset : 'str'
            'wind' or 'solar'

        Returns
        ---------
        meta : 'pandas.DataFrame'
            DataFrame of resource meta
        """
        if dataset == 'wind':
            meta = self.wind_meta
        elif dataset == 'solar':
            meta = self.solar_meta
        else:
            raise ValueError("Invalid dataset type, must be 'wind' or 'solar'")

        return meta

    def get_download_size(self, dataset, numb_sites, resource_type):
        """
        Estimate download size

        Parameters
        ----------
        dataset : 'str'
            'wind' or 'solar'
        num_sites : 'int'
            Number of sites to be downloaded
        resource_type : 'str'
            type of resource
            wind -> ('power', 'fcst', 'met')
            solar -> ('power', 'fcst', 'met', 'irradiance')
        forecasts : 'bool'
            Boolean flag as to whether forecasts will be included in the
            download or not

        Returns
        ---------
        download_size : 'float'
            Estimated download size in GB
        """
        if dataset == 'wind':
            download_size = numb_sites * self.WIND_FILE_SIZES[resource_type]
        elif dataset == 'solar':
            download_size = numb_sites * self.SOLAR_FILE_SIZES[resource_type]

        return download_size / 1000

    def download(self, src, dst):
        """
        Abstract method to download src to dst

        Parameters
        ----------
        src : 'str'
            Path or URL to src file
        dst : 'str'
            Path to which file should be downloaded
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
        resource_meta = self.get_meta(dataset)

        if isinstance(node_collection, GeneratorNodeCollection):
            nearest_nodes = nearest_power_nodes(node_collection,
                                                resource_meta)
        else:
            nearest_nodes = nearest_met_nodes(node_collection,
                                              resource_meta)

        return nearest_nodes

    def download_resource(self, dataset, site_id, resource_type):
        """
        Download the resource site file from repository

        Parameters
        ----------
        dataset : 'str'
            'wind' or 'solar'
        site_ids : 'list'
            List of site ids to be downloaded
        resource_type : 'str'
            power or met or fcst
        """
        pass

    def download_resource_data(self, dataset, site_ids, resource_type):
        """
        Download resource files from repository

        Parameters
        ----------
        dataset : 'str'
            'wind' or 'solar'
        site_ids : 'list'
            List of site ids to be downloaded
        resource_type : 'str'
            power or met
        threads : 'int'
            Number of threads to use for downloading
        """
        download_size = self.get_download_size(dataset, len(site_ids),
                                               resource_type)
        self._local_cache.test_cache_size(download_size)

        if self._threads is None:
            logger.debug("Downloading sequentially")
            for site in site_ids:
                self.download_resource(dataset, site, resource_type)
        else:
            logger.debug("Downloading using {} threads".format(self._threads))
            with cf.ThreadPoolExecutor(max_workers=self._threads) as executor:
                for site in site_ids:
                    executor.submit(self.download_resource,
                                    dataset, site, resource_type)

    def get_node_resource(self, dataset, site_id, frac=None):
        """
        Initialize and return Resource class object for specified resource site

        Parameters
        ----------
        dataset : 'str'
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
        if cache:
            if dataset == 'wind':
                return WindResource(self.wind_meta.loc[site_id],
                                    self._local_cache._wind_root, frac=frac)
            elif dataset == 'solar':
                return SolarResource(self.solar_meta.loc[site_id],
                                     self._local_cache._solar_root, frac=frac)
            else:
                msg = "Invalid dataset type, must be 'wind' or 'solar'"
                raise ValueError(msg)
        else:
            raise RuntimeError('{d} site {s} is not in local cache!'
                               .format(d=dataset, s=site_id))

    def get_resource(self, node_collection, forecasts=False):
        """
        Finds nearest nodes, caches files to local datastore and assigns
        resource to node_collection

        Parameters
        ----------
        node_collection : 'NodeCollection'
            Collection of either weather of generator nodes
        forecasts : 'bool'
            Whether to download forecasts along with power data

        Returns
        ---------
        node_collection : 'NodeCollection'
            Node collection with resources assigned to nodes
        nearest_nodes : 'pandas.DataFrame'
            DataFrame of the nearest neighbor matching between nodes
            and resources
        """
        nearest_nodes = self.nearest_neighbors(node_collection)

        if isinstance(node_collection, GeneratorNodeCollection):
            if forecasts:
                resource_type = 'fcst'
            else:
                resource_type = 'power'

            site_ids = np.concatenate(nearest_nodes['site_id'].values)
            site_ids = np.unique(site_ids)
        else:
            resource_type = 'met'
            site_ids = nearest_nodes['site_id'].values

        # download sites not already in local cache
        dataset = node_collection._dataset
        to_download = [site_id for site_id in site_ids if not self._local_cache.check_cache(dataset, site_id, resource_type=resource_type)]
        logger.debug("Trying to download {} of the {} sites requested for dataset {}, resource {}".format(
            len(to_download), len(site_ids), dataset, resource_type))
        self.download_resource_data(dataset, to_download, resource_type)

        self._local_cache.update_cache_meta(dataset)

        resources = []
        for _, meta in nearest_nodes.iterrows():
            site_id = meta['site_id']
            if isinstance(site_id, list):
                fracs = meta['site_fracs']
                r = ResourceList([self.get_node_resource(dataset, site, frac=f)
                                  for site, f in zip(site_id, fracs)])
            else:
                r = self.get_node_resource(dataset, site_id)

            resources.append(r)

        if forecasts:
            node_collection.assign_resource(resources, forecasts=forecasts)
        else:
            node_collection.assign_resource(resources)

        return node_collection, nearest_nodes


class DRPower(ExternalDataStore):
    """
    Class object for External DataStore at DR Power (egrid.org)
    """
    DATA_ROOT = 'https://dtn2.pnl.gov/drpower'

    def download(self, src, dst):
        """
        Download resource data from src URL to dst file path

        Parameters
        ----------
        src : 'str'
            URL of resource data to be downloaded
        dst : 'str'
            Destination path of resource data (including file name)
        """
        logger.debug("Downloading to {} ...".format(dst))
        urlretrieve(src, dst)

    def download_resource(self, dataset, site_id, resource_type):
        """
        Download the resource site file from repo and add site to cache meta

        Parameters
        ----------
        dataset : 'str'
            'wind' or 'solar'
        site_ids : 'list'
            List of site ids to be downloaded
        resource_type : 'str'
            power or met or fcst
        """
        file_name = '{}_{}_{}.hdf5'.format(dataset, resource_type, site_id)
        src = '/'.join([self.DATA_ROOT, dataset, str(site_id), file_name])
        dst = os.path.join(self._local_cache._cache_root, dataset, file_name)
        logger.debug("Prepared to download {} from DR Power".format(src))

        self.download(src, dst)
