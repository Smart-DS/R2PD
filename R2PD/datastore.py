"""
This module provides classes for accessing site-level wind and solar data from
internal and external data stores.
"""

import os
import pandas as pds
from .resourcedata import WindResource, SolarResource


class DataStore(object):
    """
    Abstract class to define interface for accessing stores of resource data.
    """
    @classmethod
    def connect(cls):
        """
        Connects to the store (internal cache or external repository) and
        returns an instantiated DataStore object.
        """


class ExternalDataStore(DataStore):
    ROOT_PATH = None

    def __init__(self):
        if self.ROOT_PATH is not None:
            self._wind_root = os.path.join(self.ROOT_PATH, 'wind')
            wind_meta_path = os.path.join(self._wind_root,
                                          'wind_site_meta.json')
            self._wind_meta = pds.read_json(wind_meta_path)

            self._solar_root = os.path.join(self.ROOT_PATH, 'solar')
            solar_meta_path = os.path.join(self._solar_root,
                                           'solar_site_meta.json')
            self._solar_meta = pds.read_json(solar_meta_path)

    def __repr__(self):
        return '{n} for {i}'.format(n=self.__class__.__name__,
                                    i=self.ROOT_PATH)


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

    @classmethod
    def connect(cls, config=None):
        """
        Reads the configuration, if provided. From configuration and defaults,
        determines location of internal data cache. If the cache is not yet
        there, creates it. Returns an InternalDataStore object open and ready
        for querying / adding data.
        """

    @property
    def cache_size(self):
        """
        Calculate size of local cache in GB
        """
        repo_size = []
        for (path, dirs, files) in os.walk(self._cache_path):
            for file in files:
                file_name = os.path.join(path, file)
                repo_size += os.path.getsize(file_name) * 10**-9

        return repo_size

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
            return WindResource(self._wind_meta.loc[site_id], self._root_path,
                                frac=frac)
        elif dataset == 'solar':
            return SolarResource(self._solar_meta.loc[site_id],
                                 self._root_path, frac=frac)
        else:
            raise ValueError("Invalid dataset type, must be 'wind' or 'Solar'")
