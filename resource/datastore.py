"""
This module provides classes for accessing site-level wind and solar data from
internal and external data stores.
"""

from resourcedata import WindResource, SolarResource


class DataStore(object):
    """
    Abstract class to define interface for accessing stores of resource data.
    """
    def __init__(self):
        self._wind_meta = None
        self._solar_meta = None
        self._root_path = None

    @classmethod
    def connect(cls):
        """
        Connects to the store (internal cache or external repository) and
        returns an instantiated DataStore object.
        """

    def get_resource(self, dataset, site_id, frac=None):
        """
        Return resourcedata.Resource object
        If any site_id is not valid or not in the store error is raised
        """
        if dataset == 'Wind':
            return WindResource(self._wind_meta.loc[site_id], self._root_path,
                                frac=frac)
        elif dataset == 'Solar':
            return SolarResource(self._solar_meta.loc[site_id],
                                 self._root_path, frac=frac)
        else:
            raise ValueError("Invalid dataset type, must be 'Wind' or 'Solar'")


class ExternalDataStore(DataStore):
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

    def cache_data(self, location_data_tuples):
        """
        Saves each (ResourceLocation, ResourceData) tuple to disk and logs it
        in the registry / database.
        """
