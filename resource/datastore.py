"""
This module provides classes for accessing site-level wind and solar data from
internal and external data stores.
"""

class DataStore(object): 
    """
    Abstract class to define interface for accessing stores of resource data.
    """

    @classmethod
    def connect(cls, config=None): 
        """
        Connects to the store (internal cache or external repository) and 
        returns an instantiated DataStore object.
        """

    def nearest_neighbors(self, dataset, lat_long_tuples, num_neighbors=1): 
        """
        Returns list or list of lists containing resourcedata.ResourceLocation
        objects.
        """

    def get_data(self, dataset, file_ids): 
        """
        Returns list of resourcedata.ResourceData objects, one entry per 
        file_ids element. If any file_id is not valid or not in the store, 
        None is returned in that spot.
        """

class ExternalDataStore(DataStore): pass

class DRPower(ExternalDataStore): pass

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
