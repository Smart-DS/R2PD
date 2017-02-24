"""
This module provides classes for accessing site-level wind and solar data from
internal and external data stores.
"""

class DataStore(object): 
    @classmethod
    def connect(cls, config=None): pass

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
