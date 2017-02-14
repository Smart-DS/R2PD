"""
This module provides an API to the raw resource data and meta-data.
"""

class ResourceMetaData(object): 
    """
    Wrapper for key, value pairs serialized to json that describe particular 
    resource datasets.
    """

    @property
    def file_id(self): pass

    @property
    def dataset(self): 
        return self.DATASET

    @property
    def latitude(self): pass

    @property
    def longitude(self): pass

    @classmethod
    def load_from_str(cls, value): pass

    @classmethod
    def load_from_file(cls, path): pass

class WindMetaData(ResourceMetaData): pass
    DATASET='Wind'

class SolarPVMetaDAta(ResourceMetaData): pass
    DATASET='SolarPV'

class ResourceData(object): pass

class WindData(ResourceData): pass

class SolarPVData(ResourceData): pass

