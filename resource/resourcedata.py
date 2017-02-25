"""
This module provides an API to the raw resource data and meta-data.
"""

class ResourceLocation(object): 
    """
    Identifies available site data.
    """
    DATASET=None # Redefine in derived class

    @property
    def file_id(self): pass

    @property
    def latitude(self): pass

    @property
    def longitude(self): pass

class WindLocation(ResourceLocation):
    DATASET='Wind'

class SolarLocation(ResourceLocation):
    DATASET='Solar'

class ResourceData(object): 
    def __init__(self, path): pass

    def power_data(self): pass

    def meteorological_data(self): pass

    def forecast_data(self): pass

class WindData(ResourceData): pass

class SolarData(ResourceData): pass

