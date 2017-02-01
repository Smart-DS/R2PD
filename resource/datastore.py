"""
This module provides classes for accessing site-level wind and solar data from
internal and external data stores.
"""

class DataStore(object): pass

class ExternalDataStore(DataStore): pass

class DRPower(ExternalDataStore): pass

class InternalDataStore(DataStore): pass
