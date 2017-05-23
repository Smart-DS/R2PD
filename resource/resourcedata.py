"""
This module provides an API to the raw resource data and meta-data.
"""


class ResourceLocation(object):
    """
    Identifies available site data.
    """
    DATASET = None  # Redefine in derived class

    def __init__(self, loc_meta, frac=None):
        """
        Initialize ResourceLocation
        Parameters
        ----------
        loc_meta : 'pd.Series'
            meta data for resource location
        frac : 'float'
            fraction of site's capacity to be used
            Is None for weather nodes

        Returns
        ---------
        """
        self._meta = loc_meta
        self._frac = frac

    @property
    def site_id(self):
        return self._meta.name

    @property
    def latitude(self):
        return self._meta['latitude']

    @property
    def longitude(self):
        return self._meta['longitude']

    @property
    def capacity(self):
        if self._frac is not None:
            return self._meta['capacity'] * self._frac
        else:
            return None


class WindLocation(ResourceLocation):
    DATASET = 'Wind'


class SolarLocation(ResourceLocation):
    DATASET = 'Solar'


class ResourceData(object):
    def __init__(self, root_path):
        self.root_path = root_path

    def power_data(self):
        pass

    def meteorological_data(self):
        pass

    def forecast_data(self):
        pass


class WindData(ResourceData):
    pass


class SolarData(ResourceData):
    pass
