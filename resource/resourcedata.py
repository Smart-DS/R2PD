"""
This module provides an API to the raw resource data and meta-data.
"""


class Resource(object):
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
        self._id = loc_meta.name
        self._meta = loc_meta
        self._frac = frac
        self._root_path = None

    @property
    def site_id(self):
        return self._id

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

    def power_data(self):
        pass

    def meteorological_data(self):
        pass

    def forecast_data(self):
        pass


class WindResource(Resource):
    DATASET = 'Wind'
    pass


class SolarLocation(Resource):
    DATASET = 'Solar'
    pass


class ResourceData(object):
    def __init__(self, root_path):
        self.root_path = root_path


class WindData(ResourceData):
    pass


class SolarData(ResourceData):
    pass
