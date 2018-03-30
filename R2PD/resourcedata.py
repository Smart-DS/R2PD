"""
This module provides an API to the raw resource data and meta-data.
"""
import os
import h5py
import pandas as pds


class Resource(object):
    """
    Abstract Resource class containing resource site information
    """
    DATASET = None  # Redefine in derived class

    def __init__(self, loc_meta, root_path, frac=None):
        """
        Initialize Resource instance
        Parameters
        ----------
        loc_meta : 'pandas.Series'
            meta data for resource location
        root_path : 'string'
            path to internal repository
        frac : 'float'
            fraction of site's capacity to be used
            Is None for weather nodes
        """
        self._id = int(loc_meta.name)
        self._meta = loc_meta
        self._frac = frac
        self._root_path = root_path

        if self.DATASET is not None:
            self._file_name = '{d}_*_{s}.hdf5'.format(d=self.DATASET,
                                                      s=self._id)
        else:
            self._file_name = '*_{:}.hdf5'.format(self._id)

        self._file_path = os.path.join(self._root_path, self._file_name)

    def __repr__(self):
        """
        Prints the type of Resource and the site id

        Returns
        ---------
        'str'
            type of Resource and site id
        """
        return '{n} for site {i}'.format(n=self.__class__.__name__, i=self._id)

    @property
    def site_id(self):
        """
        resource site id

        Returns
        ---------
        'int'
            resource site id
        """
        return self._id

    @property
    def latitude(self):
        """
        resource site latitude

        Returns
        ---------
        'float'
            resource site latitude
        """
        return self._meta['latitude']

    @property
    def longitude(self):
        """
        resource site longitude

        Returns
        ---------
        'float'
            resource site longitude
        """
        return self._meta['longitude']

    @property
    def capacity(self):
        """
        resource site capacity in MW

        Returns
        ---------
        'float'
            resource site capacity in MW
        """
        cap = self._meta['capacity']
        if self._frac is not None:
            return cap * self._frac
        else:
            return cap

    def extract_data(self, data_type):
        """
        Abstract method to extract time series data from resource .hdf5 file

        Parameters
        ----------
        data_type : 'string'
            type of data ('met', 'power', 'fcst')

        Returns
        ---------
        data : 'pandas.DataFrame'
            Time series DataFrame of resource data
        """
        file_path = self._file_path.replace('*', data_type.split('_')[0])
        with h5py.File(file_path, 'r') as h5_file:
            data = h5_file[data_type][...]

        data = pds.DataFrame(data)
        data['time'] = pds.to_datetime(data['time'].str.decode('utf-8'))
        data = data.set_index('time')

        return data

    @property
    def power_data(self):
        """
        Extract power data

        Returns
        ---------
        power_data : 'pandas.DataFrame'
            Time series dataframe of power data
        """
        power_data = self.extract_data('power_data')

        if self._frac is not None:
            return power_data * self._frac
        else:
            return power_data

    @property
    def meteorological_data(self):
        """
        Extract weather (met) data

        Returns
        ---------
        met_data : 'pandas.DataFrame'
            Time series DataFrame of weather (met)
        """
        met_data = self.extract_data('met_data')

        return met_data

    @property
    def forecast_data(self):
        """
        Extract forecast data

        Returns
        ---------
        fcst_data : 'pandas.DataFrame'
            Time series DataFrame of forecast data
        """
        fcst_data = self.extract_data('fcst_data')

        if self._frac is not None:
            return fcst_data * self._frac
        else:
            return fcst_data

    @property
    def forecast_probabilities(self):
        """
        Extract forecast probabilities data

        Returns
        ---------
        fcst_prob : 'pandas.DataFrame'
            Time series DataFrame of forecast probabilities
        """
        fcst_prob = self.extract_data('fcst-prob_data')

        return fcst_prob * self._frac


class WindResource(Resource):
    """
    Class for wind Resource data
    """
    DATASET = 'wind'
    pass


class SolarResource(Resource):
    """
    Class for solar Resource data
    """
    DATASET = 'solar'

    @property
    def irradiance_data(self):
        """
        Extract irradiance data

        Returns
        ---------
        power_data : 'pandas.DataFrame'
            Time series DataFrame of irradiance data
        """
        file_path = self._file_path.replace('*', 'met')
        with h5py.File(file_path, 'r') as h5_file:
            data = h5_file['irradiance_data'][...]

        data = pds.DataFrame(data)
        data['time'] = pds.to_datetime(data['time'].str.decode('utf-8'))
        data = data.set_index('time')

        return data


class ResourceList(object):
    """
    Handles the aggregation of power and forecast data
    """
    def __init__(self, resources):
        """
        Initialize ResourceList instance
        Parameters
        ----------
        resources : 'list'
            List of Resource objects
        """
        self._resources = resources

    def __len__(self):
        """
        Return number of Resource sites in ResourceList

        Returns
        ---------
        'int'
            Size of ResourceList
        """
        return len(self._resources)

    @property
    def locations(self):
        """
        DataFrame of (latitude, longitude) coordinates for nodes in
        NodeCollection

        Returns
        ---------
        'pandas.DataFrame'
            Latitude and longitude for each node in NodeCollection
        """
        lat_lon = [(resource.site_id, resource.latitude, resource.longitude)
                   for resource in self._resources]
        lat_lon = pds.DataFrame(lat_lon,
                                columns=['site_id', 'latitude', 'longitude'])
        lat_lon = lat_lon.set_index('site_id')
        return lat_lon

    @property
    def power_data(self):
        """
        Extract and aggragate power data for all sites in ResourceList

        Returns
        ---------
        power_data : 'pandas.DataFrame'
            Time series DataFrame of aggragated power data
        """
        power_data = self._resources[0].power_data
        if len(self) > 1:
            for resource in self._resources[1:]:
                power_data.add(resource.power_data)

        return power_data

    @property
    def forecast_data(self):
        """
        Extract and aggragate forecast data for all sites in ResourceList

        Returns
        ---------
        fcst_data : 'pandas.DataFrame'
            Time series DataFrame of aggragated forecast data
        """
        fcst_data = self._resources[0].forecast_data
        if len(self) > 1:
            for resource in self._resources[1:]:
                fcst_data.add(resource.forecast_data)

        return fcst_data

    @property
    def forecast_probabilities(self):
        """
        Extract and aggragate forecast probabilities for all sites in
        ResourceList

        Returns
        ---------
        fcst_prob : 'pandas.DataFrame'
            Time series DataFrame of aggragated forecast probabilities
        """
        fcst_prob = self._resources[0].forecast_probabilities
        if len(self) > 1:
            for resource in self._resources[1:]:
                fcst_prob.add(resource.forecast_probabilities)

        return fcst_prob
