"""
This module provides an API to the raw resource data and meta-data.
"""
import os
import h5py
import numpy as np
import pandas as pds


class Resource(object):
    """
    Identifies available site data.
    """
    DATASET = None  # Redefine in derived class

    def __init__(self, loc_meta, root_path, frac=None):
        """
        Initialize ResourceLocation
        Parameters
        ----------
        loc_meta : 'pd.Series'
            meta data for resource location
        root_path : 'string'
            path to internal repository
        frac : 'float'
            fraction of site's capacity to be used
            Is None for weather nodes

        Returns
        ---------
        """
        self._id = int(loc_meta.name)
        self._meta = loc_meta
        self._frac = frac
        self._root_path = root_path
        self._sub_dir = loc_meta['sub_directory']

        if self.DATASET is not None:
            self._file_name = '{d}_*_{s}.hdf5'.format(d=self.DATASET,
                                                      s=self._id)
        else:
            self._file_name = '*_{:}.hdf5'.format(self._id)

        self._file_path = os.path.join(self._root_path, str(self._sub_dir),
                                       self._file_name)

    def __repr__(self):
        return '{n} for site {i}'.format(n=self.__class__.__name__, i=self._id)

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
        cap = self._meta['capacity']
        if self._frac is not None:
            return cap * self._frac
        else:
            return cap

    def extract_data(self, data_type):
        file_path = self._file_path.replace('*', data_type.split('_')[0])
        with h5py.File(file_path, 'r') as h5_file:
            data = h5_file[data_type][...]

        data = pds.DataFrame(data)
        data['time'] = pds.to_datetime(data['time'].str.decode('utf-8'))
        data = data.set_index('time')

        return data

    @property
    def power_data(self):
        power_data = self.extract_data('power_data')

        if self._frac is not None:
            return power_data * self._frac
        else:
            return power_data

    @property
    def meteorological_data(self):
        met_data = self.extract_data('met_data')

        return met_data

    @property
    def forecast_data(self):
        fcst_data = self.extract_data('fcst_data')

        if self._frac is not None:
            return fcst_data * self._frac
        else:
            return fcst_data

    @property
    def forecast_probabilities(self):
        fcst_prob = self.extract_data('fcst-prob_data')

        return fcst_prob * self._frac


class WindResource(Resource):
    DATASET = 'wind'
    pass


class SolarResource(Resource):
    DATASET = 'solar'

    @property
    def irradiance_data(self):
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
        self._resources = resources

    def __len__(self):
        return len(self._resources)

    @property
    def locations(self):
        return np.array([(resource.latitude, resource.longitude)
                         for resource in self._resources])

    @property
    def power_data(self):
        power_data = self._resources[0].power_data
        if len(self) > 1:
            for resource in self._resource[1:]:
                power_data.add(resource.power_data)

        return power_data

    @property
    def forecast_data(self):
        fcst_data = self._resources[0].forecast_data
        if len(self) > 1:
            for resource in self._resource[1:]:
                fcst_data.add(resource.forecast_data)

        return fcst_data

    @property
    def forecast_probabilities(self):
        fcst_prob = self._resources[0].forecast_probabilities
        if len(self) > 1:
            for resource in self._resource[1:]:
                fcst_prob.add(resource.forecast_probabilities)

        return fcst_prob
