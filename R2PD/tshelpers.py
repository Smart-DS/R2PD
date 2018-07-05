"""
This module defines time conventions for basic timeseries and for forecasts,
as well as abstract function calls for converting between them.
"""

import abc
from enum import Enum
import numpy as np
import pandas as pds


class TemporalParameters(object):
    """
    Class to specify temporal parameters
    """
    POINT_INTERPRETATIONS = Enum('POINT_INTERPRETATIONS',
                                 ['instantaneous',
                                  'average_next',
                                  'average_prev',
                                  'average_midpt',
                                  'integrated_next',
                                  'integrated_prev',
                                  'integrated_midpt'])

    def __init__(self, extent, point_interp='instantaneous', timezone='UTC',
                 resolution=None):
        """
        Initialize TemporalParameters

        Parameters
        ----------
        extent : 'list'|'tuple'
            Start and end datetime
        point_interp : 'POINT_INTERPRETATIONS'
            element of POINT_INTERPRETATIONS representing data
        timezone : 'str'
            timezone for timeseries
        resolution : 'str'
            resolution for timeseries, if None use data's native resolution
        """
        self.extent = list(pds.to_datetime(extent).tz_localize(timezone))
        self.point_interp = get_enum_instance(point_interp,
                                              self.POINT_INTERPRETATIONS)
        self.timezone = timezone
        self.resolution = pds.to_timedelta(resolution)

    @classmethod
    def infer_params(cls, ts, timezone=None, **kwargs):
        """
        Infer time-series temporal parameters

        Parameters
        ----------
        ts : 'pandas.DataFrame'
            Timeseries DataFrame
        timezone : 'str'
            Timezone of time-series, if None, infer
        **kwargs
            kwargs for TemporalParameters

        Returns
        -------
        ts_params : 'TemporalParameters'
        """
        time_index = ts.index
        extent = time_index[[0, -1]]
        ts_params = cls(extent, **kwargs)
        ts_params.infer_resolution(ts)

        if timezone is None:
            ts_params.infer_timezone(ts)

        return ts_params

    def infer_resolution(self, ts):
        """
        Infer time-series temporal resolution

        Parameters
        ----------
        ts : 'pandas.DataFrame'
            Timeseries DataFrame
        """
        time_index = ts.index
        resolution = np.unique(time_index[1:] - time_index[:-1])
        assert len(resolution) == 1, 'time resolution is not constant!'
        resolution = pds.to_timedelta(resolution[0])
        self.resolution = resolution

    def infer_timezone(self, ts):
        """
        Infer time-series timezone

        Parameters
        ----------
        ts : 'pandas.DataFrame'
            Timeseries DataFrame
        """
        timezone = ts.index.tz
        if timezone is None:
            timezone = 'UTC'

        self.timezone = timezone


class TimeseriesShaper(object):
    """
    Abstract class defining the call signature for reshaping timeseries to
    conform to the desired temporal parameters.
    """
    @abc.abstractmethod
    def __call__(self, ts, out_tempparams, ts_tempparams=None):
        """
        Accepts a timeseries ts that has TemporalParameters ts_tempparams, and
        returns a re-shaped timeseries conforming to out_tempparams.

        Parameters
        ----------
        ts : 'pandas.Series'|'pandas.DataFrame'
            timeseries to be reshaped
        out_tempparams : 'TemporalParameters'
            the desired temporal parameters for the output timeseries
        ts_tempparams : 'TemporalParameters'
            description of ts's temporal parameters

        Returns
        ---------
        'pandas.Series'|'pandas.DataFrame'
            Returns reshaped timeseries data
        """
        return


class ForecastParameters(object):
    """
    Describes different shapes of forecast data.

    *Discrete leadtimes* datasets are repeated timeseries where the different
    values given for the same timestamp are the value predicted for that time
    various amounts of time in advance. For example, the WindToolkit forecast
    data, for every hour lists the amount of output power predicted for that
    hour 1-hour ahead, 4-hours ahead, 6-hours ahead, and 24-hours ahead.

    *Dispatch lookahead* datasets mimic actual power system operations.
    For example, every day a day-ahead unit commitment model is run using
    forecasts for the next 1 to 2 days, with the simulation typically
    kicked-off 6 to 12 hours ahead of the modeled time.
    These forecasts happen at a certain frequency, cover a certain amount of
    lookahead time, and are computed a certain amount of time ahead of the
    modeled time.
    """
    FORECAST_TYPES = Enum('FORECAST_TYPES',
                          ['discrete_leadtimes',
                           'dispatch_lookahead'])

    def __init__(self, forecast_type, temporal_params, **kwargs):
        """
        Initialize ForecastParameters

        Parameters
        ----------
        forecast_type : 'FORECAST_TYPES
            element of FORECAST_TYPES representing type of forecast
        temporal_params : 'TemporalParameters'
            TemporalParameters instance describing timeseries parameters
        **kwargs
            kwargs specific to forecast type
        """
        self._forecast_type = get_enum_instance(forecast_type,
                                                self.FORECAST_TYPES)
        if not isinstance(temporal_params, TemporalParameters):
            msg = ("Expecting temporal_params to be instance of",
                   "TemporalParameters, but is {}."
                   .format(type(temporal_params)))
            raise RuntimeError(" ".join(msg))
        self._temporal_params = temporal_params
        self._leadtimes = None
        self._frequency = None
        self._lookahead = None
        self._leadtime = None
        self._dispatch_time = None
        # store type-specific parameters
        if self.forecast_type == self.FORECAST_TYPES.discrete_leadtimes:
            self._leadtimes = pds.to_timedelta(kwargs['leadtimes'])
        else:
            assert self.forecast_type == self.FORECAST_TYPES.dispatch_lookahead
            self._frequency = pds.to_timedelta(kwargs['frequency'])
            self._lookahead = pds.to_timedelta(kwargs['lookahead'])
            self._leadtime = pds.to_timedelta(kwargs['leadtime'])
            self._dispatch_time = pds.to_datetime(kwargs['dispatch_time'])
            self._dispatch_time = self._dispatch_time.time()

    @classmethod
    def infer_params(cls, ts, **kwargs):
        """
        Infer time-series temporal parameters

        Parameters
        ----------
        ts : 'pandas.DataFrame'
            Timeseries DataFrame
        timezone : 'str'
            Timezone of time-series, if None, infer
        **kwargs
            kwargs for TemporalParameters

        Returns
        -------
        ts_params : 'TemporalParameters'
        """
        ts_params = TemporalParameters.infer_params(ts, **kwargs)
        fcst_type = cls.FORECAST_TYPES.discrete_leadtimes
        leadtimes = list(ts.columns)
        fcst_params = cls(fcst_type, ts_params, leadtimes=leadtimes)

        return fcst_params

    @property
    def forecast_type(self):
        """
        Type of forecast

        Returns
        ---------
        'FORECAST_TYPES'
            element of FORECAST_TYPES
        """
        return self._forecast_type

    @property
    def temporal_params(self):
        """
        Temporal Parameters for forecast timeseries

        Returns
        ---------
        'TemporalParameters'
            Timeseries temporal parameters
        """
        return self._temporal_params

    @property
    def leadtimes(self):
        """
        For 'discrete_leadtimes' data, a list of the amounts of time ahead at
        which forecasts are available, e.g. [datetime.timedelta(hours=1),
        datetime.timedelta(hours=4), datetime.timedelta(hours=6),
        datetime.timedelta(hours=24)].

        Returns
        ---------
        'list'
            List of forecast leadtimes
        """
        return self._leadtimes

    @property
    def frequency(self):
        """
        For 'dispatch_lookahead' data, the frequency at which forecasts are
        needed.

        Returns
        ---------
        'datetime.timedelta'
            Frequency of lookahead forecasts
        """
        return self._frequency

    @property
    def lookahead(self):
        """
        For 'dispatch_lookahead' data, the amount of time covered by each
        forecast.

        Returns
        ---------
        'datetime.timedelta'
            Amount of time covered by each forecast
        """
        return self._lookahead

    @property
    def leadtime(self):
        """
        For 'dispatch_lookahead' data, the amount of time ahead of the start of
        the modeled time that the forecast data would need to be provided.

        Returns
        -------
        'datetime.timedelta'
            Amount of leadtime for lookahead forecast
        """
        return self._leadtime

    @classmethod
    def discrete_leadtime(cls, temporal_params, leadtimes):
        """
        Constructs ForecastParameters for leadtime forecasts

        Parameters
        ----------
        temporal_params : 'TemporalParameters'
            Timeseries temporal parameters for leadtime forecasts
        leadtimes : 'list'
            List of forecast leadtimes

        Returns
        -------
        'ForecastParameters'
            Forecast parameters for leadtime forecasts
        """
        return ForecastParameters('discrete_leadtimes', temporal_params,
                                  leadtimes=leadtimes)

    @classmethod
    def dispatch_lookahead(cls, temporal_params, frequency, lookahead,
                           leadtime):
        """
        Constructs ForecastParameters for lookahead forecasts

        Parameters
        ----------
        temporal_params : 'TemporalParameters'
            Timeseries temporal parameters for lookahead forecast
        frequency : 'datetime.timedelta'
            frequency of lookahead forecasts
        lookahead : 'datetime.timedelta'
            amount of lookahead for forecast
        leadtime : 'datetime.timedelta'
            leadtime for lookahead forcast

        Returns
        -------
        'ForecastParameters'
            Forecast parameters for lookahead forecast
        """
        return ForecastParameters('dispatch_lookahead', temporal_params,
                                  frequency=frequency, lookahead=lookahead,
                                  leadtime=leadtime)


class ForecastShaper(object):
    """
    Abstract class defining the call signature for reshaping timeseries to
    conform to the desired temporal parameters.
    """
    @abc.abstractmethod
    def __call__(self, forecast_data, out_forecast_params,
                 forecast_data_params=None):
        """
        Accepts a timeseries of forecast_data that has ForecastParameters
        forecast_data_params
        returns a re-shaped timeseries conforming to out_forecast_params

        Parameters
        ----------
        forecast_data : 'pandas.Series'|'pandas.DataFrame'
            timeseries to be reshaped
        forecast_data_params : 'TemporalParameters'
            description of forecast_data parameters
        out_forecast_params : 'TemporalParameters'
            the desired forecast parameters for the output timeseries

        Returns
        -------
        'pandas.Series'|'pandas.DataFrame'
            Returns reshaped forecast data
        """
        return


def get_enum_instance(value, enum_class):
    """
    Extracts value from enum_class if needed

    Parameters
    ----------
    value : 'str'|'enum_class'
        Either enum_class value or string for value
    enum_class : 'Enum'
        enum class object for which value belongs

    Returns
    -------
    'enum_class'
        enum_class value
    """
    return value if isinstance(value, enum_class) else enum_class[value]
