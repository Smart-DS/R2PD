"""
This module defines time conventions for basic timeseries and for forecasts, 
as well as abstract function calls for converting between them.
"""

import abc
import datetime as dt
from enum import Enum

class TemporalParameters(object):
    POINT_INTERPRETATIONS = Enum('POINT_INTERPRETATIONS',
                                 ['instantaneous',
                                  'average_next',
                                  'average_prev',
                                  'integrated_next',
                                  'integrated_prev'])

    # todo: is there a reasonable default to assign to point_interp, or does it
    #       have to be provided by the user?
    def __init__(self, extent, point_interp, timezone='UTC', resolution=None): pass

    @classmethod
    def infer_params(cls, timeseries, point_interp): 
        """
        Returns a TemporalParameters object where the extent, timezone, and 
        resolution are inferred from timeseries.index. The user must provide the 
        appropriate point_interp (element of 
        TemporalParameters.POINT_INTERPRETATIONS).
        """


class TimeseriesShaper(object):
    """
    Abstract class defining the call signature for reshaping timeseries to 
    conform to the desired temporal parameters.
    """
    
    @abc.abstractmethod
    def __call__(self, ts, ts_tempparams, out_tempparams): 
        """
        Accepts a timeseries ts that has TemporalParameters ts_tempparams, and 
        returns a re-shaped timeseries conforming to out_tempparams.

        Parameters:
        - ts (pandas.Series or pandas.DataFrame) - timeseries to be reshaped
        - ts_tempparams (TemporalParameters) - description of ts's time 
              conventions
        - out_tempparams (TemporalParameters) - the desired temporal parameters
              for the output timeseries

        Returns reshaped data (pandas.Series or pandas.DataFrame).
        """
        return


class ForecastParameters(object):
    """
    Describes different shapes of forecast data.

    *Discrete leadtimes* datasets are repeated timeseries where the different values
    given for the same timestamp are the value predicted for that time various 
    amounts of time in advance. For example, the WindToolkit forecast data, for 
    every hour lists the amount of output power predicted for that hour 1-hour 
    ahead, 4-hours ahead, 6-hours ahead, and 24-hours ahead.

    *Dispatch lookahead* datasets mimic actual power system operations. For example,
    every day a day-ahead unit commitment model is run using forecasts for the 
    next 1 to 2 days, with the simulation typically kicked-off 6 to 12 hours 
    ahead of the modeled time. These forecasts happen at a certain frequency, 
    cover a certain amount of lookahead time, and are computed a certain amount 
    of time ahead of the modeled time.
    """
    FORECAST_DATA_TYPES = Enum('FORECAST_DATA_TYPES',
                               ['discrete_leadtimes',
                                'dispatch_lookahead'])

    def __init__(self, forecast_data_type, temporal_params, **kwargs): 
        self._forecast_data_type = get_enum_instance(forecast_data_type,self.FORECAST_DATA_TYPES)
        if not isinstance(temporal_params,TemporalParameters):
            raise RuntimeError("Expecting temporal_params to be instance of " + 
                "TemporalParameters, but is {}.".format(type(temporal_params)))
        self._temporal_params = temporal_params
        self._leadtimes = None
        self._frequency = None
        self._lookahead = None
        self._leadtime = None
        # store type-specific parameters
        if self.forecast_data_type == self.FORECAST_DATA_TYPES.discrete_leadtimes:
            self._leadtimes = kwargs['leadtimes']
        else:
            assert self.forecast_data_type == self.FORECAST_DATA_TYPES.dispatch_lookahead
            self._frequency = kwargs['frequency']
            self._lookahead = kwargs['lookahead']
            self._leadtime = kwargs['leadtime']

    @property
    def forecast_data_type(self):
        return self._forecast_data_type

    @property
    def temporal_params(self):
        """
        The overall extents and resolution of the forecast data.
        """
        return self._temporal_params

    @property
    def leadtimes(self):
        """
        For 'discrete_leadtimes' data, a list of the amounts of time ahead at 
        which forecasts are available, e.g. [datetime.timedelta(hours=1),
        datetime.timedelta(hours=4), datetime.timedelta(hours=6), 
        datetime.timedelta(hours=24)].
        """
        return self._leadtimes

    @property
    def frequency(self):
        """
        For 'dispatch_lookahead' data, the frequency at which forecasts are 
        needed.
        """
        return self._frequency

    @property
    def lookahead(self):
        """
        For 'dispatch_lookahead' data, the amount of time covered by each 
        forecast.
        """
        return self._lookahead

    @property
    def leadtime(self):
        """
        For 'dispatch_lookahead' data, the amount of time ahead of the start of
        the modeled time that the forecast data would need to be provided.
        """
        return self._leadtime
        
    @classmethod
    def define_discrete_leadtime_params(cls, temporal_params, leadtimes):
        return ForecastParameters('discrete_leadtimes', temporal_params, 
                                  leadtimes=leadtimes)

    @classmethod
    def define_dispatch_lookahead_params(cls, temporal_params, frequency, lookahead, leadtime=dt.timedelta()):
        return ForecastParameters('dispatch_lookahead',temporal_params, 
                                  frequency=frequency, lookahead=lookahead, 
                                  leadtime=leadtime)


class ForecastShaper(object):

    @abc.abstractmethod
    def __call__(self, forecast_data, forecast_data_params, out_forecast_params): 
        return

def get_enum_instance(value, enum_class):
    return value if isinstance(value, enum_class) else enum[value]
