
class TemporalParameters(object):
    POINT_INTERPRETATIONS = ['instantaneous',
                             'average_next',
                             'average_prev',
                             'integrated_next',
                             'integrated_prev']

    def __init__(self, extent, timezone='UTC', resolution=None, point_interp=None): pass

    @classmethod
    def infer_params(cls, timeseries, point_interp): 
        """
        Returns a TemporalParameters object where the extent, timezone, and 
        resolution are inferred from timeseries. The user must provide the 
        appropriate point_interp (element of 
        TemporalParameters.POINT_INTERPRETATIONS).
        """


class TimeseriesShaper(object):
    """
    Abstract class defining the call signature for reshaping timeseries to 
    conform to the desired temporal parameters.
    """

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
        """


class ForecastParameters(object):
    def __init__(self, frequency, lead_time): pass


class ForecastShaper(object):

    def __call__(self, forecast_data, tempparams, foreparams): pass
