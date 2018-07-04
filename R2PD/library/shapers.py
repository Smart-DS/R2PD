"""
Libary of Time-series and Forecast Shapers
"""
import numpy as np
import pandas as pds
from R2PD.tshelpers import (TemporalParameters, ForecastParameters,
                            TimeseriesShaper, ForecastShaper)


class DefaultTimeseriesShaper(TimeseriesShaper):
    """
    Default set of functions to reshape timeseries data
    """
    POINT_INTERPS = TemporalParameters.POINT_INTERPRETATIONS

    def __call__(self, ts, out_tempparams, ts_tempparams=None):
        """
        Convert time series to conform with desired temporal parameters

        Parameters
        ----------
        ts : 'pandas.DataFrame'
            Input timeseries to be shaped
        out_tempparams : 'TemporalParameters'
            Temporal parameters desired
        ts_tempparams : 'TemporalParameters'
            Temporal parameters of input timeseries,
            if None they will be infered

        Returns
        -------
        'pandas.DataFrame'
            Reshaped timeseries
        """
        if ts_tempparams is None:
            ts_tempparams = TemporalParameters.infer_params(ts)

        self.ts_params = ts_tempparams

        if out_tempparams.resolution is None:
            out_tempparams.resolution = self.ts_params.resolution

        self.out_params = out_tempparams

        if ts.index.tz is None:
            ts = ts.tz_localize(self.ts_params.timezone)

        if self.ts_params.timezone != self.out_params.timezone:
            ts = self.tz_shift(ts)

        if self.out_params.resolution < self.ts_params.resolution:
            ts = self.interpolate(ts)

        point_interp = self.out_params.point_interp
        if point_interp in (self.POINT_INTERPS['integrated_prev'],
                            self.POINT_INTERPS['integrated_midpt'],
                            self.POINT_INTERPS['integrated_next']):
            ts = self.integrate(ts)
        elif point_interp in (self.POINT_INTERPS['average_prev'],
                              self.POINT_INTERPS['average_midpt'],
                              self.POINT_INTERPS['average_next']):
            ts = self.average(ts)

        return self.get_extent(ts)

    def get_extent(self, ts):
        """
        Extract desired extent from time-series

        Parameters
        ----------
        ts : 'pandas.DataFrame'
            Time-series data

        Returns
        -------
        'pandas.DataFrame'
             Desired extent of time-series
        """
        out_start, out_end = self.out_params.extent
        out_dt = self.out_params.resolution
        start = self.ts_params.extent[0] <= out_start
        end = self.ts_params.extent[1] >= out_end
        if start and end:
            time_index = ts.index
            ts_pos = (time_index >= out_start) & (time_index <= out_end)
            ts = ts.loc[ts_pos].asfreq(out_dt)
        else:
            msg = ('Requested temporal extent must be between {s}, {e}'
                   .format(s=time_index[0], e=time_index[-1]))
            raise ValueError(msg)

        return ts

    def integrate(self, ts):
        """
        Integrate time-series

        Parameters
        ----------
        ts : 'pandas.DataFrame'
            Time-series data

        Returns
        -------
        'pandas.DataFrame'
             Integrated time-series
        """
        dt = self.out_params.resolution
        msg = ('Requested temporal resolutionmust be greater than {:}'
               .format(self.ts_params.resolution))
        assert dt > self.ts_params.resolution, msg
        point_interp = self.out_params.point_interp
        if point_interp == self.POINT_INTERPS['integrated_next']:
            ts.index += (dt - self.ts_params.resolution)

        ts = ts.resample(dt).sum()
        if point_interp == self.POINT_INTERPS['integrated_midpt']:
            ts.index += dt / 2

        return ts

    def average(self, ts):
        """
        Average time-series

        Parameters
        ----------
        ts : 'pandas.DataFrame'
            Time-series data

        Returns
        -------
        'pandas.DataFrame'
            Averaged time-series
        """
        dt = self.out_params.resolution
        msg = ('Requested temporal resolutionmust be greater than {:}'
               .format(self.ts_params.resolution))
        assert dt > self.ts_params.resolution, msg
        point_interp = self.out_params.point_interp
        if point_interp == self.POINT_INTERPS['average_next']:
            ts.index += (dt - self.ts_params.resolution)

        ts = ts.resample(dt).mean()
        if point_interp == self.POINT_INTERPS['average_midpt']:
            ts.index += dt / 2

        return ts

    def interpolate(self, ts):
        """
        Interpolate time-series

        Parameters
        ----------
        ts : 'pandas.DataFrame'
            Time-series data

        Returns
        -------
        'pandas.DataFrame'
            Interpolated time-series
        """
        dt = self.out_params.resolution
        msg = ('Requested temporal resolution must be less than {:}'
               .format(self.ts_params.resolution))
        assert dt < self.ts_params.resolution, msg
        ts = ts.resample(dt).interpolate(method='time')
        return ts

    def tz_shift(self, ts):
        """
        Shift time-series to new timezone

        Parameters
        ----------
        ts : 'pandas.DataFrame'
            Time-series data

        Returns
        -------
        'pandas.DataFrame'
             Shifted time-series
        """
        ts = ts.tz_convert(self.out_params.timezone)
        return ts


class DefaultForecastShaper(ForecastShaper):
    """
    Default set of forecast shapers. Used to refine discrete leadtime format
    or convert to dispatch lookahead format
    """
    FCST_TYPES = ForecastParameters.FORECAST_TYPES

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
        if forecast_data_params is None:
            fcst_params = ForecastParameters.infer_params(forecast_data)

        self.fcst_params = fcst_params

    @staticmethod
    def interp_leadtime(fcst_data, leadtime):
        if isinstance(leadtime, str):
            leadtime = pds.to_timedelta(leadtime)

        lead_times = pds.to_timedelta(fcst_data.columns)
        if leadtime in lead_times:
            pos = list(lead_times).index(leadtime)
            fcst_ts = fcst_data.iloc[:, pos]
        else:
            nearest = np.abs(lead_times - leadtime)
            fcst_1, fcst_2 = np.argsort(nearest)[:2]
            h_1, h_2 = lead_times[[fcst_1, fcst_2]]
            m = ((fcst_data.iloc[:, fcst_2] - fcst_data.iloc[:, fcst_1]) /
                 (h_2 - h_1).total_seconds())
            b = fcst_data.iloc[:, fcst_1] - m * h_1.total_seconds()
            fcst_ts = m * leadtime.total_seconds() + b

        fcst_ts.name = '{:g}h'.format(leadtime.total_seconds() / 3600)
        return fcst_ts.to_frame()

    def get_leadtimes(self, fcst_data):
        lead_times = [self.interp_leadtime(fcst_data, leadtime)
                      for leadtime in self.fcst_params.leadtimes]

        return pds.concat(lead_times, axis=1)
