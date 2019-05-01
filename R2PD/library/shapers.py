"""
Libary of Time-series and Forecast Shapers
"""
from datetime import datetime
import numpy as np
import pandas as pds
from R2PD.tshelpers import (TemporalParameters, ForecastParameters,
                            TimeseriesShaper, ForecastShaper)


class DefaultTimeseriesShaper(TimeseriesShaper):
    """
    Default set of functions to reshape timeseries data
    """
    POINT_INTERPS = TemporalParameters.POINT_INTERPRETATIONS

    def __call__(self, ts, out_tempparams, ts_tempparams=None, **kwargs):
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
            ts_tempparams = TemporalParameters.infer_params(ts, **kwargs)

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
        elif point_interp == self.POINT_INTERPS['instantaneous']:
            pass
        else:
            msg = ("{} is not a valid Point Interpretation"
                   .format(point_interp))
            raise RuntimeError(msg)

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
                 forecast_data_params=None, ts_shaper=DefaultTimeseriesShaper):
        """
        Accepts a timeseries of forecast_data that has ForecastParameters
        forecast_data_params
        returns a re-shaped timeseries conforming to out_forecast_params

        Parameters
        ----------
        forecast_data : 'pandas.Series'|'pandas.DataFrame'
            Timeseries to be reshaped
        out_forecast_params : 'TemporalParameters'
            The desired forecast parameters for the output timeseries
        forecast_data_params : 'TemporalParameters'
            Description of forecast_data parameters desired
        ts_shaper : 'TimeseriesShaper'
            Time-series shaper to use during Forecast shaping

        Returns
        -------
        'pandas.Series'|'pandas.DataFrame'
            Returns reshaped forecast data
        """
        if forecast_data_params is None:
            fcst_params = ForecastParameters.infer_params(forecast_data)

        if fcst_params.forecast_type != self.FCST_TYPES['discrete_leadtimes']:
            msg = "Can only reshape Discrete Leadtime forecasts!"
            raise RuntimeError(msg)

        self.out_params = out_forecast_params

        ts_shaper = ts_shaper()
        forecast_data = ts_shaper(forecast_data,
                                  self.out_params._temporal_params)

        fcst_type = self.out_params.forecast_type
        if fcst_type == self.FCST_TYPES['discrete_leadtimes']:
            fcst = self.get_leadtimes(forecast_data)
        elif fcst_type == self.FCST_TYPES['dispatch_lookahead']:
            fcst = self.get_dispatch_lookahead(forecast_data)
        else:
            msg = ("{} is not a valid Forecast Type"
                   .format(fcst_type))
            raise RuntimeError(msg)

        return fcst

    @staticmethod
    def interp_leadtime(fcst_data, leadtime):
        """
        Interpolate discrete leadtimes forecasts to desired leadtime

        Parameters
        ----------
        fcst_data : 'pandas.DataFrame'
                Time-series discrete leadtime forecast data

        Returns
        -------
        'pandas.DataFrame'
             Time-series discrete leadtime forecast
        """
        if isinstance(leadtime, str):
            leadtime = pds.to_timedelta(leadtime)

        lead_times = pds.to_timedelta(fcst_data.columns)
        if leadtime in lead_times:
            pos = list(lead_times).index(leadtime)
            fcst_ts = fcst_data.iloc[:, pos]
        else:
            pos = lead_times < leadtime
            if pos.any():
                h_1 = lead_times[pos].max()
            else:
                h_1 = None

            pos = lead_times > leadtime
            if pos.any():
                h_2 = lead_times[pos].min()
            else:
                h_2 = None

            if h_1 is None or h_2 is None:
                nearest = np.abs(lead_times - leadtime)
                h_1, h_2 = sorted(lead_times[np.argsort(nearest)[:2]])

            fcst_1, fcst_2 = [fcst_data.iloc[:,
                                             np.where(lead_times == h)[0][0]]
                              for h in [h_1, h_2]]
            m = ((fcst_2 - fcst_1) / (h_2 - h_1).total_seconds())
            b = fcst_1 - m * h_1.total_seconds()
            fcst_ts = m * leadtime.total_seconds() + b

        fcst_ts.name = '{:g}h'.format(leadtime.total_seconds() / 3600)
        return fcst_ts.to_frame()

    def get_leadtimes(self, fcst_data):
        """
        Interpolate discrete leadtimes forecasts to desired leadtimes

        Parameters
        ----------
        fcst_data : 'pandas.DataFrame'
                Time-series discrete leadtime forecast data

        Returns
        -------
        'pandas.DataFrame'
             Time-series discrete leadtime forecasts
        """
        lead_times = [self.interp_leadtime(fcst_data, leadtime)
                      for leadtime in self.out_params.leadtimes]

        return pds.concat(lead_times, axis=1)

    def get_dispatch_lookahead(self, fcst_data):
        """
        Convert discrete leadtime forecasts to dispatch lookahead forecast

        Parameters
        ----------
        fcst_data : 'pandas.DataFrame'
                Time-series discrete leadtime forecast data

        Returns
        -------
        'pandas.DataFrame'
             FESTIV formated dispatch lookahead forecast
        """
        s, e = self.out_params._temporal_params.extent
        s = datetime.combine(s.date(), self.out_params.dispatch_time)
        tz = self.out_params._temporal_params.timezone
        s = pds.to_datetime(s).tz_localize(tz)
        dispatch_times = pds.date_range(s, e,
                                        freq=self.out_params.frequency)

        lead_times = self.get_leadtimes(fcst_data)
        dispatch_fcst = []
        for lt, ts in lead_times.iteritems():
            lt = pds.to_timedelta(lt)
            fcst_times = dispatch_times + lt
            df = pds.DataFrame({'dispatch_time': dispatch_times,
                                'fcst_time': fcst_times,
                                'fcst': ts.loc[fcst_times].values})
            dispatch_fcst.append(df)

        dispatch_fcst = pds.concat(dispatch_fcst)
        dispatch_fcst = dispatch_fcst.sort_values(['dispatch_time',
                                                   'fcst_time'])
        return dispatch_fcst
