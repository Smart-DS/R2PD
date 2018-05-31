from R2PD.tshelpers import (TemporalParameters,
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
            raise ValueError('Requested temporal extent must be between \
{s}, {e}'.format(s=time_index[0], e=time_index[-1]))

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
        assert dt > self.ts_params.resolution, 'Requested temporal resolution\
 must be greater than {:}'.format(self.ts_params.resolution)
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
        assert dt > self.ts_params.resolution, 'Requested temporal resolution\
 must be greater than {:}'.format(self.ts_params.resolution)
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
        assert dt < self.ts_params.resolution, 'Requested temporal resolution\
 must be less than {:}'.format(self.ts_params.resolution)
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
    pass
