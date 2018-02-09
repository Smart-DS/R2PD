import numpy as np
import pandas as pds
import pytz
from R2PD.tshelpers import (TemporalParameters,
                            TimeseriesShaper, ForecastShaper)


class DefaultTimeseriesShaper(TimeseriesShaper):
    POINT_INTERPS = TemporalParameters.POINT_INTERPRETATIONS
    def __call__(self, ts, ts_tempparams, out_tempparams):
        """
        This method compares ts_tempparams and out_tempparams to determine
        what operations need to be performed.
        """
        if ts_tempparams.resolution is None:
            ts_tempparams.infer_resolution(ts)
        self.ts_params = ts_tempparams

        if out_tempparams.resolution is None:
            out_tempparams.resolution = self.ts_params.resolution
        self.out_params

        if ts.index.tz is None:
            ts = ts.tz_localize(self.ts_params.timezone)

        # aggegregating or disaggregating?

        # do i need to shift?

        # do i need to average

    def get_extent(self, ts):
        time_index = ts.index
        out_start, out_end = self.out_params.extent
        out_dt = self.out_params.resolution
        out_time = pds.date_range(out_start, out_end, freq=out_dt,
                                  closed='left', tz=self.out_params.tz)

        if not np.array_equal(out_time, time_index):
            start = time_index[0] <= out_time[0]
            end = time_index[-1] >= out_time[-1]
            assert start and end, 'Requested temporal extent must be between \
{s}, {e}'.format(s=time_index[0], e=time_index[-1])
            ts = ts.loc[out_time]

        return ts

    def aggregate(self, ts):
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

    def tz_shift(self, ts):
        ts = ts.tz_convert(self.out_params.timezone)
        return ts


class DefaultForecastShaper(ForecastShaper):
    pass
