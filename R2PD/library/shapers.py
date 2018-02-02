import numpy as np
import pandas as pds
import pytz
from R2PD.tshelpers import TimeseriesShaper, ForecastShaper


class DefaultTimeseriesShaper(TimeseriesShaper):
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
            ts = self.ts_params.localize_ts(ts)

        # aggegregating or disaggregating?

        # do i need to shift?

        # do i need to average

    def get_extent(self, ts):
        time_index = ts.index
        out_start, out_end = self.out_params.extent
        out_dt = self.out_params.resolution
        out_time = pds.date_range(out_start, out_end, freq=out_dt,
                                  closed='left', tz=self.out_params.timezone)

        if not np.array_equal(out_time, time_index):
            start = time_index[0] <= out_time[0]
            end = time_index[-1] >= out_time[-1]
            assert start and end, 'Requested temporal extent must be between \
{s}, {e}'.format(s=time_index[0], e=time_index[-1])
            ts = ts.loc[out_time]

        return ts

    def aggregate(self, ts):
        pass


class DefaultForecastShaper(ForecastShaper):
    pass
