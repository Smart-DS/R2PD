import numpy as np
import pandas as pds
from R2PD.tshelpers import TimeseriesShaper, ForecastShaper


class DefaultTimeseriesShaper(TimeseriesShaper):
    def __call__(self, ts, ts_tempparams, out_tempparams):
        """
        This method compares ts_tempparams and out_tempparams to determine
        what operations need to be performed.
        """
        time_index = ts.index

        out_start, out_end = out_tempparams.extent
        if out_tempparams is None:
            out_dt = ts_tempparams.resolution
        else:
            out_dt = out_tempparams.resolution

        out_time = pds.date_range(out_start, out_end, freq=out_dt, closed='left')

        # aggegregating or disaggregating?

        # do i need to shift?

        # do i need to average

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
