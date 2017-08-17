from R2PD.tshelpers import TimeseriesShaper, ForecastShaper


class DefaultTimeseriesShaper(TimeseriesShaper):
    def __call__(self, ts, ts_tempparams, out_tempparams):
        """
        This method compares ts_tempparams and out_tempparams to determine
        what operations need to be performed.
        """
        # aggegregating or disaggregating?

        # do i need to shift?

        # do i need to average

    def aggregate(self, ts):
        pass


class DefaultForecastShaper(ForecastShaper):
    pass
