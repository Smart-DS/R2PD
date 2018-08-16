from tshelpers import TemporalParameters
from library.shapers import DefaultTimeseriesShaper

def test_agg_wind_to_hourly_PLEXOS():
    # import a 5 minute wind timeseries we've aggregated before

    # import the 1 hour aggregation made externally

    tp_original = TemporalParameters.infer_params(ts_5min,
                      TemporalParameters.POINT_INTERPRETATIONS.instantaneous)
    tp_desired = # whatever PLEXOS wants
    shaper = DefaultTimeseriesShaper()
    reshaped = shaper(ts_5min, tp_original, tp_desired)
    # Assert that reshaped is equal enough to externally aggregated
