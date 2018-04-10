import argparse
import datetime as dt
import dateutil
import logging
import numpy as np
import pandas as pds
import time

from .datastore import DRPower, Peregrine
from .powerdata import (NodeCollection, WindGeneratorNode, SolarGeneratorNode,
                        WindMetNode, SolarMetNode)
from .queue import get_resource_data
from .tshelpers import TemporalParameters, ForecastParameters


logger = logging.getLogger(__name__)


def cli_parser():
    parser = argparse.ArgumentParser(description='''Get wind, solar, or weather
                 data for power system modeling.''')

    parser.add_argument('-e', '--external-datastore',
                        choices=['DRPower', 'Peregrine'],
                        default='DRPower',
                        help='''Name of the external datastore to query for
                        resource data not yet cached locally.''')
    parser.add_argument('-ds', '--ds-config', help=''''Path to
        datastore configuration file.''')

    subparsers = parser.add_subparsers(dest='mode')
    actual_parser = subparsers.add_parser('actual-power')
    forecast_parser = subparsers.add_parser('forecast-power')
    weather_parser = subparsers.add_parser('weather')

    def append_common_data_args(parser):
        parser.add_argument('outdir', help='Directory for output data.')
        parser.add_argument('nodes',
                            help='''Path to csv file describing nodes, or list
                            of tuples describing nodes. Each tuple or each row
                            of the csv file should contain (node_id, latitude,
                            longitude).''')
        parser.add_argument('temporal_extent',
                            help='''Start and end datetimes for output
                            data.''', nargs=2, type=dateutil.parser.parse)
        parser.add_argument('point_interpretation',
                            help='''Interpretation that will be assumed for
                            output timeseries values. Can affect exactly which
                            raw data points are pulled, and any upscaling or
                            downscaling that is applied.''',
                            choices=[interp.name for interp
                                     in TemporalParameters.POINT_INTERPRETATIONS])
        parser.add_argument('-tz', '--timezone',
                            help='''Timezone for all output data. Also used in
                            interpreting temporal-extent if no explicit
                            timezone is provided for those inputs.''',
                            choices=['UTC'], default='UTC')
        parser.add_argument('-r', '--temporal-resolution',
                            help='''Resolution for output timeseries data.
                            Default is to retain the native resolution.''')
        parser.add_argument('-f', '--formatter',
                            help='''Name of function to use in formatting
                            output data for disk.''')
        return

    for p in [actual_parser, forecast_parser, weather_parser]:
        append_common_data_args(p)

    # todo: split back up into wind versus solar. solar needs generator types
    #       and datasource for residential and commercial blends
    def append_generator_args(parser, forecasts=False):
        parser.add_argument('generators', help='''Path to csv file describing
                            generators, or list of tuples. Each tuple or each
                            row of the csv file should contain (node_id,
                            generator_capacity), where generator_capacity
                            is in MW.''')
        and_forecast = ''
        if forecasts:
            and_forecast = 'and forecast '
            subparsers = parser.add_subparsers(dest='forecast_type')
            # todo: set default forecast_type?

            disc_leads_parser = subparsers.add_parser('discrete_leadtimes')
            disc_leads_parser.add_argument('leadtimes', nargs='*',
                                           help='''List of leadtimes at which
                                           forecasts are desired for each
                                           timestamp.''')

            dispatch_parser = subparsers.add_parser('dispatch_lookahead')
            dispatch_parser.add_argument('frequency',
                                         help='''Frequency at which forecasts
                                         are needed.''', type=dt.timedelta)
            dispatch_parser.add_argument('lookahead',
                                         help='''Amount of time being modeled
                                         in each forecast/dispatch model
                                         run.''', type=dt.timedelta)
            dispatch_parser.add_argument('-l', '--leadtime',
                                         help='''Amount of time before modeled
                                         time that forecast data would need to
                                         be supplied.''', type=dt.timedelta)
        parser.add_argument('-s', '--shaper',
                            help='''Name of function or other callable to use
                            in shaping the timeseries data to conform to the
                            temporal {:} parameters'''.format(and_forecast))
        return

    def append_weather_args(parser):
        # todo: Implement downselect of weather variables if needed
        parser.add_argument('-s', '--shaper',
                            help='''Name of function or other callable to use
                            in shaping the weather timeseries data to conform
                            to the temporal parameters''')
        return

    resource_types = ['wind', 'solar']

    def append_generator_subparsers(parser, forecasts=False):
        subparsers = parser.add_subparsers(dest='type')
        for resource_type in resource_types:
            parser = subparsers.add_parser(resource_type)
            append_generator_args(parser, forecasts=forecasts)

    def append_weather_subparsers(parser):
        subparsers = parser.add_subparsers(dest='type')
        for resource_type in resource_types:
            parser = subparsers.add_parser(resource_type)
            append_weather_args(parser)

    append_generator_subparsers(actual_parser)
    append_generator_subparsers(forecast_parser, forecasts=True)
    append_weather_subparsers(weather_parser)

    parser.add_argument('-d', '--debug', action='store_true', default=False,
                        help="Option to output debug information.")

    return parser


def cli_main():
    parser = cli_parser()
    args = parser.parse_args()

    log_level = logging.DEBUG if args.debug else logging.WARN
    fmt = '%(asctime)s|%(levelname)s|%(name)s|\n    %(message)s'
    logging.basicConfig(format=fmt, level=log_level)  # to console

    # 0. Set up logging, connect to data stores, and make output directory
    # assert args.external_datastore == 'DRPower'
    assert args.external_datastore == 'Peregrine'
    # todo: Implmement library mechanism for finding external datastore options
    #       and matching string description to class.
    # 1 connect to external datastore
    # ext_store = DRPower.connect(config=args.ext_ds_config)
    ext_store = Peregrine.connect(config=args.ds_config)
    total_size, wind_size, solar_size = ext_store._local_cache.cache_size
    max_size = ext_store._local_cache._size
    print('''Local Cache Initialized: \n
    Maximum size = {m:.2f} GB\n
    Current size = {t:.2f} GB\n
    \t Cached wind data = {w:.2f} GB \n
    \t Cached solar data = {s:.2f} GB
    '''.format(m=max_size, t=total_size, w=wind_size, s=solar_size))

    # 2. Load node data and initialize NodeCollections
    nodes = None
    NodeClass = None
    if args.mode == 'weather':
        NodeClass = WindMetNode if args.type == 'wind' else SolarMetNode
        if isinstance(args.nodes, (list, tuple)):
            nodes = pds.DataFrame(args.nodes,
                                  columns=['node_id', 'lat', 'long'])
        else:
            nodes = pds.read_csv(args.nodes)
    else:
        NodeClass = WindGeneratorNode if args.type == 'wind' else SolarGeneratorNode
        if isinstance(args.nodes, (list, tuple)):
            nodes = pds.DataFrame(args.nodes,
                                  columns=['node_id', 'lat', 'long'])
        else:
            nodes = pds.read_csv(args.nodes)

        if isinstance(args.generators, (list, tuple)):
            generators = pds.DataFrame(args.generators,
                                       columns=['node_id', 'capacity'])
        else:
            generators = pds.read_csv(args.generators)

        nodes = pds.merge(nodes, generators, on='node_id', how='inner')

    nodes = [NodeClass(*tuple(node_info))
             for ind, node_info in nodes.iterrows()]
    nodes = NodeCollection.factory(nodes)

    print('Finding resource for {l} {c}s'
          .format(l=len(nodes), c=NodeClass.__name__))

    # 3 Download, cache, and apply resource to nodes
    ts = time.time()
    print('Identifying resource sites and downloading if necessary')
    nodes, nearest = get_resource_data(nodes, ext_store)
    t_run = (time.time() - ts) / 60

    if args.mode == 'weather':
        sites = len(nearest['site_id'].values)
    else:
        sites = len(np.unique(np.concatenate(nearest['site_id'].values)))

    print('{n} resource sites extracted in {t:.4f} minutes'
          .format(n=sites, t=t_run))

    # 4. Format and save to disk

    temporal_params = TemporalParameters(args.temporal_extent,
                                         args.point_interpretation,
                                         timezone=args.timezone,
                                         resolution=args.temporal_resolution)

    # todo: Set up library and match shaper and formatter arguments to objects
    shaper = args.shaper
    formatter = args.formatter
    print('Saving processed resource data to {:}'.format(args.outdir))

    if args.mode == 'weather':
        nodes.get_weather(temporal_params, shaper=shaper)
        nodes.save_weather(args.outdir, formatter=formatter)
    elif args.mode == 'actual-power':
        nodes.get_power(temporal_params, shaper=shaper)
        nodes.save_power(args.outdir, formatter=formatter)
    else:  # TODO how to handle getting power and forecast in one call?
        assert args.mode == 'forecast-power'
        forecast_params = None
        if args.forecast_type == 'discrete_leadtimes':
            forecast_params = ForecastParameters.define_discrete_leadtime_params(
                temporal_params, args.leadtimes)
        else:
            assert args.forecast_type == 'dispatch_lookahead'
            forecast_params = ForecastParameters.define_dispatch_lookahead_params(
                temporal_params, args.frequency, args.lookahead, args.leadtime)
        nodes.get_forecasts(temporal_params, forecast_params, shaper=shaper)
        nodes.save_forecasts(args.outdir, formatter=formatter)
