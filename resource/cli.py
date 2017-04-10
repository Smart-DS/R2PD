import argparse
import datetime as dt

import pandas as pds

from resource.datastore import DRPower, InternalDataStore
from resource.powerdata import NodeCollection, WindGeneratorNode, SolarGeneratorNode, WeatherNode
from resource.tshelpers import TemporalParameters, ForecastParameters

def cli_parser():
    parser = argparse.ArgumentParser(description='''Get wind, solar, or weather 
                 data for power system modeling.''')

    parser.add_argument('-e','--external-datastore',choices=['DRPower'],
        default='DRPower',help='''Name of the external datastore to query for 
        resource data not yet cached locally.''')
    parser.add_argument('-ec','--ext-ds-config',help=''''Path to external
        datastore configuration file.''')
    parser.add_argument('-ic','--int-ds-config',help='''Path to internal 
        datastore (cache) configuration file.''')

    subparsers = parser.add_subparsers(dest='mode')
    actual_parser = subparsers.add_parser('actual-power')
    forecast_parser = subparsers.add_parser('forecast-power')
    weather_parser = subparsers.add_parser('weather')    

    def append_common_data_args(parser):
        parser.add_argument('outdir',help='Directory for output data.')
        parser.add_argument('nodes',help='''Path to csv file describing nodes, or 
                            list of tuples describing nodes. Each tuple or each 
                            row of the csv file should contain (node_id, 
                            latitude, longitude).''')
        parser.add_argument('temporal-extent',help='''Start and end datetimes 
                            for output data.''',nargs=2,type=dt.datetime)
        parser.add_argument('point-interpretation',help='''Interpretation that 
                            will be assumed for output timeseries values. Can 
                            affect exactly which raw data points are pulled, and
                            any upscaling or downscaling that is applied.''',
                            choices=[interp.name for interp in TemporalParameters.POINT_INTERPRETATIONS])
        parser.add_argument('-tz','--timezone',help='''Timezone for all output
                            data. Also used in interpreting temporal-extent if no 
                            explicit timezone is provided for those inputs.''',
                            choices=['UTC'],default='UTC') #todo: Make actual list of choices
        parser.add_argument('-r','--temporal-resolution',help='''Resolution for 
                            output timeseries data. Default is to retain the native 
                            resolution.''')
        parser.add_argument('-f','--formatter',help='''Name of function to use in 
                            formatting output data for disk.''')
        return
    
    for p in [actual_parser, forecast_parser, weather_parser]:
        append_common_data_args(p)

    # todo: split back up into wind versus solar. solar needs generator types
    #       and datasource for residential and commercial blends
    def append_generator_args(parser,forecasts=False):
        parser.add_argument('generators',help='''Path to csv file describing 
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
            disc_leads_parser.add_argument('leadtimes',nargs='*',help='''List of 
                leadtimes at which forecasts are desired for each timestamp.''')
            
            dispatch_parser = subparsers.add_parser('dispatch_lookahead')
            dispatch_parser.add_argument('frequency',help='''Frequency at which 
                forecasts are needed.''',type=dt.timedelta)
            dispatch_parser.add_argument('lookahead',help='''Amount of time being 
                modeled in each forecast/dispatch model run.''',type=dt.timedelta)
            dispatch_parser.add_argument('-l','--leadtime',help='''Amount of 
                time before modeled time that forecast data would need to be 
                supplied.''',type=dt.timedelta)
        parser.add_argument('-s','--shaper',help='''Name of function or other 
                            callable to use in shaping the timeseries data to 
                            conform to the temporal {}parameters'''.format(and_forecast))
        return

    def append_weather_args(parser):
        # todo: Implement downselect of weather variables if needed
        parser.add_argument('-s','--shaper',help='''Name of function or other 
                            callable to use in shaping the weather timeseries 
                            data to conform to the temporal parameters''')
        return

    resource_types = ['wind','solar']
    def append_generator_subparsers(parser,forecasts=False):
        subparsers = parser.add_subparsers(dest='type')
        for resource_type in resource_types:
            parser = subparsers.add_parser(resource_type)
            append_generator_args(parser,forecasts=forecasts)

    append_generator_subparsers(actual_parser)
    append_generator_subparsers(forecast_parser,forecasts=True)
    
    append_weather_args(weather_parser)

    return parser

def cli_main():
    parser = cli_parser()
    args = parser.parse_args()

    # 0. Set up logging, connect to data stores, and make output directory
    assert args.external_datastore == 'DRPower'
    # todo: Implmement library mechanism for finding external datastore options 
    #       and matching string description to class.
    ext_store = DRPower.connect(config=args.ext_ds_config)
    int_store = InternalDataStore.connect(config=args.int_ds_config)

    # 1. Load node data and identify nodes with generators connected
    nodes = None; NodeClass = None
    if args.mode == 'weather':
        NodeClass = WeatherNode
        nodes = pds.DataFrame(args.nodes,columns=['node_id','lat','long'])
    else:
        NodeClass = WindGeneratorNode if args.type == 'wind' else SolarGeneratorNode
        nodes = pds.DataFrame(args.nodes,columns=['node_id','lat','long'])
        generators = pds.DataFrame(args.generators,columns=['node_id','capacity'])
        nodes = pds.merge(nodes, generators, on='node_id', how='inner')
    nodes = [NodeClass(*tuple(node_info)) for ind, node_info in nodes.iterrows()]
    nodes = NodeCollection.factory(nodes)

    def get_resource_data(dataset, locations, num_neighbors):
        locs = ext_store.nearest_neighbors(dataset, locations, num_neighbors=num_neighbors)        
        file_ids = []
        if isinstance(locs[0],list):
            file_ids = [loc.file_id for neighbors in locs for loc in neighbors]
        else:
            file_ids = [loc.file_id for loc in locs]
        data = int_store.get_data(dataset, file_ids)
        # HERE 
        # for None entries, pull from external store, cache in internal store
        # now reshape data to be same shape as locs and return
        return data

    # 3. Pull the resource data
    if args.mode == 'weather':
        wind_resource = get_resource_data('wind',nodes.locations,1)
        solar_resource = get_resource_data('solar',nodes.locations,1)
        resource_data = zip(wind_resource, solar_resource)
    else:
        resource_data = get_resource_data(args.type,nodes.locations,3)
    # 4. Assign it to the nodes
    for i, node in enumerate(nodes.nodes):
            node.assign_resource(resource_data[i])

    # 5. Calculate the data
    # 6. Format and save to disk
    temporal_params = TemporalParameters(args.temporal_extent,
                                         args.point_interpretation,
                                         timezone=args.timezone,
                                         resolution=args.temporal_resolution)
    # todo: Set up library and match shaper and formatter arguments to objects
    shaper = args.shaper
    formatter = args.formatter
    if args.mode == 'weather':
        nodes.get_weather(temporal_params,shaper=shaper)
        nodes.save_weather(args.outdir,formatter=formatter)
    elif args.mode == 'actual-power':
        nodes.get_power(temporal_params,shaper=shaper)
        nodes.save_power(args.outdir,formatter=formatter)
    else:
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
        nodes.save_forecasts(args.outdir,formatter=formatter)
