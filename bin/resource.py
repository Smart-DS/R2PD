import argparse
import datetime as dt

def main():
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
        parser.add_argument('-tz','--time-zone',help='''Timezone for all output
                            data. Also used in interpreting temporal-extent if no 
                            explicit timezone is provided for those inputs.''',
                            choices=['UTC'],default='UTC') # TODO: Make actual list of choices
        parser.add_argument('-f','--formatter',help='''Name of function to use in 
                            formatting output data for disk.''')
    
    for p in [actual_parser, forecast_parser, weather_parser]:
        append_common_data_args(p)

    args = parser.parse_args()
    print(args)

    # 0. Set up logging, connect to data stores, and make output directory

    # 1. Load node data and identify nodes with generators connected

    # 2. Query DataStore for resource data closest to nodes

    # 3. Download and cache data

    # 4. Pull needed data from cache

    # 5. Downselect fields

    # 6. Apply any needed transforms

    # 7. Output to file

if __name__ == "__main__":
    main()