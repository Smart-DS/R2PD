import argparse

def main():
    parser = argparse.ArgumentParser(description='''Get wind and solar actual 
                 and forecast data for power system modeling.''')
    parser.add_argument('outdir',help='Directory for output data.')
    parser.add_argument('-n','--nodes',help='''Path to csv file describing 
                        nodes, or list of tuples describing nodes. Each tuple 
                        or each row of the csv file should contain (node_id, 
                        latitude, longitude).''')
    # temporal extent
    # temporal resolution
    # output formatting options
    parser.add_argument('-ds','--external_datastore',help='''Name of the 
                        external datastore to hit for resource data not yet 
                        cached internally.''',default='DRPower')
    parser.add_argument('-ec','--ext_store_config',help='''Path to external 
                        datastore config file.''')
    parser.add_argument('-ic','--int_store_config',help='''Path to internal
                        datastore config file.''')


    # data options depend on whether pulling wind or solar
    subparsers = parser.add_subparsers(dest='type')
    wind_parser = subparsers.add_parser('wind')
    # (node_id, capacity)
    # list of actual fields to provide
    # forecast parameters here or in the main parser?
    pv_parser = subparsers.add_parser('pv')
    # (node_id, capacity, PV type, ...) if PV type = DPV, can put in direction and tilt
    # list of actual fields to provide
    # forecast parameters here or in the main parser?

    args = parser.parse_args()

    # 0. Set up logging and make output directory

    # 1. Load node data and identify nodes with generators connected

    # 2. Query DataStore for resource data closest to nodes

    # 3. Download and cache data

    # 4. Pull needed data from cache

    # 5. Downselect fields

    # 6. Apply any needed transforms

    # 7. Output to file

if __name__ == "__main__":
    main()