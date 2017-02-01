import argparse

def main():
    parser = argparse.ArgumentParser(description='''Get wind and solar actual 
                 and forecast data for power system modeling.''')
    parser.add_argument('outdir', help='Directory for output data.')
    # HERE -- Should each node be tagged with a generator type, or should each
    # call to the CLI focus on one generator type? I was originally thinking the
    # former, but am now leaning toward the latter.
    parser.add_argument('-n','--nodes', help='''Path to file describing nodes, 
                        or list of tuples describing nodes.''')

if __name__ == "__main__":
    main()