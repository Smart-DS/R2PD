"""
R2PD Command Line Interface (CLI)
"""
import ast
import click
import logging
import os
import pandas as pds

from R2PD.datastore import DRPower
from R2PD.powerdata import (NodeCollection, WindGeneratorNode,
                            SolarGeneratorNode, WindMetNode, SolarMetNode)
from R2PD.tshelpers import TemporalParameters, ForecastParameters

logger = logging.getLogger(__name__)

POINT_INTERPS = TemporalParameters.POINT_INTERPRETATIONS
FCST_TYPES = ForecastParameters.FORECAST_TYPES


class ListParamType(click.ParamType):
    name = 'list'

    def convert(self, value, param, ctx):
        try:
            if value is None:
                out = None
            else:
                out = ast.literal_eval(value)

            return out
        except ValueError:
            self.fail('{:} is not a valid integer'.format(value), param, ctx)


LIST = ListParamType()


@click.group()
@click.option('-ds', '--ds_config', default=None,
              type=click.Path(exists=True),
              help='Path to datastore configuration file.')
@click.option('-n', '--node', type=float, nargs=2, default=None,
              help="""(latitude, longitude) of node of interest,
              node_id isset to 0""")
@click.option('-ns', '--nodes', type=click.Path(exists=True), default=None,
              help="""Path to csv file describing nodes, each row
              of the csv file should contain (node_id, latitude,
              longitude).""")
@click.option('-t', '--resource_type', required=True,
              type=click.Choice(['solar', 'wind']),
              help="Resource type, 'solar' or 'wind'")
@click.option('-te', '--temporal_extent', required=True, nargs=2,
              help='Start and end datetimes for output data.')
@click.option('-pi', '--point_interpretation', default='instantaneous',
              type=click.Choice([interp.name for interp in POINT_INTERPS]),
              help="""Interpretation that will be assumed for
              output timeseries values. Can affect exactly which
              raw data points are pulled, and any upscaling or
              downscaling that is applied.""")
@click.option('-tz', '--timezone', default='UTC',
              help="""Timezone for all output data. Also used in
              interpreting temporal-extent if no explicit
              timezone is provided for those inputs. See
              https://gist.github.com/heyalexej/8bf688fd67d7199be4a1682b3eec7568
              for valid timezones.""")
@click.option('-tr', '--temporal_resolution', default=None,
              help="""Resolution for output timeseries data in str accecptable
              by pandas.to_timedelta. Default is to retain the native
              resolution.""")
@click.option('-o', '--out_dir', required=True, type=click.Path(),
              help='Directory for output data.')
@click.option('-f', '--formatter', default=None,
              help="""Name of function to use in formatting
              output data for disk.""")
@click.option('-s', '--shaper', default=None,
              help="""Name of the function to use in re-shaping the
              timeseries data.""")
@click.pass_context
def main(ctx, ds_config, node, nodes, resource_type, temporal_extent,
         point_interpretation, timezone, temporal_resolution, out_dir,
         formatter, shaper):
    """
    Get wind or solar weather or power data for power system modeling.
    """
    repo = DRPower.connect(config=ds_config)
    total_size, wind_size, solar_size = repo._local_cache.cache_size
    max_size = repo._local_cache._size
    click.echo("Local Cache Initialized: "
               "Maximum size = {m:.2f} GB "
               "Current size = {t:.2f} GB "
               "Cached wind data = {w:.2f} GB "
               "Cached solar data = {s:.2f} GB"
               .format(m=max_size, t=total_size, w=wind_size, s=solar_size))

    if node:
        nodes_df = pds.DataFrame({'node_id': 0, 'latitude': node[0],
                                 'longitude': node[1]}, index=[0])
    elif nodes:
        nodes_df = pds.read_csv(nodes)
    else:
        raise RuntimeError("Must supply a '--node/-n' or '--nodes/-ns'")

    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    out_ts_params = TemporalParameters(temporal_extent,
                                       point_interp=point_interpretation,
                                       timezone=timezone,
                                       resolution=temporal_resolution)

    ctx.obj = {'repo': repo,
               'nodes': nodes_df,
               'resource_type': resource_type,
               'out_ts_params': out_ts_params,
               'out_dir': out_dir,
               'formatter': formatter,
               'shaper': shaper}


@main.command()
@click.pass_context
def weather(ctx):
    """
    Get source wind or solar weather data for the nearest site to each node.
    """
    nodes = ctx.obj['nodes']
    re_type = ctx.obj['resource_type']
    NodeClass = WindMetNode if re_type == 'wind' else SolarMetNode
    nodes = [NodeClass(*tuple(node_info))
             for ind, node_info in nodes.iterrows()]
    nodes = NodeCollection.factory(nodes)

    nodes, _ = ctx.obj['repo'].get_resource(nodes)
    nodes.get_weather(ctx.obj['out_ts_params'], shaper=ctx.obj['formatter'])
    nodes.save_weather(ctx.obj['out_dir'], formatter=ctx.obj['formatter'])


@main.group()
@click.option('-c', '--capacity', default=None, type=float,
              help="Capacity of generator(s) on each node in MW")
@click.option('-g', '--generators', default=None,
              type=click.Path(exists=True),
              help="""Path to csv file describing
              generators, or list of tuples. Each tuple or each
              row of the csv file should contain (node_id,
              generator_capacity), where generator_capacity
              is in MW.""")
@click.pass_context
def power(ctx, capacity, generators):
    """
    Subgroup to handle power requests, (actual or forecast)
    """
    nodes = ctx.obj['nodes']
    if generators:
        generators = pds.read_csv(generators)
        nodes = pds.merge(nodes, generators, on='node_id', how='inner')
    else:
        nodes['capacity'] = capacity

    re_type = ctx.obj['resource_type']
    NodeClass = WindGeneratorNode if re_type == 'wind' else SolarGeneratorNode
    nodes = [NodeClass(*tuple(node_info))
             for ind, node_info in nodes.iterrows()]
    nodes = NodeCollection.factory(nodes)
    ctx.obj['nodes'] = nodes


@power.command()
@click.pass_context
def actual(ctx):
    """
    Get real time wind or solar power data aggregated to the desired capacity
    at each node.
    """
    nodes, _ = ctx.obj['repo'].get_resource(ctx.obj['nodes'])
    nodes.get_power(ctx.obj['out_ts_params'], shaper=ctx.obj['shaper'])
    nodes.save_power(ctx.obj['out_dir'], formatter=ctx.obj['formatter'])


@power.command()
@click.option('-ft', '--forecast_type', default='discrete_leadtimes',
              type=click.Choice([fcst.name for fcst in FCST_TYPES]),
              help="Type of forecasts to be created for output data.")
@click.option('-lts', '--leadtimes', type=LIST,
              help="""For 'discrete_leadtimes' the list of times in advance
              that each forecast represents.""")
@click.option('-lt', '--leadtime',
              help="""For 'dispatch_lookahead' data, the amount of time ahead
              of the start of the modeled time that the forecast data would
              need to be provided.""")
@click.option('-f', '--frequency',
              help=""" for 'dispatch_lookahead' data, the frequency at which
              forcasts are run""")
@click.option('-la', '--lookahead',
              help="""For 'dispatch_lookahead' data, the amount of time
              covered by each forecast.""")
@click.option('-dt', '--dispatch_time',
              help="""For 'dispatch_lookahead' data, the time of day that the
              forecast model is run.""")
@click.pass_context
def forecast(ctx, forecast_type, leadtimes, leadtime, frequency, lookahead,
             dispatch_time):
    """
    Get forecast wind or solar power data aggregated to the desired capacity
    at each node.
    """
    nodes, _ = ctx.obj['repo'].get_resource(ctx.obj['nodes'], forcasts=True)

    out_ts_params = ctx.obj['out_ts_params']
    if forecast_type == 'discrete_leadtimes':
        fcst_params = ForecastParameters.discrete_leadtime(out_ts_params,
                                                           leadtimes)
    elif forecast_type == 'dispatch_lookahead':
        fcst_params = ForecastParameters.dispatch_lookahead(out_ts_params,
                                                            dispatch_time,
                                                            frequency,
                                                            lookahead,
                                                            leadtime)

    nodes.get_forecasts(fcst_params, shaper=ctx.obj['formatter'])
    nodes.save_forecasts(ctx.obj['out_dir'], formatter=ctx.obj['formatter'])


if __name__ == '__main__':
    # logging.basicConfig(level=logging.DEBUG)
    main()
