"""
This module transforms wind and solar PV resource data into forms usable by
power system modelers. Functionalities needed include:

    - Select raw resource data (wind speeds, irradiance) or power outputs
    - Scale power timeseries to the desired capacity
    - Provide data for the temporal extents and resolutions desired, 
      expressed in units of a user-chosen standard-time timezone
    - Blend multiple resource data timeseries into composite curves for 
      distributed PV based on a default or user-supplied distribution of 
      orientations
    - Reshape forecast data into forms usable by operation simulators
    - Sum multiple timeseries to represent the combined output over larger areas
      all tied into the same node
"""

class Node(object): 
    def __init__(self, id, lat, long): pass

    def assign_resource(self, resource_data): pass


class GeneratorNode(Node):

    def __init__(self, id, lat, long, capacity):
        super(GeneratorNode, self).__init__(id, lat, long)

    def assign_resource(self, resource_data): 
        """
        Parameters:
            - resource_data, ResourceData or list of ResourceData - nearby site(s) 
                  from which power actuals and/or forecast data will be constructed.
                  ResourceData type must match the GeneratorNode type.
        """
    
    def get_power(self, temporal_params): pass

    def get_forecasts(self, temporal_params, forecast_params): pass

    def save_power(self, filename, formatter=None): pass

    def save_forecasts(self, filename, formatter=None): pass

    @classmethod
    def _get_power(cls, ts_or_df, temporal_params): pass

    @classmethod
    def _get_forecasts(cls, ts_or_df, temporal_params, forecast_params): pass


class WindGeneratorNode(GeneratorNode): pass


class SolarGeneratorNode(GeneratorNode): pass


class WeatherNode(Node): 
    def assign_resource(self, resource_data): 
        """
        Parameters:
            - resource_data (2-tuple) - (WindData, SolarData)
        """

    def get_weather(self, temporal_params): pass

    def save_weather(self, filename, formatter=None): pass


class NodeCollection(object):
    """
    List of nodes of the same type, with resource data already definied. This
    class is provided to give a DataFrame, rather than a Series, interface for 
    processing timeseries data in bulk.
    """
    def __init__(self, nodes): pass

    @classmethod
    def factory(self, nodes): 
        """
        Constructs the right type of NodeCollection based on the type of nodes.
        """


class GeneratorNodeCollection(NodeCollection):

    def get_power(self, temporal_params): pass

    def get_forecasts(self, temporal_params, forecast_params): pass

    def save_power(self, filename_or_dir, formatter=None): pass

    def save_forecasts(self, filename_or_dir, formatter=None): pass    


class WeatherNodeCollection(NodeCollection): 

    def get_weather(self, temporal_params): pass

    def save_weather(self, filename_or_dir, formatter=None): pass
