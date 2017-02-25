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

import inspect

class Node(object): 
    def __init__(self, node_id, latitude, longitude): 
        self._resource_assigned = False
        self.id = node_id
        self.latitude = latitude
        self.longitude = longitude

    def assign_resource(self, resource_data): pass

    def _require_resource(self):
        if not self._resource_assigned:
            caller = inspect.getouterframes(inspect.currentframe(), 2)[1][3]
            raise RuntimeError("Resource must be defined before calling " + caller + ".")


class GeneratorNode(Node):

    def __init__(self, node_id, latitude, longitude, capacity):
        super(GeneratorNode, self).__init__(node_id, latitude, longitude)

    def assign_resource(self, resource_data): 
        """
        Parameters:
            - resource_data, ResourceData or list of ResourceData - nearby site(s) 
                  from which power actuals and/or forecast data will be constructed.
                  ResourceData type must match the GeneratorNode type.
        """
    
    def get_power(self, temporal_params, shaper=None): 
        """
        Parameters:
            - temporal_params (TemporalParameters) - requirements for timeseries
                 output
            - shaper (function or callable conforming to the TimeseriesShaper.__call__ interface) - 
                 method for converting between the resource_data time convenctions, 
                 to those defined by temporal_params

        Returns actual power timeseries for this GeneratorNode based on the 
        resource_data that has already been assigned.
        """
        self._require_resource()

    def get_forecasts(self, temporal_params, forecast_params, shaper=None): pass

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

    def get_weather(self, temporal_params, shaper=None): pass

    def save_weather(self, filename, formatter=None): pass


class NodeCollection(object):
    """
    List of nodes of the same type, with resource data already definied. This
    class is provided to give a DataFrame, rather than a Series, interface for 
    processing timeseries data in bulk.
    """
    def __init__(self, nodes): 
        # todo: implement iterating over this class work to avoid 
        #       for node in nodes.nodes:
        self.nodes = nodes

    @classmethod
    def factory(self, nodes): 
        """
        Constructs the right type of NodeCollection based on the type of nodes.
        """
        if isinstance(nodes[0],WeatherNode):
            return WeatherNodeCollection(nodes)
        return GeneratorNodeCollection(nodes)

    @property
    def locations(self):
        return [(node.latitude, node.longitude) for node in self.nodes]


class GeneratorNodeCollection(NodeCollection):

    def get_power(self, temporal_params, shaper=None): pass

    def get_forecasts(self, temporal_params, forecast_params, shaper=None): pass

    def save_power(self, filename_or_dir, formatter=None): pass

    def save_forecasts(self, filename_or_dir, formatter=None): pass    


class WeatherNodeCollection(NodeCollection): 

    def get_weather(self, temporal_params, shaper=None): pass

    def save_weather(self, filename_or_dir, formatter=None): pass
