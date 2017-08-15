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
    - Sum multiple timeseries to represent the combined output over larger
      areas all tied into the same node
"""

import inspect
import numpy as np
from .tshelpers import TemporalParameters


class Node(object):
    def __init__(self, node_id, latitude, longitude):
        self._resource_assigned = False
        self.id = int(node_id)
        self.latitude = latitude
        self.longitude = longitude

    def __repr__(self):
        return '{n} {i}'.format(n=self.__class__.__name__, i=self.id)

    def assign_resource(self, resource):
        """
        Parameters:
            - resource_data, ResourceData or list of ResourceData
                - nearby site(s) from which power actuals and/or forecast data
                  will be constructed.
                  ResourceData type must match the GeneratorNode type.
        """
        self._resource_assigned = True
        self._resource = resource

    def _require_resource(self):
        if not self._resource_assigned:
            caller = inspect.getouterframes(inspect.currentframe(), 2)[1][3]
            raise RuntimeError("Resource must be defined before calling " +
                               caller + ".")


class GeneratorNode(Node):
    def __init__(self, node_id, latitude, longitude, capacity):
        super(GeneratorNode, self).__init__(node_id, latitude, longitude)
        self.capacity = capacity

    def get_power(self, temporal_params, shaper=None):
        """
        Parameters:
            - temporal_params (TemporalParameters)
                - requirements for timeseries output
            - shaper (function or callable conforming to the
                TimeseriesShaper.__call__ interface)
                    -  method for converting between the resource_data time
                    convenctions, to those defined by temporal_params

        Returns actual power timeseries for this GeneratorNode based on the
        resource_data that has already been assigned.
        """
        self._require_resource()
        power_data = self._resource.power_data

        if shaper is None:
            return power_data
        else:
            p_interp = 'instantaneous'
            ts_params = TemporalParameters.infer_params(power_data,
                                                        point_interp=p_interp)
            shaper(power_data, ts_params, temporal_params)

    def get_forecasts(self, temporal_params, forecast_params, shaper=None):
        pass

    def save_power(self, filename, formatter=None):
        pass

    def save_forecasts(self, filename, formatter=None):
        pass

    @classmethod
    def _get_power(cls, ts_or_df, temporal_params):
        pass

    @classmethod
    def _get_forecasts(cls, ts_or_df, temporal_params, forecast_params):
        pass


class WindGeneratorNode(GeneratorNode):
    pass


class SolarGeneratorNode(GeneratorNode):
    pass


class WeatherNode(Node):
    def get_weather(self, temporal_params, shaper=None):
        self._require_resource()
        met_data = self._resource.meteorological_data

        if shaper is None:
            return met_data
        else:
            p_interp = 'instantaneous'
            ts_params = TemporalParameters.infer_params(met_data,
                                                        point_interp=p_interp)
            shaper(met_data, ts_params, temporal_params)

    def save_weather(self, filename, formatter=None):
        pass


class WindMetNode(WeatherNode):
    pass


class SolarMetNode(WeatherNode):
    def get_irradiance(self, temporal_params, shaper=None):
        self._require_resource()
        met_data = self._resource.met_data

        if shaper is None:
            return met_data
        else:
            p_interp = 'instantaneous'
            ts_params = TemporalParameters.infer_params(met_data,
                                                        point_interp=p_interp)
            shaper(met_data, ts_params, temporal_params)


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

    def __repr__(self):
        return '{c} contains {n} nodes'.format(c=self.__class__.__name__,
                                               n=len(self.nodes))

    def __getitem__(self, index):
            """
            Exract variable 'variable_name' from dataset.
            Parameters
            ----------
            variable_name : 'sting'
                Variable key

            Returns
            ---------
            'nc.dataset.variable'
                variable instance from dataset, to get values call [:]
            """
            if index >= len(self.nodes):
                raise IndexError
            return self.nodes[index]

    def assign_resource(self, resource_list):
        for node, resource in zip(self.nodes, resource_list):
            node.assign_resource(resource)

    @classmethod
    def factory(cls, nodes):
        """
        Constructs the right type of NodeCollection based on the type of nodes.
        """
        if isinstance(nodes[0], WeatherNode):
            return WeatherNodeCollection(nodes)
        return GeneratorNodeCollection(nodes)

    @property
    def locations(self):
        return [(node.latitude, node.longitude) for node in self.nodes]


class GeneratorNodeCollection(NodeCollection):
    @property
    def node_data(self):
        node_data = [(node.id, node.latitude, node.longitude, node.capacity)
                     for node in self.nodes]
        return np.array(node_data)

    def get_power(self, temporal_params, shaper=None):
        pass

    def get_forecasts(self, temporal_params, forecast_params, shaper=None):
        pass

    def save_power(self, filename_or_dir, formatter=None):
        pass

    def save_forecasts(self, filename_or_dir, formatter=None):
        pass


class WeatherNodeCollection(NodeCollection):
    @property
    def node_data(self):
        return [(node.id, node.latitude, node.longitude)
                for node in self.nodes]

    def get_weather(self, temporal_params, shaper=None):
        pass

    def save_weather(self, filename_or_dir, formatter=None):
        pass
