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
import os
from .tshelpers import TemporalParameters, ForecastParameters
from .library import DefaultTimeseriesShaper


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
            - ResourceData or list of ResourceList
                - nearby site(s) from which power actuals and/or forecast data
                  will be constructed.
                  ResourceData type must match the GeneratorNode type.
        """
        self._resource_assigned = True
        self._resource = resource

    def _require_resource(self):
        if not self._resource_assigned:
            caller = inspect.getouterframes(inspect.currentframe(), 2)[1][3]
            raise RuntimeError("Resource must be defined for node {:} before \
calling ".format(self.id) + caller + ".")

    @classmethod
    def _save_csv(cls, ts_or_df, filename):
        filename = os.path.splitext(filename)[0] + '.csv'
        ts_or_df.to_csv(filename)


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
        if temporal_params is None:
            self.power = power_data
        else:
            if shaper is None:
                shaper = DefaultTimeseriesShaper()

            ts_params = TemporalParameters.infer_params(power_data)
            self.power = shaper(power_data, ts_params, temporal_params)

    def get_forecasts(self, forecast_params, shaper=None):
        #assert self._fcst
        self._require_resource()
        fcst_data = self._resource.forecast_data
        if forecast_params is None:
            self.fcst = fcst_data
        else:
            if shaper is None:
                self.fcst = fcst_data
            else:
                ts_params = TemporalParameters.infer_params(fcst_data)
                ts_params = ForecastParameters('discrete_leadtimes', ts_params,
                                               leadtimes=[24, 1, 4, 6])
                self.fcst = shaper(fcst_data, ts_params, forecast_params)

    def save_power(self, filename, formatter=None):
        if formatter is None:
            self._save_csv(self.power, filename)
        else:
            pass

    def save_forecasts(self, filename, formatter=None):
        if formatter is None:
            self._save_csv(self.fcst, filename)
        else:
            pass


class WindGeneratorNode(GeneratorNode):
    pass


class SolarGeneratorNode(GeneratorNode):
    pass


class WeatherNode(Node):
    def get_weather(self, temporal_params, shaper=None):
        self._require_resource()
        met_data = self._resource.meteorological_data

        if temporal_params is None:
            self.met = met_data
        else:
            if shaper is None:
                shaper = DefaultTimeseriesShaper()

            ts_params = TemporalParameters.infer_params(met_data,)
            self.met = shaper(met_data, ts_params, temporal_params)

    def save_weather(self, filename, formatter=None):
        if formatter is None:
            self._save_csv(self.met, filename)
        else:
            pass


class WindMetNode(WeatherNode):
    pass


class SolarMetNode(WeatherNode):
    def get_irradiance(self, temporal_params, shaper=None):
        self._require_resource()
        irradiance_data = self._resource.irradiance_data

        if temporal_params is None:
            self.irradiance = irradiance_data
        else:
            if shaper is None:
                shaper = DefaultTimeseriesShaper

            ts_params = TemporalParameters.infer_params(irradiance_data)
            self.irradiance = shaper(irradiance_data, ts_params,
                                     temporal_params)

    def save_irradiance(self, filename, formatter=None):
        if formatter is None:
            self._save_csv(self.irradiance, filename)
        else:
            pass


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
        self._ids = [node.id for node in self.nodes]

    def __repr__(self):
        return '{c} contains {n} nodes'.format(c=self.__class__.__name__,
                                               n=len(self.nodes))

    def __getitem__(self, node_id):
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
            if node_id in self._ids:
                pos = self._ids.index(node_id)
                return self.nodes[pos]
            else:
                raise IndexError

    def __len__(self):
        return(len(self.nodes))

    def assign_resource(self, resources, node_ids=None):
        if node_ids is None:
            assert len(self) == len(resources), 'number of resources ({r}) \
does not match number of nodes ({n})'.format(r=len(resources), n=len(self))
            for node, resource in zip(self.nodes, resources):
                node.assign_resource(resource)
        else:
            assert len(node_ids) == len(resources), 'number of resources ({r}) \
does not match number of nodes ({n})'.format(r=len(resources), n=len(node_ids))
            for i, resource in zip(node_ids, resources):
                self[i].assign_resource(resource)

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
        return np.array([(node.latitude, node.longitude)
                         for node in self.nodes])


class GeneratorNodeCollection(NodeCollection):
    def __init__(self, nodes):
        super(GeneratorNodeCollection, self).__init__(nodes)
        if isinstance(self.nodes[0], WindGeneratorNode):
            self._dataset = 'wind'
        elif isinstance(self.nodes[0], SolarGeneratorNode):
            self._dataset = 'solar'
        else:
            raise RuntimeError('Must be a collection of either \
solar or wind nodes')

    @property
    def node_data(self):
        node_data = [(node.id, node.latitude, node.longitude, node.capacity)
                     for node in self.nodes]
        return np.array(node_data)

    def get_power(self, temporal_params, shaper=None):
        for node in self.nodes:
            node.get_power(temporal_params, shaper=shaper)

    def get_forecasts(self, forecast_params, shaper=None):
        for node in self.nodes:
            node.get_fcst(forecast_params, shaper=shaper)

    def save_power(self, out_dir, file_prefix=None, formatter=None):
        for node in self.nodes:
            i = node.id
            if file_prefix is None:
                file_name = '{d}_power_{i}'.format(d=self._dataset, i=i)
            else:
                file_name = '{f}_{i}'.format(f=file_prefix, i=i)

            file_name = os.path.join(out_dir, file_name)

            if formatter is None:
                node.save_power(file_name)
            else:
                pass

    def save_forecasts(self, out_dir, file_prefix=None, formatter=None):
        for node in self.nodes:
            i = node.id
            if file_prefix is None:
                file_name = '{d}_fcst_{i}'.format(d=self._dataset, i=i)
            else:
                file_name = '{f}_{i}'.format(f=file_prefix, i=i)

            file_name = os.path.join(out_dir, file_name)

            if formatter is None:
                node.save_fcst(file_name)
            else:
                pass


class WeatherNodeCollection(NodeCollection):
    def __init__(self, nodes):
        super(WeatherNodeCollection, self).__init__(nodes)
        if isinstance(self.nodes[0], WindMetNode):
            self._dataset = 'wind'
        elif isinstance(self.nodes[0], SolarMetNode):
            self._dataset = 'solar'
        else:
            raise RuntimeError('Must be a collection of either \
solar or wind nodes')

    @property
    def node_data(self):
        node_data = [(node.id, node.latitude, node.longitude)
                     for node in self.nodes]
        return np.array(node_data)

    def get_weather(self, temporal_params, shaper=None):
        for node in self.nodes:
            node.get_weather(temporal_params, shaper=shaper)

    def save_weather(self, out_dir, file_prefix=None, formatter=None):
        for node in self.nodes:
            i = node.id
            if file_prefix is None:
                file_name = '{d}_met_{i}'.format(d=self._dataset, i=i)
            else:
                file_name = '{f}_{i}'.format(f=file_prefix, i=i)

            file_name = os.path.join(out_dir, file_name)

            if formatter is None:
                node.save_weather(file_name)
            else:
                pass
