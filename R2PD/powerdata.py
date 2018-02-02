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
import os
import pandas as pds
from .tshelpers import TemporalParameters, ForecastParameters
#from .library import DefaultTimeseriesShaper


class Node(object):
    """
    Abstract class for a single Node
    """
    def __init__(self, node_id, latitude, longitude):
        """
        Initialize generic Node object

        Parameters
        ----------
        node_id : 'string'|'int'
            Node id, must be an integer
        latitude : 'float'
            Latitude of node
        longitude : 'float'
            Longitude of node
        """
        self._resource_assigned = False
        self.id = int(node_id)
        self.latitude = latitude
        self.longitude = longitude

    def __repr__(self):
        """
        Prints the type of node and its id

        Returns
        ---------
        'str'
            type of Node and its id
        """
        return '{n} {i}'.format(n=self.__class__.__name__, i=self.id)

    def assign_resource(self, resource):
        """
        Assign resource to Node

        Parameters
        ----------
        resource : 'Resource'|'ResourceList'
            Resource or ResourceList instance with resource site(s) for node
        """
        self._resource_assigned = True
        self._resource = resource

    def _require_resource(self):
        """
        Checks to ensure resource has been assigned
        """
        if not self._resource_assigned:
            caller = inspect.getouterframes(inspect.currentframe(), 2)[1][3]
            raise RuntimeError("Resource must be defined for node {:} before \
calling ".format(self.id) + caller + ".")

    @classmethod
    def _save_csv(cls, df, file_path):
        """
        Saves data to csv with given file_path

        Parameters
        ----------
        df : 'pandas.DataFrame'
            timeseries data to be saved
        """
        file_path = os.path.splitext(file_path)[0] + '.csv'
        df.to_csv(file_path)


class GeneratorNode(Node):
    """
    Abstract class for GeneratorNode
    """
    def __init__(self, node_id, latitude, longitude, capacity):
        """
        Initialize generic GeneratorNode object

        Parameters
        ----------
        node_id : 'string'|'int'
            Node id, must be an integer
        latitude : 'float'
            Latitude of node
        longitude : 'float'
            Longitude of node
        capacity : 'float'
            Capacity of generator in MW
        """
        super(GeneratorNode, self).__init__(node_id, latitude, longitude)
        self.capacity = capacity

    def assign_resource(self, resource, forecasts=False):
        """
        Assign resource to Node

        Parameters
        ----------
        resource : 'Resource'|'ResourceList'
            Resource or ResourceList instance with resource site(s) for node
        forecasts : 'bool'
            Are forecasts included in generator resource
        """
        self._resource_assigned = True
        self._resource = resource
        self._fcst = forecasts

    def get_power(self, temporal_params, shaper=None):
        """
        Extracts and processes power data for Node

        Parameters
        ----------
        temporal_params : 'TemporalParameters'
            Requiements for timeseries output
        shaper : 'TimeseriesShaper'|'function'
            Method to convert Resource data into required output
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
        """
        Extracts and processes forecast data for Node

        Parameters
        ----------
        forecast_params : 'ForecastParameters'
            Requiements for forecast output
        shaper : 'ForecastShaper'|'function'
            Method to convert forecast data into required output
        """
        assert self._fcst
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

    def save_power(self, file_path, formatter=None):
        """
        Save power data to disc

        Parameters
        ----------
        file_path : 'string'
            Output file path
        formatter : ''
            Method to save powerdata to desired format
        """
        if formatter is None:
            self._save_csv(self.power, file_path)
        else:
            pass

    def save_forecasts(self, file_path, formatter=None):
        """
        Save forecast data to disc

        Parameters
        ----------
        file_path : 'string'
            Output file path
        formatter : ''
            Method to save powerdata to desired format
        """
        if formatter is None:
            self._save_csv(self.fcst, file_path)
        else:
            pass


class WindGeneratorNode(GeneratorNode):
    """
    Class for Wind Generator Nodes
    """
    pass


class SolarGeneratorNode(GeneratorNode):
    """
    Class for Solar Generator Nodes
    """
    pass


class WeatherNode(Node):
    """
    Abstract Class for Weather Nodes
    """
    def get_weather(self, temporal_params, shaper=None):
        """
        Extracts and processes weather data for Node

        Parameters
        ----------
        temporal_params : 'TemporalParameters'
            Requiements for timeseries output
        shaper : 'TimeseriesShaper'|'function'
            Method to convert Resource data into required output
        """
        self._require_resource()
        met_data = self._resource.meteorological_data

        if temporal_params is None:
            self.met = met_data
        else:
            if shaper is None:
                shaper = DefaultTimeseriesShaper()

            ts_params = TemporalParameters.infer_params(met_data,)
            self.met = shaper(met_data, ts_params, temporal_params)

    def save_weather(self, file_path, formatter=None):
        """
        Save weather data to disc

        Parameters
        ----------
        file_path : 'string'
            Output file path
        formatter : ''
            Method to save powerdata to desired format
        """
        if formatter is None:
            self._save_csv(self.met, file_path)
        else:
            pass


class WindMetNode(WeatherNode):
    """
    Class for Wind Weather Nodes
    """
    pass


class SolarMetNode(WeatherNode):
    """
    Class for Solar Weather Nodes
    """
    def get_irradiance(self, temporal_params, shaper=None):
        """
        Extracts and processes irradiance data for Node

        Parameters
        ----------
        temporal_params : 'TemporalParameters'
            Requiements for timeseries output
        shaper : 'TimeseriesShaper'|'function'
            Method to convert Resource data into required output
        """
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
        """
        Save weather data to disc

        Parameters
        ----------
        file_path : 'string'
            Output file path
        formatter : ''
            Method to save powerdata to desired format
        """
        if formatter is None:
            self._save_csv(self.irradiance, filename)
        else:
            pass


class NodeCollection(object):
    """
    Abstract Class of list of nodes of the same type.
    This class is provided to interface w/ Pandas for processing timeseries
    data in bulk. (TODO)
    """
    def __init__(self, nodes):
        """
        Initialize generic NodeCollection object

        Parameters
        ----------
        nodes : 'list'
            List of Node objects
        """
        # TODO: implement iterating over this class work to avoid
        #       for node in nodes.nodes:
        self.nodes = nodes
        self._ids = [node.id for node in self.nodes]

    def __repr__(self):
        """
        Prints the type of NodeCollection and number of nodes it contains

        Returns
        ---------
        'str'
            type of NodeCollection and number of nodes
        """
        return '{c} contains {n} nodes'.format(c=self.__class__.__name__,
                                               n=len(self.nodes))

    def __getitem__(self, node_id):
        """
        Extract node with given id

        Parameters
        ----------
        node_id : 'int'
            id of node of interest

        Returns
        ---------
        'Node'
            Node object for node of interest
        """
        if node_id in self._ids:
            pos = self._ids.index(node_id)
            return self.nodes[pos]
        else:
            raise IndexError

    def __len__(self):
        """
        Return number of nodes in NodeCollection

        Returns
        ---------
        'int'
            Size of NodeCollection
        """
        return(len(self.nodes))

    def assign_resource(self, resources, node_ids=None):
        """
        Assign resource to nodes in NodeCollection

        Parameters
        ----------
        resources : 'list'
            List of Resource or ResourceList objects
        node_ids : 'list'
            node ids that correspond to Resource in resources list
        """
        if node_ids is None:
            assert len(self) == len(resources), 'number of resources ({r}) \
does not match number of nodes ({n})'.format(r=len(resources), n=len(self))
            for node, resource in zip(self.nodes, resources):
                node.assign_resource(resource)
        else:
            assert len(node_ids) == len(resources), 'number of resources ({r}) \
does not match number of nodes ({n})'.format(r=len(resources), n=len(node_ids))
            for i, resource in zip(node_ids, resources):
                pos = self._ids.index(i)
                self.nodes[pos].assign_resource(resource)

    @classmethod
    def factory(cls, nodes):
        """
        Constructs the right type of NodeCollection based on the type of nodes.

        Parameters
        ----------
        nodes : 'list'
            List of Node objects

        Returns
        ---------
        'NodeCollection'
            Proper type of node collection based on type of Nodes
        """
        if isinstance(nodes[0], WeatherNode):
            return WeatherNodeCollection(nodes)
        return GeneratorNodeCollection(nodes)

    @property
    def locations(self):
        """
        DataFrame of (latitude, longitude) coordinates for nodes in
        NodeCollection

        Returns
        ---------
        'pandas.DataFrame'
            Latitude and longitude for each node in NodeCollection
        """
        lat_lon = [(node.id, node.latitude, node.longitude)
                   for node in self.nodes]
        lat_lon = pds.DataFrame(lat_lon,
                                columns=['node_id', 'latitude', 'longitude'])
        lat_lon = lat_lon.set_index('node_id')
        return lat_lon


class GeneratorNodeCollection(NodeCollection):
    """
    Collection of GeneratorNodes
    """
    def __init__(self, nodes):
        """
        Initialize GeneratorNodeCollection object
        Determines if the nodes are wind or solar nodes

        Parameters
        ----------
        nodes : 'list'
            List of Node objects
        """
        super(GeneratorNodeCollection, self).__init__(nodes)
        if isinstance(self.nodes[0], WindGeneratorNode):
            self._dataset = 'wind'
        elif isinstance(self.nodes[0], SolarGeneratorNode):
            self._dataset = 'solar'
        else:
            raise RuntimeError('Must be a collection of either \
solar or wind nodes')

    def assign_resource(self, resources, node_ids=None, forecasts=False):
        """
        Assign resource to nodes in GeneratorNodeCollection

        Parameters
        ----------
        resources : 'list'
            List of Resource or ResourceList objects
        node_ids : 'list'
            node ids that correspond to Resource in resources list
        forecasts : 'bool'
            If forecasts are included in resources or not
        """
        if node_ids is None:
            assert len(self) == len(resources), 'number of resources ({r}) \
does not match number of nodes ({n})'.format(r=len(resources), n=len(self))
            for node, resource in zip(self.nodes, resources):
                node.assign_resource(resource, forecasts=forecasts)
        else:
            assert len(node_ids) == len(resources), 'number of resources ({r}) \
does not match number of nodes ({n})'.format(r=len(resources), n=len(node_ids))
            for i, resource in zip(node_ids, resources):
                pos = self._ids.index(i)
                self.nodes[pos].assign_resource(resource, forecasts=forecasts)

    @property
    def node_data(self):
        """
        Array of node data [id, latitude, longitude, capacity (MW)]

        Returns
        ---------
        'nd.array'
            Meta data for all nodes in GeneratorNodeCollection
            [id, latitude, longitude, capacity (MW)]
        """
        node_data = [(node.id, node.latitude, node.longitude, node.capacity)
                     for node in self.nodes]
        node_data = pds.DataFrame(node_data,
                                  columns=['node_id', 'latitude', 'longitude',
                                           'capacity (MW)'])
        node_data = node_data.set_index('node_id')
        return node_data

    def get_power(self, temporal_params, shaper=None):
        """
        Extracts and processes power data for all Nodes in
        GeneratorNodeCollection

        Parameters
        ----------
        temporal_params : 'TemporalParameters'
            Requiements for timeseries output
        shaper : 'TimeseriesShaper'|'function'
            Method to convert Resource data into required output
        """
        for node in self.nodes:
            node.get_power(temporal_params, shaper=shaper)

    def get_forecasts(self, forecast_params, shaper=None):
        """
        Extracts and processes forecast data for all nodes in
        GeneratorNodeCollection

        Parameters
        ----------
        forecast_params : 'ForecastParameters'
            Requiements for forecast output
        shaper : 'ForecastShaper'|'function'
            Method to convert forecast data into required output
        """
        for node in self.nodes:
            node.get_fcst(forecast_params, shaper=shaper)

    def save_power(self, out_dir, file_prefix=None, formatter=None):
        """
        Save power data to disc

        Parameters
        ----------
        out_dir : 'string'
            Path to root directory to save power data
        file_prefix : 'string'
            Prefix for files to be save after appending node id and extension
        formatter : ''
            Method to save powerdata to desired format
        """
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
        """
        Save forecast data to disc

        Parameters
        ----------
        out_dir : 'string'
            Path to root directory to save power data
        file_prefix : 'string'
            Prefix for files to be save after appending node id and extension
        formatter : ''
            Method to save powerdata to desired format
        """
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
        """
        Initialize WeatherNodeCollection object
        Determines if the nodes are wind or solar nodes

        Parameters
        ----------
        nodes : 'list'
            List of Node objects
        """
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
        """
        Array of node data [id, latitude, longitude]

        Returns
        ---------
        'nd.array'
            Meta data for all nodes in WeatherNodeCollection
            [id, latitude, longitude]
        """
        node_data = [(node.id, node.latitude, node.longitude)
                     for node in self.nodes]
        node_data = pds.DataFrame(node_data,
                                  columns=['node_id', 'latitude', 'longitude'])
        node_data = node_data.set_index('node_id')
        return node_data

    def get_weather(self, temporal_params, shaper=None):
        """
        Extracts and processes weather data for all nodes in
        WeatherNodeCollection

        Parameters
        ----------
        temporal_params : 'TemporalParameters'
            Requiements for timeseries output
        shaper : 'TimeseriesShaper'|'function'
            Method to convert Resource data into required output
        """
        for node in self.nodes:
            node.get_weather(temporal_params, shaper=shaper)

    def save_weather(self, out_dir, file_prefix=None, formatter=None):
        """
        Save weather data to disc

        Parameters
        ----------
        out_dir : 'string'
            Path to root directory to save power data
        file_prefix : 'string'
            Prefix for files to be save after appending node id and extension
        formatter : ''
            Method to save powerdata to desired format
        """
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
