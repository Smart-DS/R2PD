"""
This module provides classes for accessing site-level wind and solar data from
internal and external data stores.
"""
import numpy as np
import pandas as pds
from scipy.spatial import cKDTree


class DataStore(object):
    """
    Abstract class to define interface for accessing stores of resource data.
    """

    @classmethod
    def connect(cls, config=None):
        """
        Connects to the store (internal cache or external repository) and
        returns an instantiated DataStore object.
        """

    def nearest_neighbors(self, dataset, lat_long_tuples, num_neighbors=1):
        """
        Returns list or list of lists containing resourcedata.ResourceLocation
        objects.
        """

    def get_power_nodes(self, node_list, resource_meta):
        """
        Fill requested power nodes in node list with resource node
        Parameters
        ----------
        node_list : 'ndarray'
            Array of requested nodes [lat, lon, capacity]
        resource_meta : 'pd.DataFrame'
            DataFrame with resource node meta-data [lat, lon, capacity]

        Returns
        ---------
        nodes : 'pd.DataFrame'
            Requested nodes with resource ids and fractions of each id
        """
        # Create and populate DataFrame from requested list of nodes
        nodes = pds.DataFrame(columns=['lat', 'lon', 'cap', 'ids', 'fracs',
                                       'r_cap'])
        nodes['lat'] = node_list[:, 0]
        nodes['lon'] = node_list[:, 1]
        nodes['cap'] = node_list[:, 2]
        # Placeholder for remaining capacity to be filled
        nodes['r_cap'] = node_list[:, 2]

        r_nodes = resource_meta[['longitude', 'latitude', 'capacity']].copy()
        # Add placeholder for remaining capacity available at resource node
        r_nodes['r_cap'] = r_nodes['capacity']

        while True:
            # Extract resource nodes w/ remaining capacity
            r_left = r_nodes[r_nodes['r_cap'] > 0]
            r_index = r_left.index
            lat_lon = r_left.as_matrix(['latitude', 'longitude'])
            # Create cKDTree of [lat, lon] for r_nodes w/ available capacity
            tree = cKDTree(lat_lon)

            # Extract nodes that still have capacity to be filled
            nodes_left = nodes[nodes['r_cap'] > 0]
            n_index = nodes_left.index
            node_lat_lon = nodes_left.as_matrix(['lat', 'lon'])

            # Find first nearest resource node to each requested node
            dist, pos = tree.query(node_lat_lon, k=1)
            node_pairs = pds.DataFrame({'pos': pos, 'dist': dist})
            # Find the closest unique pairs of r_nodes and requested nodes
            node_pairs = node_pairs.groupby('pos')['dist'].idxmin()

            # Apply resource node to nearest requested node
            for i, n in node_pairs.iteritems():
                r_i = r_index[i]
                n_i = n_index[n]
                resource = r_nodes.iloc[r_i].copy()
                file_id = resource.name
                cap = resource['r_cap']
                node = nodes.iloc[n_i].copy()

                # Determine fraction of r_node to apply to requested node
                if node['r_cap'] > cap:
                    frac = cap / resource['capacity']
                    resource['r_cap'] = 0
                    node['r_cap'] += -1 * cap
                else:
                    frac = node['r_cap'] / resource['capacity']
                    resource['r_cap'] += -1 * node['r_cap']
                    node['r_cap'] = 0

                if np.all(pds.isnull(node['ids'])):
                    node['ids'] = [file_id]
                    node['fracs'] = [frac]
                else:
                    node['ids'] += [file_id]
                    node['fracs'] += [frac]

                r_nodes.iloc[r_i] = resource
                nodes.iloc[n_i] = node

            # Continue nearest neighbor search and resource distribution until
            # capacity is filled for all requested nodes
            if np.sum(nodes['r_cap'] > 0) == 0:
                break

        return nodes[['lat', 'lon', 'cap', 'ids', 'fracs']]

    def get_met_nodes(self, node_list, resource_meta):
        """
        Fill requested weather nodes in node list with resource node
        Parameters
        ----------
        node_list : 'ndarray'
            Array of requested nodes [lat, lon]
        resource_meta : 'pd.DataFrame'
            DataFrame with resource node meta-data [lat, lon]

        Returns
        ---------
        nodes : 'pd.DataFrame'
            Requested nodes with resource ids
        """
        # Create and populate DataFrame from requested list of nodes
        nodes = pds.DataFrame(columns=['lat', 'lon', 'id', 'empty'])
        nodes['lat'] = node_list[:, 0]
        nodes['lon'] = node_list[:, 1]
        # Placeholder for nodes remaining yet to be filled
        nodes['empty'] = True

        r_nodes = resource_meta[['longitude', 'latitude']].copy()
        # Add placeholder for unused resource node
        r_nodes['available'] = True

        while True:
            # Extract unused resource nodes
            r_left = r_nodes[r_nodes['available']]
            r_index = r_left.index
            lat_lon = r_left.as_matrix(['latitude', 'longitude'])
            # Create cKDTree of [lat, lon] for unused resource nodes
            tree = cKDTree(lat_lon)

            # Extract nodes that still have capacity to be filled
            nodes_left = nodes[nodes['empty']]
            n_index = nodes_left.index
            node_lat_lon = nodes_left.as_matrix(['lat', 'lon'])

            # Find first nearest resource node to each requested node
            dist, pos = tree.query(node_lat_lon, k=1)
            node_pairs = pds.DataFrame({'pos': pos, 'dist': dist})
            # Find the closest unique pairs of r_nodes and requested nodes
            node_pairs = node_pairs.groupby('pos')['dist'].idxmin()

            # Apply resource node to nearest requested node
            for i, n in node_pairs.iteritems():
                r_i = r_index[i]
                n_i = n_index[n]
                resource = r_nodes.iloc[r_i].copy()
                file_id = resource.name
                node = nodes.iloc[n_i].copy()

                node['id'] = file_id
                node['empty'] = False
                resource['available'] = False

                r_nodes.iloc[r_i] = resource
                nodes.iloc[n_i] = node

            # Continue nearest neighbor search and resource distribution until
            #  all requested nodes have been filled
            if np.sum(nodes['empty']) == 0:
                break

        return nodes[['lat', 'lon', 'id']]

    def get_data(self, dataset, file_ids):
        """
        Returns list of resourcedata.ResourceData objects, one entry per
        file_ids element. If any file_id is not valid or not in the store,
        None is returned in that spot.
        """


class ExternalDataStore(DataStore):
    pass


class DRPower(ExternalDataStore):
    pass


class InternalDataStore(DataStore):
    """
    This class manages an internal cache of already downloaded resource data,
    and other Resource Data Tool information that should persist.

    The default location for the internal cache will be in a place like
    Users/$User/AppData, but the user can set a different location by passing
    in a configuration file.

    A configuration file can also be used to set user library locations, for
    pointing to externally provided shapers and formatters.
    """

    @classmethod
    def connect(cls, config=None):
        """
        Reads the configuration, if provided. From configuration and defaults,
        determines location of internal data cache. If the cache is not yet
        there, creates it. Returns an InternalDataStore object open and ready
        for querying / adding data.
        """

    def cache_data(self, location_data_tuples):
        """
        Saves each (ResourceLocation, ResourceData) tuple to disk and logs it
        in the registry / database.
        """
