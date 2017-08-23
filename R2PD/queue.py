"""
This module provides classes for facilitating the transfer of data between
the external and internal store as well as processing the data using a queue.
"""
import numpy as np
import pandas as pds
from scipy.spatial import cKDTree
from .powerdata import NodeCollection, GeneratorNodeCollection


def nearest_power_nodes(node_collection, resource_meta):
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
        Requested nodes with site_ids and fractions of resource for each id
    """
    if isinstance(node_collection, NodeCollection):
        node_data = node_collection.node_data
    else:
        node_data = node_collection

    # Create and populate DataFrame from requested list of nodes
    nodes = pds.DataFrame(columns=['lat', 'lon', 'cap', 'site_ids',
                                   'site_fracs', 'r_cap'],
                          index=node_data[:, 0].astype(int))
    nodes['lat'] = node_data[:, 1]
    nodes['lon'] = node_data[:, 2]
    nodes['cap'] = node_data[:, 3]
    # Placeholder for remaining capacity to be filled
    nodes['r_cap'] = node_data[:, 3]

    r_nodes = resource_meta[['longitude', 'latitude', 'capacity']].copy()
    # Add placeholder for remaining capacity available at resource node
    r_nodes['r_cap'] = r_nodes['capacity']

    while True:
        # Extract resource nodes w/ remaining capacity
        r_left = r_nodes[r_nodes['r_cap'] > 0]
        r_index = r_left.index
        lat_lon = r_left.as_matrix(['latitude', 'longitude'])
        # Create cKDTree of [lat, lon] for resource nodes
        # w/ available capacity
        tree = cKDTree(lat_lon)

        # Extract nodes that still have capacity to be filled
        nodes_left = nodes[nodes['r_cap'] > 0]
        n_index = nodes_left.index
        node_lat_lon = nodes_left.as_matrix(['lat', 'lon'])

        # Find first nearest resource node to each requested node
        dist, pos = tree.query(node_lat_lon, k=1)
        node_pairs = pds.DataFrame({'pos': pos, 'dist': dist})
        # Find the nearest pair of resource nodes and requested nodes
        node_pairs = node_pairs.groupby('pos')['dist'].idxmin()

        # Apply resource node to nearest requested node
        for i, n in node_pairs.iteritems():
            r_i = r_index[i]
            n_i = n_index[n]
            resource = r_nodes.iloc[r_i].copy()
            file_id = resource.name
            cap = resource['r_cap']
            node = nodes.iloc[n_i].copy()

            # Determine fract of resource node to apply to requested node
            if node['r_cap'] > cap:
                frac = cap / resource['capacity']
                resource['r_cap'] = 0
                node['r_cap'] += -1 * cap
            else:
                frac = node['r_cap'] / resource['capacity']
                resource['r_cap'] += -1 * node['r_cap']
                node['r_cap'] = 0

            if np.all(pds.isnull(node['site_ids'])):
                node['site_ids'] = [file_id]
                node['site_fracs'] = [frac]
            else:
                node['site_ids'] += [file_id]
                node['site_fracs'] += [frac]

            r_nodes.iloc[r_i] = resource
            nodes.iloc[n_i] = node

        # Continue nearest neighbor search and resource distribution
        # until capacity is filled for all requested nodes
        if np.sum(nodes['r_cap'] > 0) == 0:
            break

    return nodes[['lat', 'lon', 'cap', 'site_ids', 'site_fracs']]


def nearest_met_nodes(node_collection, resource_meta):
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
        Requested nodes with resource site_ids
    """
    if isinstance(node_collection, NodeCollection):
        node_data = node_collection.node_data
    else:
        node_data = node_collection

    # Create and populate DataFrame from requested list of nodes
    nodes = pds.DataFrame(columns=['lat', 'lon', 'site_id'],
                          index=node_data[:, 0].astype(int))
    nodes['lat'] = node_data[:, 1]
    nodes['lon'] = node_data[:, 2]

    # Extract resource nodes lat, lon
    lat_lon = resource_meta.as_matrix(['latitude', 'longitude'])
    # Create cKDTree of [lat, lon] for resource nodes w/ available capacity
    tree = cKDTree(lat_lon)

    # Extract requested lat, lon
    node_lat_lon = nodes.as_matrix(['lat', 'lon'])
    # Find first nearest resource node to each requested node
    _, site_id = tree.query(node_lat_lon, k=1)
    nodes['site_id'] = site_id

    return nodes[['lat', 'lon', 'site_id']]


def download_resource(site_ids, repo, dataset, resource_type, forecast=False,
                      cores=None):
    if dataset == 'wind':
        if resource_type == 'power':
            data_size = len(site_ids) * repo.WIND_FILE_SIZES['power']
            if forecasts:
                data_size += len(site_ids) * repo.WIND_FILE_SIZES['fcst']
        else:
            data_size = len(site_ids) * repo.WIND_FILE_SIZES['met']
    else:
        if resource_type == 'power':
            data_size = len(site_ids) * repo.SOLAR_FILE_SIZES['power']
            if forecasts:
                data_size += len(site_ids) * repo.SOLAR_FILE_SIZES['fcst']
        else:
            data_size = len(site_ids) * repo.SOLAR_FILE_SIZES['met']
            data_size += len(site_ids) * repo.SOLAR_FILE_SIZES['irradiance']

    if repo._local_cache.size is not None:
        cache_size, wind_size, solar_size = repo._local_cache.cache_size
        open_cache = repo._local_cache.max_size - cache_size
        if open_cache < data_size:
            raise RuntimeError('Not enough space available in local cache: \
\nDownload size = {d}GB \
\nLocal cache = {c}GB of {m}GB in use \
\n\tCached wind data = {w}GB \
\n\tCached solar data = {s}GB'.format(d=data_size, c=cache_size,
                                      m=repo._local_cache.size,
                                      w=wind_size, s=solar_size))
        else:
            if dataset == 'wind':


def get_resource_data(node_collection, repo,  **kwargs):
    """
    Finds nearest nodes, caches files to local datastore and assigns resource
    to node_collection
    """
    nearest_nodes = repo.nearest_nodes(node_collection)

    if isinstance(node_collection, GeneratorNodeCollection):
        resource_type = 'power'
        site_ids = np.concatenate(nearest_nodes['site_ids'].values)
        site_ids = np.unique(site_ids)
    else:
        resource_type = 'met'
        site_ids = nearest_nodes['site_ids'].values

    dataset = node_collection._dataset
