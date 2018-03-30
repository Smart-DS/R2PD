"""
This module provides classes for facilitating the transfer of data between
the external and internal store as well as processing the data using a queue.
"""
import concurrent.futures as cf
import filelock
import numpy as np
import os
import pandas as pds
from scipy.spatial import cKDTree
from .powerdata import NodeCollection, GeneratorNodeCollection
from .resourcedata import ResourceList


def nearest_power_nodes(node_collection, resource_meta):
    """
    Fill requested power nodes in node_collection with resource sites in
    resource_meta

    Parameters
    ----------
    node_collection : 'pandas.DataFrame'|'GeneratorNodeCollection'
        DataFrame of requested nodes
            [node_id(index), latitude, longitude, capacity]
        or NodeCollection instance
    resource_meta : 'pd.DataFrame'
        DataFrame with resource node meta-data
            [site_id(index), latitude, longitude, capacity]

    Returns
    ---------
    nodes : 'pd.DataFrame'
        Requested nodes with site_ids and fractions of resource for each node
    """
    if isinstance(node_collection, NodeCollection):
        node_data = node_collection.node_data
    else:
        node_data = node_collection

    # Create and populate DataFrame from requested list of nodes
    nodes = pds.DataFrame(columns=['latitude', 'longitude', 'capacity',
                                   'site_id', 'site_fracs', 'r_cap'],
                          index=node_data.index)

    nodes.loc[:, ['latitude', 'longitude', 'capacity']] = node_data.values
    nodes.loc[:, 'r_cap'] = node_data['capacity (MW)']

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
        node_lat_lon = nodes_left.as_matrix(['latitude', 'longitude'])

        # Find first nearest resource node to each requested node
        dist, pos = tree.query(node_lat_lon, k=1)
        node_pairs = pds.DataFrame({'pos': pos, 'dist': dist})
        # Find the nearest pair of resource nodes and requested nodes
        node_pairs = node_pairs.groupby('pos')['dist'].idxmin()

        # Apply resource node to nearest requested node
        for i, n in node_pairs.iteritems():
            r_i = r_index[i]
            n_i = n_index[n]
            resource = r_nodes.loc[r_i].copy()
            file_id = resource.name
            cap = resource['r_cap']
            node = nodes.loc[n_i].copy()

            # Determine fract of resource node to apply to requested node
            if node['r_cap'] > cap:
                frac = cap / resource['capacity']
                resource['r_cap'] = 0
                node['r_cap'] += -1 * cap
            else:
                frac = node['r_cap'] / resource['capacity']
                resource['r_cap'] += -1 * node['r_cap']
                node['r_cap'] = 0

            if np.all(pds.isnull(node['site_id'])):
                node['site_id'] = [file_id]
                node['site_fracs'] = [frac]
            else:
                node['site_id'] += [file_id]
                node['site_fracs'] += [frac]

            r_nodes.loc[r_i] = resource
            nodes.loc[n_i] = node

        # Continue nearest neighbor search and resource distribution
        # until capacity is filled for all requested nodes
        if np.sum(nodes['r_cap'] > 0) == 0:
            break

    return nodes[['latitude', 'longitude', 'capacity', 'site_id',
                  'site_fracs']]


def nearest_met_nodes(node_collection, resource_meta):
    """
    Fill requested weather nodes in node_collection with resource sites in
    resource_meta

    Parameters
    ----------
    node_collection : 'pandas.DataFrame'|'WeatherNodeCollection'
        DataFrame of requested nodes
            [node_id(index), latitude, longitude]
        or NodeCollection instance
    resource_meta : 'pd.DataFrame'
        DataFrame with resource node meta-data
            [site_id(index), latitude, longitude]

    Returns
    ---------
    nodes : 'pd.DataFrame'
        Requested nodes with site_id of resource for each node
    """
    if isinstance(node_collection, NodeCollection):
        node_data = node_collection.node_data
    else:
        node_data = node_collection

    # Create and populate DataFrame from requested list of nodes
    nodes = pds.DataFrame(columns=['latitude', 'longitude', 'site_id'],
                          index=node_data.index)

    nodes.loc[:, ['latitude', 'longitude']] = node_data.values

    # Extract resource nodes lat, lon
    lat_lon = resource_meta.as_matrix(['latitude', 'longitude'])
    # Create cKDTree of [lat, lon] for resource nodes w/ available capacity
    tree = cKDTree(lat_lon)

    # Extract requested lat, lon
    node_lat_lon = nodes.as_matrix(['latitude', 'longitude'])
    # Find first nearest resource node to each requested node
    _, site_id = tree.query(node_lat_lon, k=1)
    nodes['site_id'] = site_id

    return nodes[['latitude', 'longitude', 'site_id']]

#TODO: Move to datastore
def cache_resource(site_file, dataset, repo):
    """
    Download the resource site file from repo and add site to cache meta

    Parameters
    ----------
    site_file : 'string'
        Path to resource file for site
    dataste : 'string'
        'wind' or 'solar'
    repo : 'ExternalDataStore'
        ExternalDataStore instance
    """
    if dataset == 'wind':
        src = repo._wind_root
        dst = repo._local_cache._wind_root
    elif dataset == 'solar':
        src = repo._solar_root
        dst = repo._local_cache._solar_root
    else:
        raise ValueError("Invalid dataset type, must be 'wind' or 'solar'")

    lock_file = os.path.join(dst, '{:}_lock'.format(dataset))
    src = os.path.join(src, site_file)
    dst = os.path.join(dst, site_file)

    try:
        repo.download(src, dst)
    except Exception:
        raise
    finally:
        with filelock.FileLock(lock_file):
            repo._local_cache.cache_site(dataset, dst)


def download_resource_data(site_ids, dataset, resource_type, repo,
                           forecasts=False, cores=None):
    """
    Download resources from repo

    Parameters
    ----------
    site_ids : 'list'
        List of site ids to be downloaded
    dataset : 'string'
        'wind' or 'solar'
    resource_type : 'string'
        'power' or 'met'
    repo : 'ExternalDataStore'
        ExternalDataStore instance
    forecasts : 'bool'
        Download forecast along with power
    cores : 'int'
        Number of cores to use for parallel downloads
        If None download in series
    """
    if dataset == 'wind':
        meta = repo.wind_meta
        if resource_type == 'power':
            data_size = len(site_ids) * repo.WIND_FILE_SIZES['power']
            if forecasts:
                data_size += len(site_ids) * repo.WIND_FILE_SIZES['fcst']
        else:
            data_size = len(site_ids) * repo.WIND_FILE_SIZES['met']
    elif dataset == 'solar':
        meta = repo.solar_meta
        if resource_type == 'power':
            data_size = len(site_ids) * repo.SOLAR_FILE_SIZES['power']
            if forecasts:
                data_size += len(site_ids) * repo.SOLAR_FILE_SIZES['fcst']
        else:
            data_size = len(site_ids) * repo.SOLAR_FILE_SIZES['met']
            data_size += len(site_ids) * repo.SOLAR_FILE_SIZES['irradiance']
    else:
        raise ValueError("Invalid dataset type, must be 'wind' or 'solar'")

    data_size = data_size / 1000

    if repo._local_cache._size is not None:
        cache_size, wind_size, solar_size = repo._local_cache.cache_size
        open_cache = repo._local_cache._size - cache_size
        if open_cache < data_size:
            raise RuntimeError('Not enough space available in local cache: \
\nDownload size = {d:.2f}GB \
\nLocal cache = {c:.2f}GB of {m:.2f}GB in use \
\n\tCached wind data = {w:.2f}GB \
\n\tCached solar data = {s:.2f}GB'.format(d=data_size, c=cache_size,
                                          m=repo._local_cache._size,
                                          w=wind_size, s=solar_size))

    files = []
    for site in site_ids:
        sub_dir = str(meta.loc[site, 'sub_directory'])
        if dataset == 'wind':
            dir_path = os.path.join(repo._local_cache._wind_root, sub_dir)
        elif dataset == 'solar':
            dir_path = os.path.join(repo._local_cache._solar_root, sub_dir)
        else:
            raise ValueError("Invalid dataset type, must be 'wind' or 'solar'")

        if not os.path.exists(dir_path):
            os.makedirs(dir_path)

        f_name = '{d}_{r}_{s}.hdf5'.format(d=dataset,
                                           r=resource_type,
                                           s=site)
        files.append(os.path.join(sub_dir, f_name))

        if resource_type == 'power' and forecasts:
            f_name = '{d}_fcst_{s}.hdf5'.format(d=dataset,
                                                s=site)
            files.append(os.path.join(sub_dir, f_name))

        if dataset == 'solar' and resource_type == 'met':
            f_name = 'solar_irradiance_{:}.hdf5'.format(site)
            files.append(os.path.join(sub_dir, f_name))

    if cores is None:
        for site in files:
            cache_resource(site, dataset, repo)
    else:
        if 'ix' not in os.name:
            EXECUTOR = cf.ThreadPoolExecutor
        else:
            EXECUTOR = cf.ProcessPoolExecutor

        with EXECUTOR(max_workers=cores) as executor:
            for site in files:
                executor.submit(cache_resource, site, dataset, repo)


def get_resource_data(node_collection, repo, forecasts=False, **kwargs):
    """
    Finds nearest nodes, caches files to local datastore and assigns resource
    to node_collection

    Parameters
    ----------
    node_collection : 'NodeCollection'
        Collection of either weather of generator nodes
    repo : 'ExternalDataStore'
        External datastore from which to get resouce data
    forecasts : 'bool'
        Whether to download forecasts along with power data
    **kwargs
        Internal kwargs

    Returns
    ---------
    node_collection : 'NodeCollection'
        Node collection with resources assigned to nodes
    nearest_nodes : 'pandas.DataFrame'
        DataFrame of the nearest neighbor matching between nodes and resources
    """
    nearest_nodes = repo.nearest_neighbors(node_collection)

    if isinstance(node_collection, GeneratorNodeCollection):
        resource_type = 'power'
        site_ids = np.concatenate(nearest_nodes['site_id'].values)
        site_ids = np.unique(site_ids)
    else:
        resource_type = 'met'
        site_ids = nearest_nodes['site_id'].values

    dataset = node_collection._dataset

    download_resource_data(site_ids, dataset, resource_type, repo,
                           forecasts=forecasts, **kwargs)

    resources = []
    for node, meta in nearest_nodes.iterrows():
        site_id = meta['site_id']
        if isinstance(site_id, list):
            fracs = meta['site_fracs']
            resource = ResourceList([repo.get_resource(dataset, site, frac=f)
                                     for site, f in zip(site_id, fracs)])
        else:
            resource = repo.get_resource(dataset, site_id)
        resources.append(resource)

    if forecasts:
        node_collection.assign_resource(resources, forecasts=forecasts)
    else:
        node_collection.assign_resource(resources)

    return node_collection, nearest_nodes
