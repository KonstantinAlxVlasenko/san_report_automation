"""Module to separate storage and servers links. Servers are grouped based on link description
(all servers with the same link description are in signle shape). Servers are grouped based on
device name (each storage is a single shape) or link description (list of storages with the equal links description
are single shape). If number of storages in fabric exceeds threshold then link description merging approach is applied.
If fabric swith pairs exceeds threshold then two Visio draws is created (with switches only and switches and edge devices)"""

import pandas as pd

import utilities.dataframe_operations as dfop

from .shape_grouping import (group_device_on_link_description,
                             group_device_on_name)


def create_device_shape_links(connected_devices_df, san_graph_sw_pair_df, san_graph_grid_df, pattern_dct, san_topology_constantants_sr):
    """Function to create device shapes and link shapes to the switch shapes.
    Devices shapes for severs and storages if number of storages in the fabric_name
    exceeds STORAGE_NUMBER_PER_FABRIC_THRESHOLD are group of devices with the same
    link description. Resturn dataframe with device shapes and its details
    to build Visio graph"""

    # count storages and libraries number in the fabric_name
    connected_devices_df = count_fabric_storage(connected_devices_df)
    # separate pure switches graph and switches + edge devices graph
    connected_devices_df, san_graph_sw_pair_group_df, fabric_name_duplicated_lst, fabric_name_dev_lst = \
        create_device_fabric_name(connected_devices_df, san_graph_sw_pair_df, san_topology_constantants_sr)
    # create storages and servers visio master shapes details  
    storage_shape_links_df = create_storage_shape_links(connected_devices_df, san_graph_grid_df, pattern_dct, san_topology_constantants_sr)
    server_shape_links_df = create_server_shape_links(connected_devices_df, san_graph_grid_df)
    return connected_devices_df, storage_shape_links_df, server_shape_links_df, san_graph_sw_pair_group_df, fabric_name_duplicated_lst, fabric_name_dev_lst



def count_fabric_storage(connected_devices_df):
    """Function to count storages and libraries number in the fabric_name.
    Returns connected_devices_df with 'Storage_lib_number_in_fabric' column."""

    # check if storage need to be grouped
    mask_storage_lib = connected_devices_df['deviceType'].isin(['STORAGE', 'LIB'])
    connected_storages_df = connected_devices_df.loc[mask_storage_lib].copy()
    storage_number_df = connected_storages_df.groupby(
        by=['Fabric_name'])['Device_Host_Name'].nunique().to_frame(name='Storage_lib_number_in_fabric')
    connected_devices_df = pd.merge(connected_devices_df, storage_number_df, how='left', on='Fabric_name')
    connected_devices_df['Storage_lib_number_in_fabric'].fillna(0, inplace=True)
    return connected_devices_df


def create_device_fabric_name(connected_devices_df, san_graph_sw_pair_df, san_topology_constantants_sr):
    """Function to verify if Visio page with pure san graph without edge devices will present in Visio document.
    If number of switch pairs in the fabric ecxceeds SW_PAIR_NUMBER_PER_FABRIC_THRESHOLD then devices from it 
    are moved to the fabric with the tag DEVICE_FABRIC_NAME_TAG.
    Retturns connected_devices_df with updated Fabric_name and san_graph_sw_pair_df with added device fabrics
    to perform grouping in Visio page of switch pairs master shapes"""

    SW_PAIR_NUMBER_PER_FABRIC_THRESHOLD = int(san_topology_constantants_sr['sw_pair_number_per_fabric_threshold'])
    DEVICE_FABRIC_NAME_TAG = san_topology_constantants_sr['device_fabric_name_tag']
    
    # find fabric_names where edge devices are connected
    fabric_name_with_dev = connected_devices_df['Fabric_name'].unique()
    # count switch_pairs in each fabric
    fabric_name_sw_pair_count_df = san_graph_sw_pair_df.groupby(by=['Fabric_name'])['switchName_Wwn'].count().to_frame().reset_index()
    fabric_name_sw_pair_count_df.rename(columns={'switchName_Wwn': 'switchPair_quantity'}, inplace=True)
    # find fabric_names with devices with and where switch_pair number exceeds threshold
    # these fabrics need to be copied
    mask_fabric_with_dev = fabric_name_sw_pair_count_df['Fabric_name'].isin(fabric_name_with_dev)
    mask_sw_pair_threshold = fabric_name_sw_pair_count_df['switchPair_quantity'] > SW_PAIR_NUMBER_PER_FABRIC_THRESHOLD
    fabric_name_duplicated_lst = fabric_name_sw_pair_count_df.loc[mask_fabric_with_dev & mask_sw_pair_threshold, 'Fabric_name'].to_list()
    # create names for device fabrics if they need to be copied
    fabric_name_dev_lst = [fabric_name + DEVICE_FABRIC_NAME_TAG for fabric_name in fabric_name_duplicated_lst]
    # rename fabric_name in connecetd_devices_df
    connected_devices_df['Fabric_name_cp'] = connected_devices_df['Fabric_name']
    connected_devices_df['Fabric_name'].replace(to_replace=fabric_name_duplicated_lst, value=fabric_name_dev_lst, inplace=True)
    
    # update san_graph_sw_pair
    # filter fabirc_names which are need to be copied
    san_graph_sw_pair_duplicated_df = san_graph_sw_pair_df.loc[san_graph_sw_pair_df['Fabric_name'].isin(fabric_name_duplicated_lst)].copy()
    # change fabric_names to the new names
    san_graph_sw_pair_duplicated_df['Fabric_name'].replace(to_replace=fabric_name_duplicated_lst, value=fabric_name_dev_lst, inplace=True)
    # add renamed fabic_names to original one to use later to group master shapes in pure switches and switches + edge devices graphs
    san_graph_sw_pair_group_df = pd.concat([san_graph_sw_pair_df, san_graph_sw_pair_duplicated_df])
    return connected_devices_df, san_graph_sw_pair_group_df, fabric_name_duplicated_lst, fabric_name_dev_lst


def create_storage_shape_links(connected_devices_df, san_graph_grid_df, pattern_dct, san_topology_constantants_sr):
    """Function to create switch master shape -> storage and library master shape link details.
    If number of storages and libraries in the fabric exceeds STORAGE_NUMBER_PER_FABRIC_THRESHOLD
    then to avoid graph overload storages are grouped based on Link_description so storages connected
    to the same pair(s) nof switches with same link speeds, npiv modes and link number number presented
    with single master shape"""
    
    STORAGE_NUMBER_PER_FABRIC_THRESHOLD = int(san_topology_constantants_sr['storage_number_per_fabric_threshold'])
    
    # storages links
    mask_storage_lib = connected_devices_df['deviceType'].isin(['STORAGE', 'LIB'])
    mask_threshold_exceeded = connected_devices_df['Storage_lib_number_in_fabric'] > STORAGE_NUMBER_PER_FABRIC_THRESHOLD
    
    storage_under_threshold_df = connected_devices_df.loc[mask_storage_lib & ~mask_threshold_exceeded].copy()
    storage_above_threshold_df = connected_devices_df.loc[mask_storage_lib & mask_threshold_exceeded].copy()
    storage_shape_links_df = pd.concat([group_device_on_name(storage_under_threshold_df, san_graph_grid_df, pattern_dct), 
                                        group_device_on_link_description(storage_above_threshold_df, san_graph_grid_df)])
    return storage_shape_links_df


def create_server_shape_links(connected_devices_df, san_graph_grid_df):
    """Function to create switch master shape -> server and unknown master shape link details.
    To avoid graph overload servers are grouped based on Link_description and enclosure so servers connected
    to the same pair(s) of switches with same link speeds, npiv modes, enclosure and link number number presented
    with single master shape"""

    # server and unknown links
    mask_srv_unknown = connected_devices_df['deviceType'].str.contains('SRV|UNKNOWN')
    server_df = connected_devices_df.loc[mask_srv_unknown].copy()
    server_shape_links_df = group_device_on_link_description(server_df, san_graph_grid_df)
    return server_shape_links_df