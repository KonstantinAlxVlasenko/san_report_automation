"""Module to find switch and VC module pairs automatically"""

import numpy as np
import pandas as pd
import utilities.database_operations as dbop
import utilities.dataframe_operations as dfop
import utilities.module_execution as meop

from .device_filter import *
from .switch_filter import *
from .switch_pair_search import *
from .switch_pair_verification import verify_switch_pair_match
from .switch_pair_correction import create_wwn_name_match_series

# min connected device match ratio for the switch and the pair switch
min_device_number_match_ratio = 0.5
# min switch name match ratio for switch and the pair switch 
min_sw_name_match_ratio = 0.8

sw_pair_columns = ['Connected_device_number', 'Device_number_match', 'Device_match_ratio', 
                   'Switch_pairing_type', 'switchName_pair', 'switchWwn_pair', 'switchName_pair_by_labels',
                   'switchName_pair_max_device_connected', 'switchWwn_pair_max_device_connected']



def auto_switch_pairing(switch_params_aggregated_df, portshow_aggregated_df, fcr_xd_proxydev_df):
    """Function to find switch and VC module pairs automatically"""

    mask_valid_fabric = ~switch_params_aggregated_df[['Fabric_name', 'Fabric_label']].isin(['x', '-']).any(axis=1)
    fabric_labels_lst = switch_params_aggregated_df.loc[mask_valid_fabric, 'Fabric_label'].unique().tolist()

    # brocade switch pairs
    switch_pair_brocade_df = search_brocade_pairs(switch_params_aggregated_df, portshow_aggregated_df, fcr_xd_proxydev_df, fabric_labels_lst)
    # translate domain, front domain pairs
    switch_pair_fd_xd = search_fd_xd_pairs(switch_params_aggregated_df, fcr_xd_proxydev_df, fabric_labels_lst)
    # vc and cisco pairs
    vc_cisco_pair_df = search_vc_cisco_pairs(portshow_aggregated_df, fabric_labels_lst)
    
    switch_pair_df = pd.concat([switch_pair_brocade_df, switch_pair_fd_xd, vc_cisco_pair_df])
    switch_pair_df = verify_switch_pair_match(switch_pair_df)
    return switch_pair_df
    
def search_fd_xd_pairs(switch_params_aggregated_df, fcr_xd_proxydev_df, fabric_labels_lst):
    """Function to find front domain and translate domain pairs.
    1. Translate domain proxy device match.
    2. Switchname match for front domains"""
    
    switch_pair_fd_xd = create_fd_xd_dataframe(switch_params_aggregated_df)
    if switch_pair_fd_xd.empty:
        return switch_pair_fd_xd
    # series with wwn and switch name correspondance
    sw_fd_xd_wwn_name_match_sr = create_wwn_name_match_series(switch_pair_fd_xd)
    # translate domain switchType in device list DataFrame
    fcr_xd_proxydev_df['switchType'] = 602
    fcr_xd_proxydev_df['switchMode'] = 'Native'

    # find translate domain pairs with highest proxy device match
    switch_pair_fd_xd[sw_pair_columns] = switch_pair_fd_xd.apply(
        lambda series: find_nonzero_device_connected_switch_pair(
            series, sw_fd_xd_wwn_name_match_sr, fcr_xd_proxydev_df, 
            fabric_labels_lst, sw_pair_columns, min_device_number_match_ratio, min_sw_name_match_ratio, proxy_only=True), 
            axis=1)
    # find switch pairs with highest switchName match for swithes without device connection (front domain)
    switch_pair_fd_xd[sw_pair_columns[3:7]] = switch_pair_fd_xd.apply(
        lambda series: find_zero_device_connected_switchname_match(series, switch_pair_fd_xd.copy(), sw_fd_xd_wwn_name_match_sr, 
                                                                    sw_pair_columns, min_sw_name_match_ratio), axis=1)
    return switch_pair_fd_xd


def search_brocade_pairs(switch_params_aggregated_df, portshow_aggregated_df, fcr_xd_proxydev_df, fabric_labels_lst):
    """Function to find pairs for Brocade switches. 
    1. Device match (both npiv and direct connect) for switches with collected configs.
    2. Enclosure match for switches with collected configs but without device (strorage, server, library) connection.
    3. Switchname match for switches with collected configs but without device (strorage, server, library) connection.
    4. NPIV devices match for AG switches without configs collected but with device connected 
    and detected in Native switch configuration as NPIV ports."""
    
    # filter Brocade swithes for which pair switch need to be found
    switch_pair_brocade_df = create_sw_brocade_dataframe(switch_params_aggregated_df)
    # series with wwn and switch name correspondance
    sw_brocade_wwn_name_match_sr = create_wwn_name_match_series(switch_pair_brocade_df)
    # find devices connected to Brocade switches
    brocade_connected_devices_df = find_sw_brocade_connected_devices(portshow_aggregated_df)
    # find switch pairs with highest connected device match
    switch_pair_brocade_df[sw_pair_columns] = switch_pair_brocade_df.apply(
        lambda series: find_nonzero_device_connected_switch_pair(
            series, sw_brocade_wwn_name_match_sr, brocade_connected_devices_df, 
            fabric_labels_lst, sw_pair_columns, min_device_number_match_ratio, min_sw_name_match_ratio, npiv_only=False), 
            axis=1)


    # find switch pairs within the same enclosure
    switch_pair_brocade_df = find_enclosure_pair_switch(switch_pair_brocade_df)
    # add enclosure switch pair for switches without device connection 
    switch_pair_brocade_df = find_zero_device_connected_enclosure_sw_pair(switch_pair_brocade_df)
    # find switch pairs with highest switchName match for swithes without device connection
    switch_pair_brocade_df[sw_pair_columns[3:7]] = switch_pair_brocade_df.apply(lambda series: find_zero_device_connected_switchname_match(series, switch_pair_brocade_df.copy(), 
                                                                                                                                           sw_brocade_wwn_name_match_sr, sw_pair_columns, 
                                                                                                                                           min_sw_name_match_ratio), axis=1)
    # find switch pairs if for any of switch in pair config is not present
    portshow_npiv_devices_df = find_sw_npv_ag_connected_devices(switch_pair_brocade_df, portshow_aggregated_df, merge_column='oui_board_sn')
    switch_pair_brocade_df[sw_pair_columns] = switch_pair_brocade_df.apply(
        lambda series: find_nonzero_device_connected_switch_pair(series, sw_brocade_wwn_name_match_sr, portshow_npiv_devices_df, 
                                                                fabric_labels_lst, sw_pair_columns, 
                                                                min_device_number_match_ratio, min_sw_name_match_ratio, 
                                                                npiv_only=True), axis=1)
    return switch_pair_brocade_df


def search_vc_cisco_pairs(portshow_aggregated_df, fabric_labels_lst):
    """Function to find pairs for VC modules and Cisco switches. 
    1. NPIV devices match for VC modules and Cisco switches without configs collected but with device connected 
    and detected in Native switch configuration as NPIV ports.
    2. Switchname match for switches with collected configs but without device (strorage, server, library) connection."""
    
    # filter NPV connected switches except Brocade (VC, CISCO)
    vc_cisco_pair_df = create_sw_npv_dataframe(portshow_aggregated_df)
    # series with wwn and switch name correspondance
    vc_cisco_wwn_name_match_sr = create_wwn_name_match_series(vc_cisco_pair_df)
    
    if vc_cisco_pair_df.empty:
        return vc_cisco_pair_df
    # find devices connected to VC and Cisco switches
    portshow_vc_cisco_devices_df = find_sw_npv_ag_connected_devices(vc_cisco_pair_df, portshow_aggregated_df, merge_column='NodeName')
    # find switch pairs with highest connected device match
    vc_cisco_pair_df[sw_pair_columns] = vc_cisco_pair_df.apply(
        lambda series: find_nonzero_device_connected_switch_pair(series, vc_cisco_wwn_name_match_sr, portshow_vc_cisco_devices_df, 
                                                                fabric_labels_lst, sw_pair_columns, 
                                                                min_device_number_match_ratio, min_sw_name_match_ratio, npiv_only=True), axis=1)
    # find switch pairs with highest switchName match for swithes without device connection
    vc_cisco_pair_df[sw_pair_columns[3:7]] = vc_cisco_pair_df.apply(
        lambda series: find_zero_device_connected_switchname_match(series, vc_cisco_pair_df.copy(), vc_cisco_wwn_name_match_sr, 
                                                                    sw_pair_columns, min_sw_name_match_ratio), axis=1)     
    return vc_cisco_pair_df