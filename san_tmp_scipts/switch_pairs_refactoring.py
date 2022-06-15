# -*- coding: utf-8 -*-
"""
Created on Thu Jan 27 16:15:23 2022

@author: vlasenko
"""

import os
script_dir = r'C:\Users\vlasenko\OneDrive - Hewlett Packard Enterprise\Documents\05.PYTHON\Projects\san_tmp_scipts'
# Change the current working directory
os.chdir(script_dir)
import general_cmd_module as dfop

import os
# import re
# import sqlite3
import numpy as np
import pandas as pd

# import openpyxl
# from datetime import date
# import warnings
# import subprocess
# import sys
from difflib import SequenceMatcher
# from collections import defaultdict




# db_path = r"C:\Users\vlasenko\OneDrive - Hewlett Packard Enterprise\Documents\01.CUSTOMERS\MTS\SAN Assessment\NOV2021\mts_south\database_MTS_south"
# db_file = r"MTS_south_analysis_database.db"


# db_path = r"C:\Users\vlasenko\OneDrive - Hewlett Packard Enterprise\Documents\01.CUSTOMERS\MTS\SAN Assessment\NOV2021\mts_spb\database_MTS_spb"
# db_file = r"MTS_spb_analysis_database.db"

db_path = r"C:\Users\vlasenko\OneDrive - Hewlett Packard Enterprise\Documents\01.CUSTOMERS\Megafon\MegafonMSK\SAN Assessment\MAY21\database_MegafonMSK"
db_file = r"MegafonMSK_analysis_database.db"




data_names = ['switch_params_aggregated', 'portshow_aggregated']

switch_params_aggregated_df, portshow_aggregated_df = dfop.read_database(db_path, db_file, *data_names)


# min connetceted device match ratio for the switch and the pair switch
min_device_number_match_ratio = 0.5
# min switch name match ratio for switch and the pair switch 
min_sw_name_match_ratio = 0.8

sw_pair_columns = ['Connected_device_number', 'Device_number_match', 'Device_match_ratio', 
                   'Switch_pairing_type', 'switchName_pair', 'switchWwn_pair', 'switchName_pair_by_labels',
                   'switchName_pair_max_device_connected', 'switchWwn_pair_max_device_connected']

sw_pair_match_columns = ['Fabric_name', 
                         'switchWwn_occurrence_in_switchWwn_pair', 'switchWwn_pair_occurrence_in_switchWwn', 
                         'switchWwn_pair_duplication']

def find_switch_pairs(switch_params_aggregated_df, portshow_aggregated_df):
    


    mask_valid_fabric = ~switch_params_aggregated_df[['Fabric_name', 'Fabric_label']].isin(['x', '-']).any(axis=1)
    fabric_labels_lst = switch_params_aggregated_df.loc[mask_valid_fabric, 'Fabric_label'].unique().tolist()

    switch_pair_brocade_df = verify_brocade_pairs(switch_params_aggregated_df, portshow_aggregated_df, fabric_labels_lst)
    vc_cisco_pair_df = verify_vc_cisco_pairs(portshow_aggregated_df, fabric_labels_lst)
    
    switch_pair_df = pd.concat([switch_pair_brocade_df, vc_cisco_pair_df])
    switch_pair_df = verify_switch_pair_match(switch_pair_df)
    
    
  
    
    if all_switch_pairs_matched(switch_pair_df):
        print("All switch pairs have matched")
    else:
        print("SwitchWwn pair mismatch found")
        wwn_occurrence_stats_df = count_switch_pairs_match_stats(switch_pair_df)
        print(wwn_occurrence_stats_df)
        
        switch_pair_df = update_switch_pair_dataframe(switch_pair_df)
        


def assign_switch_pair_id(switch_pair_df):
    """Function to assighn switch pair Id based on sorted combination of switchWwn and switchWwn_paired"""
    
    # merge wwns of the paired swithes
    switch_pair_df = dfop.merge_columns(switch_pair_df, 'switch_pair_wwns', merge_columns=['switchWwn', 'switchWwn_pair'], drop_merge_columns=False)
    # sort merged wwns in cells to have identical cell values for both of the paired rows
    dfop.sort_cell_values(switch_pair_df, 'switch_pair_wwns')
    # numbering identical switch_pair_wwns
    switch_pair_df['switch_pair_id'] = switch_pair_df.groupby(['switch_pair_wwns']).ngroup()
    switch_pair_df.sort_values(by=['Fabric_name', 'switch_pair_id'], inplace=True)
    return switch_pair_df

def update_switch_pair_dataframe(switch_pair_df):
    """Function to update switchName and switchWwn occurrence columns after manual 
    switchWwn_pair correction in switch_pair_df change"""
    
    # correct switch names
    sw_wwn_name_match_sr = create_wwn_name_match_series(switch_pair_df)
    switch_pair_df['switchName_pair'] = switch_pair_df.apply(lambda series: switch_name_correction(series, sw_wwn_name_match_sr), axis=1)
    # correct switchWwn occurence
    switch_pair_df = verify_switch_pair_match(switch_pair_df)
    return switch_pair_df


def create_wwn_name_match_series(switch_pair_df):
    """Function to create series containing switchWwn to switchName match"""
    
    sw_wwn_name_match_sr = dfop.series_from_dataframe(switch_pair_df.drop_duplicates(subset=['switchWwn']), 
                                                      index_column='switchWwn', value_column='switchName')
    return sw_wwn_name_match_sr

def switch_name_correction(series, sw_wwn_name_match_sr):
    """Function to correct switchName of the paired switch after manual switchWwn_pair correction"""
    
    if pd.isna(series['switchWwn_pair']):
        return np.nan
    
    sw_name_lst = [sw_wwn_name_match_sr[wwn] for wwn in series['switchWwn_pair'].split(', ')]
    if sw_name_lst:
        return ', '.join(sw_name_lst)
    

def verify_switch_pair_match(switch_pair_df):
    """Function to verify switchWwn and switchWwn_pair columns match. 
    All Wwns of switchWwn column present in switchWwn_pair column and vice versa. 
    Count occurence of each column value in othjer column (switchWwn_occurence_in_switchWwn_pair, switchWwn_pair_occurence_in_switchWwn). 
    Also count occurence each Wwn in switchWwn_pair in the same column (switchWwn_pair_duplication).
    If all values are 1 then all switch pairs are matched"""
    
    switch_pair_df['switchWwn_occurrence_in_switchWwn_pair'] = switch_pair_df['switchWwn'].apply(lambda value: dfop.verify_value_occurence_in_series(value, switch_pair_df['switchWwn_pair']))
    switch_pair_df['switchWwn_pair_occurrence_in_switchWwn'] = switch_pair_df['switchWwn_pair'].apply(lambda value: dfop.verify_value_occurence_in_series(value, switch_pair_df['switchWwn']))
    switch_pair_df['switchWwn_pair_duplication'] = switch_pair_df.groupby(['switchWwn_pair'])['switchWwn'].transform('count')
    
    # for 'switchWwn_occurrence_in_switchWwn_pair', 'switchWwn_pair_occurrence_in_switchWwn' columns 
    # if switchWwn is not present then ocсurrence number is 0
    for column in sw_pair_match_columns[1:3]:
        switch_pair_df[column].fillna(0, inplace=True)  
    
    return switch_pair_df


def all_switch_pairs_matched(switch_pair_df):
    """Function to verify if all switch pairs matched. 
    All switchWwn values are in switchWwn_pair columns and vice versa.
    SwitchWwn_pair column has no duplicated values. 
    """
    
    return (switch_pair_df[sw_pair_match_columns[1:]] == 1).all(axis=1).all()
    

def count_switch_pairs_match_stats(switch_pair_df):
    """Function to count switch pair match statistics for each fabric name.
    Stat DataFrame has three columns: 
        ok - switchWwn quantity which are present in all trhree sw_pair_match_columns columns only once,
        absent - switchWwn quantity which are not resent in pair column (switchWwn in switchWwn_pair or vice verca),
        duplicated - switchWwn quantity which are present in pair column or in the same switchWwn_pair column more then once."""
    
    wwn_occurrence_stats_df = pd.DataFrame()
    wwn_occurrence_df = switch_pair_df[sw_pair_match_columns].copy()
    for column in sw_pair_match_columns[1:]:
        # replace 0, 1, >2 with absent, ok and duplicated. nan stays nan
        wwn_occurrence_df[column] = np.select([wwn_occurrence_df[column] == 0, wwn_occurrence_df[column] == 1, wwn_occurrence_df[column] > 1], 
                                              ['absent', 'ok', 'duplicated'], default=None)
        # count wwn occurrence stats for each fabric name
        wwn_stat_df = wwn_occurrence_df.groupby(by=['Fabric_name', column])[column].count().unstack()
        # add column name dor which stats is counted for
        wwn_stat_df['Switch_pair_stat_type'] = column
        # add current column stats to total stats DataFrame
        wwn_occurrence_stats_df = pd.concat([wwn_occurrence_stats_df, wwn_stat_df])
    # change total stats DataFrame presentation
    wwn_occurrence_stats_df.reset_index(inplace=True)
    wwn_occurrence_stats_df = dfop.move_column(wwn_occurrence_stats_df, cols_to_move='Switch_pair_stat_type', ref_col='Fabric_name')
    wwn_occurrence_stats_df.sort_values(by=['Fabric_name', 'Switch_pair_stat_type'], inplace=True)
    wwn_occurrence_stats_df.reset_index(inplace=True, drop=True)
    return wwn_occurrence_stats_df


def verify_brocade_pairs(switch_params_aggregated_df, portshow_aggregated_df, fabric_labels_lst):
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
    switch_pair_brocade_df[sw_pair_columns] = switch_pair_brocade_df.apply(lambda series: find_nonzero_device_connected_switch_pair(series, sw_brocade_wwn_name_match_sr, brocade_connected_devices_df, 
                                                                                                                                    fabric_labels_lst, min_device_number_match_ratio, min_sw_name_match_ratio, 
                                                                                                                                    npiv_only=False), axis=1)
    # find switch pairs within the same enclosure
    switch_pair_brocade_df = find_enclosure_pair_switch(switch_pair_brocade_df)
    # add enclosure switch pair for switches without device connection 
    switch_pair_brocade_df = find_zero_device_connected_enclosure_sw_pair(switch_pair_brocade_df)
    # find switch pairs with highest switchName match for swithes without device connection
    switch_pair_brocade_df[sw_pair_columns[3:7]] = switch_pair_brocade_df.apply(lambda series: find_zero_device_connected_switchname_match(series, switch_pair_brocade_df.copy(), 
                                                                                                                                           sw_brocade_wwn_name_match_sr, min_sw_name_match_ratio), axis=1)
    # find switch pairs if for any of switch in pair config is not present
    portshow_npiv_devices_df = find_sw_npv_ag_connected_devices(switch_pair_brocade_df, portshow_aggregated_df, merge_column='oui_board_sn')
    switch_pair_brocade_df[sw_pair_columns] = switch_pair_brocade_df.apply(lambda series: find_nonzero_device_connected_switch_pair(series, sw_brocade_wwn_name_match_sr, portshow_npiv_devices_df, 
                                                                                                                                    fabric_labels_lst, min_device_number_match_ratio, min_sw_name_match_ratio, 
                                                                                                                                    npiv_only=True), axis=1)
    return switch_pair_brocade_df


def verify_vc_cisco_pairs(portshow_aggregated_df, fabric_labels_lst):
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
    vc_cisco_pair_df[sw_pair_columns] = vc_cisco_pair_df.apply(lambda series: find_nonzero_device_connected_switch_pair(series, vc_cisco_wwn_name_match_sr, portshow_vc_cisco_devices_df, 
                                                                                                                        fabric_labels_lst, min_device_number_match_ratio, min_sw_name_match_ratio, 
                                                                                                                        npiv_only=True), axis=1)
    # find switch pairs with highest switchName match for swithes without device connection
    vc_cisco_pair_df[sw_pair_columns[3:7]] = vc_cisco_pair_df.apply(lambda series: find_zero_device_connected_switchname_match(series, vc_cisco_pair_df.copy(), vc_cisco_wwn_name_match_sr, min_sw_name_match_ratio), axis=1)     
    return vc_cisco_pair_df
    
    
    
def create_sw_brocade_dataframe(switch_params_aggregated_df):
    """Function to filter Brocade swithes for which pair switch need to be found"""
    
    mask_valid_fabric = ~switch_params_aggregated_df[['Fabric_name', 'Fabric_label']].isin(['x', '-']).any(axis=1)
    switch_columns = ['configname', 'Fabric_name', 'Fabric_label', 'Device_Location', 
                       'chassis_name', 'chassis_wwn', 'switchName', 'switchWwn', 
                       'switchType', 'ModelName', 'switchMode', 'LS_type_report']
    switch_pair_brocade_df = switch_params_aggregated_df.loc[mask_valid_fabric,switch_columns].copy()
    return switch_pair_brocade_df


def create_sw_npv_dataframe(portshow_aggregated_df):
    """Function to filter NPV connected switches except Brocade (VC, CISCO) for which pair VC module or switch need to be found"""
    
    mask_valid_fabric = ~portshow_aggregated_df[['Fabric_name', 'Fabric_label']].isin(['x', '-']).any(axis=1)
    mask_npiv = portshow_aggregated_df['Connected_NPIV'] == 'yes'
    mask_vc_cisco = portshow_aggregated_df['deviceSubtype'].isin(['VC FC', 'VC FLEX', 'CISCO'])
    vc_cisco_columns = ['Fabric_name', 'Fabric_label', 'Device_Host_Name', 'NodeName', 'deviceType', 'deviceSubtype']
    vc_cisco_pair_df = portshow_aggregated_df.loc[mask_valid_fabric & mask_npiv & mask_vc_cisco, vc_cisco_columns].drop_duplicates().copy()
    vc_cisco_pair_df.rename(columns={'Device_Host_Name': 'switchName', 'NodeName': 'switchWwn'}, inplace=True)
    
    # add switchType, switchMode, configname columns for run function to find pair switch (pair candidate must have same switcType and switchMode)
    vc_cisco_pair_df['switchType'] = vc_cisco_pair_df['deviceSubtype']
    vc_cisco_pair_df['switchType'].replace(to_replace={'VC FC': 501, 'VC FLEX': 502, 'CISCO': 503, 'HUAWEI': 504}, inplace=True)
    
    vc_cisco_pair_df['switchMode'] = vc_cisco_pair_df['deviceSubtype']
    vc_cisco_pair_df['switchMode'].replace(to_replace={'VC FC': 'Access Gateway Mode', 'VC FLEX': 'Access Gateway Mode', 'CISCO': 'NPV', 'HUAWEI': 'NPV'}, inplace=True)
    
    vc_cisco_pair_df['configname'] = np.nan
    vc_cisco_pair_df['switchWwn_pair'] = np.nan
    
    return vc_cisco_pair_df
    
    
def find_sw_brocade_connected_devices(portshow_aggregated_df):
    """Function to find devices connected to Brocade switches. 
    Devices connected to Native mode switches with direct or through NPIV port connection.
    Devices connecetd directly to AG mode switches"""

    mask_devicetype_notna = portshow_aggregated_df['deviceType'].notna()
    mask_device_lib_srv_stor = ~portshow_aggregated_df['deviceType'].isin(['SWITCH', 'VC'])
    brocade_connected_devices_df = portshow_aggregated_df.loc[mask_devicetype_notna & mask_device_lib_srv_stor]
    return brocade_connected_devices_df


def find_sw_npv_ag_connected_devices(switch_pair_df, portshow_aggregated_df, merge_column):
    """Function to find deviced connected behind NPIV switch ports (AG mode switches or VC module and Cisco switches).
    AG switchWwn identified based on AG switchPort Wwpn except first two symbols pairs(merge_column='oui_board_sn).
    VC and Cisco switchWwn retrived directly from device Wwnn (merge_column='NodeName').
    Parameter merge_column idetifies column in switch_pair_df which values used to filter 
    AG switch, Cisco switch or VC module ports in in portshow_aggregated_df."""
    
    mask_native = portshow_aggregated_df['switchMode'] == 'Native'
    portshow_cp_df = portshow_aggregated_df.loc[mask_native].copy()
    # extract Domain ID and Area ID from FCID to identify all devices connected throught the same NPIV port
    portshow_cp_df['Connected_Domain_Area_Id'] = portshow_cp_df['Connected_portId'].str.extract(r'^(.{4})(?:.{2})')
    
    # create dataframe containing oui, NodeName correspondance with switchWwn
    switch_wwn_oui_df = switch_pair_df[['Fabric_name', 'Fabric_label', 'switchWwn']].copy()
    switch_wwn_oui_df['oui_board_sn'] = switch_pair_df['switchWwn'].str.extract(r'^[\da-f:]{6}(.+)')
    switch_wwn_oui_df['NodeName'] = switch_pair_df['switchWwn']
    # switchWwn is already present in portshow_cp_df thus Connected_switchWwn column is used
    switch_wwn_oui_df['Connected_switchWwn'] = switch_wwn_oui_df['switchWwn']
    
    # add connected AG, VC or NPV switch Wwnn to Native switch port information
    portshow_cp_df = dfop.dataframe_fillna(portshow_cp_df, switch_wwn_oui_df, join_lst=['Fabric_name', 'Fabric_label', merge_column], filled_lst=['Connected_switchWwn'])
    # filter AG, VC or NPV switch ports connected to Native switches based on merge_column
    mask_connected_wwn_oui = portshow_cp_df[merge_column].isin(switch_wwn_oui_df[merge_column])
    portshow_sw_df = portshow_cp_df.loc[mask_connected_wwn_oui].copy()

    # add switchWwn information to all devices within the same domain ID, Area ID (adding Wwnn of the switch where all devices directly connected)
    portshow_cp_df = dfop.dataframe_fillna(portshow_cp_df, portshow_sw_df, join_lst=['Fabric_name', 'Fabric_label', 'Connected_Domain_Area_Id'], 
                                                                           filled_lst=['Connected_NPIV', 'Connected_switchWwn'])
    # filter device ports only connecetd to the AG, VC and NPV switches from switch_pair_df
    mask_devicetype_notna = portshow_cp_df['deviceType'].notna()
    mask_device_lib_srv_stor = ~portshow_cp_df['deviceType'].isin(['SWITCH', 'VC'])
    mask_connected_switchwwn_notna = portshow_cp_df['Connected_switchWwn'].notna()
    device_columns = ['Fabric_name', 'Fabric_label', 'Connected_switchWwn', 'Device_Host_Name']
    npv_ag_connected_devices_df = portshow_cp_df.loc[mask_devicetype_notna & mask_device_lib_srv_stor & mask_connected_switchwwn_notna, device_columns]
    # switchWwn, switchType, switchMode columns used to run function to find pair switch (pair candidate must have same switcType and switchMode)
    npv_ag_connected_devices_df.rename(columns={'Connected_switchWwn': 'switchWwn'}, inplace=True)
    npv_ag_connected_devices_df = dfop.dataframe_fillna(npv_ag_connected_devices_df, switch_pair_df, join_lst=['switchWwn'], filled_lst=['switchType', 'switchMode'])
    return npv_ag_connected_devices_df



def find_nonzero_device_connected_switch_pair(switch_sr, sw_wwn_name_match_sr, portshow_devices_df, fabric_labels_lst, 
                                              min_device_number_match_ratio, min_sw_name_match_ratio, npiv_only=False):
    """Function to find pair switch for the switch_sr. Candidates switches have to be same switchType and switchMode.
    Then candidate switches are checked for connected devices. 
    Switch with the largest number of matched devices and exceeded min_device_number_match_ratio considered to be pair switch.
    If more then one switch in the fabric correspond to that criteria then name match performed. 
    Switch with the largest name match considered to be pair. 
    If still more then one switch correspond to name match criteria then all switches considered to be pair 
    and manual pair switch assigment should be performed later"""

    print('------------')
    print(switch_sr['switchName'], npiv_only)
    
    # swithes with no configs collected checked through npiv port of Native switch only
    if not npiv_only and pd.isna(switch_sr['configname']):
        return pd.Series([np.nan]*7)
    
    if npiv_only and pd.notna(switch_sr['switchWwn_pair']):
        return pd.Series([switch_sr[column] for column in sw_pair_columns])   
    
    # find devices connected to the current switch
    mask_current_switch = portshow_devices_df['switchWwn'] == switch_sr['switchWwn']
    sw_current_devices_sr = portshow_devices_df.loc[mask_current_switch, 'Device_Host_Name']
    connected_device_number = sw_current_devices_sr.count()
        
    # list of fabric labels to verify (all fabric labels except fabric label of the switch being checked)
    verified_label_lst = [fabric_label for fabric_label in fabric_labels_lst if fabric_label != switch_sr['Fabric_label']]
    
    print(switch_sr['Fabric_name'], verified_label_lst)
    
    if sw_current_devices_sr.empty:
        if len(verified_label_lst) == 1:
            match_statistics = [0, 0]
        else:
            match_statistics = [', '.join(map(str, [0]*len(verified_label_lst)))]*2
        return pd.Series([0, *match_statistics, *[np.nan]*6])
    
    # lists with names and wwnns of the pair switches for all fabric_labels in verified_label_lst
    sw_pair_wwn_final_lst = []
    sw_pair_name_final_lst = []
    # list with pair sw names if more then two faric_labels present in the fabric
    sw_pair_name_final_with_label_lst = []
    
    sw_pair_wwn_max_device_connected_lst = []
    sw_pair_name_max_device_connected_lst = []

    max_device_match_number_lst = []
    max_device_match_ratio_lst = []
    sw_pairing_type_lst = ['npiv_device_list'] if npiv_only else ['device_list']
    
    for verified_label in verified_label_lst:
        
        print(verified_label)
        
        # find candidate pair switches with the same switchType and switchMode within the same Fabric_name in verified Fabric_label
        mask_same_sw_type_mode = (portshow_devices_df[['switchType', 'switchMode']] == switch_sr[['switchType', 'switchMode']]).all(axis=1)
        mask_same_fabric_name = portshow_devices_df['Fabric_name'] == switch_sr['Fabric_name']
        mask_verified_label = portshow_devices_df['Fabric_label'] == verified_label
        portshow_sw_candidates_df = portshow_devices_df.loc[mask_same_sw_type_mode & mask_same_fabric_name & mask_verified_label]
        
        # check portshow_sw_candidates_df to find switches with the largest connected device match
        max_device_match_number, max_device_match_number_ratio, sw_pair_name_lst, sw_pair_wwn_lst = \
            find_max_device_match_switch(sw_wwn_name_match_sr, sw_current_devices_sr, portshow_sw_candidates_df, min_device_number_match_ratio)
        max_device_match_number_lst.append(max_device_match_number)
        max_device_match_ratio_lst.append(max_device_match_number_ratio)
        
        if sw_pair_wwn_lst:
            print(sw_pair_wwn_lst)
            print('__________')
            sw_pair_wwn_max_device_connected_lst.extend(sw_pair_wwn_lst)
            sw_pair_name_max_device_connected_lst.extend(sw_pair_name_lst)
            
            # if sw_pair_wwn_lst contains more then one switch then choose one with the highest name match ratio
            if len(sw_pair_wwn_lst) > 1:
                sw_pair_name_lst, sw_pair_wwn_lst = \
                    find_max_switchname_match(switch_sr['switchName'], sw_pair_name_lst, sw_pair_wwn_lst, sw_wwn_name_match_sr, min_sw_name_match_ratio)
                sw_pairing_type_lst.append('switch_name')
                
            sw_pair_wwn_final_lst.extend(sw_pair_wwn_lst)
            sw_pair_name_final_lst.extend(sw_pair_name_lst)
                
            # if there are more then one fabric label to verify then add fabric label to paired switch name
            if len(verified_label_lst) > 1:
                sw_pair_name_final_with_label_lst.append('(' + verified_label + ': ' + ', '.join(sw_pair_name_lst) + ')')

    if sw_pair_wwn_final_lst:
        # if more then one fabric_label to check
        if len(max_device_match_number_lst) == 1:
            match_statistics = [connected_device_number, max_device_match_number_lst[0], max_device_match_ratio_lst[0]]
        else:
            match_statistics = [connected_device_number, ', '.join(map(str, max_device_match_number_lst)), ', '.join(map(str, max_device_match_ratio_lst))]
        # summary containing all results
        sw_pair_summary_lst = [sw_pairing_type_lst, 
                               sw_pair_name_final_lst, sw_pair_wwn_final_lst, 
                               sw_pair_name_final_with_label_lst, sw_pair_name_max_device_connected_lst, sw_pair_wwn_max_device_connected_lst]
        sw_pair_summary_lst = [', '.join(lst) if lst else np.nan for lst in sw_pair_summary_lst ]
        return pd.Series([*match_statistics, *sw_pair_summary_lst])


def find_max_switchname_match(switch_name, sw_pair_name_lst, sw_pair_wwn_lst, sw_wwn_name_match_sr, min_sw_name_match_ratio):
    """Auxiliary function to find switches in the sw_pair_name_lst which names have highest match with switch_name"""
    
    # find highest name match ratio 
    name_match_ratio_lst = [round(SequenceMatcher(None, switch_name, sw_pair_name).ratio(), 2) for sw_pair_name in sw_pair_name_lst]
    max_name_match_ratio = max(name_match_ratio_lst)
    print(max_name_match_ratio)
    # hisghest name match ration should exceed min_sw_name_match_ratio
    if max_name_match_ratio >= min_sw_name_match_ratio:
        # find indexes with the hisghest name match ratio
        max_idx_lst = [i for i, name_match_ration in enumerate(name_match_ratio_lst) if name_match_ration == max_name_match_ratio]
        # choose switches with the highest name match ratio indexes
        sw_pair_wwn_lst = [sw_pair_wwn_lst[i] for i in max_idx_lst]
        sw_pair_name_lst = [sw_wwn_name_match_sr[wwn] for wwn in sw_pair_wwn_lst]
        print(sw_pair_name_lst, sw_pair_wwn_lst)
        return sw_pair_name_lst, sw_pair_wwn_lst
    else:
        return (None,)*2


def find_max_device_match_switch(sw_wwn_name_match_sr, sw_current_devices_sr, portshow_sw_candidates_df, min_device_number_match_ratio):
    """Auxiliary function to find switches which have maximum connected device match with the switch 
    for which pair switch is being checked for(in sw_current_devices_sr)"""
    
    # list with wwns of the candidate switches to be pair with the current switch
    sw_candidates_wwn_lst = portshow_sw_candidates_df['switchWwn'].unique().tolist()
    
    if not sw_candidates_wwn_lst:
        return [np.nan]*3
    
    # list with the number of device matches of each candidate switch with the current switch
    # how many devices from current switch connected to switch being verified
    device_match_number_lst = []
    
    for sw_candidate_wwn in sw_candidates_wwn_lst:
        mask_sw_candidate_wwn = portshow_sw_candidates_df['switchWwn'] == sw_candidate_wwn
        # find devices connected to the candidate switch
        sw_candidate_devices_sr = portshow_sw_candidates_df.loc[mask_sw_candidate_wwn, 'Device_Host_Name']
        # count device match number
        device_match_number_lst.append(sw_current_devices_sr.isin(sw_candidate_devices_sr).sum())
    
    # if there is switch with at least 80 percentage of device match
    max_device_match_number = max(device_match_number_lst)
    max_device_match_number_ratio = round(max_device_match_number/sw_current_devices_sr.count(), 2)
    
    if max_device_match_number_ratio > min_device_number_match_ratio:
        # find switch wwns with maximum device match number
        sw_pair_wwn_lst = [sw_candidates_wwn_lst[i] for i in range(len(sw_candidates_wwn_lst)) if device_match_number_lst[i] == max_device_match_number]
        # find switch names with maximum device match number
        sw_pair_name_lst = [sw_wwn_name_match_sr[sw_wwn] for sw_wwn in sw_pair_wwn_lst]
        return max_device_match_number, max_device_match_number_ratio, sw_pair_name_lst, sw_pair_wwn_lst
    else:
        return max_device_match_number, max_device_match_number_ratio, np.nan, np.nan
    

    
def find_enclosure_pair_switch(switch_pair_df):
    """Function to find pair switches in single enclosure"""
    
    enclosure_fabric_columns = ['Fabric_name', 'Fabric_label', 'Enclosure']
    enclosure_switch_columns = ['switchName_pair_in_enclosure', 'switchWwn_pair_in_enclosure']
    
    if switch_pair_df['Device_Location'].isna().all():
        return switch_pair_df
    
    # extract enclosure name
    switch_pair_df['Enclosure'] = switch_pair_df['Device_Location'].str.extract(r'^Enclosure (.+) bay')
    fabric_labels_lst = switch_pair_df['Fabric_label'].unique().tolist()
    switch_pair_enclosure_filled_total_df = pd.DataFrame()
    
    for fabric_label in fabric_labels_lst:
        
        mask_fabric_label = switch_pair_df['Fabric_label'] == fabric_label
        # switches for the all fabric_labels except the one which are being checked for paired switches
        switch_pair_location_df = switch_pair_df.loc[~mask_fabric_label].copy()
        # join all switchName and switchWwn for the switches in the verified fabric labels and merge both DataFrames
        switch_pair_location_name_df = switch_pair_location_df.groupby(by=['Fabric_name', 'Enclosure'])['switchName'].agg(lambda x: ', '.join(x)).to_frame()
        switch_pair_location_wwn_df = switch_pair_location_df.groupby(by=['Fabric_name', 'Enclosure'])['switchWwn'].agg(lambda x: ', '.join(x)).to_frame()
        switch_pair_location_df = switch_pair_location_name_df.merge(switch_pair_location_wwn_df, how='left', left_index=True, right_index=True)
        # switchName and switchWwn columns in verified fabric labels renamed to pair switchName and pair switchWwn 
        switch_pair_location_df.rename(columns={'switchName': 'switchName_pair_in_enclosure', 'switchWwn': 'switchWwn_pair_in_enclosure'}, inplace=True)
        switch_pair_location_df.reset_index(inplace=True)
        # Fabric_label assigned to value of fabric label being checked for pair switches
        switch_pair_location_df['Fabric_label'] = fabric_label
        # switchpair DataFrame for fabric label being checked
        switch_pair_enclosure_filled_current_df = switch_pair_df.loc[mask_fabric_label]
        # add switchName and switchWwn of the paired switches
        switch_pair_enclosure_filled_current_df = dfop.dataframe_fillna(switch_pair_enclosure_filled_current_df, switch_pair_location_df, join_lst=enclosure_fabric_columns, filled_lst=enclosure_switch_columns)
        # concatenate switchPair DataFrames for all fabric labels
        if switch_pair_enclosure_filled_total_df.empty:
            switch_pair_enclosure_filled_total_df = switch_pair_enclosure_filled_current_df.copy()
        else:
            switch_pair_enclosure_filled_total_df = pd.concat([switch_pair_enclosure_filled_total_df, switch_pair_enclosure_filled_current_df])
    # add switchName and switchWwn of the paired switches to the final result DataFrame
    switch_pair_df = dfop.dataframe_fillna(switch_pair_df, switch_pair_enclosure_filled_total_df, join_lst=enclosure_fabric_columns, filled_lst=enclosure_switch_columns)
        
    return switch_pair_df
    
def find_zero_device_connected_enclosure_sw_pair(switch_pair_df):
    """Function to add enclosure switch pair for switches with no connected devices"""
    
    if dfop.verify_columns_in_dataframe(switch_pair_df, columns=['switchName_pair_in_enclosure', 'switchWwn_pair_in_enclosure']):
    
        mask_zero_device_connected = switch_pair_df['Connected_device_number'] == 0
        mask_sw_pair_empty = switch_pair_df[['switchName_pair', 'switchWwn_pair']].isna().all(axis=1)
        mask_enclosure_sw_notna = switch_pair_df[['switchName_pair_in_enclosure', 'switchWwn_pair_in_enclosure']].notna().all(axis=1)
        
        # fill switchName, switchWwn and pairing type for switches with no device connected and which are in Blade or Synergy enclosures
        switch_pair_df.loc[mask_zero_device_connected & mask_sw_pair_empty & mask_enclosure_sw_notna, 'switchName_pair'] = switch_pair_df.loc[mask_zero_device_connected & mask_sw_pair_empty, 'switchName_pair_in_enclosure']
        switch_pair_df.loc[mask_zero_device_connected & mask_sw_pair_empty & mask_enclosure_sw_notna, 'switchWwn_pair'] = switch_pair_df.loc[mask_zero_device_connected & mask_sw_pair_empty, 'switchWwn_pair_in_enclosure']
        switch_pair_df.loc[mask_zero_device_connected & mask_sw_pair_empty & mask_enclosure_sw_notna, 'Switch_pairing_type'] = 'enclosure'

    return switch_pair_df



def find_zero_device_connected_switchname_match(switch_sr, switch_pair_df, sw_wwn_name_match_sr, min_sw_name_match_ratio):
    """Function to find highest match switchName for switches with no device connected"""
    
    print('-------')
    print(switch_sr['switchName'])
    
    # print(switch_sr['Connected_device_number'] != 0)
    
    sw_pairing_type = 'switch_name'
    
    if switch_sr['Connected_device_number'] != 0 or pd.notna(switch_sr['switchWwn_pair']):
        return pd.Series([switch_sr[column] for column in sw_pair_columns[3:7]])
    
    # list of fabric labels to verify (all fabric labels except fabric label of the switch being checked)
    verified_label_lst = [fabric_label for fabric_label in 
                          switch_pair_df['Fabric_label'].unique().tolist() 
                          if fabric_label != switch_sr['Fabric_label']]
    
    # lists with names and wwnns of the pair switches
    sw_pair_wwn_final_lst = []
    sw_pair_name_final_lst = []
    sw_pair_name_final_with_label_lst = []
    
    for verified_label in verified_label_lst:
        # pair switch candidates in verified fabric label with zero device connected with the same switchType and switchMode
        mask_zero_device_connected = switch_pair_df['Connected_device_number'] == 0
        mask_same_fabric_name = switch_pair_df['Fabric_name'] == switch_sr['Fabric_name']
        mask_verified_label = switch_pair_df['Fabric_label'] == verified_label
        mask_same_sw_type_mode = (switch_pair_df[['switchType', 'switchMode']] == switch_sr[['switchType', 'switchMode']]).all(axis=1)
        sw_candidates_name_lst = switch_pair_df.loc[mask_zero_device_connected & mask_same_fabric_name & mask_verified_label & mask_same_sw_type_mode, 'switchName'].tolist()
        sw_candidates_wwn_lst = switch_pair_df.loc[mask_zero_device_connected & mask_same_fabric_name & mask_verified_label & mask_same_sw_type_mode, 'switchWwn'].tolist()
        print(sw_candidates_name_lst)
        if sw_candidates_wwn_lst:
            # find switches with highest switchName match
            sw_pair_name_lst, sw_pair_wwn_lst = find_max_switchname_match(switch_sr['switchName'], sw_candidates_name_lst, sw_candidates_wwn_lst, sw_wwn_name_match_sr, min_sw_name_match_ratio)
            print(sw_pair_name_lst, sw_pair_wwn_lst)
            if sw_pair_wwn_lst:
                sw_pair_wwn_final_lst.extend(sw_pair_wwn_lst)
                sw_pair_name_final_lst.extend(sw_pair_name_lst)
                # if there are more then one fabric label to verify then add fabric label to paired switch name and wwn
                if len(verified_label_lst) > 1:
                    sw_pair_name_final_with_label_lst.append('(' + verified_label + ': ' + ', '.join(sw_pair_name_final_lst) + ')')

    sw_pair_summary_lst = [sw_pair_name_final_lst, sw_pair_wwn_final_lst, sw_pair_name_final_with_label_lst]
    sw_pair_summary_lst = [', '.join(lst) if lst else np.nan for lst in sw_pair_summary_lst ]

    if sw_pair_wwn_final_lst:
        return pd.Series([sw_pairing_type, *sw_pair_summary_lst])
    else:
        return pd.Series([sw_pairing_type, *(np.nan,)*3])