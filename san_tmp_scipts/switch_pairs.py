# -*- coding: utf-8 -*-
"""
Created on Thu Jan 20 17:50:54 2022

@author: vlasenko
"""

import os
import re
import sqlite3
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


db_path = r"C:\Users\vlasenko\OneDrive - Hewlett Packard Enterprise\Documents\01.CUSTOMERS\MTS\SAN Assessment\NOV2021\mts_spb\database_MTS_spb"
db_file = r"MTS_spb_analysis_database.db"

db_path = r"C:\Users\vlasenko\OneDrive - Hewlett Packard Enterprise\Documents\01.CUSTOMERS\Megafon\MegafonMSK\SAN Assessment\MAY21\database_MegafonMSK"
db_file = r"MegafonMSK_analysis_database.db"

data_names = ['switch_params_aggregated', 'portshow_aggregated']

switch_params_aggregated_df, portshow_aggregated_df = read_database(db_path, db_file, *data_names)

switch_params_aggregated_df.columns.tolist()





def find_switch_pairs(switch_params_aggregated_df, portshow_aggregated_df):
    
    min_device_number_match_ratio = 0.5
    min_sw_name_match_ratio = 0.8
    
    
    mask_valid_fabric = ~switch_params_aggregated_df[['Fabric_name', 'Fabric_label']].isin(['x', '-']).any(axis=1)

    switch_pair_df = switch_params_aggregated_df.loc[mask_valid_fabric, ['configname', 'Device_Location', 'Fabric_name', 'Fabric_label',  
                                                'chassis_name', 'chassis_wwn', 'switchName', 'switchWwn', 
                                                'switchType', 'ModelName', 'switchMode', 'LS_type_report']].copy()
    
    fabric_labels_lst = switch_pair_df['Fabric_label'].unique().tolist()
    
    
    switch_pair_df = find_enclosure_pair_switch(switch_pair_df)
    
   
    mask_devicetype_notna = portshow_aggregated_df['deviceType'].notna()
    mask_device_lib_srv_stor = ~portshow_aggregated_df['deviceType'].isin(['SWITCH', 'VC'])
    portshow_devices_df = portshow_aggregated_df.loc[mask_devicetype_notna & mask_device_lib_srv_stor]
    
    # sw_wwn_name_match_sr = switch_params_aggregated_df.drop_duplicates(subset=['switchWwn'])
    sw_wwn_name_match_sr = series_from_dataframe(switch_params_aggregated_df.drop_duplicates(subset=['switchWwn']), index_column='switchWwn', value_column='switchName')
    
    switch_pair_df[['Connected_device_number', 'Device_number_match', 'Device_match_ratio', 'Switch_pairing_type',
                    'switchName_pair', 'switchWwn_pair', 'switchName_pair_by_labels',
                    'switchNamer_pair_max_device_connected', 'switchWwn_pair_max_device_connected']] = switch_pair_df.apply(lambda series: find_switch_pair(series, sw_wwn_name_match_sr, portshow_devices_df, fabric_labels_lst, min_device_number_match_ratio, min_sw_name_match_ratio), axis=1)
    
    switch_pair_df = find_enclosure_pair_switch(switch_pair_df)
    switch_pair_df = find_zero_device_enclosure_sw_pair(switch_pair_df)

    # switch_pair_df[['switchName_pair', 'switchWwn_pair', 'switchName_pair_by_labels']] = switch_pair_df.apply(lambda series: find_zero_device_switchname_match(series, switch_pair_df.copy(), sw_wwn_name_match_sr), axis=1)    
    switch_pair_df[['Switch_pairing_type', 'switchName_pair', 'switchWwn_pair', 'switchName_pair_by_labels']] = switch_pair_df.apply(lambda series: find_zero_device_switchname_match(series, switch_pair_df.copy(), sw_wwn_name_match_sr, min_sw_name_match_ratio), axis=1)



    portshow_npiv_devices_df = find_npiv_connected_devices(switch_pair_df, portshow_aggregated_df)
    
    switch_pair_df[['Connected_device_number', 'Device_number_match', 'Device_match_ratio', 'Switch_pairing_type',
                    'switchName_pair', 'switchWwn_pair', 'switchName_pair_by_labels',
                    'switchNamer_pair_max_device_connected', 'switchWwn_pair_max_device_connected']] = switch_pair_df.apply(lambda series: find_switch_pair(series, sw_wwn_name_match_sr, portshow_npiv_devices_df, fabric_labels_lst, min_device_number_match_ratio, min_sw_name_match_ratio, npiv_only=True), axis=1)

    vc_cisco_pair_df, vc_cisco_wwn_name_match_sr = find_vc_cisco_switches(portshow_aggregated_df)
    
    portshow_vc_cisco_devices_df = find_npiv_connected_devices(vc_cisco_pair_df, portshow_aggregated_df, merge_column='NodeName')

    vc_cisco_pair_df[['Connected_device_number', 'Device_number_match', 'Device_match_ratio', 'Switch_pairing_type',
                    'switchName_pair', 'switchWwn_pair', 'switchName_pair_by_labels',
                    'switchNamer_pair_max_device_connected', 'switchWwn_pair_max_device_connected']] = vc_cisco_pair_df.apply(lambda series: find_switch_pair(series, vc_cisco_wwn_name_match_sr, portshow_vc_cisco_devices_df, fabric_labels_lst, min_device_number_match_ratio, min_sw_name_match_ratio, npiv_only=True), axis=1)

    switch_pair_cp_df = switch_pair_df.copy()
    
    



def find_vc_cisco_switches(portshow_aggregated_df):
    """Function to find VC, CISCO switches to verify pair device"""
    
    
    mask_npiv = portshow_aggregated_df['Connected_NPIV'] == 'yes'
    mask_vc_cisco = portshow_aggregated_df['deviceSubtype'].isin(['VC FC', 'VC FLEX', 'CISCO'])
    vc_cisco_columns = ['Fabric_name', 'Fabric_label', 'Device_Host_Name', 'NodeName', 'deviceType', 'deviceSubtype']
    vc_cisco_pair_df = portshow_aggregated_df.loc[mask_npiv & mask_vc_cisco, vc_cisco_columns].drop_duplicates().copy()
    vc_cisco_pair_df.rename(columns={'Device_Host_Name': 'switchName', 'NodeName': 'switchWwn'}, inplace=True)
    
    vc_cisco_pair_df['switchType'] = vc_cisco_pair_df['deviceSubtype']
    vc_cisco_pair_df['switchType'].replace(to_replace={'VC FC': 501, 'VC FLEX': 502, 'CISCO': 503, 'HUAWEI': 504}, inplace=True)
    
    vc_cisco_pair_df['switchMode'] = vc_cisco_pair_df['deviceSubtype']
    vc_cisco_pair_df['switchMode'].replace(to_replace={'VC FC': 'Access Gateway Mode', 'VC FLEX': 'Access Gateway Mode', 'CISCO': 'NPV', 'HUAWEI': 'NPV'}, inplace=True)
    
    vc_cisco_pair_df['configname'] = np.nan
    vc_cisco_pair_df['switchWwn_pair'] = np.nan
    
    vc_cisco_wwn_name_match_sr = series_from_dataframe(vc_cisco_pair_df.drop_duplicates(subset=['switchWwn']), index_column='switchWwn', value_column='switchName')
    
    return vc_cisco_pair_df, vc_cisco_wwn_name_match_sr
    



def find_npiv_connected_devices(switch_pair_df, portshow_aggregated_df, merge_column):
    """Function to find deviced connected behind NPIV switch ports"""
    
    portshow_cp_df = portshow_aggregated_df.copy()
    portshow_cp_df['Connected_Domain_Area_Id'] = portshow_cp_df['Connected_portId'].str.extract(r'^(.{4})(?:.{2})')
    
    
    switch_wwn_oui_df = switch_pair_df[['Fabric_name', 'Fabric_label', 'switchWwn']].copy()
    if merge_column == 'oui_board_sn':
        switch_wwn_oui_df[merge_column] = switch_pair_df['switchWwn'].str.extract(r'^[\da-f:]{6}(.+)')
    elif merge_column == 'NodeName':
        switch_wwn_oui_df[merge_column] = switch_pair_df['switchWwn']
    
    switch_wwn_oui_df['Connected_switchWwn'] = switch_wwn_oui_df['switchWwn']
    
    portshow_cp_df = dataframe_fillna(portshow_cp_df, switch_wwn_oui_df, join_lst=['Fabric_name', 'Fabric_label', merge_column], filled_lst=['Connected_switchWwn'])
    
    mask_connected_wwn_oui = portshow_cp_df[merge_column].isin(switch_wwn_oui_df[merge_column])
    mask_native = portshow_cp_df['switchMode'] == 'Native'
    portshow_sw_df = portshow_cp_df.loc[mask_native & mask_connected_wwn_oui].copy()

    
    portshow_cp_df = dataframe_fillna(portshow_cp_df, portshow_sw_df, join_lst=['Fabric_name', 'Fabric_label', 'Connected_Domain_Area_Id'], filled_lst=['Connected_NPIV', 'Connected_switchWwn'])
    
    mask_devicetype_notna = portshow_cp_df['deviceType'].notna()
    mask_device_lib_srv_stor = ~portshow_cp_df['deviceType'].isin(['SWITCH', 'VC'])
    mask_connected_switchwwn_notna = portshow_cp_df['Connected_switchWwn'].notna()
    
    device_columns = ['Fabric_name', 'Fabric_label', 'Connected_switchWwn', 'Device_Host_Name']
    portshow_npiv_devices_df = portshow_cp_df.loc[mask_devicetype_notna & mask_device_lib_srv_stor & mask_connected_switchwwn_notna, device_columns]

    portshow_npiv_devices_df.rename(columns={'Connected_switchWwn': 'switchWwn'}, inplace=True)
    portshow_npiv_devices_df = dataframe_fillna(portshow_npiv_devices_df, switch_pair_df, join_lst=['switchWwn'], filled_lst=['switchType', 'switchMode'])
    
    return portshow_npiv_devices_df





# def find_npiv_connected_devices(switch_pair_df, portshow_aggregated_df):
#     """Function to find deviced connected behind NPIV switch ports"""
    
#     portshow_cp_df = portshow_aggregated_df.copy()
#     portshow_cp_df['Connected_Domain_Area_Id'] = portshow_cp_df['Connected_portId'].str.extract(r'^(.{4})(?:.{2})')
    
    
#     switch_wwn_oui_df = switch_pair_df[['Fabric_name', 'Fabric_label', 'switchWwn']].copy()
#     switch_wwn_oui_df['oui_board_sn'] = switch_pair_df['switchWwn'].str.extract(r'r'^[\da-f:]{6}(.+)'')
    
#     switch_wwn_oui_df['Connected_switchWwn'] = switch_wwn_oui_df['switchWwn']
    
#     portshow_cp_df = dataframe_fillna(portshow_cp_df, switch_wwn_oui_df, join_lst=['Fabric_name', 'Fabric_label', 'oui_board_sn'], filled_lst=['Connected_switchWwn'])
    
#     mask_connected_wwn_oui = portshow_cp_df['oui_board_sn'].isin(switch_wwn_oui_df['oui_board_sn'])
#     mask_native = portshow_cp_df['switchMode'] == 'Native'
#     portshow_sw_df = portshow_cp_df.loc[mask_native & mask_connected_wwn_oui].copy()

    
#     portshow_cp_df = dataframe_fillna(portshow_cp_df, portshow_sw_df, join_lst=['Fabric_name', 'Fabric_label', 'Connected_Domain_Area_Id'], filled_lst=['Connected_NPIV', 'Connected_switchWwn'])
    
#     mask_devicetype_notna = portshow_cp_df['deviceType'].notna()
#     mask_device_lib_srv_stor = ~portshow_cp_df['deviceType'].isin(['SWITCH', 'VC'])
#     mask_connected_switchwwn_notna = portshow_cp_df['Connected_switchWwn'].notna()
    
#     device_columns = ['Fabric_name', 'Fabric_label', 'Connected_switchWwn', 'Device_Host_Name']
#     portshow_npiv_devices_df = portshow_cp_df.loc[mask_devicetype_notna & mask_device_lib_srv_stor & mask_connected_switchwwn_notna, device_columns]

#     portshow_npiv_devices_df.rename(columns={'Connected_switchWwn': 'switchWwn'}, inplace=True)
#     portshow_npiv_devices_df = dataframe_fillna(portshow_npiv_devices_df, switch_pair_df, join_lst=['switchWwn'], filled_lst=['switchType', 'switchMode'])
    
#     return portshow_npiv_devices_df




# number switch pairs
switch_pair_df = merge_columns(switch_pair_df, 'switch_pair_wwns', merge_columns=['switchWwn', 'switchWwn_pair'], drop_merge_columns=False)
sort_cell_values(switch_pair_df, 'switch_pair_wwns')
switch_pair_df['switch_pair_id'] = switch_pair_df.groupby(['switch_pair_wwns']).ngroup()



def sort_cell_values(df, *args, sep=', '):
    """Function to sort values in cells of columns (args)"""
    
    for column in args:
        mask_notna = df[column].notna()
        df[column] = df.loc[mask_notna, column].str.split(sep).apply(sorted).str.join(sep).str.strip(',')



def find_switch_pair(switch_sr, sw_wwn_name_match_sr, portshow_devices_df, fabric_labels_lst, min_device_number_match_ratio, min_sw_name_match_ratio, npiv_only=False):
    """Function to find switch pair for the switch_sr based on the same devices connected"""

    print('------------')
    print(switch_sr['switchName'])
    
    # device_number_match_ratio = 0.5
    
    if not npiv_only and pd.isna(switch_sr['configname']):
        return pd.Series([np.nan]*7)
    
    if npiv_only and pd.notna(switch_sr['switchWwn_pair']):
        return pd.Series([switch_sr['Connected_device_number'], switch_sr['Device_number_match'], switch_sr['Device_match_ratio'], switch_sr['Switch_pairing_type'],
                          switch_sr['switchName_pair'], switch_sr['switchWwn_pair'], switch_sr['switchName_pair_by_labels'], 
                          switch_sr['switchNamer_pair_max_device_connected'], switch_sr['switchWwn_pair_max_device_connected']])     
    
    
    # find devices connected to current switch
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
        return pd.Series([0, *match_statistics, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan])
    
    # lists with names and wwnns of the pair switches
    sw_pair_wwn_final_lst = []
    sw_pair_name_final_lst = []
    sw_pair_name_final_with_label_lst = []
    
    sw_pair_wwn_max_device_connected_lst = []
    sw_pair_name_max_device_connected_lst = []

    max_device_match_number_lst = []
    max_device_match_ratio_lst = []
    sw_pairing_type_lst = ['device_list'] if not npiv_only else ['npiv_device_list']   


    
    for verified_label in verified_label_lst:
        
        print(verified_label)
        
        # find candidate pair switches with the same switchType and switchMode within the same Fabric_name in verified Fabric_label
        mask_same_sw_type_mode = (portshow_devices_df[['switchType', 'switchMode']] == switch_sr[['switchType', 'switchMode']]).all(axis=1)
        mask_same_fabric_name = portshow_devices_df['Fabric_name'] == switch_sr['Fabric_name']
        mask_verified_label = portshow_devices_df['Fabric_label'] == verified_label
        portshow_sw_candidates_df = portshow_devices_df.loc[mask_same_sw_type_mode & mask_same_fabric_name & mask_verified_label]
        

        
        max_device_match_number, sw_pair_name_lst, sw_pair_wwn_lst = find_maximum_device_match_switch(sw_wwn_name_match_sr, sw_current_devices_sr, portshow_sw_candidates_df, min_device_number_match_ratio)
        max_device_match_number_lst.append(max_device_match_number)
        max_device_match_ratio_lst.append(round(max_device_match_number/connected_device_number, 2))
        if sw_pair_wwn_lst:
            
            print(sw_pair_wwn_lst)
            print('__________')
            sw_pair_wwn_max_device_connected_lst.extend(sw_pair_wwn_lst)
            sw_pair_name_max_device_connected_lst.extend(sw_pair_name_lst)
            
            # if sw_pair_wwn_lst contains more then one switch then choose one with the highest name match
            if len(sw_pair_wwn_lst) > 1:
                sw_pair_name_lst, sw_pair_wwn_lst = find_maximum_switchname_match(switch_sr['switchName'], sw_pair_name_lst, sw_pair_wwn_lst, min_sw_name_match_ratio)
                sw_pairing_type_lst.append('switch_name')
                
            sw_pair_wwn_final_lst.extend(sw_pair_wwn_lst)
            sw_pair_name_final_lst.extend(sw_pair_name_lst)
                
            # if there are more then one fabric label to verify then add fabric label to paired switch name and wwn
            if len(verified_label_lst) > 1:
                sw_pair_name_final_with_label_lst.append('(' + verified_label + ': ' + ', '.join(sw_pair_name_lst) + ')')

    if sw_pair_wwn_final_lst:

        if len(max_device_match_number_lst) == 1:
            match_statistics = [connected_device_number, max_device_match_number_lst[0], max_device_match_ratio_lst[0]]
        else:
            match_statistics = [connected_device_number, ', '.join(map(str, max_device_match_number_lst)), ', '.join(map(str, max_device_match_ratio_lst))]
            
        sw_pair_summary_lst = [sw_pairing_type_lst, 
                               sw_pair_name_final_lst, sw_pair_wwn_final_lst, 
                               sw_pair_name_final_with_label_lst, sw_pair_name_max_device_connected_lst, sw_pair_wwn_max_device_connected_lst]
        sw_pair_summary_lst = [', '.join(lst) if lst else np.nan for lst in sw_pair_summary_lst ]
        return pd.Series([*match_statistics, *sw_pair_summary_lst])



def find_zero_device_switchname_match(switch_sr, switch_pair_df, sw_wwn_name_match_sr, min_sw_name_match_ratio):
    
    print('-------')
    print(switch_sr['switchName'])
    
    # print(switch_sr['Connected_device_number'] != 0)
    
    sw_pairing_type = 'switch_name'
    
    if switch_sr['Connected_device_number'] != 0 or pd.notna(switch_sr['switchWwn_pair']):
        return pd.Series([switch_sr['Switch_pairing_type'], switch_sr['switchName_pair'], switch_sr['switchWwn_pair'], switch_sr['switchName_pair_by_labels']])
    
    # list of fabric labels to verify (all fabric labels except fabric label of the switch being checked)
    verified_label_lst = [fabric_label for fabric_label in 
                          switch_pair_df['Fabric_label'].unique().tolist() 
                          if fabric_label != switch_sr['Fabric_label']]
    
    # mask_zero_device_connected = switch_pair_df['Connected_device_number'] == 0
    # switch_zero_device_cconnected_df = switch_pair_df.loc[mask_zero_device_connected].copy()
    
    # lists with names and wwnns of the pair switches
    sw_pair_wwn_final_lst = []
    sw_pair_name_final_lst = []
    sw_pair_name_final_with_label_lst = []
    
    for verified_label in verified_label_lst:
    
        mask_zero_device_connected = switch_pair_df['Connected_device_number'] == 0
        mask_same_fabric_name = switch_pair_df['Fabric_name'] == switch_sr['Fabric_name']
        mask_verified_label = switch_pair_df['Fabric_label'] == verified_label
        sw_candidates_name_lst = switch_pair_df.loc[mask_zero_device_connected & mask_same_fabric_name & mask_verified_label, 'switchName'].tolist()
        sw_candidates_wwn_lst = switch_pair_df.loc[mask_zero_device_connected & mask_same_fabric_name & mask_verified_label, 'switchWwn'].tolist()
        print(sw_candidates_name_lst)
        if sw_candidates_wwn_lst:
            sw_pair_name_lst, sw_pair_wwn_lst = find_maximum_switchname_match(switch_sr['switchName'], sw_candidates_name_lst, sw_candidates_wwn_lst, min_sw_name_match_ratio)
            print(sw_pair_name_lst, sw_pair_wwn_lst)
            if sw_pair_wwn_lst:
                sw_pair_wwn_final_lst.extend(sw_pair_wwn_lst)
                sw_pair_name_final_lst.extend(sw_pair_name_lst)
                
                # if there are more then one fabric label to verify then add fabric label to paired switch name and wwn
                if len(verified_label_lst) > 1:
                    sw_pair_name_final_with_label_lst.append('(' + verified_label + ': ' + ', '.join(sw_pair_name_final_lst) + ')')

    sw_pair_summary_lst = [ 
                           sw_pair_name_final_lst, sw_pair_wwn_final_lst, 
                           sw_pair_name_final_with_label_lst]
    sw_pair_summary_lst = [', '.join(lst) if lst else np.nan for lst in sw_pair_summary_lst ]

    if sw_pair_wwn_final_lst:

        return pd.Series([sw_pairing_type, *sw_pair_summary_lst])
    else:
        return pd.Series([sw_pairing_type, *(np.nan,)*3])
    

def find_maximum_switchname_match(switch_name, sw_pair_name_lst, sw_pair_wwn_lst, min_sw_name_match_ratio):
    """Auxiliary function to find switches which name have maximum match with switch_name"""
    
    name_match_ratio_lst = [round(SequenceMatcher(None, switch_name, sw_pair_name).ratio(), 2) for sw_pair_name in sw_pair_name_lst]
    max_name_match_ratio = max(name_match_ratio_lst)
    print(max_name_match_ratio)
    if max_name_match_ratio >= min_sw_name_match_ratio:
        max_idx_lst = [i for i, name_match_ration in enumerate(name_match_ratio_lst) if name_match_ration == max_name_match_ratio]
        sw_pair_wwn_lst = [sw_pair_wwn_lst[i] for i in max_idx_lst]
        sw_pair_name_lst = [sw_wwn_name_match_sr[wwn] for wwn in sw_pair_wwn_lst]
        print(sw_pair_name_lst, sw_pair_wwn_lst)
        return sw_pair_name_lst, sw_pair_wwn_lst
    else:
        return (None,)*2



def find_maximum_device_match_switch(sw_wwn_name_match_sr, sw_current_devices_sr, portshow_sw_candidates_df, device_number_match_ratio):
    """Auxiliary function to find switches which have maximum connected device match with devices in sw_current_devices_sr"""
    
    # list with wwns of the candidate swithes to be pair with the current switch
    sw_candidates_wwn_lst = portshow_sw_candidates_df['switchWwn'].unique().tolist()
    
    if not sw_candidates_wwn_lst:
        return (np.nan,)*3
    
    # list with the number of device matches of each candidate switch with the current switch
    # how many devices from current switch connected to switch being verified
    device_match_number_lst = []
    
    for sw_candidate_wwn in sw_candidates_wwn_lst:
        mask_sw_candidate_wwn = portshow_sw_candidates_df['switchWwn'] == sw_candidate_wwn
        # find devices connected to the candidate switch
        sw_candidate_devices_sr = portshow_sw_candidates_df.loc[mask_sw_candidate_wwn, 'Device_Host_Name']
        # count device match number
        device_match_number_lst.append(sw_current_devices_sr.isin(sw_candidate_devices_sr).sum())
        
        # print(switch_params_aggregated_df.loc[switch_params_aggregated_df['switchWwn'] == sw_candidate_wwn, 'switchName'].values[0], sw_current_devices_sr.isin(sw_candidate_devices_sr).sum())
    
    # if there is switch with at least 80 percentage of device match
    max_device_match_number = max(device_match_number_lst)
    if max_device_match_number/sw_current_devices_sr.count() > device_number_match_ratio:
        
        # find switch wwns with maximum device match number
        sw_pair_wwn_lst = [sw_candidates_wwn_lst[i] for i in range(len(sw_candidates_wwn_lst)) if device_match_number_lst[i] == max_device_match_number]
        # find switch names with maximum device match number
        sw_pair_name_lst = [sw_wwn_name_match_sr[sw_wwn] for sw_wwn in sw_pair_wwn_lst]
            
        return max_device_match_number, sw_pair_name_lst, sw_pair_wwn_lst
    else:
        return max_device_match_number, np,nan, np.nan



def verify_value_occurence_in_series(value, series):
    series_values_occurence = series.value_counts()
    if value in series_values_occurence:
        return series_values_occurence[value]
    
switch_pair_df['switchWwn_occurence_in_switchWwn_pair'] = switch_pair_df['switchWwn'].apply(lambda value: verify_value_occurence_in_series(value, switch_pair_df['switchWwn_pair']))
switch_pair_df['switchWwn_pair_occurence_in_switchWwn'] = switch_pair_df['switchWwn_pair'].apply(lambda value: verify_value_occurence_in_series(value, switch_pair_df['switchWwn']))
switch_pair_df['switchWwn_pair_duplication'] = switch_pair_df.groupby(['switchWwn_pair'])['switchWwn'].transform('count')





def find_enclosure_pair_switch(switch_pair_df):
    """Function to find pair switches in single enclosure"""
    
    enclosure_fabric_columns = ['Fabric_name', 'Fabric_label', 'Enclosure']
    enclosure_switch_columns = ['switchName_pair_in_enclosure', 'switchWwn_pair_in_enclosure']
    
    if switch_pair_df['Device_Location'].isna().all():
        return switch_pair_df
    
    switch_pair_df['Enclosure'] = switch_pair_df['Device_Location'].str.extract(r'^Enclosure (.+) bay')
    fabric_labels_lst = switch_pair_df['Fabric_label'].unique().tolist()
    switch_pair_enclosure_filled_total_df = pd.DataFrame()
    
    for fabric_label in fabric_labels_lst:
        mask_fabric_label = switch_pair_df['Fabric_label'] == fabric_label
        switch_pair_location_df = switch_pair_df.loc[~mask_fabric_label].copy()
        switch_pair_location_name_df = switch_pair_location_df.groupby(by=['Fabric_name', 'Enclosure'])['switchName'].agg(lambda x: ', '.join(x)).to_frame()
        switch_pair_location_wwn_df = switch_pair_location_df.groupby(by=['Fabric_name', 'Enclosure'])['switchWwn'].agg(lambda x: ', '.join(x)).to_frame()
        switch_pair_location_df = switch_pair_location_name_df.merge(switch_pair_location_wwn_df, how='left', left_index=True, right_index=True)
        switch_pair_location_df.rename(columns={'switchName': 'switchName_pair_in_enclosure', 'switchWwn': 'switchWwn_pair_in_enclosure'}, inplace=True)
        switch_pair_location_df.reset_index(inplace=True)
        switch_pair_location_df['Fabric_label'] = fabric_label
        switch_pair_enclosure_filled_current_df = switch_pair_df.loc[mask_fabric_label]
        switch_pair_enclosure_filled_current_df = dataframe_fillna(switch_pair_enclosure_filled_current_df, switch_pair_location_df, join_lst=enclosure_fabric_columns, filled_lst=enclosure_switch_columns)
        if switch_pair_enclosure_filled_total_df.empty:
            switch_pair_enclosure_filled_total_df = switch_pair_enclosure_filled_current_df.copy()
        else:
            switch_pair_enclosure_filled_total_df = pd.concat([switch_pair_enclosure_filled_total_df, switch_pair_enclosure_filled_current_df])
        
    switch_pair_df = dataframe_fillna(switch_pair_df, switch_pair_enclosure_filled_total_df, join_lst=enclosure_fabric_columns, filled_lst=enclosure_switch_columns)
        
    return switch_pair_df
    
def find_zero_device_enclosure_sw_pair(switch_pair_df):
    """Function to add enclosure switch pair for switches with no connected devices"""
    
    if verify_columns_in_dataframe(switch_pair_df, columns=['switchName_pair_in_enclosure', 'switchWwn_pair_in_enclosure']):
    
        mask_zero_device_connected = switch_pair_df['Connected_device_number'] == 0
        mask_sw_pair_empty = switch_pair_df[['switchName_pair', 'switchWwn_pair']].isna().all(axis=1)
        mask_enclosure_sw_notna = switch_pair_df[['switchName_pair_in_enclosure', 'switchWwn_pair_in_enclosure']].notna().all(axis=1)
        
    
        switch_pair_df.loc[mask_zero_device_connected & mask_sw_pair_empty & mask_enclosure_sw_notna, 'switchName_pair'] = switch_pair_df.loc[mask_zero_device_connected & mask_sw_pair_empty, 'switchName_pair_in_enclosure']
        switch_pair_df.loc[mask_zero_device_connected & mask_sw_pair_empty & mask_enclosure_sw_notna, 'switchWwn_pair'] = switch_pair_df.loc[mask_zero_device_connected & mask_sw_pair_empty, 'switchWwn_pair_in_enclosure']
        switch_pair_df.loc[mask_zero_device_connected & mask_sw_pair_empty & mask_enclosure_sw_notna, 'Switch_pairing_type'] = 'enclosure'

    return switch_pair_df

def merge_columns(df, summary_column: str, merge_columns: list, sep=', ', drop_merge_columns=True):
    """Function to concatenate values in several columns (merge_columns) into summary_column 
    with separator. If drop flag is True all merged columns except summary column are dropped"""
    
    merge_columns = [column for column in merge_columns if column in df.columns]
    if not merge_columns:
        return df
    df[summary_column] = df[merge_columns].stack().groupby(level=0).agg(sep.join)
    # drop merge_columns
    if drop_merge_columns:
        drop_columns = [column for column in merge_columns if column != summary_column]
        df.drop(columns=drop_columns, inplace=True)
    return df


def read_database(db_path, db_file, *args):
    """Function to read data from SQL.
    Args are comma separated DataFrames names.
    Returns list of loaded DataFrames or None if no data found.
    """

    db_filepath = os.path.join(db_path, db_file)

    # list to store loaded data
    data_imported = []
    conn = sqlite3.connect(db_filepath)

    for data_name in args:


        info = f'Reading {data_name} from database................'
        print(info, end="")
        data_name_in_db = conn.execute(
            f"""SELECT name FROM sqlite_master WHERE type='table' 
            AND name='{data_name}'; """).fetchall()
        if data_name_in_db:
            df = pd.read_sql(f"select * from {data_name}", con=conn)
            substitute_names(df)
            # revert single column DataFrame to Series
            if 'index' in df.columns:
                df.set_index('index', inplace=True)
                df = df.squeeze()
            data_imported.append(df)
            print('ok')
        else:
            data_imported.append(None)
            print('no data')
    conn.close()
    return data_imported if len(data_imported)>1 else data_imported[0]


def substitute_names(df):
    """Function to avoid column names duplication in sql.
    Capital and lower case letters don't differ iin sql.
    Function adds tag to duplicated column name when writes to the database
    and removes tag when read from the database."""

    masking_tag = '_sql'
    duplicated_names = ['SwitchName', 'Fabric_Name', 'SwitchMode', 'Memory_Usage', 'Flash_Usage', 'Speed']
    replace_dct = {orig_name + masking_tag: orig_name for orig_name in duplicated_names}
    df.rename(columns=replace_dct, inplace=True)
    
    
def series_from_dataframe(df, index_column: str, value_column: str=None):
    """"Function to convert DataFrame to Series"""

    if len(df.columns) > 2:
        df = df[[index_column, value_column]].copy()
    else:
        df = df.copy()
    df.set_index(index_column, inplace=True)
    sr = df.squeeze()
    sr.name = value_column
    return  sr



def dataframe_fillna(left_df, right_df, join_lst, filled_lst, remove_duplicates=True, drop_na=True):
    """
    Function to fill null values with values from another DataFrame with the same column names.
    Function accepts left Dataframe with null values, right DataFrame with filled values,
    list of columns join_lst used to join left and right DataFrames on,
    list of columns filled_lst where null values need to be filled. join_lst
    columns need to be present in left and right DataFrames. filled_lst must be present in right_df.
    If some columns from filled_lst missing in left_df it is added and the filled with values from right_df.
    If drop duplicate values in join columns of right DataFrame is not required pass remove_duplicates as False.
    If drop nan values in join columns in right DataFrame is not required pass drop_na as False.
    Function returns left DataFrame with filled null values in filled_lst columns 
    """

    # add missing columns to left_df from filled_lst if required
    left_df_columns_lst = left_df.columns.to_list()
    add_columns_lst = [column for column in filled_lst if column not in left_df_columns_lst]
    if add_columns_lst:
        left_df = left_df.reindex(columns = [*left_df_columns_lst, *add_columns_lst])

    # cut off unnecessary columns from right DataFrame
    right_join_df = right_df.loc[:, join_lst + filled_lst].copy()
    # drop rows with null values in columns to join on
    if drop_na:
        right_join_df.dropna(subset = join_lst, inplace = True)
    # if required (deafult) drop duplicates values from join columns 
    # to avoid rows duplication in left DataDrame
    if remove_duplicates:
        right_join_df.drop_duplicates(subset = join_lst, inplace = True)
    # rename columns with filled values for right DataFrame
    filled_join_lst = [name+'_join' for name in filled_lst]
    right_join_df.rename(columns = dict(zip(filled_lst, filled_join_lst)), inplace = True)
    # left join left and right DataFrames on join_lst columns
    left_df = left_df.merge(right_join_df, how = 'left', on = join_lst)
    # for each columns pair (w/o (null values) and w _join prefix (filled values)
    for filled_name, filled_join_name in zip(filled_lst, filled_join_lst):
        # copy values from right DataFrame column to left DataFrame if left value ios null 
        left_df[filled_name].fillna(left_df[filled_join_name], inplace = True)
        # drop column with _join prefix
        left_df.drop(columns = [filled_join_name], inplace = True)
    return left_df

def verify_columns_in_dataframe(df, columns):
    """Function to verify if columns are in DataFrame"""

    if not isinstance(columns, list):
        columns = [columns]
    return set(columns).issubset(df.columns)