import pandas as pd
import numpy as np
import utilities.dataframe_operations as dfop
from .switchname_approach import find_max_switchname_match


def find_nonzero_device_connected_switch_pair(switch_sr, sw_wwn_name_match_sr, portshow_devices_df, fabric_labels_lst, sw_pair_columns,
                                              min_device_number_match_ratio, min_sw_name_match_ratio, npiv_only=False):
    """Function to find pair switch for the switch_sr. Candidates switches have to be same switchType and switchMode.
    Then candidate switches are checked for connected devices. 
    Switch with the largest number of matched devices and exceeded min_device_number_match_ratio considered to be pair switch.
    If more then one switch in the fabric correspond to that criteria then name match performed. 
    Switch with the largest name match considered to be pair. 
    If still more then one switch correspond to name match criteria then all switches considered to be pair 
    and manual pair switch assigment should be performed later"""

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
        
        # print('\n')
        # print(switch_sr['switchName'])
        # print(sw_pair_wwn_max_device_connected_lst)
        # print(sw_pair_wwn_lst)
        # print(bool(sw_pair_wwn_lst))
        # print(max_device_match_number, max_device_match_number_ratio, sw_pair_name_lst, sw_pair_wwn_lst)

        if sw_pair_wwn_lst:
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


def find_max_device_match_switch(sw_wwn_name_match_sr, sw_current_devices_sr, portshow_sw_candidates_df, min_device_number_match_ratio):
    """Auxiliary function to find switches which have maximum connected device match with the switch 
    for which pair switch is being checked for(in sw_current_devices_sr)"""
    
    # list with wwns of the candidate switches to be pair with the current switch
    sw_candidates_wwn_lst = portshow_sw_candidates_df['switchWwn'].unique().tolist()
    
    if not sw_candidates_wwn_lst:
        return [None]*3
    
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
        return max_device_match_number, max_device_match_number_ratio, None, None







