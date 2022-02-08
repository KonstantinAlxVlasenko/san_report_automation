from difflib import SequenceMatcher

import numpy as np
import pandas as pd


def find_zero_device_connected_switchname_match(switch_sr, switch_pair_df, sw_wwn_name_match_sr, sw_pair_columns, min_sw_name_match_ratio):
    """Function to find highest match switchName for switches with no device connected"""
    
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
        if sw_candidates_wwn_lst:
            # find switches with highest switchName match
            sw_pair_name_lst, sw_pair_wwn_lst = find_max_switchname_match(switch_sr['switchName'], sw_candidates_name_lst, sw_candidates_wwn_lst, sw_wwn_name_match_sr, min_sw_name_match_ratio)
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


def find_max_switchname_match(switch_name, sw_pair_name_lst, sw_pair_wwn_lst, sw_wwn_name_match_sr, min_sw_name_match_ratio):
    """Auxiliary function to find switches in the sw_pair_name_lst which names have highest match with switch_name"""
    
    # find highest name match ratio 
    name_match_ratio_lst = [round(SequenceMatcher(None, switch_name, sw_pair_name).ratio(), 2) for sw_pair_name in sw_pair_name_lst]
    max_name_match_ratio = max(name_match_ratio_lst)
    # hisghest name match ration should exceed min_sw_name_match_ratio
    if max_name_match_ratio >= min_sw_name_match_ratio:
        # find indexes with the hisghest name match ratio
        max_idx_lst = [i for i, name_match_ration in enumerate(name_match_ratio_lst) if name_match_ration == max_name_match_ratio]
        # choose switches with the highest name match ratio indexes
        sw_pair_wwn_lst = [sw_pair_wwn_lst[i] for i in max_idx_lst]
        sw_pair_name_lst = [sw_wwn_name_match_sr[wwn] for wwn in sw_pair_wwn_lst]
        return sw_pair_name_lst, sw_pair_wwn_lst
    else:
        return (None,)*2

