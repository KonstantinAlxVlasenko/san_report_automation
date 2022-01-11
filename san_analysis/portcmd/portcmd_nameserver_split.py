"""Module to split PortSymb and NodeSymb columns, extract device and host information"""


import re

import pandas as pd


def nsshow_symb_split(nsshow_join_df, pattern_dct):
    """
    Function to split PortSymb and NodeSymb columns of local NameServer to group of device and HBA columns.
    Returns DataFrame with splitted columns and DataFrame with rows that function was not able to split up. 
    """

    # columns list to which PortSymb and NodeSymb information splitted up to
    nsshow_symb_columns = [
        'portSymbUsed', 'portSymbPattern', 'nodeSymbUsed', 'nodeSymbPattern', 'Device_Manufacturer', 'Device_Model', 
        'Device_SN', 'Device_Name', 'Device_Name_reserved', 'Device_Port', 'Device_Fw', 'Device_Location', 'IP_Address', 
        'HBA_Manufacturer', 'HBA_Model', 'HBA_Description', 'Host_Name', 'Host_OS', 'HBA_Firmware', 'HBA_Driver'
        ]

    # add  and HBA information empty columns to NameServer DataFrame
    nsshow_join_df = nsshow_join_df.reindex(columns=[*nsshow_join_df.columns.tolist(), *nsshow_symb_columns])
    # split up PortSymb and NodeSymb columns
    nsshow_join_df[nsshow_symb_columns] = nsshow_join_df.apply(lambda series: _symb_split(series, pattern_dct, nsshow_symb_columns), axis = 1)
    
    # show unsplit PortSymb and NodeSymb
    # mask shows rows where neither PortSymb nor NodeSymb was split up
    mask1 = nsshow_join_df[['portSymbUsed', 'nodeSymbUsed']].isna().all(axis=1)
    # mask with shows rows containing PortSymb or NodeSymb values
    mask2 = nsshow_join_df[['PortSymb',	'NodeSymb']].notna().any(axis=1)
    nsshow_unsplit_df = nsshow_join_df.loc[mask1&mask2]

    return nsshow_join_df, nsshow_unsplit_df


def _symb_split(series, pattern_dct, nsshow_symb_columns):
    """Function to extract  and HBA information from PortSymb, NodeSymb columns of the NameServer DataFrame"""

    # regular expression patterns TO_REMOVE
    # comp_keys, match_keys, comp_dct = re_pattern_lst
    
    port_symb = series['PortSymb']
    node_symb = series['NodeSymb']

    match_port_dct = dict()
    match_node_dct = dict()
    # create dictionary with PortSymb cell match results
    if not pd.isnull(port_symb): 
        # match_port_dct = {match_key: comp_dct[comp_key].match(port_symb) for comp_key, match_key in zip(comp_keys, match_keys)}
        match_port_dct = {pattern_name: pattern_dct[pattern_name].match(port_symb) for pattern_name in pattern_dct.keys()}
    # create dictionary with NodeSymb cell match results
    if not pd.isnull(node_symb):
        # match_node_dct = {match_key: comp_dct[comp_key].match(node_symb) for comp_key, match_key in zip(comp_keys, match_keys)}
        match_node_dct = {pattern_name: pattern_dct[pattern_name].match(node_symb) for pattern_name in pattern_dct.keys()}

    # 3par_node_match node_symb
    if not pd.isnull(node_symb) and match_node_dct['3par_node']:
        match = match_node_dct['3par_node']
        series['Device_Manufacturer'] = match.group(3)
        series['Device_Model'] = match.group(2)
        series['Device_SN'] = match.group(4)
        series['Device_Name_reserved'] = match.group(1)
        series['Device_Fw'] = match.group(5)
        series['nodeSymbUsed'] = 'yes'
        series['nodeSymbPattern'] = 9
        # 3par_port_match port_symb
        if not pd.isnull(port_symb) and match_port_dct['3par_port']:
            match = match_port_dct['3par_port']
            series['Device_Port'] = match.group(1)
            series['HBA_Model'] = match.group(2)
            series['portSymbUsed'] = 'yes'
            series['portSymbPattern'] = 10
    # netapp_node_match node_symb
    elif not pd.isnull(node_symb) and match_node_dct['netapp_node']:
        match = match_node_dct['netapp_node']
        series['Device_Manufacturer'] = match.group(2)
        series['Device_Model'] = match.group(1)
        series['Device_Name'] = match.group(3)
        series['nodeSymbUsed'] = 'yes'
        series['nodeSymbPattern'] = 21
        # netapp_port_match port_symb
        if not pd.isnull(port_symb) and match_port_dct['netapp_port']:
            match = match_port_dct['netapp_port']
            series['Device_Port'] = match.group(1)
            series['portSymbUsed'] = 'yes'
            series['portSymbPattern'] = 26
    # qlogic_match node_symb
    elif not pd.isnull(node_symb) and match_node_dct['qlogic']:
        match = match_node_dct['qlogic']
        series['HBA_Model'] = match.group(1)
        series['HBA_Firmware'] = match.group(2)
        series['HBA_Driver'] = match.group(3)
        series['nodeSymbUsed'] = 'yes'
        series['nodeSymbPattern'] = 2
        # xp_msa_match port_symb
        if not pd.isnull(port_symb) and match_port_dct['xp_msa']:
            match = match_port_dct['xp_msa']
            series['Device_Manufacturer'] = match.group(2)
            series['Device_Model'] = match.group(1)
            series['Device_Fw'] = match.group(3)
            series['portSymbUsed'] = 'yes'
            series['portSymbPattern'] = 14
        # qlogic_emulex_port_match port symb
        elif not pd.isnull(port_symb) and match_port_dct['qlogic_emulex_port']:
            match = match_port_dct['qlogic_emulex_port']
            series['HBA_Manufacturer'] = match.group(1)
            series['portSymbUsed'] = 'yes'
            series['portSymbPattern'] = 3
        # infinibox_match port_symb
        elif not pd.isnull(port_symb) and match_port_dct['infinibox']:
            match = match_port_dct['infinibox']
            series['Device_Manufacturer'] = match.group(2)
            series['Device_Model'] = match.group(1)
            series['portSymbUsed'] = 'yes'
            series['portSymbPattern'] = 15
    # emulex_match node_symb
    elif not pd.isnull(node_symb) and match_node_dct['emulex']:
        match = match_node_dct['emulex']
        series['HBA_Manufacturer'] = match.group(1)
        series['HBA_Model'] = match.group(2)
        series['HBA_Firmware'] = match.group(3)
        if match.group(4):
            series['HBA_Driver'] = match.group(4).rstrip('.')
        if match.group(5) and not re.search(r'localhost|none', match.group(5)):
            series['Host_Name'] = match.group(5).rstrip('.')
        if match.group(6):
            series['Host_OS'] = match.group(6).rstrip('.')
        series['nodeSymbUsed'] = 'yes'
        series['nodeSymbPattern'] = 4
    # qlogic_fcoe_match node_symb
    elif not pd.isnull(node_symb) and match_node_dct['qlogic_fcoe']:
        match = match_node_dct['qlogic_fcoe']
        series['HBA_Manufacturer'] = match.group(1)
        series['HBA_Model'] = match.group(2)
        series['HBA_Driver'] = match.group(3)
        series['HBA_Firmware'] = match.group(4)
        series['Host_Name'] = match.group(5).rstrip('.')
        series['nodeSymbUsed'] = 'yes'
        series['nodeSymbPattern'] = 20
    # ag_switch_match node_symb
    elif not pd.isnull(node_symb) and match_node_dct['ag_switch']:
        match = match_node_dct['ag_switch']
        series['Device_Name'] = match.group(1)
        series['IP_Address'] = match.group(2)
        series['Device_Fw'] = match.group(3)
        series['nodeSymbUsed'] = 'yes'
        series['nodeSymbPattern'] = 25
    # hpux_match node_symb
    elif not pd.isnull(node_symb) and match_node_dct['hpux']:
        match = match_node_dct['hpux']
        if match.group(1) and not re.search(r'localhost|none', match.group(1)):
            series['Host_Name'] = match.group(1)
        series['Host_OS'] = match.group(2)
        series['nodeSymbUsed'] = 'yes'
        series['nodeSymbPattern'] = 6
    # ultrium_match node_symb
    elif not pd.isnull(node_symb) and match_node_dct['ultrium']:
        match = match_node_dct['ultrium']
        series['Device_Manufacturer'] = match.group(2)
        series['Device_Model'] = match.group(1)
        series['Device_SN'] = match.group(4)
        series['Device_Fw'] = match.group(3)
        series['nodeSymbUsed'] = 'yes'
        series['nodeSymbPattern'] = 11
    # emc_vplex_match node_symb
    elif not pd.isnull(node_symb) and match_node_dct['emc_vplex']:
        match = match_node_dct['emc_vplex']
        series['Device_Manufacturer'] = match.group(1)
        series['Device_Model'] = match.group(1) + " " + match.group(2)
        series['Device_SN'] = match.group(3)
        series['Device_Name'] = match.group(1) + " " + match.group(2) +  " " +  match.group(3)
        series['nodeSymbUsed'] = 'yes'
        series['nodeSymbPattern'] = 16
        # emc_vplex_match port_symb
        if not pd.isnull(port_symb) and match_port_dct['emc_vplex']:
            match = match_port_dct['emc_vplex']
            series['Device_Port'] = match.group(4)
            series['portSymbUsed'] = 'yes'
            series['portSymbPattern'] = 16
    # huawei_manufacturer node_symb
    elif not pd.isnull(node_symb) and match_node_dct['huawei_manufacturer']:
        match = match_node_dct['huawei_manufacturer']
        series['Device_Manufacturer'] = match.group(1)
        series['nodeSymbUsed'] = 'yes'
        series['nodeSymbPattern'] = 32
    # skip_symb node_symb
    elif not pd.isnull(node_symb) and match_node_dct['skip_symb']:
        match = match_node_dct['skip_symb']
        series['nodeSymbUsed'] = 'yes'
        series['nodeSymbPattern'] = 35
    # qlogic_brocade_match port_symb
    elif not pd.isnull(port_symb) and match_port_dct['qlogic_brocade']:
        match = match_port_dct['qlogic_brocade']
        series['HBA_Manufacturer'] = match.group(2)
        series['HBA_Model'] = match.group(1)
        series['HBA_Driver'] = match.group(3)
        series['Host_Name'] = match.group(4)
        series['Host_OS'] = match.group(5)
        series['portSymbUsed'] = 'yes'
        series['portSymbPattern'] = 27
    # storeonce_port_match port_symb
    elif not pd.isnull(port_symb) and match_port_dct['storeonce_port']:
        match = match_port_dct['storeonce_port']
        series['Device_Manufacturer'] = match.group(2)
        series['Device_Model'] = match.group(1)
        series['Device_SN'] = match.group(3)
        if match.group(3):
            series['Device_Name'] = match.group(1) + " " + match.group(3)
        series['Device_Port'] = match.group(4)
        series['portSymbUsed'] = 'yes'
        series['portSymbPattern'] = 8
    # storeonce_node_match node_symb
    elif not pd.isnull(node_symb) and match_node_dct['storeonce_node']:
        match = match_node_dct['storeonce_node']
        series['Device_Manufacturer'] = match.group(2)
        series['Device_Model'] = match.group(1)
        series['Device_SN'] = match.group(3)
        if match.group(3):
            series['Device_Name'] = match.group(1) + " " + match.group(3)
        series['nodeSymbUsed'] = 'yes'
        series['nodeSymbPattern'] = 7
    # data_domain_match node_symb
    elif not pd.isnull(node_symb) and match_node_dct['data_domain']:
        match = match_node_dct['data_domain']
        series['Device_Model'] = match.group(1)
        series['nodeSymbUsed'] = 'yes'
        series['nodeSymbPattern'] = 23
    # xp_msa_match port_symb
    elif not pd.isnull(port_symb) and match_port_dct['xp_msa']:
        match = match_port_dct['xp_msa']
        series['Device_Manufacturer'] = match.group(2)
        series['Device_Model'] = match.group(1)
        series['Device_Fw'] = match.group(3)
        series['portSymbUsed'] = 'yes'
        series['portSymbPattern'] = 14
    # eva_match port_symb
    elif not pd.isnull(port_symb) and match_port_dct['eva']:
        match = match_port_dct['eva']
        series['Device_Model'] = match.group(1)
        series['Device_Name'] = match.group(2)
        series['portSymbUsed'] = 'yes'
        series['portSymbPattern'] = 13
    # library_match port_symb
    elif not pd.isnull(port_symb) and match_port_dct['library']:
        match = match_port_dct['library']
        series['Device_Model'] = match.group(1)
        # series['Device_Name'] = match.group(1)
        series['Device_SN'] = match.group(2)
        series['Device_Port'] = match.group(3)
        series['portSymbUsed'] = 'yes'
        series['portSymbPattern'] = 12
    # qlogic_cna_match port_symb duplicate with cna_adapter_match node_symb 19
    elif not pd.isnull(port_symb) and match_port_dct['qlogic_cna']:
        match = match_port_dct['qlogic_cna']
        series['HBA_Model'] = match.group(1)
        series['HBA_Driver'] = match.group(2)
        series['Device_Port'] = match.group(3)
        series['portSymbUsed'] = 'yes'
        series['portSymbPattern'] = 5
    # clariion_match port_symb
    elif not pd.isnull(port_symb) and match_port_dct['clarion']:
        match = match_port_dct['clarion']
        series['Device_Manufacturer'] = 'EMC'
        series['Device_Model'] = 'EMC ' + match.group(1)
        series['Device_Port'] = match.group(2)
        series['portSymbUsed'] = 'yes'
        series['portSymbPattern'] = 17
    # ibm_flash_match port_symb
    elif not pd.isnull(port_symb) and match_port_dct['ibm_flash']:
        match = match_port_dct['ibm_flash']
        series['Device_Manufacturer'] = match.group(2)
        series['Device_Model'] = match.group(1)
        series['Device_SN'] = match.group(3)
        series['portSymbUsed'] = 'yes'
        series['portSymbPattern'] = 22
    # qlogic_emulex_port_match port_symb when node_symb is empty
    elif not pd.isnull(port_symb) and match_port_dct['qlogic_emulex_port']:
        match = match_port_dct['qlogic_emulex_port']
        series['HBA_Manufacturer'] = match.group(1)
        series['portSymbUsed'] = 'yes'
        series['portSymbPattern'] = 3
    # dell_storage_match port_symb
    elif not pd.isnull(port_symb) and match_port_dct['dell_storage']:
        match = match_port_dct['dell_storage']
        series['Device_Model'] = 'Compellent ' + match.group(2)
        series['Device_Name'] = match.group(3)
        series['Device_Port'] = match.group(1)
        series['portSymbUsed'] = 'yes'
        series['portSymbPattern'] = 28
    # symmetrix_storage_match port_symb
    elif not pd.isnull(port_symb) and match_port_dct['symmetrix_storage']:
        match = match_port_dct['symmetrix_storage']
        series['Device_Model'] = match.group(1)
        # series['Device_Name'] = match.group(1)
        series['Device_Port'] = match.group(2)
        series['portSymbUsed'] = 'yes'
        series['portSymbPattern'] = 33
    # cisco_sw_match port_symb
    elif not pd.isnull(port_symb) and match_port_dct['cisco_sw']:
        match = match_port_dct['cisco_sw']
        series['Device_Name'] = match.group(1)
        series['Device_Port'] = match.group(2)
        series['portSymbUsed'] = 'yes'
        series['portSymbPattern'] = 34
    # if no match was found copy values with no split
    else:
        if not pd.isnull(node_symb) and pd.isnull(series['nodeSymbUsed']):
            series['Device_Name'] = series['NodeSymb']
        if not pd.isnull(port_symb) and pd.isnull(series['portSymbUsed']):
            series['Device_Port'] = series['PortSymb']

    return pd.Series([series[column] for column in nsshow_symb_columns])
