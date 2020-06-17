"""
Module to retrieve storage, host, HBA information from Name Server service data.
Auxiliary to analysis_portcmd module.
"""


import re

import numpy as np
import pandas as pd


def nsshow_analysis_main(nsshow_df, nscamshow_df, fdmi_df, fabric_labels_df, re_pattern_lst):

    # label DataFrames
    nsshow_labeled_df, nscamshow_labeled_df, fdmi_labeled_df = fabric_labels(nsshow_df, nscamshow_df, fdmi_df, fabric_labels_df)

    # local Name Server (NS) Device_type (Initiatir, Target) information fillna 
    nsshow_labeled_df = device_type_fillna(nsshow_labeled_df, nscamshow_labeled_df)

    # remove unnecessary symbols in  PortSymb and NodeSymb columns in NameServer DataFrame
    nsshow_join_df = nsshow_clean(nsshow_labeled_df, re_pattern_lst)
    # split up PortSymb and NodeSymb columns in NameServer DataFrame
    nsshow_join_df, nsshow_unsplit_df = nsshow_symb_split(nsshow_join_df, re_pattern_lst)

    # fillna hba information
    nsshow_join_df = hba_fillna(nsshow_join_df, fdmi_labeled_df, re_pattern_lst)

    return nsshow_join_df, nsshow_unsplit_df


def fabric_labels(nsshow_df, nscamshow_df, fdmi_df, fabric_labels_df):
    """Function to label nsshow_df, nscamshow_df, fdmi_df Dataframes with Fabric labels"""

    # create fabric labels DataFrame
    fabric_labels_lst = ['configname', 'chassis_name', 'chassis_wwn', 'switchName', 'switchWwn', 'Fabric_name', 'Fabric_label']
    # fabric_labels_df = switch_params_aggregated_df.loc[:, fabric_labels_lst].copy()

    # copy DataFrames
    nsshow_labeled_df = nsshow_df.copy()
    nscamshow_labeled_df = nscamshow_df.copy()
    fdmi_labeled_df = fdmi_df.copy()

    df_lst = [nsshow_labeled_df, nscamshow_labeled_df, fdmi_labeled_df]

    for i, df in enumerate(df_lst):
        # rename switchname column for merging
        df.rename(columns = {'SwitchName': 'switchName'}, inplace = True)
        # label switches and update DataFrane in the list
        df_lst[i] = df.merge(fabric_labels_df, how = 'left', on = fabric_labels_lst[:5])

    return df_lst


def device_type_fillna(nsshow_labeled_df, nscamshow_labeled_df):
    """Function to fillna local Name Server (NS) Device_type (Initiatir, Target) information"""

    # drop duplcate WWNs in labeled nscamshow DataFrame with remote devices in the Name Server (NS) cache
    nscamshow_labeled_df.drop_duplicates(subset = ['Fabric_name', 'Fabric_label', 'PortName'], inplace = True)
    nsshow_labeled_df.drop_duplicates(subset = ['Fabric_name', 'Fabric_label', 'PortName'], inplace = True)
    # set Fabric_name, Fabric_label, PortName as index in order to perform fillna
    nscamshow_labeled_df.set_index(keys = ['Fabric_name', 'Fabric_label', 'PortName'], inplace = True)
    nsshow_labeled_df.set_index(keys = ['Fabric_name', 'Fabric_label', 'PortName'], inplace = True)
    # fillna empty device type cells
    nsshow_labeled_df[['Device_type', 'PortSymb', 'NodeSymb']] = nsshow_labeled_df[['Device_type', 'PortSymb', 'NodeSymb']].fillna(nscamshow_labeled_df[['Device_type', 'PortSymb', 'NodeSymb']])

    # nsshow_join_df.Device_type.fillna(nscamshow_labeled_df.Device_type, inplace = True)
    # reset index
    nsshow_labeled_df.reset_index(inplace = True)

    return nsshow_labeled_df


def nsshow_clean(nsshow_labeled_df, re_pattern_lst):
    """Function to clean (remove unnecessary symbols) PortSymb and NodeSymb columns in NameServer DataFrame"""

    # regular expression patterns
    comp_keys, _, comp_dct = re_pattern_lst

    # columns of Name Server (NS) registered devices DataFrame
    nsshow_lst = [
        'Fabric_name', 'Fabric_label', 'configname', 'chassis_name', 'chassis_wwn', 
        'switchName', 'switchWwn', 'PortName', 'NodeName', 'PortSymb', 'NodeSymb', 'Device_type', 
        'LSAN', 'Slow_Drain_Device', 'Connected_through_AG', 'Real_device_behind_AG'
        ]

    nsshow_join_df = nsshow_labeled_df.loc[:, nsshow_lst]
    
    # nsshow_join_df['PortSymbOrig'] = nsshow_join_df['PortSymb']
    # nsshow_join_df['NodeSymbOrig'] = nsshow_join_df['NodeSymb']

    # columns to clean
    symb_columns = ['PortSymb', 'NodeSymb']

    # clean 'PortSymb' and 'NodeSymb' columns
    for symb_column in symb_columns:
        # symb_clean_comp. removes brackets and quotation marks
        nsshow_join_df[symb_column] = nsshow_join_df[symb_column].str.extract(comp_dct[comp_keys[1]])
        # replace multiple whitespaces with single whitespace
        nsshow_join_df[symb_column].replace(to_replace = r' +', value = r' ', regex = True, inplace = True)
        # replace cells with one digit or whatespaces only with None value
        nsshow_join_df[symb_column].replace(to_replace = r'^\d$|^\s*$', value = np.nan, regex = True, inplace = True)
        # remove whitespace from the right and left side
        nsshow_join_df[symb_column] = nsshow_join_df[symb_column].str.strip()
        # 
        # hostname_clean_comp
        nsshow_join_df[symb_column].replace(to_replace = r'Embedded-AG', value = np.nan, regex=True, inplace = True)
    
    return nsshow_join_df


def nsshow_symb_split(nsshow_join_df, re_pattern_lst):
    """
    Function to split PortSymb and NodeSymb columns of local NameServer to group of device and HBA columns.
    Returns DataFrame with splitted columns and DataFrame with rows that function was not able to split up. 
    """

    # columns list to which PortSymb and NodeSymb information splitted up to
    nsshow_symb_columns = [
        'portSymbUsed', 'nodeSymbUsed', 'Device_Manufacturer', 'Device_Model', 
        'Device_SN', 'Device_Name', 'Device_Port', 'Device_Fw', 'Device_Location', 'IP_Address', 'HBA_Manufacturer', 
        'HBA_Model', 'HBA_Description', 'Host_Name', 'Host_OS', 'HBA_Firmware', 'HBA_Driver'
        ]

    # add  and HBA information empty columns to NameServer DataFrame
    nsshow_join_df = nsshow_join_df.reindex(columns=[*nsshow_join_df.columns.tolist(), *nsshow_symb_columns])
    # split up PortSymb and NodeSymb columns
    nsshow_join_df[nsshow_symb_columns] = nsshow_join_df.apply(lambda series: _symb_split(series, re_pattern_lst, nsshow_symb_columns), axis = 1)
    
    # show unsplit PortSymb and NodeSymb
    # mask shows rows where neither PortSymb nor NodeSymb was split up
    mask1 = nsshow_join_df[['portSymbUsed', 'nodeSymbUsed']].isna().all(axis=1)
    # mask with shows rows containing PortSymb or NodeSymb values
    mask2 = nsshow_join_df[['PortSymb',	'NodeSymb']].notna().any(axis=1)
    nsshow_unsplit_df = nsshow_join_df.loc[mask1&mask2]
    # drop columns with split flags 
    nsshow_join_df.drop(columns = ['portSymbUsed', 'nodeSymbUsed'], inplace = True)

    return nsshow_join_df, nsshow_unsplit_df


def _symb_split(series, re_pattern_lst, nsshow_symb_columns):
    """Function to extract  and HBA information from PortSymb, NodeSymb columns of the NameServer DataFrame"""

    # regular expression patterns
    comp_keys, match_keys, comp_dct = re_pattern_lst
    
    port_symb = series['PortSymb']
    node_symb = series['NodeSymb']

    # create dictionary with PortSymb cell match results
    if not pd.isnull(port_symb): 
        match_port_dct = {match_key: comp_dct[comp_key].match(port_symb) for comp_key, match_key in zip(comp_keys, match_keys)}
    # create dictionary with NodeSymb cell match results
    if not pd.isnull(node_symb):
        match_node_dct = {match_key: comp_dct[comp_key].match(node_symb) for comp_key, match_key in zip(comp_keys, match_keys)}

    # 3par_node_match node_symb
    if not pd.isnull(node_symb) and match_node_dct[match_keys[9]]:
        match = match_node_dct[match_keys[9]]
        series['Device_Manufacturer'] = match.group(3)
        series['Device_Model'] = match.group(2)
        series['Device_SN'] = match.group(4)
        series['Device_Name'] = match.group(1)
        series['Device_Fw'] = match.group(5)
        series['nodeSymbUsed'] = 'yes'
        # 3par_port_match port_symb
        if not pd.isnull(port_symb) and match_port_dct[match_keys[10]]:
            match = match_port_dct[match_keys[10]]
            series['Device_Port'] = match.group(1)
            series['HBA_Model'] = match.group(2)
            series['portSymbUsed'] = 'yes'
    # qlogic_match node_symb
    elif not pd.isnull(node_symb) and match_node_dct[match_keys[2]]:
        match = match_node_dct[match_keys[2]]
        series['HBA_Model'] = match.group(1)
        series['HBA_Firmware'] = match.group(2)
        series['HBA_Driver'] = match.group(3)
        series['nodeSymbUsed'] = 'yes'
        # xp_msa_match port_symb
        if not pd.isnull(port_symb) and match_port_dct[match_keys[14]]:
            match = match_port_dct[match_keys[14]]
            series['Device_Manufacturer'] = match.group(2)
            series['Device_Model'] = match.group(1)
            series['Device_Fw'] = match.group(3)
            series['portSymbUsed'] = 'yes'
        # qlogic_emulex_port_match port symb
        elif not pd.isnull(port_symb) and match_port_dct[match_keys[3]]:
            match = match_port_dct[match_keys[3]]
            series['HBA_Manufacturer'] = match.group(1)
            series['portSymbUsed'] = 'yes'
        # infinibox_match port_symb
        elif not pd.isnull(port_symb) and match_port_dct[match_keys[15]]:
            match = match_port_dct[match_keys[15]]
            series['Device_Manufacturer'] = match.group(2)
            series['Device_Model'] = match.group(1)
            series['portSymbUsed'] = 'yes'
    # emulex_match node_symb
    elif not pd.isnull(node_symb) and match_node_dct[match_keys[4]]:
        match = match_node_dct[match_keys[4]]
        'HBA_Manufacturer', 'HBA_Model', 'Host_Name', 'Host_OS', 'HBA_Firmware', 'HBA_Driver'
        series['HBA_Manufacturer'] = match.group(1)
        series['HBA_Model'] = match.group(2)
        'a', 'b', 'c'
        series['HBA_Firmware'] = match.group(3)
        if match.group(4):
            series['HBA_Driver'] = match.group(4).rstrip('.')
        if match.group(5) and not re.search(r'localhost|none', match.group(5)):
            series['Host_Name'] = match.group(5).rstrip('.')
        if match.group(6):
            series['Host_OS'] = match.group(6).rstrip('.')
        series['nodeSymbUsed'] = 'yes'
    # hpux_match node_symb
    elif not pd.isnull(node_symb) and match_node_dct[match_keys[6]]:
        match = match_node_dct[match_keys[6]]
        if match.group(1) and not re.search(r'localhost|none', match.group(1)):
            series['Host_Name'] = match.group(1)
        series['Host_OS'] = match.group(2)
        series['nodeSymbUsed'] = 'yes'
    # ultrium_match node_symb
    elif not pd.isnull(node_symb) and match_node_dct[match_keys[11]]:
        match = match_node_dct[match_keys[11]]
        series['Device_Manufacturer'] = match.group(2)
        series['Device_Model'] = match.group(1)
        series['Device_SN'] = match.group(4)
        series['Device_Fw'] = match.group(3)
        series['nodeSymbUsed'] = 'yes'
    # emc_vplex_match node_symb
    elif not pd.isnull(node_symb) and match_node_dct[match_keys[16]]:
        match = match_node_dct[match_keys[16]]
        series['Device_Manufacturer'] = match.group(1)
        series['Device_Model'] = match.group(1) + " " + match.group(2)
        series['Device_SN'] = match.group(3)
        series['Device_Name'] = match.group(1) + " " + match.group(2) +  " " +  match.group(3)
        series['nodeSymbUsed'] = 'yes'
        # emc_vplex_match port_symb
        if not pd.isnull(port_symb) and match_port_dct[match_keys[16]]:
            match = match_port_dct[match_keys[16]]
            series['Device_Port'] = match.group(4)
            series['portSymbUsed'] = 'yes'
    # cna_adapter_match node_symb
    elif not pd.isnull(node_symb) and match_node_dct[match_keys[19]]:
        match = match_node_dct[match_keys[19]]
        series['HBA_Driver'] = match.group(1)
        series['Device_Port'] = match.group(2)
        series['nodeSymbUsed'] = 'yes'     
    # storeonce_port_match port_symb
    elif not pd.isnull(port_symb) and match_port_dct[match_keys[8]]:
        match = match_port_dct[match_keys[8]]
        series['Device_Manufacturer'] = match.group(2)
        series['Device_Model'] = match.group(1)
        series['Device_SN'] = match.group(3)
        if match.group(3):
            series['Device_Name'] = match.group(1) + " " + match.group(3)
        series['Device_Port'] = match.group(4)
        series['portSymbUsed'] = 'yes'
    # storeonce_node_match node_symb
    elif not pd.isnull(node_symb) and match_node_dct[match_keys[7]]:
        match = match_node_dct[match_keys[7]]
        series['Device_Manufacturer'] = match.group(2)
        series['Device_Model'] = match.group(1)
        series['Device_SN'] = match.group(3)
        if match.group(3):
            series['Device_Name'] = match.group(1) + " " + match.group(3)
        series['nodeSymbUsed'] = 'yes'
    # xp_msa_match port_symb
    elif not pd.isnull(port_symb) and match_port_dct[match_keys[14]]:
        match = match_port_dct[match_keys[14]]
        series['Device_Manufacturer'] = match.group(2)
        series['Device_Model'] = match.group(1)
        series['Device_Fw'] = match.group(3)
        series['portSymbUsed'] = 'yes'
    # eva_match port_symb
    elif not pd.isnull(port_symb) and match_port_dct[match_keys[13]]:
        match = match_port_dct[match_keys[13]]
        series['Device_Model'] = match.group(1)
        series['Device_Name'] = match.group(2)
        series['portSymbUsed'] = 'yes'
    # library_match port_symb
    elif not pd.isnull(port_symb) and match_port_dct[match_keys[12]]:
        match = match_port_dct[match_keys[12]]
        series['Device_Model'] = match.group(1)
        # series['Device_Name'] = match.group(1)
        series['Device_SN'] = match.group(2)
        series['Device_Port'] = match.group(3)
        series['portSymbUsed'] = 'yes'
    # qlogic_brocade_cna_match port_symb
    elif not pd.isnull(port_symb) and match_port_dct[match_keys[5]]:
        match = match_port_dct[match_keys[5]]
        series['HBA_Manufacturer'] = match.group(1)
        series['HBA_Model'] = match.group(1) + " " + match.group(2)
        series['HBA_Driver'] = match.group(3)
        series['portSymbUsed'] = 'yes'
    # clariion_match port_symb
    elif not pd.isnull(port_symb) and match_port_dct[match_keys[17]]:
        match = match_port_dct[match_keys[17]]
        series['Device_Manufacturer'] = 'EMC'
        series['Device_Model'] = 'EMC ' + match.group(1)
        series['Device_Port'] = match.group(2)
        series['portSymbUsed'] = 'yes'
    # qlogic_emulex_port_match port_symb when node_symb is empty
    elif not pd.isnull(port_symb) and match_port_dct[match_keys[3]]:
        match = match_port_dct[match_keys[3]]
        series['HBA_Manufacturer'] = match.group(1)
        series['portSymbUsed'] = 'yes'
    # if no match was found copy values with no split
    else:
        if not pd.isnull(node_symb):
            series['Device_Name'] = series['NodeSymb']
        if not pd.isnull(port_symb):
            series['Device_Port'] = series['PortSymb']

    return pd.Series([series[column] for column in nsshow_symb_columns])


def hba_fillna(nsshow_join_df, fdmi_labeled_df, re_pattern_lst):
    """Function to fillna values in HBA related columns of local Name Server (NS) DataFrame"""

    # regular expression patterns
    comp_keys, _, comp_dct = re_pattern_lst

    # fill empty cells in PortName column with values from WWNp column
    fdmi_labeled_df.PortName.fillna(fdmi_labeled_df.WWNp, inplace = True)
    # hostname_clean_comp
    fdmi_labeled_df.Host_Name = fdmi_labeled_df.Host_Name.replace(comp_dct[comp_keys[0]], np.nan, regex=True)
    # perenthesis_remove_comp
    fdmi_labeled_df.HBA_Driver = fdmi_labeled_df.HBA_Driver.str.extract(comp_dct[comp_keys[18]])
    fdmi_labeled_df.HBA_Firmware = fdmi_labeled_df.HBA_Firmware.str.extract(comp_dct[comp_keys[18]])
    fdmi_labeled_df.Host_OS = fdmi_labeled_df.Host_OS.str.extract(comp_dct[comp_keys[18]])

    # drop duplcate WWNs in labeled fdmi DataFrame
    fdmi_labeled_df.drop_duplicates(subset = ['Fabric_name', 'Fabric_label', 'PortName'], inplace = True)
    # set Fabric_name, Fabric_label, PortName as index in order to perform fillna
    fdmi_labeled_df.set_index(keys = ['Fabric_name', 'Fabric_label', 'PortName'], inplace = True)
    nsshow_join_df.drop_duplicates(subset = ['Fabric_name', 'Fabric_label', 'PortName'], inplace = True)
    nsshow_join_df.set_index(keys = ['Fabric_name', 'Fabric_label', 'PortName'], inplace = True)
    # fillna empty device type cells in nsshow_join DataFrame with values from fdmi DataFrame
    # HBA_Manufacturer, HBA_Model, HBA_Description,	HBA_Driver,	HBA_Firmware, Host_OS
    hba_columns_lst = nsshow_join_df.columns[-7:]
    nsshow_join_df[hba_columns_lst] = nsshow_join_df[hba_columns_lst].fillna(fdmi_labeled_df[hba_columns_lst])
    # reset index
    nsshow_join_df.reset_index(inplace = True)

    nsshow_join_df['Device_Host_Name'] = nsshow_join_df.Host_Name
    nsshow_join_df.Device_Host_Name.fillna(nsshow_join_df.Device_Name, inplace = True)

    return nsshow_join_df
