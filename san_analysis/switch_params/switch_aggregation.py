"""Module to combine chassis, switch, maps parameters DataFrames and label it with fabric labels"""


import re

import numpy as np
import pandas as pd

import utilities.dataframe_operations as dfop


def switch_param_aggregation(fabric_clean_df, chassis_params_df, chassisshow_df, switch_params_df, maps_params_df, 
                                switch_models_df, switch_rack_df, ag_principal_df, pattern_dct):
    """Function to combine chassis, switch, maps parameters DataFrames and label it with fabric labels"""

    # complete fabric DataFrame with information from switch_params DataFrame
    switch_params_aggregated_df = fabric_clean_df.merge(switch_params_df, how = 'left', on = ['switchWwn', 'switchName'])
    switch_params_aggregated_df['SwitchName'].fillna(switch_params_aggregated_df['switchName'], inplace=True)
    switch_params_aggregated_df['switchName'].fillna(switch_params_aggregated_df['SwitchName'], inplace=True)
    switch_params_aggregated_df['boot.ipa'].fillna(switch_params_aggregated_df['Enet_IP_Addr'], inplace=True)
    switch_params_aggregated_df['switchMode'].fillna(switch_params_aggregated_df['SwitchMode'], inplace=True)
    # fill empty sn, add factory sn and oem id
    chassis_params_df = add_chassis_sn_oemid(chassis_params_df, chassisshow_df)
    # complete DataFrame with information from chassis_params DataFrame
    switch_params_aggregated_df = switch_params_aggregated_df.merge(chassis_params_df, how = 'left', on=['configname', 'chassis_name', 'chassis_wwn'])
    switch_params_aggregated_df = dfop.concatenate_columns(switch_params_aggregated_df, summary_column='timezone_hm', 
                                                            merge_columns=['timezone_h', 'timezone_m'], sep=':', drop_merge_columns=False)
    switch_params_aggregated_df['config_collection_date_ymd'] =  pd.to_datetime(switch_params_aggregated_df['config_collection_date']).dt.date
    # add AG and VC switch information from principal switch
    switch_params_aggregated_df = ag_switch_info(switch_params_aggregated_df, ag_principal_df)
    
    # # add chassis wwn in case if chassis_wwn missing
    # switch_params_aggregated_df['chassis_wwn'].fillna(switch_params_aggregated_df['boot.licid'], inplace=True)
    
    # identify logical ('base', 'default', 'logical') or physical switch type
    switch_params_aggregated_df = verify_ls_type(switch_params_aggregated_df)
    switch_params_aggregated_df = verify_base_in_chassis(switch_params_aggregated_df)

    # convert switch_index to the same type
    maps_params_df.switch_index = maps_params_df.switch_index.astype('float64', errors='ignore')
    switch_params_aggregated_df.switch_index = switch_params_aggregated_df.switch_index.astype('float64', errors='ignore')
    # complete DataFrame with information from maps_params DataFrame
    switch_params_aggregated_df = switch_params_aggregated_df.merge(maps_params_df, how = 'left', on = ['configname', 'chassis_name', 'switch_index'])
    # count sddq ports and verify if sddq limit has been reached
    switch_params_aggregated_df = verify_sddq_reserve(switch_params_aggregated_df, pattern_dct)
    # maps.activePolicy is in configshow,  Active_policy is in AMS_MAPS file. 
    switch_params_aggregated_df['maps.activePolicy'].fillna(switch_params_aggregated_df['Active_policy'], inplace=True)
    # convert switchType to the same type
    switch_params_aggregated_df.switchType = switch_params_aggregated_df.switchType.astype('float64', errors='ignore')
    # remove fractional part from switchType
    switch_params_aggregated_df.switchType = np.floor(switch_params_aggregated_df.switchType)
    switch_models_df.switchType = switch_models_df.switchType.astype('float64', errors='ignore')
    # complete DataFrame with information from switch_models DataFrame
    switch_params_aggregated_df = switch_params_aggregated_df.merge(switch_models_df, how='left', on='switchType')
    # add switch location
    switch_params_aggregated_df = dfop.dataframe_fillna(switch_params_aggregated_df, switch_rack_df, 
                                                        join_lst=['switchWwn'], filled_lst=['Device_Rack'], 
                                                        remove_duplicates=True, drop_na=True)
    # create column with switch models (hpe or brocade model)
    switch_params_aggregated_df['ModelName'] = switch_params_aggregated_df['HPE_modelName']
    switch_params_aggregated_df['ModelName'].replace(to_replace={'-': np.nan}, inplace=True)
    switch_params_aggregated_df['ModelName'].fillna(switch_params_aggregated_df['Brocade_modelName'], inplace=True)
    # sorting DataFrame
    sort_switches(switch_params_aggregated_df)
    # add empty column FOS suuported to fill manually 
    switch_params_aggregated_df['FW_Supported'] = pd.Series(dtype='object')

    # license check
    license_dct = {'Trunking_license': 'Trunking', 'Fabric_Vision_license': 'Fabric Vision', 'Expiry_date': 'Expiry', 'Expired_license': 'expired'}
    for lic_check, lic_name in license_dct.items():
        switch_params_aggregated_df[lic_check] = \
            switch_params_aggregated_df.loc[switch_params_aggregated_df['licenses'].notnull(), 'licenses'].apply(lambda x: lic_name in x)
        switch_params_aggregated_df[lic_check].replace(to_replace={True: 'Да', False: 'Нет'}, inplace = True)

    # check if chassis_name and switch_name columns are equal
    # if yes then no need to use chassis information in tables
    # remove switches with unparsed data
    # chassis_names_check_df = switch_params_aggregated_df.dropna(subset=['chassis_name', 'SwitchName'], how = 'all')
    chassis_names_check_df = switch_params_aggregated_df.dropna(subset=['chassis_name'], how = 'all')
    if all(chassis_names_check_df.chassis_name == chassis_names_check_df.SwitchName):
        chassis_column_usage = 0
    else:
        chassis_column_usage = 1
    # Check number of Fabric_names. 
    # If there is only one Fabric_name then no need to use Fabric_name column in report Dataframes
    fabric_name_usage = 1 if switch_params_aggregated_df.Fabric_name.nunique() > 1 else 0
    report_columns_usage_sr = pd.Series([fabric_name_usage, chassis_column_usage], 
                                            index=['fabric_name_usage', 'chassis_info_usage'], name='usage')
    return switch_params_aggregated_df, report_columns_usage_sr


def sort_switches(switch_params_aggregated_df):
    """Function to sort switches"""

    # create sort_weights based on switch class. unassigned class have maximimum weight
    switch_params_aggregated_df['switchClass_weight'] = switch_params_aggregated_df['switchClass']
    switchClass_weight_dct = {'DIR': 1, 'ENTP': 2, 'MID': 3, 'ENTRY': 4, 'EMB': 5, 'EXT': 6}
    mask_assigned_switch_class = switch_params_aggregated_df['switchClass'].isin(switchClass_weight_dct.keys())
    switch_params_aggregated_df.loc[~mask_assigned_switch_class, 'switchClass_weight'] = np.nan
    switch_params_aggregated_df['switchClass_weight'].replace(switchClass_weight_dct, inplace=True)
    switch_params_aggregated_df['switchClass_weight'].fillna(7, inplace=True)
    switch_params_aggregated_df.sort_values(by=['Fabric_name', 'Fabric_label', 
                                                'switchClass_weight', 'switchType', 
                                                'chassis_name', 'switch_index'], \
                                            ascending=[True, True, True, False, True, True], inplace=True)
    # reset index values
    switch_params_aggregated_df.reset_index(inplace=True, drop=True)


def ag_switch_info(switch_params_aggregated_df, ag_principal_df):
    """Function to add AG switches and VC's switchtype, fw version collected 
    from Principal switch configuration to switch_params_aggrefated_df DataFrame"""

    # extract required columns from ag_principal_df DataFrame and translate it's
    # titles to correspond columns in switch_params_aggregated_df DataFrame
    ag_columns_lst = ['AG_Switch_WWN', 'AG_Switch_Type', 'AG_Switch_Firmware_Version']
    switch_columns_lst = ['switchWwn', 'switchType', 'FOS_version']
    ag_translate_dct = dict(zip(ag_columns_lst, switch_columns_lst))
    ag_fw_type_df = ag_principal_df.copy()
    ag_fw_type_df = ag_fw_type_df.loc[:, ag_columns_lst]
    ag_fw_type_df.rename(columns=ag_translate_dct, inplace=True)
    # fill information for AG switches and VC
    switch_params_aggregated_df = \
        dfop.dataframe_fillna(switch_params_aggregated_df, ag_fw_type_df, join_lst=switch_columns_lst[0:1], 
                                                                    filled_lst=switch_columns_lst[1:])
    return switch_params_aggregated_df


def verify_sddq_reserve(switch_params_aggregated_df, pattern_dct):
    """Function to count sddq ports and verify if sddq limit is reached"""

    if 'Quarantined_Ports' in switch_params_aggregated_df.columns:
        # clean value if it doesn't contain port information
        maps_clean_pattern = pattern_dct['maps_clean']
        switch_params_aggregated_df['Quarantined_Ports_clean'] = switch_params_aggregated_df['Quarantined_Ports']
        
        if switch_params_aggregated_df['Quarantined_Ports_clean'].notna().any():
            switch_params_aggregated_df['Quarantined_Ports_clean'].replace(to_replace={maps_clean_pattern: np.nan}, regex=True, inplace=True)
        # check after replacement
        if switch_params_aggregated_df['Quarantined_Ports_clean'].notna().any():
            # count sddq port quantity
            switch_params_aggregated_df['SDDQ_switch_quantity'] = \
                switch_params_aggregated_df['Quarantined_Ports_clean'].str.split(',').str.len()
            # if there is no sddq port than value is 0
            switch_params_aggregated_df['SDDQ_switch_quantity'].fillna(0, inplace=True)
        else:
            switch_params_aggregated_df['SDDQ_switch_quantity'] = 0
        switch_params_aggregated_df.drop(columns=['Quarantined_Ports_clean'], inplace=True)        
    else:
        switch_params_aggregated_df['SDDQ_switch_quantity'] = 0

    # count sddq port quantity per chassis 
    chassis_grp = [ 'configname', 'chassis_name', 'chassis_wwn']    
    switch_params_aggregated_df['SDDQ_chassis_quantity'] = \
        switch_params_aggregated_df.groupby(chassis_grp)['SDDQ_switch_quantity'].transform('sum')

    # count sddq reserverd ports (how many ports could be quarantined) per chassis
    if 'fos.sddqChassisLimit' in switch_params_aggregated_df.columns:
        switch_params_aggregated_df['fos.sddqChassisLimit_float'] = \
            switch_params_aggregated_df['fos.sddqChassisLimit'].astype('float64')
        switch_params_aggregated_df['SDDQ_chassis_reserve'] = \
            switch_params_aggregated_df['fos.sddqChassisLimit_float'] - switch_params_aggregated_df['SDDQ_chassis_quantity']
    return switch_params_aggregated_df


def verify_base_in_chassis(switch_params_aggregated_df):
    """Function t verify if base switch present in chassis"""

    grp_columns = ['configname', 'chassis_name', 'chassis_wwn']

    switch_params_aggregated_df['Base_switch_in_chassis'] = switch_params_aggregated_df['Base_Switch']
    switch_params_aggregated_df['Base_switch_in_chassis'].fillna('_', inplace=True)
    # join base switch tags for all logical switches in chassis
    switch_params_aggregated_df['Base_switch_in_chassis'] = switch_params_aggregated_df.groupby(by=grp_columns, dropna=False)['Base_switch_in_chassis'].transform(', '.join)
    # if base switch tag present on any switch in chassis then 'Base_switch_in_chassis' tag is 'Yes'
    mask_base_in_chassis =  switch_params_aggregated_df['Base_switch_in_chassis'].str.contains('Yes')
    switch_params_aggregated_df['Base_switch_in_chassis'] = np.where(mask_base_in_chassis, 'Yes', pd.NA)
    switch_params_aggregated_df['Base_switch_in_chassis'].fillna(np.nan, inplace=True)
    return switch_params_aggregated_df


def verify_ls_type(switch_params_aggregated_df):
    """Function to identify logical ('base', 'default', 'logical') or physical switch type"""

    mask_base = switch_params_aggregated_df['Base_Switch'] == 'Yes'
    mask_not_base = switch_params_aggregated_df['Base_Switch'] == 'No'        
    mask_default = switch_params_aggregated_df['Default_Switch'] == 'Yes'
    mask_logical = switch_params_aggregated_df['Default_Switch'] == 'No'
    mask_non_ls_mode = switch_params_aggregated_df['LS_mode'] == 'OFF'
    mask_empty_ip = switch_params_aggregated_df['Enet_IP_Addr'] == '0.0.0.0'
    mask_front_domain = switch_params_aggregated_df['switchName'].str.contains('fcr_fd_\d+', case=False, na=False)
    mask_translate_domain = switch_params_aggregated_df['switchName'].str.contains('fcr_xd_\d+', case=False, na=False)
    switch_params_aggregated_df['LS_type'] = \
        np.select([mask_base, mask_default, mask_not_base & mask_logical, mask_non_ls_mode, mask_empty_ip & mask_front_domain, mask_empty_ip & mask_translate_domain],
                                            ['base', 'default', 'logical', 'physical', 'front_domain', 'translate_domain'], default='unknown')
    switch_params_aggregated_df['LS_type_report'] = switch_params_aggregated_df['LS_type']
    mask_router = switch_params_aggregated_df['FC_Router'] == 'ON'
    # add router if base switch in fc_routing mode
    switch_params_aggregated_df.loc[mask_router, 'LS_type_report'] = \
        switch_params_aggregated_df.loc[mask_router, 'LS_type_report'] + ', router'
    return switch_params_aggregated_df


def add_chassis_sn_oemid(chassis_params_df, chassisshow_df):
    """Function to fill empty sn, add factory sn and oem id"""

    # filter chassis/wwn slots
    mask_slot_wwn = chassisshow_df['Slot_Unit_name'].str.contains(pat='wwn', regex=True, flags=re.IGNORECASE, na=None)
    chassisshow_wwn_df = chassisshow_df.loc[mask_slot_wwn].copy()
    # clean 'none' ssn
    mask_sn_none = chassis_params_df['ssn'] == 'none'
    chassis_params_df.loc[mask_sn_none, 'ssn'] = None
    # chassis/wwn sn (OEM switches with sn)
    chassis_params_df = chassis_param_fillna(chassis_params_df, chassisshow_wwn_df, chassisshow_column='Serial_Num', filled_column='ssn')
    # chassis factory sn (directors)
    chassis_params_df = chassis_param_fillna(chassis_params_df, chassisshow_wwn_df, chassisshow_column='Chassis_Factory_Serial_Num', filled_column='factory_sn')
    # chassis/wwn factory sn (Brocade switches with factory sn)
    chassis_params_df = chassis_param_fillna(chassis_params_df, chassisshow_wwn_df, chassisshow_column='Factory_Serial_Num', filled_column='factory_sn')
    # add OEM_ID
    chassis_params_df = chassis_param_fillna(chassis_params_df, chassisshow_wwn_df, chassisshow_column='OEM_ID', filled_column='OEM_ID')
    return chassis_params_df


def chassis_param_fillna(chassis_params_df, chassisshow_wwn_df, chassisshow_column, filled_column):
    """Function to fill chassisshow_column in chassis_params_df dataframe with values
    from filled_column of chassisshow_wwn_df dataframe"""
    
    # drop rows with empty values in chassisshow_column
    chassisshow_wwn_nafree_df = chassisshow_wwn_df.dropna(subset=[chassisshow_column]).copy()
    # for directors join slot factory serial numbers
    if chassisshow_wwn_nafree_df[chassisshow_column].notna().any():
        chassisshow_wwn_nafree_df = chassisshow_wwn_nafree_df.groupby('configname')[chassisshow_column].agg(', '.join).reset_index()
    # fill empty values in chassis_params_df
    chassisshow_wwn_nafree_df[filled_column] = chassisshow_wwn_nafree_df[chassisshow_column]
    chassis_params_df = dfop.dataframe_fillna(chassis_params_df, chassisshow_wwn_nafree_df, join_lst=['configname'], filled_lst=[filled_column])
    return chassis_params_df