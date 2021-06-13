import re

import numpy as np
import pandas as pd

from analysis_portshow_npiv_stat_notes import add_notes
from common_operations_dataframe import dataframe_fillna, dataframe_join
from common_operations_switch import (count_all_row, count_statistics,
                                      count_summary, summarize_statistics,
                                      verify_lic, verify_max_link_speed)

link_group_columns = ['Fabric_name', 'Fabric_label',  
                     'chassis_name', 'switchName',  'switchWwn', 
                     'Device_Host_Name', 'NodeName']

cfg_native_columns = ['speed', 'Speed_Cfg', 'Transceiver_speed', 'Trunk_Port']
cfg_ag_columns = ['Connected_' + cfg for cfg in cfg_native_columns]
cfg_columns = [x for xs in zip(cfg_native_columns, cfg_ag_columns) for x in xs]

service_columns = ['F_Trunk', 'Aoq', 'FEC', 'Credit_Recovery']

def npiv_link_aggregated(portshow_sfp_aggregated_df, switch_params_aggregated_df):

    npiv_link_columns = ['configname', 'chassis_name', 'chassis_wwn',
                         'portIndex', 'slot', 'port',
                         'Connected_portId', 'Connected_portWwn',
                         'portId', 'portWwn', 'Logical_portWwn',
                         'FEC', 'Credit_Recovery', 'Aoq', 'F_Trunk',
                         'switchName', 'switchWwn',
                         'speed', 'portType', 'connection_details',
                         'Fabric_name', 'Fabric_label',
                         'switchState', 'switchMode',
                         'switchType', 'Generation', 'HPE_modelName',
                         'PortName', 'NodeName', 'Device_type',
                         'Slow_Drain_Device',
                         'Device_Manufacturer', 'Device_Model', 'Device_SN',
                         'Device_Name', 'Device_Port', 'Device_Fw',
                         'Device_Location', 'IP_Address',
                         'HBA_Manufacturer', 'HBA_Description',
                         'Virtual_Channel', 
                         'deviceType', 'deviceSubtype',
                         'Index_slot_port', 'Connected_NPIV',
                         'NPIV_link_number', 'Device_Host_Name',
                         'Device_Host_Name_per_fabric_name_and_label',
                         'Device_Host_Name_per_fabric_label',
                         'Device_Host_Name_per_fabric_name',
                         'Device_Host_Name_total_fabrics',
                         'Transceiver_speedMax', 'Transceiver_category', 'Transceiver_mode',
                         'Speed_Cfg', 'Trunk_Port',
                         'Long_Distance', 'VC_Link_Init',
                         'NPIV_PP_Limit', 'NPIV_FLOGI_Logout',
                         'QOS_Port', 'Rate_Limit', 'Credit_Recovery_cfg',
                         '10G/16G_FEC', 'FEC_cfg']

    # portshow DataFrame with replaced port setting OFF (presented as '..') as np.nan
    portshow_sfp_cp_df = portshow_sfp_aggregated_df.replace(to_replace='^\.\.$', value=np.nan, regex=True).copy()
    # DataFrame contaning devices (switches, VC modules) connected through NPIV
    mask_npiv = portshow_sfp_aggregated_df['Connected_NPIV'] == 'yes'
    portshow_npiv_df = portshow_sfp_cp_df.loc[mask_npiv, npiv_link_columns].copy()
    # add AG ports information
    portshow_npiv_df = fillna_ag_link(portshow_npiv_df, portshow_sfp_cp_df, switch_params_aggregated_df)

    portshow_npiv_df['NPIV_link_number'] = portshow_npiv_df['NPIV_link_number'].astype('float64', errors='ignore')
    portshow_npiv_df['NPIV_link_number'] = portshow_npiv_df['NPIV_link_number'].astype('int64', errors='ignore')

    return portshow_npiv_df


def fillna_ag_link(portshow_npiv_df, portshow_sfp_cp_df, switch_params_aggregated_df):
    """Function to fill npiv ports information of portshow_npiv_df from portshow_sfp_cp_df."""
        
    # columns of portshow_sfp_cp_df used for to fill npiv ports information
    port_columns = [ 'switchWwn', 'Index_slot_port', 
                    'Generation', 'speed', 'portType',
                    'FEC_cfg', '10G/16G_FEC',
                    'Credit_Recovery_cfg',
                    'QOS_Port', 
                    'Trunk_Port',
                    'Rate_Limit', 'Speed_Cfg',
                    'Transceiver_mode', 'Transceiver_speedMax', 'Transceiver_category']
    port_columns = [column for column in port_columns if column in portshow_sfp_cp_df.columns]
    # columns of portshow_npiv_df that going to be filled
    npiv_port_columns = ['Connected_' + column for column in port_columns]
    # rename columns in portshow_sfp_cp_df to correspond columns in portshow_npiv_df
    rename_columns_dct = {k:v for k, v in zip(port_columns, npiv_port_columns)}
    # rename_columns_dct = {column: 'Connected_' + column for column in port_columns}
    portshow_sfp_cp_df.rename(columns=rename_columns_dct, inplace=True)
    # columns identifying npiv switch and port
    npiv_port_idx_сolumns = ['Connected_switchWwn', 'Connected_Index_slot_port']
    portshow_npiv_df[['Connected_switchWwn', 'Connected_Index_slot_port']] = portshow_npiv_df[['NodeName', 'Device_Port']]
    # split columns on which merge is performed and columns to be filled
    for column in npiv_port_idx_сolumns:
        npiv_port_columns.remove(column)
    # add detailed npiv port information from portshow_sfp_cp_df
    portshow_npiv_df = dataframe_fillna(portshow_npiv_df, portshow_sfp_cp_df,
                                        join_lst=npiv_port_idx_сolumns, filled_lst=npiv_port_columns)
    # add Native and npiv switch related information (licenses and max speed)
    portshow_npiv_df = dataframe_join(portshow_npiv_df, switch_params_aggregated_df, 
                                      columns_lst=['switchWwn', 'licenses', 'switch_speedMax'], 
                                      columns_join_index = 1)
    # verify if link between Native and NPIV switch operates at maximim speed
    portshow_npiv_df = verify_max_link_speed(portshow_npiv_df)
    # verify if trunking license installed on both switches
    portshow_npiv_df = verify_lic(portshow_npiv_df, 'licenses', 'Trunking')
    return portshow_npiv_df
    
    
def prior_prepearation(portshow_npiv_df, re_pattern_lst):
    """Function to modify portshow_npiv_df to count statistics"""

        # regular expression patterns
    *_, comp_dct = re_pattern_lst

    portshow_npiv_cp_df = portshow_npiv_df.copy()
    native_tag = 'Native_'
    ag_tag = 'AG_'
    
    # max and reduced speed tags
    portshow_npiv_cp_df['Link_speedActualMax'].replace(to_replace={'Yes': 'Speed_Max', 'No': 'Speed_Reduced'}, inplace=True)
    # auto and fixed speed tags
    portshow_npiv_cp_df[['Speed_Cfg', 'Connected_Speed_Cfg']] = \
        portshow_npiv_cp_df[['Speed_Cfg', 'Connected_Speed_Cfg']].replace(regex={r'AN': 'Speed_Auto', r'\d+G': 'Speed_Fixed'})
    # port_quantity tag
    portshow_npiv_cp_df['port'] = 'Port_quantity'
    # convert link number value to str type for concatenation with 'Link' tag
    portshow_npiv_cp_df.rename(columns={'NPIV_link_number': 'Link'}, inplace=True)    
    portshow_npiv_cp_df['Link'] = portshow_npiv_cp_df['Link'].astype('str', errors='ignore')
    mask_npiv_num_notna = portshow_npiv_cp_df['Link'].notna()
    portshow_npiv_cp_df.loc[mask_npiv_num_notna, 'Link'] = \
        'Link' + '_' + portshow_npiv_cp_df['Link'].astype('int', errors='ignore').astype('str', errors='ignore')

    # extract available transceiver speed values
    sfp_speed_re = comp_dct['transceiver_speed']
    # sfp_speed_re =  r'((?:\d+,){2}\d+_\w+)' TO_REMOVE
    portshow_npiv_cp_df['Transceiver_speed'] = \
        portshow_npiv_cp_df['Transceiver_mode'].str.extract(sfp_speed_re)
    if portshow_npiv_cp_df['Connected_Transceiver_mode'].notna().any():
        portshow_npiv_cp_df['Connected_Transceiver_speed'] = \
            portshow_npiv_cp_df['Connected_Transceiver_mode'].str.extract(sfp_speed_re)
    else:
        portshow_npiv_cp_df['Connected_Transceiver_speed'] = np.nan

    # add column name for Transceiver_speed
    transceiver_columns = ['Transceiver_speed', 'Connected_Transceiver_speed']
    for column in transceiver_columns:
        if column in portshow_npiv_cp_df.columns:
            mask_notna = portshow_npiv_cp_df[column].notna()
            # tag = re.search(r'^(?:Connected_)?(.+?)(?:_Port)?', column).group(1)
            portshow_npiv_cp_df.loc[mask_notna, column] = 'Transceiver_speed_' + portshow_npiv_cp_df[column]

    # for services remove 'Inactive' status and add service name instead 'Active' status
    for column in service_columns:
        mask_active = portshow_npiv_cp_df[column] == 'Active'
        portshow_npiv_cp_df[column] = portshow_npiv_cp_df[column].where(mask_active, np.nan)
        portshow_npiv_cp_df[column] = portshow_npiv_cp_df[column].where(~mask_active, column)
    
    # add AG or Native to tag and column name to the value
    for cfg_column in cfg_columns:
        mask_notna = portshow_npiv_cp_df[cfg_column].notna()
        tag = ag_tag if 'Connected_' in cfg_column else native_tag
        if not 'speed' in cfg_column.lower():
            port_settings_re = comp_dct['port_settings']
            if re.match(port_settings_re, cfg_column):
            # if re.match(r'^(?:Connected_)?(.+?)(?:_Port)', cfg_column): TO_REMOVE
                # cfg_name = re.match(r'^(?:Connected_)?(.+?)(?:_Port)', cfg_column).group(1).upper() TO_REMOVE
                cfg_name = re.match(port_settings_re, cfg_column).group(1).upper()
                tag += cfg_name + '_'
        portshow_npiv_cp_df[cfg_column] = \
            portshow_npiv_cp_df[cfg_column].where(~mask_notna, tag + portshow_npiv_cp_df[cfg_column])
    return portshow_npiv_cp_df


def npiv_statistics(portshow_npiv_df, re_pattern_lst):
    """Function to count NPIV connection statistics"""
    
    portshow_npiv_cp_df = prior_prepearation(portshow_npiv_df, re_pattern_lst)
    # count statistics for stat columns
    stat_columns = ['port', 'Link', *service_columns, 'Link_speedActualMax', *cfg_columns]
    npiv_statistics_df = count_statistics(portshow_npiv_cp_df, link_group_columns, stat_columns, 
                                            port_qunatity_column = 'port', speed_column = 'speed')
    npiv_statistics_df.fillna(0, inplace=True)
    # add trunk lic for both switches column
    npiv_statistics_df = dataframe_fillna(npiv_statistics_df, portshow_npiv_cp_df, 
                                          join_lst=link_group_columns, filled_lst=['Trunking_lic_both_switches'])
    # add notes to statistics DataFrame
    npiv_statistics_df = add_notes(npiv_statistics_df, portshow_npiv_cp_df, link_group_columns, re_pattern_lst)

    # insert 'Device_quantity' column to place it to correct location in final statistics DataFrame 
    insert_index = npiv_statistics_df.columns.get_loc('Port_quantity')
    npiv_statistics_df.insert(loc=insert_index, column='Device_quantity', value=1)
    # summarize statistics for fabric_name and fabric_label, for fabric_name and for all fabrics in total
    count_columns = npiv_statistics_df.columns.tolist()
    connection_symmetry_columns = ['Device_quantity', 'Port_quantity', 'Bandwidth_Gbps']
    sort_columns = ['Fabric_name', 'Fabric_label', 'switchName', 'Device_Host_Name']
    npiv_statistics_df = summarize_statistics(npiv_statistics_df, count_columns, 
                                                connection_symmetry_columns, sort_columns)
    return npiv_statistics_df


    def npiv_report(portshow_npiv_df, npiv_statistics_df):
        pass



