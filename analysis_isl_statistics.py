"""Module to present all links between pair of switches in a single row, count ISL statistics 
and provide notes if any ISL criteria is violated"""

import numpy as np
import pandas as pd

from analysis_isl_statistics_notes import add_notes
from common_operations_dataframe import count_total, сoncatenate_columns

isl_group_columns = ['Fabric_name', 'Fabric_label',  
                     'chassis_name', 'SwitchName',  'switchWwn', 
                     'Connected_SwitchName', 'Connected_switchWwn']


def isl_statistics(isl_aggregated_df, re_pattern_lst, report_data_lst):
    """Main function to count ISL statistics"""
    
    # isl_statistics_df = isl_aggregated_df[['Fabric_name', 'Fabric_label', 'chassis_name', 'chassis_wwn', 'SwitchName', 'switchWwn']].copy()
    isl_statistics_df = pd.DataFrame()

    if not isl_aggregated_df.empty:
        # change isl_aggregated_df DataFrame values reresenatation in order to count statistics
        isl_aggregated_modified_df = prior_prepearation(isl_aggregated_df, re_pattern_lst)
        # count connection bandwidth of each pair of switches 
        isl_bandwidth_df = count_isl_bandwidth(isl_aggregated_modified_df)
        # count connection statistics of each pair of switches
        isl_statistics_df = count_isl_statistics(isl_aggregated_modified_df, isl_bandwidth_df)
        # sort rows (each row is a pair of switches connection) so that ISL information about 
        # the same connection (from both switches) appear in a adjacent rows
        isl_statistics_df = sort_isl(isl_statistics_df)
        # verify if Trunking licence installed on both switches of connection
        isl_statistics_df = verify_trunking_lic(isl_aggregated_modified_df, isl_statistics_df)
        # count switches connection statistics for Fabric_lable and Fabric_name levels
        isl_statistics_summary_df = isl_statistics_summary(isl_statistics_df, report_data_lst)
        # verify if fabrics are symmetric
        isl_statistics_summary_df = verify_isl_symmetry(isl_statistics_summary_df)
        # add notes to isl_statistics_df if any violations present
        isl_statistics_df = add_notes(isl_statistics_df, isl_aggregated_modified_df, isl_group_columns, re_pattern_lst)
        # count row with index All containing total values of isl_statistics_summary_df for all fabrics
        isl_statistics_total_df = count_all_row(isl_statistics_summary_df)
        # insert 'Switch_quantity' column to place it to correct location in final statistics DataFrame 
        insert_index = isl_statistics_df.columns.get_loc('Port_quantity')
        isl_statistics_df.insert(loc=insert_index, column='Switch_quantity', value=1)
        # concatenate statistics dataframes
        isl_statistics_df = pd.concat([isl_statistics_df, isl_statistics_summary_df])
        isl_statistics_df.sort_values(by=['Fabric_name', 'Fabric_label', 'sort_column_1', 'sort_column_2'], inplace=True)
        isl_statistics_df = pd.concat([isl_statistics_df, isl_statistics_total_df])
        # reset indexes in final statistics DataFrame
        isl_statistics_df.reset_index(inplace=True, drop=True)

    return isl_statistics_df    


def prior_prepearation(isl_aggregated_df, re_pattern_lst):
    """Function to prepare data in isl_aggregated_df to count statistics"""

    # regular expression patterns
    comp_keys, _, comp_dct = re_pattern_lst

    columns_lst =  ['Fabric_name', 'Fabric_label',  'chassis_name', 'SwitchName',  'switchWwn', 
                    'Connected_SwitchName', 'Connected_switchWwn', 
                    'port', 'ISL_number', 'portType', 'speed', 'Speed_Cfg', 'Link_speedActualMax', 
                    'Distance', 'Transceiver_mode', 'Trunking_license',  'Connected_Trunking_license', 
                    'Trunk_Port', 'Credit_Recovery', 'Encryption', 'Compression', 'QOS_Port', 'QOS_E_Port', 'FEC', '10G/16G_FEC',
                    'Long_Distance', 'VC_Link_Init', 'ISL_R_RDY_Mode']

    xisl_columns = ['Base_Switch', 'Allow_XISL_Use', 'Base_switch_in_chassis',
                    'Connected_Base_Switch', 'Connected_Allow_XISL_Use', 'Connected_Base_switch_in_chassis']

    columns_lst.extend(xisl_columns)
    
    isl_aggregated_modified_df = isl_aggregated_df[columns_lst].copy()
    
    # clean port settings defined as '..' (indicate 'OFF' value)
    isl_aggregated_modified_df.replace(regex=[r'^\.\.$'], value=np.nan, inplace=True)
    
    # transceiver speed and mode extraction
    sfp_speed_re = comp_dct.get('transceiver_speed')
    isl_aggregated_modified_df['Transceiver_speed'] = isl_aggregated_modified_df['Transceiver_mode'].str.extract(sfp_speed_re)
    sfp_mode_re = comp_dct.get('transceiver_mode')
    isl_aggregated_modified_df['Transceiver_mode'] = isl_aggregated_modified_df['Transceiver_mode'].str.extract(sfp_mode_re)
    # max and reduced speed tags
    isl_aggregated_modified_df['Link_speedActualMax'].replace(to_replace={'Yes': 'Speed_Max', 'No': 'Speed_Reduced'}, inplace=True)
    # auto and fixed speed tags
    isl_aggregated_modified_df['Speed_Cfg'].replace(regex={r'AN': 'Speed_Auto', r'\d+G': 'Speed_Fixed'}, inplace=True)
    # port_quantity tag
    isl_aggregated_modified_df['port'] = 'Port_quantity'
    # port settings
    isl_aggregated_modified_df.rename(columns={'ISL_number': 'ISL', 'Trunk_Port': 'TRUNK', 'QOS_Port': 'QOS'}, inplace=True)
    # merge QOS and QOS_E_Port port settings
    isl_aggregated_modified_df['QOS'].fillna(isl_aggregated_modified_df['QOS_E_Port'], inplace=True)
    # merge FEC and 10G/16G_FEC port settings
    isl_aggregated_modified_df['FEC'].fillna(isl_aggregated_modified_df['10G/16G_FEC'], inplace=True)
    
    port_settings_columns = ['Distance', 'Transceiver_speed', 'Transceiver_mode', 'ISL', 'TRUNK',  'Encryption', 
                             'Compression', 'QOS', 'FEC', 'Long_Distance', 'VC_Link_Init', 'ISL_R_RDY_Mode', 'Credit_Recovery']
    # convert ISL number value to str type for concatenation with 'ISL' tag
    isl_aggregated_modified_df['ISL'] = isl_aggregated_modified_df['ISL'].astype('str', errors='ignore')
    # add column name for each non empty value in port_settings_columns
    for column in port_settings_columns:
        mask_setting_on = isl_aggregated_modified_df[column].notna()
        isl_aggregated_modified_df.loc[mask_setting_on, column] = column + '_' + isl_aggregated_modified_df[column]

    # mark logical isl links.
    # base switch: NO, Allow_XISL: ON, chassis contains base switch: 'Yes (for switch and connecetd switch)
    mask_xisl = (isl_aggregated_modified_df[xisl_columns] == ['No', 'ON', 'Yes']*2).all(axis=1)
    mask_unavailable_speed = isl_aggregated_modified_df['speed'] == '--'

    isl_aggregated_modified_df.loc[mask_xisl & mask_unavailable_speed, 'speed'] = \
        isl_aggregated_modified_df.loc[mask_xisl & mask_unavailable_speed, 'speed'] = 'XISL'
        
    return isl_aggregated_modified_df


def count_isl_bandwidth(isl_aggregated_modified_df):
    """Function to count total ISLs bandwidth between each pair of switches"""
    
    # extract speed values
    isl_aggregated_modified_df['Bandwidth_Gbps'] = isl_aggregated_modified_df['speed'].str.extract(r'(\d+)')
    isl_aggregated_modified_df['Bandwidth_Gbps'] = isl_aggregated_modified_df['Bandwidth_Gbps'].astype('int64', errors='ignore')
    # group ISLs so all ISLs between two switches are in one group and count total bandwidth for each ISL group
    # count total bandwidth for each ISL group
    isl_bandwidth_df = isl_aggregated_modified_df.groupby(by=isl_group_columns)['Bandwidth_Gbps'].sum()
    
    return isl_bandwidth_df


def count_isl_statistics(isl_aggregated_modified_df, isl_bandwidth_df):
    """Function to count ISLs ports quantity, speed, parameters, 
    applied transceivers speed and mode statistics for each pair of switches connection"""

    isl_statistics_df = pd.DataFrame()
    statistics_lst =  ['port', 'ISL', 'speed', 'Speed_Cfg', 'Link_speedActualMax', 
                    'Transceiver_speed', 'Transceiver_mode', 'Distance',
                    'TRUNK', 'Encryption', 'Compression', 'QOS', 'FEC',
                    'Long_Distance', 'VC_Link_Init', 'ISL_R_RDY_Mode', 'Credit_Recovery']
    # drop empty columns from the list
    statistics_lst = [column for column in statistics_lst if isl_aggregated_modified_df[column].notna().any()]
    # index list to groupby switches connection on to count statistics
    index_lst = [isl_aggregated_modified_df[column] for column in isl_group_columns]

    # count statistcics for each column from statistics_lst in isl_aggregated_modified_df DataFrame
    for column in statistics_lst:
        # count statistics for current column
        current_statistics_df = pd.crosstab(index = index_lst,
                                columns = isl_aggregated_modified_df[column])
        # add isl bandwidth column after column with port quantity 
        if column == 'port':
            current_statistics_df = current_statistics_df.merge(isl_bandwidth_df, how='left',
                                                                left_index=True, right_index=True)
        # add current_statistics_df DataFrame to isl_statistics_df DataFRame
        if isl_statistics_df.empty:
            isl_statistics_df = current_statistics_df.copy()
        else:
            isl_statistics_df = isl_statistics_df.merge(current_statistics_df, how='left', 
                                                                                    left_index=True, right_index=True)
    isl_statistics_df.reset_index(inplace=True)
                
    return isl_statistics_df
    

def verify_trunking_lic(isl_aggregated_modified_df, isl_statistics_df):
    """Function to verify if Trunking license is present on both switches of the ISL"""
    
    isl_aggregated_modified_df['Trunking_lic_both_switches'] = np.nan
    # masks licence info present and licence installed or not
    mask_trunking_lic = isl_aggregated_modified_df[['Trunking_license',  'Connected_Trunking_license']].notna().all(axis=1)
    mask_all_yes = (isl_aggregated_modified_df[['Trunking_license',  'Connected_Trunking_license']] == 'Yes').all(axis=1)
    mask_any_no = (isl_aggregated_modified_df[['Trunking_license',  'Connected_Trunking_license']] == 'No').any(axis=1)
    
    isl_aggregated_modified_df['Trunking_lic_both_switches'] = np.select(
        [mask_trunking_lic & mask_all_yes, mask_trunking_lic & mask_any_no],
        ['Yes', 'No'], default='Unknown')
    
    # extract licence information for each pair of connected switches
    trunking_lic_columns = [*isl_group_columns, 'Trunking_lic_both_switches']
    switches_trunking_lic_df = isl_aggregated_modified_df[trunking_lic_columns].copy()
    switches_trunking_lic_df.drop_duplicates(inplace=True)
    
    # add Trunking_lic_both_switches column to isl_statistics_df DataFrame
    isl_statistics_df = isl_statistics_df.merge(switches_trunking_lic_df, how='left', on=isl_group_columns)
    
    return isl_statistics_df


def sort_isl(isl_statistics_df):
    """Function to sort so that ISL information about the same link (from both switches) appear in a adjacent rows"""

    """Create two DataFrames as copy from isl_statistics_df and add two sort columns (sort_column_1, sort_column_2) 
    containing switcname and connected switchname values. 
    First DataFrame isl_statistics_switch_df as sort columns has values from switchWwn and Connected_switchWwn columns respectively.
    Second DaraFrame isl_statistics_connected_switch_df is vice versa has Connected_switchWwn and switchWwn 
    as sort_column_2 and sort_column_1 respecively."""
    
    # direct sort columns
    isl_statistics_switch_df = isl_statistics_df.copy()
    isl_statistics_switch_df['sort_column_1'] = isl_statistics_switch_df['switchWwn']
    isl_statistics_switch_df['sort_column_2'] = isl_statistics_switch_df['Connected_switchWwn']

    # vice versa sort columns
    isl_statistics_connected_switch_df = isl_statistics_df.copy()
    isl_statistics_connected_switch_df['sort_column_1'] = isl_statistics_connected_switch_df['Connected_switchWwn']
    isl_statistics_connected_switch_df['sort_column_2'] = isl_statistics_connected_switch_df['switchWwn']
    
    """Vertically concatenate both DataFrames with direct and vice versa sort columns.
    After sorting columns on sort_column_1 and sort_column_2 connection details of the same pair of switches 
    from both sides of connection will appear in adjacent rows with the connection details for one side
    from direct sort columns DataFrame and for other side from the vice versa sort columns DataFrame.
    When drop duplicates based on switchWwn and Connected_switchWwn second encountered duplicated connection details row is dropped
    thus leaving original rows with connection details from both sides of pair of swithes in case if there are configuration data
    from both pair of switches or from one side only in case of front domain switch presence or if there is configuration data from
    one switch of connection pair present only"""
    isl_statistics_sort_df = pd.concat([isl_statistics_switch_df, isl_statistics_connected_switch_df])
    isl_statistics_sort_df.sort_values(by=['Fabric_name', 'Fabric_label', 'sort_column_1', 'sort_column_2'], inplace=True)
    isl_statistics_sort_df.drop_duplicates(subset=['Fabric_name', 'Fabric_label',  'switchWwn',  'Connected_switchWwn'], inplace=True)
    
    isl_statistics_sort_df.reset_index(inplace=True, drop=True)
    isl_statistics_df = isl_statistics_sort_df.copy()
    
    return isl_statistics_df
    

def isl_statistics_summary(isl_statistics_df, report_data_lst):
    """Function to count ISL statistics summary for fabric_label and fabric_name levels.
    Total Bandwidwidth counts for link. Others statistics for ports (for both switches of ISL link)"""

    # columns grouping is performed on to count summary statistics
    grp_columns = ['Fabric_name', 'Fabric_label']
    # columns to calculate port summary (sum function for corresponding level)
    port_summary_columns = isl_statistics_df.columns.tolist()
    
    """isl_statistics_df DataFrame contains links bandwidth for both sides of pair of switches
    thus sum of links bandwidth give wrong total ISL bandwidth result.
    It is required to use different approach to count total ISL bandwidth"""
    
    port_summary_columns.remove('Bandwidth_Gbps')
    # count sum statistics for each fabric
    isl_port_total_df = count_total(isl_statistics_df, grp_columns.copy(), port_summary_columns, sum)
    

    """In order to count total ISL bandwidth it is required to allocate unique ISLs only. 
    Since for each connection details of a pair of switches sort_column_1 and sort_column_2 values are identical
    it is enough to drop duplicates by both sorting columns thus leaving unique links only""" 
    
    unique_isl_columns = ['Fabric_name', 'Fabric_label',  'sort_column_1', 'sort_column_2']                 
    unique_isl_statistics_df = isl_statistics_df.drop_duplicates(subset=unique_isl_columns).copy()
    # count sum bandwidth statistics for each fabric
    unique_isl_statistics_df['Bandwidth_Gbps'] = \
        unique_isl_statistics_df['Bandwidth_Gbps'].astype('int64', errors='ignore')
    unique_isl_bandwidth_total_df = count_total(unique_isl_statistics_df, grp_columns.copy(), 'Bandwidth_Gbps', sum)
    
    # switch_quantity summary
    switch_quantity_total_df = count_total(isl_statistics_df, grp_columns.copy(), 'SwitchName', fn='nunique')
    switch_quantity_total_df.rename(columns={'SwitchName': 'Switch_quantity'}, inplace=True)
    
    # merge ISL ports, switch quantity and ISL bandwidth statistics
    isl_statistics_summary_df = isl_port_total_df.merge(switch_quantity_total_df, how='left', on=grp_columns)
    isl_statistics_summary_df = isl_statistics_summary_df.merge(unique_isl_bandwidth_total_df, how='left', on=grp_columns)
    
    return isl_statistics_summary_df
    

def verify_isl_symmetry(isl_statistics_summary_df):
    """Function to verify if ISLs are symmetric in each Fabrics_name from 'Switch_quantity', 'Port_quantity' 
    and 'Bandwidth_Gbps point of view. Function adds Assysmetric_note to isl_statistics_summary_df.
    Column contains parameter name(s) for which ISL symmetry condition is not fullfilled"""

    isl_symmetry_columns = ['Switch_quantity', 'Port_quantity', 'Bandwidth_Gbps']
    # drop fabric summary rows (rows with empty Fabric_label), group values for each Fabric_name
    # and find number of unique values in isl_symmetry_columns
    isl_symmetry_df = isl_statistics_summary_df.dropna(subset=['Fabric_label']).groupby(by='Fabric_name')[isl_symmetry_columns].agg('nunique')
    # temporary ineqaulity_notes columns for  isl_symmetry_columns
    isl_symmetry_notes = [column + '_ineqaulity' for column in isl_symmetry_columns]
    for column, column_note in zip(isl_symmetry_columns, isl_symmetry_notes):
        isl_symmetry_df[column_note] = np.nan
        # if fabrics are symmetric then number of unique values in groups should be equal to one 
        mask_values_nonuniformity = isl_symmetry_df[column] == 1
        # use current column name as value in column_note for rows where number of unique values exceeds one 
        isl_symmetry_df[column_note].where(mask_values_nonuniformity, column.lower(), inplace=True)
        
    # merge temporary ineqaulity_notes columns to Asymmetry_note column and drop temporary columns
    isl_symmetry_df = сoncatenate_columns(isl_symmetry_df, summary_column='Asymmetry_note', merge_columns=isl_symmetry_notes)
    # drop columns with quantity of unique values
    isl_symmetry_df.drop(columns=isl_symmetry_columns, inplace=True)
    # add Asymmetry_note column to isl_statistics_summary_df
    isl_statistics_summary_df = isl_statistics_summary_df.merge(isl_symmetry_df, how='left', on=['Fabric_name'])
    
    return isl_statistics_summary_df


def count_all_row(isl_statistics_summary_df):
    """Function to count row with index All containing total values of isl_statistics_summary_df
    for all fabrics"""
    
    # extract row containing total values for Fabric_name
    mask_empty_fabric_label = isl_statistics_summary_df['Fabric_label'].isna()
    isl_statistics_total_df = isl_statistics_summary_df.loc[mask_empty_fabric_label].copy()
    # sum values
    isl_statistics_total_df.loc['All']= isl_statistics_total_df.sum(numeric_only=True, axis=0)
    # rename Fabric_name to All
    isl_statistics_total_df.loc['All', 'Fabric_name'] = 'All'
    
    # drop all rows except 'All'
    mask_fabric_name_all = isl_statistics_total_df['Fabric_name'] == 'All'
    isl_statistics_total_df = isl_statistics_total_df.loc[mask_fabric_name_all].copy()
    
    # drop Assymetry_note column
    isl_statistics_total_df.drop(columns=['Asymmetry_note'], inplace=True)
    isl_statistics_total_df.reset_index(inplace=True, drop=True)
    
    return isl_statistics_total_df
    
