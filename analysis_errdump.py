
"""Module to parse errdump log messages and find events which frequenly appeared (more then 3 times per month) 
within a period of six month prior to the switch configuration collection date """

import re
import warnings

import numpy as np
import pandas as pd

from common_operations_dataframe import (dataframe_fabric_labeling,
                                         dataframe_fillna,
                                         dataframe_segmentation)
from common_operations_filesystem import load_data, save_data, save_xlsx_file
from common_operations_miscellaneous import (force_extract_check, status_info,
                                             verify_data, verify_force_run)
from common_operations_servicefile import data_extract_objects


def errdump_main(errdump_df, switchshow_df, switch_params_aggregated_df, portshow_aggregated_df, report_columns_usage_dct, report_data_lst):
    """Main function to get most frequently appeared log messages"""
    
   # report_data_lst contains information: 
   # customer_name, dir_report, dir to save obtained data, max_title, report_steps_dct
    *_, max_title, report_steps_dct = report_data_lst

    # names to save data obtained after current module execution
    data_names = ['errdump_aggregated', 'raslog_counter', 'Журнал']
    # service step information
    print(f'\n\n{report_steps_dct[data_names[0]][3]}\n')
    
    # loading data if were saved on previous iterations 
    data_lst = load_data(report_data_lst, *data_names)
    # unpacking DataFrames from the loaded list with data
    # pylint: disable=unbalanced-tuple-unpacking
    errdump_aggregated_df, raslog_counter_df, raslog_report_df = data_lst

    # list of data to analyze from report_info table
    analyzed_data_names = [ 'chassis_parameters', 'switch_parameters', 'switchshow_ports', 
                            'maps_parameters', 'portshow_aggregated', 'fabric_labels']

    # force run when any data from data_lst was not saved (file not found) or 
    # procedure execution explicitly requested for output data or data used during fn execution  
    force_run = verify_force_run(data_names, data_lst, report_steps_dct, 
                                            max_title, analyzed_data_names)
    if force_run:
        # data imported from init file (regular expression patterns) to extract values from data columns
        # re_pattern list contains comp_keys, match_keys, comp_dct    
        _, _, *re_pattern_lst = data_extract_objects('raslog', max_title)

        # current operation information string
        info = f'Counting RASLog messages'
        print(info, end =" ")

        # get aggregated DataFrames
        errdump_aggregated_df, raslog_counter_df, raslog_frequent_df = \
            errdump_aggregated(errdump_df, switchshow_df, switch_params_aggregated_df, portshow_aggregated_df, re_pattern_lst)
        # after finish display status
        status_info('ok', max_title, len(info))      

        # partition aggregated DataFrame to required tables
        raslog_report_df = raslog_report(raslog_frequent_df, data_names, report_columns_usage_dct, max_title)

        # create list with partitioned DataFrames
        data_lst = [errdump_aggregated_df, raslog_counter_df, raslog_report_df]
        # saving fabric_statistics and fabric_statistics_summary DataFrames to csv file
        save_data(report_data_lst, data_names, *data_lst)
    # verify if loaded data is empty and replace information string with empty DataFrame
    else:
        errdump_aggregated_df, raslog_counter_df, raslog_report_df = \
            verify_data(report_data_lst, data_names, *data_lst)
        data_lst = [errdump_aggregated_df, raslog_counter_df, raslog_report_df]
    # save data to service file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        save_xlsx_file(data_frame, data_name, report_data_lst)

    return errdump_aggregated_df, raslog_counter_df


def errdump_aggregated(errdump_df, switchshow_df, switch_params_aggregated_df, portshow_aggregated_df, re_pattern_lst):
    """Function to label errdump_df and count filtered messages"""
    
    # label errdumm_df, add switchWwn, datetime config collection
    errdump_aggregated_df = prior_preparation(errdump_df, switchshow_df, switch_params_aggregated_df)
    # extract values from Events descriprion (Event, slot, port etc) to corresponding columns
    errdump_aggregated_df = message_extract(errdump_aggregated_df, re_pattern_lst)
    # add port details and connected to port device information
    errdump_aggregated_df = errdump_portshow(errdump_aggregated_df, portshow_aggregated_df)
    # filter log messages based on date, message tag and merge devices behind same port for single event
    errdump_filtered_df = errdump_filter(errdump_aggregated_df)
    # count how many times event appears during one month for the last six months 
    raslog_counter_df, raslog_frequent_df = errdump_statistics(errdump_filtered_df)
    
    return errdump_aggregated_df, raslog_counter_df, raslog_frequent_df


def switchshow_join(errdump_df, switchshow_df):
    """Function to add switchWwn information to errdump_df DataFrame"""
    
    # columns labels reqiured for join operation
    switchshow_lst = ['configname', 'chassis_name', 'chassis_wwn', 'switchName', 'switchWwn']
    # create left DataFrame for join operation
    switchshow_join_df = switchshow_df.loc[:, switchshow_lst].copy()
    switchshow_join_df.drop_duplicates(inplace=True)
    # portshow_df and switchshow_join_df DataFrames join operation
    errdump_aggregated_df = errdump_df.merge(switchshow_join_df, how = 'left', on = switchshow_lst[:4])

    return errdump_aggregated_df


def prior_preparation(errdump_df, switchshow_df, switch_params_aggregated_df):
    """Function to add switchWwn information, date and time config was collected for each switch,
     fabric labeling and convert message dates and config collection dates to date format"""

    # add switchWwn information
    errdump_aggregated_df = switchshow_join(errdump_df, switchshow_df)
    # fabric labeling
    errdump_aggregated_df = dataframe_fabric_labeling(errdump_aggregated_df, switch_params_aggregated_df)
    # add config collection date
    errdump_aggregated_df = dataframe_fillna(errdump_aggregated_df, switch_params_aggregated_df, ['configname', 'chassis_name', 'chassis_wwn'], 
                                             ['config_collection_date'], drop_na=False)    
    # convert dates columns
    errdump_aggregated_df['Message_date'] = pd.to_datetime(errdump_aggregated_df['Message_date'])
    errdump_aggregated_df['config_collection_date'] = pd.to_datetime(errdump_aggregated_df['config_collection_date'])
    
    return errdump_aggregated_df


def message_extract(errdump_aggregated_df, re_pattern_lst):
    """Function to parse errdump Message column into corresponding set of columns and label each message
    with extracted, copied or ignored tag"""
    
    # regular expression patterns
    comp_keys, _, comp_dct = re_pattern_lst

    # list with regex pattern and columns values to be extracted to pairs
    extract_pattern_columns_lst = [
        [comp_dct['port_idx_slot_number'], ['Message_portIndex', 'Message_portType', 'slot', 'port']],
        [comp_dct['pid'], ['Message_portId']],
        [comp_dct['event_portidx'], ['Condition', 'Message_portIndex']],
        [comp_dct['event_slot_portidx'], ['Condition', 'slot', 'Message_portIndex']],
        [comp_dct['bottleneck_detected'], ['Condition', 'slot', 'port', 'Current_value']],
        [comp_dct['bottleneck_cleared'], ['slot', 'port', 'Condition']],
        [comp_dct['severe_bottleneck'], ['Condition', 'Message_portType', 'slot', 'port']],
        [comp_dct['maps_current_value'],  ['Condition', 'Current_value', 'Dashboard_category']],
        [comp_dct['maps_object'], ['Condition', 'obj']],
        [comp_dct['c2_message_1'], ['Condition', 'slot', 'port', 'Message_portIndex', 'Message_portId']],
        [comp_dct['slow_drain_device'], ['slot', 'port', 'Message_portIndex', 'Condition']],
        ]        
    
    # extract corresponding values if regex pattern applicable
    for pattern, extracted_columns in extract_pattern_columns_lst:
        # pattern contains groups but str.cotains used to identify mask
        # supress warning message
        warnings.filterwarnings("ignore", 'This pattern has match groups')
        mask = errdump_aggregated_df['Message'].str.contains(pattern, regex=True)
        errdump_aggregated_df.loc[mask, extracted_columns] = errdump_aggregated_df.loc[mask, 'Message'].str.extract(pattern).values
        
    # add empty columns if they were not extracted
    extracted_columns = [column for _, columns in extract_pattern_columns_lst for column in columns]
    extracted_columns = list(set(extracted_columns))
    add_empty_columns = [column for column in extracted_columns if not column in errdump_aggregated_df.columns]
    errdump_aggregated_df[add_empty_columns] = np.nan       
    
    # masks for labeleing messages with tags
    mask_condition_na = errdump_aggregated_df['Condition'].isna()
    mask_ignored_message = errdump_aggregated_df['Message_ID'].str.contains(comp_dct['ignore_message'], regex=True)
    # mark messages with extracted, ingnored or copied tag
    errdump_aggregated_df.loc[~mask_condition_na, 'Message_status'] = 'extracted'
    errdump_aggregated_df.loc[mask_ignored_message, 'Message_status'] = 'ignored'
    errdump_aggregated_df['Message_status'].fillna('copied', inplace=True)
    
    # copy messages which were not extracted but shouldn't be ignored to Condition column
    errdump_aggregated_df.loc[mask_condition_na & ~mask_ignored_message, 'Condition'] = \
        errdump_aggregated_df.loc[mask_condition_na & ~mask_ignored_message, 'Condition'].fillna(errdump_aggregated_df['Message'])
    
    return errdump_aggregated_df
    

def errdump_portshow(errdump_aggregated_df, portshow_aggregated_df):
    """Function to add port and connected device information to errdump_aggregated_df"""

    mask_device_name = portshow_aggregated_df['Device_Host_Name'].notna()
    # if Message_portIndex is present but port number is not then fillna slot and port number from portshow
    if (errdump_aggregated_df['Message_portIndex'].notna() & errdump_aggregated_df['port'].isna()).any():
        
        portshow_join_df = portshow_aggregated_df.loc[mask_device_name].copy()
        portshow_join_df.rename(columns={'portIndex': 'Message_portIndex'}, inplace=True)
        portshow_join_columns = ['configname', 'chassis_name', 'chassis_wwn', 'Message_portIndex']
        errdump_aggregated_df = dataframe_fillna(errdump_aggregated_df, portshow_join_df, 
                                                 join_lst=portshow_join_columns, 
                                                 filled_lst=['slot', 'port'], remove_duplicates=False, drop_na=False)

    # if port number present but not slot number then slot number is zero
    mask_slot_na = errdump_aggregated_df['slot'].isna()
    mask_portnumber =  errdump_aggregated_df['port'].notna()
    errdump_aggregated_df.loc[mask_portnumber & mask_slot_na, 'slot'] = \
        errdump_aggregated_df.loc[mask_portnumber & mask_slot_na, 'slot'].fillna('0')
        
    # add port and connected device information based chassis info, slot and port number
    portshow_columns = ['configname', 'chassis_name', 'chassis_wwn',
                          'slot', 'port', 'portIndex', 'Index_slot_port',
                          'portType', 'portState', 'speed',
                          'Connected_portId', 'Connected_portWwn',
                          'Device_Host_Name', 'Device_Port', 'Device_Location',
                          'deviceType', 'deviceSubtype']
    
    portshow_join_df = portshow_aggregated_df[portshow_columns].copy()
    if errdump_aggregated_df[['slot', 'port']].notna().all(axis=1).any():
        errdump_aggregated_df = errdump_aggregated_df.merge(portshow_join_df, how='left', on=portshow_columns[:5])
    
    # TO_REMOVE
    # errdump_aggregated_df = dataframe_fillna(errdump_aggregated_df, portshow_aggregated_df, portshow_columns[:5], portshow_columns[5:], remove_duplicates=False, drop_na=False)

    # add empty columns if there was no merge with portshow_join_df
    add_empty_columns = [column for column in portshow_columns[3:] if not column in errdump_aggregated_df.columns]
    errdump_aggregated_df[add_empty_columns] = np.nan   
    
    # add device information based in pid
    if errdump_aggregated_df['Message_portId'].notna().any():
        portshow_join_df = portshow_aggregated_df.loc[mask_device_name].copy()
        portshow_join_df['Message_portId'] = portshow_join_df['Connected_portId']
        errdump_aggregated_df = dataframe_fillna(errdump_aggregated_df, portshow_join_df, 
                                                join_lst=[*portshow_columns[:3], 'Message_portId'], 
                                                filled_lst=portshow_columns[3:], remove_duplicates=False, drop_na=False)

    # add fabric name and label for switches with chassis info, slot and port
    errdump_aggregated_df = \
        dataframe_fillna(errdump_aggregated_df, portshow_aggregated_df, portshow_columns[:5], ['Fabric_name', 'Fabric_label'])

    # concatenate devce name and device port columns
    errdump_aggregated_df['Device_Host_Name_Port'] = errdump_aggregated_df[['Device_Host_Name', 'Device_Port']].stack().groupby(level=0).agg(' '.join)
    
    return errdump_aggregated_df


def errdump_filter(errdump_aggregated_df):
    """Function to filter log messages based on date (only log messages with a period of six month
    prior to the collection date are counted), message tag (messages with ignore tag should be filtered out).
    All device names behind one port with the same event and message datatime are joined. 
    Function process errdump_aggregate_df to prepare for statistics
    counting"""

    # mask exclude logs that older than 6 months from collection date
    mask_period = errdump_aggregated_df['Message_date'] >= \
        errdump_aggregated_df['config_collection_date'] - pd.DateOffset(months=6)
    # mask exclude igonored messages
    mask_not_ignored = errdump_aggregated_df['Message_status'] != 'ignored'
    errdump_filtered_df = errdump_aggregated_df.loc[mask_period & mask_not_ignored].copy()
    
    # join multiple Device_Host_Name_Ports behinde one port
    # groupby drop rows with nan values
    errdump_filtered_df.fillna('na_cell', inplace=True)
    
    # External_sequence_number is removed to group devices with equal messages behind the same port
    errdump_grp_columns = ['configname', 'chassis_name', 'chassis_wwn', 
                           'Message_date', 'Message_ID', 
                           'Security audit flag', 'Severity',
                           'switchName', 'switchWwn', 'Fabric_name', 'Fabric_label',
                           'config_collection_date', 'Message_portIndex', 'Message_portType',
                           'slot', 'port', 'Condition', 'Current_value', 'Dashboard_category',
                           'obj', 'Message_status', 'portIndex', 'Index_slot_port', 'portType', 'portState',
                           'speed', 'Connected_portId', 'Connected_portWwn', 'Device_Host_Name_Port', 'deviceType']
    
    errdump_filtered_df = errdump_filtered_df[errdump_grp_columns].copy()
    errdump_filtered_df = errdump_filtered_df.groupby(by=errdump_grp_columns[:-4]).agg(', '.join)
    errdump_filtered_df.reset_index(inplace=True)
    """remove duplicate values from Connected_portId and Device_Host_Name_Port cells
    thus all identical events for the device behind the single port with idenitical Message_date
    considered to be one event"""
    for column in ['Connected_portId', 'Connected_portWwn', 'Device_Host_Name_Port', 'deviceType']:
        errdump_filtered_df[column] = errdump_filtered_df[column].str.split(', ').apply(set).str.join(', ')
    
    return errdump_filtered_df


def errdump_statistics(errdump_filtered_df):
    """Function to count how many times log message appears during one month for the last six months.
    Log messages that appear less than 3 times a month are droppped"""
    
    # set message date and time as index
    errdump_filtered_df.set_index(errdump_filtered_df['Message_date'], inplace=True)
    errdump_filtered_df.drop(columns=['Message_date'], inplace=True)
    
    # grouping and message counting
    errdump_grp_columns = ['configname', 'chassis_name', 'chassis_wwn',
                           'switchName', 'switchWwn',
                           'Fabric_name', 'Fabric_label', 'config_collection_date',
                           'Message_ID', 'Severity', 'Message_portIndex', 'Message_portType','slot', 'port',
                           'Condition', 'Dashboard_category', 'obj', 'Message_status',
                           'portIndex', 'Index_slot_port', 'portType', 'portState', 'speed', 
                           'Connected_portId', 'Connected_portWwn', 'Device_Host_Name_Port', 'deviceType']
    
    # group log messages by month, device and log message
    errdump_grouper = errdump_filtered_df.groupby([pd.Grouper(freq='M', kind='period'), *errdump_grp_columns])
    # count events in each group
    raslog_counter_sr = errdump_grouper['Condition'].count()
    raslog_counter_df = pd.DataFrame(raslog_counter_sr)
    # rename column Condition to avoid duplication with one of Indexies
    raslog_counter_df.rename(columns={'Condition': 'Quantity'}, inplace=True)
    raslog_counter_df.reset_index(inplace=True)
    # replace na_cell
    raslog_counter_df.replace({'na_cell': np.nan}, inplace=True)

    # apply date format to remove day and time
    raslog_counter_df['config_collection_date'] =  pd.to_datetime(raslog_counter_df['config_collection_date']).dt.date
    raslog_counter_df['Message_date'] = raslog_counter_df['Message_date'].dt.strftime('%Y-%m')
    # sort values
    errdump_sort_columns = ['chassis_name', 'Fabric_label', 'Fabric_name', 'switchName', 'Message_date', 'Quantity']
    raslog_counter_df.sort_values(by=errdump_sort_columns, ascending=[*[True]*5, False], inplace=True)
    raslog_counter_df.reset_index(drop=True, inplace=True)    
    # drop columns with all empty empty cells
    raslog_counter_df.dropna(axis=1, how='all', inplace=True)
    
    # find log messages which appear more then three times a month
    mask_frequent = raslog_counter_df['Quantity'] > 3
    raslog_frequent_df = raslog_counter_df.loc[mask_frequent].copy()
    
    # remove INFO Messages for report DataFrame
    mask_not_info = raslog_frequent_df['Severity'] != 'INFO'
    raslog_frequent_df = raslog_frequent_df.loc[mask_not_info].copy()

    raslog_frequent_df.reset_index(drop=True, inplace=True)    
    
    return raslog_counter_df, raslog_frequent_df


def raslog_report(raslog_frequent_df, data_names, report_columns_usage_dct, max_title):
    """Function to check if it is required to use chassis_name columns. RASLog sometimes uses it's own
    chname not equal to switchname or chassis name thus it's better to keep default chassis names
    for visibility even if it was allowed to drop chassiss_name column before"""

    # make copy of default report_columns_usage_dct in order to avoid change it
    report_columns_usage_upd_dct = report_columns_usage_dct.copy()
    chassis_column_usage = report_columns_usage_upd_dct['chassis_info_usage']

    # if chassis_name column to be dropped
    if not chassis_column_usage:
        # if all switchnames and chassis names are not identical
        if not all(raslog_frequent_df.chassis_name == raslog_frequent_df.switchName):
            # change keep chassis_name column tag to True 
            report_columns_usage_upd_dct['chassis_info_usage'] = True

    raslog_report_df, = dataframe_segmentation(raslog_frequent_df, [data_names[2]], report_columns_usage_upd_dct, max_title)

    return raslog_report_df

        

