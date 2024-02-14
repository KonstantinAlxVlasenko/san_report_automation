"""Module filter messages which occure more than RASLOG_REPEATER_THRESHOLD a month during RASLOG_PERIOD"""

import numpy as np
import pandas as pd

import utilities.dataframe_operations as dfop
from san_automation_constants import RASLOG_PERIOD, RASLOG_REPEATER_THRESHOLD


def errdump_statistics(errdump_aggregated_df, raslog_message_details_df, raslog_message_id_details_df):
    """Function to count how many times log message appears during one month for the last six months.
    Log messages that appear less than 3 times a month are droppped"""
    
    # construct columns names for Source and Destination devices
    sid_did_base = ['switchName' , 'Index_slot_port', 'portState', 'portType', 'speed', 
                    'Connected_portWwn', 'Device_Host_Name_Port', 'alias', 'deviceType']
    sid_columns = ['sid_' + column for column in sid_did_base]
    did_columns = ['did_' + column for column in sid_did_base]
    sid_did_device_columns = sid_columns + did_columns

    connected_device_columns = ['Connected_portId', 'Connected_portWwn', 'Device_Host_Name_Port', 'alias', 'deviceType']
    message_quantity_columns = ['Message_occured_quantity_unique', 'Message_occured_quantity_multiple']

    # filter out log messages older than RASLOG_PERIOD and 'ignore' tag messages
    errdump_filtered_df = errdump_filter(errdump_aggregated_df)
    # joind devices for the same message_id and behind the same port
    errdump_filtered_df = join_message_devices(errdump_filtered_df, connected_device_columns, sid_did_device_columns)
    # add extracted message repeater values for the message id
    errdump_filtered_df = add_message_repeater(errdump_filtered_df, errdump_aggregated_df, message_quantity_columns)
    # count message appearance for the same device for each month durng RASLOG_PERIOD
    raslog_counter_df = count_month_message_occurrence(errdump_filtered_df, connected_device_columns, 
                                                    sid_did_device_columns, message_quantity_columns)
    # sort raslog message occurrance by month and occurrance quantity
    raslog_counter_df = sort_raslog_counter(raslog_counter_df)
    # add message details and recommended actions based on message condition
    raslog_counter_df = dfop.dataframe_fillna(raslog_counter_df, raslog_message_details_df, join_lst=['Condition'], 
                                            filled_lst=['Details',  'Recommended_action']) 
    # add message details and recommended actions based on message_id
    raslog_counter_df =  dfop.dataframe_fillna(raslog_counter_df, raslog_message_id_details_df, join_lst=['Message_ID'], 
                                            filled_lst=['Details',  'Recommended_action']) 
    # filter off messages which occur less then RASLOG_REPEATER_THRESHOLD times a month and INFO severity messages
    raslog_frequent_df = raslog_counter_filter(raslog_counter_df)
    return raslog_counter_df, raslog_frequent_df


def count_month_message_occurrence(errdump_filtered_df, connected_device_columns, 
                                    sid_did_device_columns, message_quantity_columns):
    """Function to count message occurance for the same device for each month durng RASLOG_PERIOD """

    # set message date and time as index
    errdump_filtered_df.set_index(errdump_filtered_df['Message_date'], inplace=True)
    errdump_filtered_df.drop(columns=['Message_date'], inplace=True)
        
    errdump_grp_columns = ['configname', 'chassis_name', 'chassis_wwn',
                           'switchName', 'switchWwn',
                           'Fabric_name', 'Fabric_label', 'config_collection_date',
                           'Message_ID', 'Severity', 'Message_portIndex', 'Message_portType','slot', 'port',
                           'Condition', 'Dashboard_category', 'obj', 'Message_status',
                           'portIndex', 'Index_slot_port', 'portType', 'portState', 'speed',
                           'tx_port', 'rx_port', 'sid', 'did', 'wwn', 'IP_Address']

    # group log messages by month, device and message and sum occurance values
    errdump_grouper = errdump_filtered_df.groupby([pd.Grouper(freq='M', kind='period'), 
                                                   *(errdump_grp_columns + connected_device_columns + sid_did_device_columns
                                                     )])[message_quantity_columns].sum()
    raslog_counter_df = errdump_grouper.copy()
    raslog_counter_df.reset_index(inplace=True)
    return raslog_counter_df


def sort_raslog_counter(raslog_counter_df):
    """Function to remove 'na_cell' values and 
    sort raslog message occurrance by month and occurrance quantity"""

    # clean 'na_cell' values 
    raslog_counter_df.replace({'na_cell': np.nan}, inplace=True)
    if raslog_counter_df['alias'].notna().any():
        raslog_counter_df['alias'].replace('na_cell(?:, )?', value='', regex=True, inplace=True)
        raslog_counter_df['alias'] = raslog_counter_df['alias'].str.rstrip(', ')

    # apply date format to remove day and time
    raslog_counter_df['config_collection_date'] =  pd.to_datetime(raslog_counter_df['config_collection_date']).dt.date
    raslog_counter_df['Message_date'] = raslog_counter_df['Message_date'].dt.strftime('%Y-%m')
    # sort values
    errdump_sort_columns = ['chassis_name', 'Fabric_label', 'Fabric_name', 'switchName', 'Message_date', 'Message_occured_quantity_multiple']
    raslog_counter_df.sort_values(by=errdump_sort_columns, ascending=[*[True]*5, False], inplace=True)
    raslog_counter_df.reset_index(drop=True, inplace=True)    
    # drop columns with all empty empty cells
    raslog_counter_df.dropna(axis=1, how='all', inplace=True)
    return raslog_counter_df


def raslog_counter_filter(raslog_counter_df):
    """Function to filter off messages which occur less then RASLOG_REPEATER_THRESHOLD times a month,
    messages with INFO severity"""

    # find log messages which appear more then three times a month
    mask_frequent = raslog_counter_df['Message_occured_quantity_multiple'] > RASLOG_REPEATER_THRESHOLD
    raslog_frequent_df = raslog_counter_df.loc[mask_frequent].copy()
    
    # remove INFO Messages for report DataFrame except security violation, clock issue and frame detected messages
    mask_not_info = raslog_frequent_df['Severity'] != 'INFO'
    mask_sec_violation_condition = raslog_frequent_df['Condition'].str.contains('security violation', case=False, na=False)
    mask_sec_violation_dashboard = raslog_frequent_df['Dashboard_category'].str.contains('security violation', case=False, na=False)
    mask_clock_server_rplcmnt = raslog_frequent_df['Condition'].str.contains('used instead of', case=False, na=False)
    mask_frame_detected = raslog_frequent_df['Condition'].str.contains('frame.+detected', case=False, na=False, regex=True)
    mask_message_filter = mask_not_info | mask_sec_violation_condition | mask_sec_violation_dashboard | mask_clock_server_rplcmnt | mask_frame_detected
    # filter messages
    raslog_frequent_df = raslog_frequent_df.loc[mask_message_filter].copy()
    raslog_frequent_df.reset_index(drop=True, inplace=True)
    return raslog_frequent_df


def errdump_filter(errdump_aggregated_df):
    """Function to filter log messages based on date (only log messages with a period of RASLOG_PERIOD 
    (6 month by default) prior to the collection date are counted), message tag (messages with ignore tag should be filtered out)"""

    # mask exclude logs that older than 6 months from collection date
    mask_period = errdump_aggregated_df['Message_date'] >= \
        errdump_aggregated_df['config_collection_date'] - pd.DateOffset(months=RASLOG_PERIOD)
    # mask exclude igonored messages
    mask_not_ignored = errdump_aggregated_df['Message_status'] != 'ignored'
    errdump_filtered_df = errdump_aggregated_df.loc[mask_period & mask_not_ignored].copy()
    return errdump_filtered_df



def join_message_devices(errdump_filtered_df, connected_device_columns, sid_did_device_columns):
    """All device names behind the same port with the same message external_sequence_number and message datatime are joined"""

    errdump_grp_columns = ['configname', 'chassis_name', 'chassis_wwn', 
                           'Message_date', 'Message_ID', 'External_sequence_number',
                           'Security_audit_flag', 'Severity',
                           'switchName', 'Message', 'switchWwn', 'Fabric_name', 'Fabric_label',
                           'config_collection_date', 'Message_portIndex', 'Message_portType',
                           'slot', 'port', 'Condition', 'Current_value', 'Dashboard_category',
                           'obj', 'Message_status', 'portIndex', 'Index_slot_port', 'portType', 'portState',
                           'speed', 'tx_port', 'rx_port', 'sid', 'did', 'wwn', 'IP_Address']
                           
    errdump_filtered_df = errdump_filtered_df.reindex(columns=errdump_grp_columns + connected_device_columns + sid_did_device_columns)
    
    # fill nan values with 'na_cell'
    for column in errdump_filtered_df.columns:
        if errdump_filtered_df[column].isna().any():
            errdump_filtered_df[column] = errdump_filtered_df[column].fillna('na_cell')

    # join values in connected_device_columns and sid_did_device_columns for the same values in errdump_grp_columns
    errdump_filtered_df = errdump_filtered_df.groupby(by=errdump_grp_columns).agg(', '.join)
    errdump_filtered_df.reset_index(inplace=True)
    # remove duplicated values from connected_device_columns and sid_did_device_columns columns
    for column in connected_device_columns + sid_did_device_columns:
        errdump_filtered_df[column] = errdump_filtered_df[column].str.split(', ').apply(set).str.join(', ')
    return errdump_filtered_df


def add_message_repeater(errdump_filtered_df, errdump_aggregated_df, message_quantity_columns):
    """Function to add message repeater value and message triggered value for the message external_sequence_number"""

    # columns identifiying unique message in the log
    message_grp_columns = ['configname', 'chassis_name', 'chassis_wwn', 
                           'Message_date', 'Message_ID', 'External_sequence_number',
                           'Security_audit_flag', 'Severity','Message']

    errdump_filtered_df = dfop.dataframe_fillna(errdump_filtered_df, errdump_aggregated_df, join_lst=message_grp_columns, 
                                                filled_lst=message_quantity_columns, remove_duplicates=True)
    return errdump_filtered_df



# def errdump_filter(errdump_aggregated_df, connected_device_columns, sid_did_device_columns, message_quantity_columns):
#     """Function to filter log messages based on date (only log messages with a period of RASLOG_PERIOD (6 month by default)
#     prior to the collection date are counted), message tag (messages with ignore tag should be filtered out).
#     All device names behind one port with the same event and message datatime are joined. 
#     Function process errdump_aggregate_df to prepare for statistics
#     counting"""

#     # mask exclude logs that older than 6 months from collection date
#     mask_period = errdump_aggregated_df['Message_date'] >= \
#         errdump_aggregated_df['config_collection_date'] - pd.DateOffset(months=RASLOG_PERIOD)
#     # mask exclude igonored messages
#     mask_not_ignored = errdump_aggregated_df['Message_status'] != 'ignored'
#     errdump_filtered_df = errdump_aggregated_df.loc[mask_period & mask_not_ignored].copy()

#     # External_sequence_number is removed to group devices with equal messages behind the same port
    
#     # errdump_grp_columns = ['configname', 'chassis_name', 'chassis_wwn', 
#     #                        'Message_date', 'Message_ID', 
#     #                        'Security audit flag', 'Severity',
#     #                        'switchName', 'switchWwn', 'Fabric_name', 'Fabric_label',
#     #                        'config_collection_date', 'Message_portIndex', 'Message_portType',
#     #                        'slot', 'port', 'Condition', 'Current_value', 'Dashboard_category',
#     #                        'obj', 'Message_status', 'portIndex', 'Index_slot_port', 'portType', 'portState',
#     #                        'speed', 'tx_port', 'rx_port', 'sid', 'did', 'wwn', 'IP_Address',
#     #                        'Connected_portId', 'Connected_portWwn', 'Device_Host_Name_Port', 'alias', 'deviceType']
    

#     # External_sequence_number is back
#     errdump_grp_columns = ['configname', 'chassis_name', 'chassis_wwn', 
#                            'Message_date', 'Message_ID', 'External_sequence_number',
#                            'Security_audit_flag', 'Severity',
#                            'switchName', 'Message', 'switchWwn', 'Fabric_name', 'Fabric_label',
#                            'config_collection_date', 'Message_portIndex', 'Message_portType',
#                            'slot', 'port', 'Condition', 'Current_value', 'Dashboard_category',
#                            'obj', 'Message_status', 'portIndex', 'Index_slot_port', 'portType', 'portState',
#                            'speed', 'tx_port', 'rx_port', 'sid', 'did', 'wwn', 'IP_Address']
                           
    
#     errdump_filtered_df = errdump_filtered_df.reindex(columns=errdump_grp_columns + connected_device_columns + sid_did_device_columns)
    
#     # join multiple Device_Host_Name_Ports behind one port
#     # groupby drop rows with nan values
#     for column in errdump_filtered_df.columns:
#         if errdump_filtered_df[column].isna().any():
#             errdump_filtered_df[column] = errdump_filtered_df[column].fillna('na_cell')
#     # errdump_filtered_df.fillna('na_cell', inplace=True) # depricated method
#     # errdump_filtered_df = errdump_filtered_df.groupby(by=errdump_grp_columns[:-5]).agg(', '.join)
#     errdump_filtered_df = errdump_filtered_df.groupby(by=errdump_grp_columns).agg(', '.join)
#     errdump_filtered_df.reset_index(inplace=True)
    
#     """remove duplicate values from Connected_portId and Device_Host_Name_Port cells
#     thus all identical events for the device behind the single port with idenitical Message_date
#     considered to be one event"""
#     # for column in ['Connected_portId', 'Connected_portWwn', 'Device_Host_Name_Port', 'alias', 'deviceType']:
#     for column in connected_device_columns + sid_did_device_columns:
#         errdump_filtered_df[column] = errdump_filtered_df[column].str.split(', ').apply(set).str.join(', ')

#     # add counter for each message ID extracted from triggered value and messgae repeat value
#     message_grp_columns = ['configname', 'chassis_name', 'chassis_wwn', 
#                            'Message_date', 'Message_ID', 'External_sequence_number',
#                            'Security_audit_flag', 'Severity','Message']

#     errdump_filtered_df = dfop.dataframe_fillna(errdump_filtered_df, errdump_aggregated_df, join_lst=message_grp_columns, 
#                                                 filled_lst=message_quantity_columns, remove_duplicates=True)

#     return errdump_filtered_df




# def errdump_statistics(errdump_aggregated_df, raslog_message_details_df, raslog_message_id_details_df):
#     """Function to count how many times log message appears during one month for the last six months.
#     Log messages that appear less than 3 times a month are droppped"""
    
#     # construct columns names for Source and Destination devices
#     sid_did_base = ['switchName' , 'Index_slot_port', 'portState', 'portType', 'speed', 
#                     'Connected_portWwn', 'Device_Host_Name_Port', 'alias', 'deviceType']
#     sid_columns = ['sid_' + column for column in sid_did_base]
#     did_columns = ['did_' + column for column in sid_did_base]
#     sid_did_device_columns = sid_columns + did_columns

#     connected_device_columns = ['Connected_portId', 'Connected_portWwn', 'Device_Host_Name_Port', 'alias', 'deviceType']
#     message_quantity_columns = ['Message_occured_quantity_unique', 'Message_occured_quantity_multiple']


#     # filter log messages based on date, message tag and merge devices behind same port for single event
#     errdump_filtered_df = errdump_filter(errdump_aggregated_df, connected_device_columns, sid_did_device_columns, message_quantity_columns)

#     # set message date and time as index
#     errdump_filtered_df.set_index(errdump_filtered_df['Message_date'], inplace=True)
#     errdump_filtered_df.drop(columns=['Message_date'], inplace=True)
        
#     errdump_grp_columns = ['configname', 'chassis_name', 'chassis_wwn',
#                            'switchName', 'switchWwn',
#                            'Fabric_name', 'Fabric_label', 'config_collection_date',
#                            'Message_ID', 'Severity', 'Message_portIndex', 'Message_portType','slot', 'port',
#                            'Condition', 'Dashboard_category', 'obj', 'Message_status',
#                            'portIndex', 'Index_slot_port', 'portType', 'portState', 'speed',
#                            'tx_port', 'rx_port', 'sid', 'did', 'wwn', 'IP_Address']

    
#     # group log messages by month, device and log message
#     errdump_grouper = errdump_filtered_df.groupby([pd.Grouper(freq='M', kind='period'), 
#                                                    *(errdump_grp_columns + connected_device_columns + sid_did_device_columns
#                                                      )])[message_quantity_columns].sum()



#     # count events in each group
#     # raslog_counter_sr = errdump_grouper['Condition'].count()
#     # raslog_counter_df = pd.DataFrame(raslog_counter_sr)


#     # raslog_counter_df = pd.DataFrame(errdump_grouper)
#     raslog_counter_df = errdump_grouper.copy()

#     # rename column Condition to avoid duplication with one of Indexies
#     # raslog_counter_df.rename(columns={'Condition': 'Quantity'}, inplace=True)
#     raslog_counter_df.reset_index(inplace=True)
    
    
    
#     # replace na_cell
#     raslog_counter_df.replace({'na_cell': np.nan}, inplace=True)
#     if raslog_counter_df['alias'].notna().any():
#         raslog_counter_df['alias'].replace('na_cell(?:, )?', value='', regex=True, inplace=True)
#         raslog_counter_df['alias'] = raslog_counter_df['alias'].str.rstrip(', ')

#     # apply date format to remove day and time
#     raslog_counter_df['config_collection_date'] =  pd.to_datetime(raslog_counter_df['config_collection_date']).dt.date
#     raslog_counter_df['Message_date'] = raslog_counter_df['Message_date'].dt.strftime('%Y-%m')
#     # sort values
#     errdump_sort_columns = ['chassis_name', 'Fabric_label', 'Fabric_name', 'switchName', 'Message_date', 'Message_occured_quantity_multiple']
#     raslog_counter_df.sort_values(by=errdump_sort_columns, ascending=[*[True]*5, False], inplace=True)
#     raslog_counter_df.reset_index(drop=True, inplace=True)    
#     # drop columns with all empty empty cells
#     raslog_counter_df.dropna(axis=1, how='all', inplace=True)

#     raslog_counter_df = dfop.dataframe_fillna(raslog_counter_df, raslog_message_details_df, join_lst=['Condition'], 
#                                             filled_lst=['Details',  'Recommended_action']) 

#     raslog_counter_df =  dfop.dataframe_fillna(raslog_counter_df, raslog_message_id_details_df, join_lst=['Message_ID'], 
#                                             filled_lst=['Details',  'Recommended_action']) 
    
#     # find log messages which appear more then three times a month
#     mask_frequent = raslog_counter_df['Message_occured_quantity_multiple'] > 3
#     raslog_frequent_df = raslog_counter_df.loc[mask_frequent].copy()
    
#     # remove INFO Messages for report DataFrame except security violations messages
#     mask_not_info = raslog_frequent_df['Severity'] != 'INFO'
#     mask_sec_violation_condition = raslog_frequent_df['Condition'].str.contains('security violation', case=False, na=False)
#     mask_sec_violation_dashboard = raslog_frequent_df['Dashboard_category'].str.contains('security violation', case=False, na=False)
#     raslog_frequent_df = raslog_frequent_df.loc[mask_not_info | mask_sec_violation_condition | mask_sec_violation_dashboard].copy()

#     raslog_frequent_df.reset_index(drop=True, inplace=True)
#     return raslog_counter_df, raslog_frequent_df