
"""Module to parse errdump log messages and find events which frequenly appeared (more then 3 times per month) 
within a period of six month prior to the switch configuration collection date """

import numpy as np
import pandas as pd
# import utilities.data_structure_operations as dsop
import utilities.database_operations as dbop
import utilities.dataframe_operations as dfop
# import utilities.filesystem_operations as fsop
import utilities.module_execution as meop
import utilities.servicefile_operations as sfop

from .errdump_aggregation import errdump_aggregated



def errdump_analysis(errdump_df, switchshow_df, switch_params_aggregated_df, 
                portshow_aggregated_df, project_constants_lst):
    """Main function to get most frequently appeared log messages"""
    
    # # report_steps_dct contains current step desciption and force and export tags
    # # report_headers_df contains column titles, 
    # # report_columns_usage_sr show if fabric_name, chassis_name and group_name of device ports should be used
    # report_constant_lst, report_steps_dct, report_headers_df, report_columns_usage_sr = report_creation_info_lst
    # # report_constant_lst contains information: customer_name, project directory, database directory, max_title
    # *_, max_title = report_constant_lst

    project_steps_df, max_title, data_dependency_df, _, report_headers_df, report_columns_usage_sr, *_ = project_constants_lst

    # names to save data obtained after current module execution
    data_names = ['errdump_aggregated', 'raslog_counter', 'Журнал']
    # service step information
    print(f'\n\n{project_steps_df.loc[data_names[0], "step_info"]}\n')
    
    # reade data from database if they were saved on previos program execution iteration
    data_lst = dbop.read_database(project_constants_lst, *data_names)
    
    # list of data to analyze from report_info table
    analyzed_data_names = [ 'chassis_parameters', 'switch_parameters', 'switchshow_ports', 
                            'maps_parameters', 'portshow_aggregated', 'fabric_labels']

    # force run when any data from data_lst was not saved (file not found) or 
    # procedure execution explicitly requested for output data or data used during fn execution  
    force_run = meop.verify_force_run(data_names, data_lst, project_steps_df, 
                                            max_title, analyzed_data_names)
    if force_run:
        # data imported from init file (regular expression patterns) to extract values from data columns
        pattern_dct, _ = sfop.regex_pattern_import('raslog_split', max_title)
        raslog_message_details_df = sfop.dataframe_import('raslog_details', max_title)
        raslog_message_id_details_df = sfop.dataframe_import('raslog_id_details', max_title, columns=['Message_ID', 'Details', 'Recommended_action'])

        # current operation information string
        info = f'Counting RASLog messages'
        print(info, end =" ")

        # get aggregated DataFrames
        errdump_aggregated_df = errdump_aggregated(errdump_df, switchshow_df, switch_params_aggregated_df, 
                                                    portshow_aggregated_df, pattern_dct)
        # count how many times event appears during one month for the last six months 
        raslog_counter_df, raslog_frequent_df = errdump_statistics(errdump_aggregated_df, raslog_message_details_df, raslog_message_id_details_df)
        # after finish display status
        meop.status_info('ok', max_title, len(info))      
        # partition aggregated DataFrame to required tables
        raslog_report_df = raslog_report(raslog_frequent_df, data_names, report_headers_df, report_columns_usage_sr)

        # create list with partitioned DataFrames
        data_lst = [errdump_aggregated_df, raslog_counter_df, raslog_report_df]
        # writing data to sql
        dbop.write_database(project_constants_lst, data_names, *data_lst)
    # verify if loaded data is empty and replace information string with empty DataFrame
    else:
        data_lst = dbop.verify_read_data(max_title, data_names, *data_lst)
        errdump_aggregated_df, raslog_counter_df, *_ = data_lst
    # save data to service file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        dfop.dataframe_to_excel(data_frame, data_name, project_constants_lst)
    return errdump_aggregated_df, raslog_counter_df


def errdump_statistics(errdump_aggregated_df, raslog_message_details_df, raslog_message_id_details_df):
    """Function to count how many times log message appears during one month for the last six months.
    Log messages that appear less than 3 times a month are droppped"""
    
    # filter log messages based on date, message tag and merge devices behind same port for single event
    errdump_filtered_df = errdump_filter(errdump_aggregated_df)

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
                           'tx_port', 'rx_port', 'sid', 'did', 'wwn', 'IP_Address',
                           'Connected_portId', 'Connected_portWwn', 'Device_Host_Name_Port', 'alias', 'deviceType']
    
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
    if raslog_counter_df['alias'].notna().any():
        raslog_counter_df['alias'].replace('na_cell(?:, )?', value='', regex=True, inplace=True)
        raslog_counter_df['alias'] = raslog_counter_df['alias'].str.rstrip(', ')

    # apply date format to remove day and time
    raslog_counter_df['config_collection_date'] =  pd.to_datetime(raslog_counter_df['config_collection_date']).dt.date
    raslog_counter_df['Message_date'] = raslog_counter_df['Message_date'].dt.strftime('%Y-%m')
    # sort values
    errdump_sort_columns = ['chassis_name', 'Fabric_label', 'Fabric_name', 'switchName', 'Message_date', 'Quantity']
    raslog_counter_df.sort_values(by=errdump_sort_columns, ascending=[*[True]*5, False], inplace=True)
    raslog_counter_df.reset_index(drop=True, inplace=True)    
    # drop columns with all empty empty cells
    raslog_counter_df.dropna(axis=1, how='all', inplace=True)

    raslog_counter_df = dfop.dataframe_fillna(raslog_counter_df, raslog_message_details_df, join_lst=['Condition'], 
                                            filled_lst=['Details',  'Recommended_action']) 

    raslog_counter_df =  dfop.dataframe_fillna(raslog_counter_df, raslog_message_id_details_df, join_lst=['Message_ID'], 
                                            filled_lst=['Details',  'Recommended_action']) 
    
    # find log messages which appear more then three times a month
    mask_frequent = raslog_counter_df['Quantity'] > 3
    raslog_frequent_df = raslog_counter_df.loc[mask_frequent].copy()
    
    # remove INFO Messages for report DataFrame except securitu violations messages
    mask_not_info = raslog_frequent_df['Severity'] != 'INFO'
    mask_sec_violation_condition = raslog_frequent_df['Condition'].str.contains('security violation', case=False, na=False)
    mask_sec_violation_dashboard = raslog_frequent_df['Dashboard_category'].str.contains('security violation', case=False, na=False)
    raslog_frequent_df = raslog_frequent_df.loc[mask_not_info | mask_sec_violation_condition | mask_sec_violation_dashboard].copy()

    raslog_frequent_df.reset_index(drop=True, inplace=True)    
    return raslog_counter_df, raslog_frequent_df


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

    # External_sequence_number is removed to group devices with equal messages behind the same port
    errdump_grp_columns = ['configname', 'chassis_name', 'chassis_wwn', 
                           'Message_date', 'Message_ID', 
                           'Security audit flag', 'Severity',
                           'switchName', 'switchWwn', 'Fabric_name', 'Fabric_label',
                           'config_collection_date', 'Message_portIndex', 'Message_portType',
                           'slot', 'port', 'Condition', 'Current_value', 'Dashboard_category',
                           'obj', 'Message_status', 'portIndex', 'Index_slot_port', 'portType', 'portState',
                           'speed', 'tx_port', 'rx_port', 'sid', 'did', 'wwn', 'IP_Address',
                           'Connected_portId', 'Connected_portWwn', 'Device_Host_Name_Port', 'alias', 'deviceType']

    errdump_filtered_df = errdump_filtered_df.reindex(columns=errdump_grp_columns)
    
    # join multiple Device_Host_Name_Ports behind one port
    # groupby drop rows with nan values
    errdump_filtered_df.fillna('na_cell', inplace=True)
    errdump_filtered_df = errdump_filtered_df.groupby(by=errdump_grp_columns[:-5]).agg(', '.join)
    errdump_filtered_df.reset_index(inplace=True)
    
    """remove duplicate values from Connected_portId and Device_Host_Name_Port cells
    thus all identical events for the device behind the single port with idenitical Message_date
    considered to be one event"""
    for column in ['Connected_portId', 'Connected_portWwn', 'Device_Host_Name_Port', 'alias', 'deviceType']:
        errdump_filtered_df[column] = errdump_filtered_df[column].str.split(', ').apply(set).str.join(', ')

    return errdump_filtered_df


def raslog_report(raslog_frequent_df, data_names, report_headers_df, report_columns_usage_sr):
    """Function to check if it is required to use chassis_name columns. RASLog sometimes uses it's own
    chname not equal to switchname or chassis name thus it's better to keep default chassis names
    for visibility even if it was allowed to drop chassiss_name column before"""

    # make copy of default report_columns_usage_sr in order to avoid change it
    report_columns_usage_upd_sr = report_columns_usage_sr.copy()
    chassis_column_usage = report_columns_usage_upd_sr['chassis_info_usage']
    # if chassis_name column to be dropped
    if not chassis_column_usage:
        # if all switchnames and chassis names are not identical
        if not all(raslog_frequent_df.chassis_name == raslog_frequent_df.switchName):
            # change keep chassis_name column tag to True 
            report_columns_usage_upd_sr['chassis_info_usage'] = True

    raslog_report_df = dfop.generate_report_dataframe(raslog_frequent_df, report_headers_df, report_columns_usage_upd_sr, data_names[2])
    raslog_report_df.dropna(axis=1, how = 'all', inplace=True)
    return raslog_report_df
