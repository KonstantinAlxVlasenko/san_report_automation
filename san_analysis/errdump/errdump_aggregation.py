"""Module to label errdump, extract information from error messages and
verify match with portshow DataFrame"""

import warnings

import numpy as np
import pandas as pd

import utilities.dataframe_operations as dfop
import utilities.data_structure_operations as dsop


def errdump_aggregated(errdump_df, switchshow_df, switch_params_aggregated_df, portshow_aggregated_df, pattern_dct):
    """Function to label errdump_df and count filtered messages"""
    
    # label errdumm_df, add switchWwn, datetime config collection
    errdump_aggregated_df = prior_preparation(errdump_df, switchshow_df, switch_params_aggregated_df)
    # extract values from Events descriprion (Event, slot, port etc) to corresponding columns
    errdump_aggregated_df = message_extract(errdump_aggregated_df, pattern_dct)
    # count how many times each event occurred
    errdump_aggregated_df = count_event_quantity(errdump_aggregated_df)
    # add port details and connected to port device information
    errdump_aggregated_df = errdump_portshow(errdump_aggregated_df, portshow_aggregated_df)
    return errdump_aggregated_df


def count_event_quantity(errdump_aggregated_df):
    """Function to count how many times each event (message) occurred in the raslog"""

    # move repeted times value one row up (it's value shown on next line log entry)
    errdump_aggregated_df['Message_repeated_times'] = errdump_aggregated_df['Message_repeated_times'].shift(-1)
    # convert counters to the float
    errdump_aggregated_df['Message_repeated_times'] = errdump_aggregated_df['Message_repeated_times'].astype(float)
    errdump_aggregated_df['Message_triggered_times'] = errdump_aggregated_df['Message_triggered_times'].astype(float)
    # if no repeat message present then no event repeat took place
    errdump_aggregated_df['Message_repeated_times_filled'] = errdump_aggregated_df['Message_repeated_times'].fillna(0)
    # if no triggered value is extracted then message triggered (occured) only once
    errdump_aggregated_df['Message_triggered_times_filled'] = errdump_aggregated_df['Message_triggered_times'].fillna(1)
    # count how many times message occurred if triggered value is taken into account 
    # message_occurrance = triggered_value + triggered_value * repeat_value
    errdump_aggregated_df['Message_occured_quantity_multiple'] = errdump_aggregated_df['Message_triggered_times_filled'] + \
        errdump_aggregated_df['Message_triggered_times_filled'] * errdump_aggregated_df['Message_repeated_times_filled']
    # count how many times message occurred if triggered value is not taken into account (one line = single message)
    # message_occurrance = 1 + 1 * repeat_value
    errdump_aggregated_df['Message_occured_quantity_unique'] = 1 + errdump_aggregated_df['Message_repeated_times_filled']
    # move calculated columns to the source columns
    errdump_aggregated_df = dfop.move_column(errdump_aggregated_df, cols_to_move=['Message_occured_quantity_unique', 'Message_occured_quantity_multiple'], 
                                             ref_col='Message_triggered_times')
    # drop tmp columns
    errdump_aggregated_df.drop(columns=['Message_repeated_times_filled', 'Message_triggered_times_filled'], inplace=True)
    return errdump_aggregated_df


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
    errdump_aggregated_df = dfop.dataframe_fabric_labeling(errdump_aggregated_df, switch_params_aggregated_df)
    # add config collection date
    errdump_aggregated_df = dfop.dataframe_fillna(errdump_aggregated_df, switch_params_aggregated_df, ['configname', 'chassis_name', 'chassis_wwn'], 
                                             ['config_collection_date'], drop_na=False)    
    # convert dates columns
    errdump_aggregated_df['Message_date'] = pd.to_datetime(errdump_aggregated_df['Message_date'])
    errdump_aggregated_df['config_collection_date'] = pd.to_datetime(errdump_aggregated_df['config_collection_date'])
    return errdump_aggregated_df


def message_extract(errdump_aggregated_df, pattern_dct):
    """Function to parse errdump Message column into corresponding set of columns and label each message
    with extracted, copied or ignored tag"""
    
    # list with regex pattern and columns values to be extracted to pairs
    extract_pattern_columns_lst = [
        [pattern_dct['port_idx_slot_number'], ['Message_portIndex', 'Message_portType', 'slot', 'port']],
        [pattern_dct['pid'], ['Message_portId']],
        [pattern_dct['flow_sid_did'], ['sid', 'did', 'port']],
        [pattern_dct['repeated_times'], ['Message_repeated_times']],
        [pattern_dct['triggered_times'], ['Message_triggered_times']],
        [pattern_dct['event_portidx'], ['Condition', 'Message_portIndex']],
        [pattern_dct['event_slot_portidx'], ['Condition', 'slot', 'Message_portIndex']],
        [pattern_dct['bottleneck_detected'], ['Condition', 'slot', 'port', 'Current_value']],
        [pattern_dct['bottleneck_cleared'], ['slot', 'port', 'Condition']],
        [pattern_dct['severe_bottleneck'], ['Condition', 'Message_portType', 'slot', 'port']],
        [pattern_dct['maps_current_value'],  ['Condition', 'Current_value', 'Dashboard_category']],
        [pattern_dct['maps_object'], ['Condition', 'obj']],
        [pattern_dct['c2_message_1'], ['Condition', 'slot', 'port', 'Message_portIndex', 'Message_portId']],
        [pattern_dct['slow_drain_device'], ['Condition','slot', 'port', 'Message_portIndex']],
        [pattern_dct['frame_detected'], ['Condition', 'tx_port', 'rx_port', 'sid', 'did']],
        [pattern_dct['els_unzoned_device'], ['Condition', 'Message_portIndex', 'did', 'sid', 'wwn']],
        [pattern_dct['sfp_port_slot_number'], ['slot', 'port']],
        [pattern_dct['sec_violation_login_failure'], ['Dashboard_category', 'Condition', 'IP_Address']],
        [pattern_dct['sec_violation_unauthorized_host'], ['Dashboard_category', 'Condition', 'IP_Address']],
        ]        
    
    # get unique columns to extract values to
    extract_columns_lst = [extract_pattern_columns[1] for  extract_pattern_columns in extract_pattern_columns_lst]
    extract_columns_lst = dsop.flatten(extract_columns_lst)
    extract_columns_lst = dsop.remove_diplicates_from_list(extract_columns_lst)
    dfop.column_to_object(errdump_aggregated_df, *extract_columns_lst)
    
    # extract corresponding values if regex pattern applicable
    for pattern, extracted_columns in extract_pattern_columns_lst:
        # pattern contains groups but str.cotains used to identify mask
        # supress warning message
        warnings.filterwarnings("ignore", 'This pattern is interpreted as a regular expression, and has match groups.')
        mask = errdump_aggregated_df['Message'].str.contains(pattern, regex=True)
        errdump_aggregated_df.loc[mask, extracted_columns] = errdump_aggregated_df.loc[mask, 'Message'].str.extract(pattern).values
    
    # sec_violation_unauthorized_host contains tcp port number and need to be removed
    mask_unauthorized_host = errdump_aggregated_df['Message'].str.contains(pattern_dct['sec_violation_unauthorized_host'], na=False)
    errdump_aggregated_df.loc[mask_unauthorized_host, ['Message_portType', 'port']] = pd.Series([np.nan, np.nan])

    # add empty columns if they were not extracted
    extracted_columns = [column for _, columns in extract_pattern_columns_lst for column in columns]
    extracted_columns = list(set(extracted_columns))
    add_empty_columns = [column for column in extracted_columns if not column in errdump_aggregated_df.columns]
    errdump_aggregated_df[add_empty_columns] = np.nan       
    
    # masks for labeleing messages with tags
    mask_condition_na = errdump_aggregated_df['Condition'].isna()
    mask_ignored_message = errdump_aggregated_df['Message_ID'].str.contains(pattern_dct['ignore_message'], regex=True)
    # mark messages with extracted, ingnored or copied tag
    dfop.column_to_object(errdump_aggregated_df, 'Message_status')
    errdump_aggregated_df.loc[~mask_condition_na, 'Message_status'] = 'extracted'
    errdump_aggregated_df.loc[mask_ignored_message, 'Message_status'] = 'ignored'
    errdump_aggregated_df['Message_status'] = errdump_aggregated_df['Message_status'].fillna('copied')
    
    # copy messages which were not extracted but shouldn't be ignored to Condition column
    errdump_aggregated_df.loc[mask_condition_na & ~mask_ignored_message, 'Condition'] = \
        errdump_aggregated_df.loc[mask_condition_na & ~mask_ignored_message, 'Condition'].fillna(errdump_aggregated_df['Message'])
    return errdump_aggregated_df
    

def errdump_portshow(errdump_aggregated_df, portshow_aggregated_df):
    """Function to add port and connected device information to errdump_aggregated_df"""

    mask_device_name = portshow_aggregated_df['Device_Host_Name'].notna()
    # if Message_portIndex is present but port number is not then fillna slot and port number from portshow
    if (errdump_aggregated_df['Message_portIndex'].notna() & errdump_aggregated_df['port'].isna()).any():
        portshow_join_df = portshow_aggregated_df.copy()
        portshow_join_df.rename(columns={'portIndex': 'Message_portIndex'}, inplace=True)
        portshow_join_columns = ['configname', 'chassis_name', 'chassis_wwn', 'Message_portIndex']
        errdump_aggregated_df = dfop.dataframe_fillna(errdump_aggregated_df, portshow_join_df, 
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
                          'Device_Host_Name', 'Device_Port', 'alias', 'Device_Location',
                          'deviceType', 'deviceSubtype']
    
    portshow_join_df = portshow_aggregated_df[portshow_columns].copy()
    if errdump_aggregated_df[['slot', 'port']].notna().all(axis=1).any():
        errdump_aggregated_df = errdump_aggregated_df.merge(portshow_join_df, how='left', on=portshow_columns[:5])
    
    # add empty columns if there was no merge with portshow_join_df
    add_empty_columns = [column for column in portshow_columns[3:] if not column in errdump_aggregated_df.columns]
    errdump_aggregated_df[add_empty_columns] = np.nan  
    
    # add device information based in pid
    if errdump_aggregated_df['Message_portId'].notna().any():
        portshow_join_df = portshow_aggregated_df.copy()
        portshow_join_df['Message_portId'] = portshow_join_df['Connected_portId']
        errdump_aggregated_df = dfop.dataframe_fillna(errdump_aggregated_df, portshow_join_df, 
                                                join_lst=[*portshow_columns[:3], 'Message_portId'], 
                                                filled_lst=portshow_columns[3:], remove_duplicates=False, drop_na=False)
    # add 0 if sid or did contains 5 symbols only
    for column in ['sid', 'did']:
        portshow_join_df = portshow_aggregated_df.copy()
        if errdump_aggregated_df[column].notna().any():
            mask_cut_pid = errdump_aggregated_df[column].str.len() == 5
            errdump_aggregated_df.loc[mask_cut_pid, column] = '0' +  errdump_aggregated_df.loc[mask_cut_pid, column]
    
    # add fabric name and label for switches with chassis info, slot and port
    if errdump_aggregated_df[portshow_columns[:5]].notna().all(axis=1).any():
        errdump_aggregated_df = \
            dfop.dataframe_fillna(errdump_aggregated_df, portshow_aggregated_df, portshow_columns[:5], ['Fabric_name', 'Fabric_label'])

    # add device information for sid and did
    errdump_aggregated_df = errdump_sid_did_device_resolve(errdump_aggregated_df, portshow_aggregated_df)
    
    # concatenate device name and device port columns
    for devicename_port_tag in ('', 'sid_', 'did_'):
        errdump_aggregated_df[devicename_port_tag + 'Device_Host_Name_Port'] = \
            errdump_aggregated_df[[devicename_port_tag + 'Device_Host_Name', devicename_port_tag + 'Device_Port']].stack().groupby(level=0).agg(' port '.join)
    return errdump_aggregated_df


def errdump_sid_did_device_resolve(errdump_aggregated_df, portshow_aggregated_df):
    """Function to resolve device information for sid (source) and did (destination)
    pids in the raslog"""

    # columns filled for sid and did pids
    device_columns = ['switchName', 'slot', 'port', 'portIndex', 'Index_slot_port',
                        'portType', 'portState', 'speed',
                        'Connected_portWwn',
                        'Device_Host_Name', 'Device_Port', 'alias',
                        'deviceType', 'deviceSubtype']


    # drop empty fabric rows
    mask_fabric_notna = portshow_aggregated_df[['Fabric_name', 'Fabric_label']].notna().all(axis=1)
    portshow_join_df = portshow_aggregated_df.loc[mask_fabric_notna].copy()

    # drop ignored fabric rows
    mask_ignored_fabric = portshow_join_df[['Fabric_name', 'Fabric_label']].isin(['-', 'x']).any(axis=1)
    portshow_join_df = portshow_join_df.loc[~mask_ignored_fabric].copy()

    # pid address (sid or did) is unique for fabric
    # fabric_name column is dropped if analysis performed to the single fabric_name 
    if portshow_join_df['Fabric_name'].nunique() > 1:
        pid_columns = ['Fabric_name', 'Fabric_label', 'Connected_portId']
    else:
        pid_columns = ['Fabric_label', 'Connected_portId']

    for pid in ('sid', 'did'):
        portshow_current_pid_df = portshow_join_df.copy()
        pid_prefix = pid + '_'
        # rename portID column to sid or did
        portshow_current_pid_df.rename(columns={'Connected_portId': pid}, inplace=True)
        pid_columns[-1] = pid
        # add tag 'sid' or 'did' to the connected device columns
        current_pid_device_rename_columns = {k:pid_prefix + k for k in device_columns}
        portshow_current_pid_df.rename(columns=current_pid_device_rename_columns, inplace=True)
        current_pid_device_columns = [pid_prefix + column for column in device_columns]
        # add device information for sid and did 
        errdump_aggregated_df = dfop.dataframe_fillna(errdump_aggregated_df, portshow_current_pid_df, 
                                                join_lst=pid_columns, 
                                                filled_lst=current_pid_device_columns, remove_duplicates=False, drop_na=False)
    return errdump_aggregated_df






