"""Module to add sfp readings, sfp model details and find sfp redings intervals"""

import re

import numpy as np
import pandas as pd

import utilities.dataframe_operations as dfop


def port_sfp_join(portshow_aggregated_df, sfpshow_df, sfp_model_df, pattern_dct):
    """Function to add sfp readings, sfp model details and find sfp redings intervals"""
    
    join_columns_lst = ['configname', 'chassis_name', 'chassis_wwn', 
                    'switchName', 'switchWwn', 'slot', 'port']
    
    sfpshow_df.rename(columns={'SwitchName': 'switchName'}, inplace=True)
    # add sfp readings
    port_complete_df = portshow_aggregated_df.merge(sfpshow_df, how='left', on=join_columns_lst).copy()
    # add sfp model details
    port_complete_df = port_complete_df.merge(sfp_model_df, how='left', on=['Transceiver_PN'])
    # verify if transceiver is supported
    port_complete_df['Transceiver_Supported'] = port_complete_df.apply(lambda series: verify_sfp_support(series), axis='columns')
    
    # mark intervals between lower_threshold and upper_threshold
    # readings column name, lower threshold, upper threshold, step, filter_online flag
    readings_lst = [['RX_Power_dBm', -6, 2, 2, True], 
                    ['TX_Power_dBm', -6, 2, 2, True],
                    ['RX_Power_uW', 200, 1100, 200, True],
                    ['TX_Power_uW', 200, 1100, 200, True],
                    ['Temperature_Centigrade', 40, 70, 20, False],
                    ['Pwr_On_Time_years', 1, 3, 2, False]]

    pwr_column_pattern = r"(.+?)_(?:dBm|uW)"

    for readings_column in readings_lst:
        # fill empty RX, TX with Not Available
        if re.search(pwr_column_pattern, readings_column[0]):
            na_power_column = re.search(pwr_column_pattern, readings_column[0]).group(1)
            port_complete_df[readings_column[0]].fillna(port_complete_df[na_power_column], inplace=True)
        find_readings_intervals(port_complete_df, pattern_dct, *readings_column)
    
    port_complete_df.drop_duplicates(inplace=True)
    return port_complete_df


def verify_sfp_support(series):
    """Function to check if transceiver is supported based 
    on transceiver part number and switch generation"""
    
    # no transceiver installed
    if pd.isna(series['Transceiver_PN']):
        return np.nan
    # transceiver is not found in imported 
    # transceiver information table
    if pd.isna(series['Transceiver_switch_gen']):
        return 'Unknown SFP'
    # switch generation is unknown
    if pd.isna(series['Generation']):
        return 'Unknown switch'
    # switch generation is in the supported list
    if series['Generation'] in series['Transceiver_switch_gen']:
        return 'Yes'
    return 'No'


def extract_floats(df, source_column: str, destination_column: str, pattern_dct):
    """Function to create column with extracted float readings and convert it from str to float"""
    
    mask_float = df[source_column].str.contains(pattern_dct['float_str'], na=False)
    df[destination_column] = df.loc[mask_float, source_column]
    df[destination_column] = df[destination_column].astype('float64')


def find_readings_intervals(portshow_sfp_aggregated_df, pattern_dct, readings_column: str, 
                           lower_threshold: int, upper_threshold: int, step: int, 
                           filter_online: bool=True) -> pd.DataFrame:
    """Function to mark sfp redings with intervals in which they fall from lower_threshold
    to upper_threshold. Ports with SFP modules are taken into account only. 
    filter_online flag used to mark intervals for online ports only"""
    
    float_readings_column = readings_column + '_float'
    interval_readings_column = readings_column + '_interval'
    interval_readings_tmp_column = interval_readings_column + '_tmp'
    
    # convert redings to float
    extract_floats(portshow_sfp_aggregated_df, readings_column, float_readings_column, pattern_dct)
    
    # filter online ports
    mask_online = portshow_sfp_aggregated_df['portState'] == 'Online'
    # filter ports with transceivers
    mask_sfp_present = (portshow_sfp_aggregated_df['portPhys'] != 'No_Module') & \
        (portshow_sfp_aggregated_df['Transceiver_Name'] != 'No SFP installed in port')
    # summary port filter
    mask_filtered_ports = mask_online & mask_sfp_present if filter_online else mask_sfp_present
    
    # mark interval less then lower threshold
    mask_lower_threshold = portshow_sfp_aggregated_df[float_readings_column] < lower_threshold
    dfop.column_to_object(portshow_sfp_aggregated_df, interval_readings_column)
    portshow_sfp_aggregated_df.loc[
        mask_filtered_ports & mask_lower_threshold, interval_readings_column] = 'x < ' + str(lower_threshold)
    
    # mark intervals between lower_threshold and upper_threshold devided by step
    current_lower_threshold = lower_threshold
    while current_lower_threshold < upper_threshold:
        current_upper_threshold = current_lower_threshold + step
        # if upper_threshold on current step exceeds global upper_threshold 
        # then reduce current_upper_threshold to global upper_threshold
        if current_upper_threshold > upper_threshold:
            current_upper_threshold = upper_threshold
        mask_within_interval = portshow_sfp_aggregated_df[float_readings_column].between(
            left=current_lower_threshold, right=current_upper_threshold, inclusive='left')

        # write values which fall into current interval to temporary column 
        dfop.column_to_object(portshow_sfp_aggregated_df, interval_readings_tmp_column)
        portshow_sfp_aggregated_df.loc[mask_filtered_ports & mask_within_interval, interval_readings_tmp_column] = \
            str(current_lower_threshold) + ' <= x < ' + str(current_upper_threshold)     
        
        # fill empty cells in interval redings column with values from temporary column
        # portshow_sfp_aggregated_df[interval_readings_column].fillna(
        #     portshow_sfp_aggregated_df[interval_readings_tmp_column], inplace=True) # depricated method
        
        portshow_sfp_aggregated_df[interval_readings_column] = \
            portshow_sfp_aggregated_df[interval_readings_column].fillna(portshow_sfp_aggregated_df[interval_readings_tmp_column])
        current_lower_threshold += step
    
    # mark interval higher then upper threshold    
    mask_upper_threshold = portshow_sfp_aggregated_df[float_readings_column] >= upper_threshold
    # write interval to tmp column
    portshow_sfp_aggregated_df.loc[
        mask_filtered_ports & mask_upper_threshold, interval_readings_tmp_column] = 'x >= ' + str(upper_threshold)
    # fill empty cells in the interval column with values from the tmp column
    portshow_sfp_aggregated_df[interval_readings_column].fillna(
        portshow_sfp_aggregated_df[interval_readings_tmp_column], inplace=True)
    
    # fill empty cells in the interval column with values for filtered ports from the readings column (not float values)
    portshow_sfp_aggregated_df[interval_readings_tmp_column] = \
        portshow_sfp_aggregated_df.loc[mask_filtered_ports, interval_readings_tmp_column]
    portshow_sfp_aggregated_df[interval_readings_column].fillna(
        portshow_sfp_aggregated_df[interval_readings_tmp_column], inplace=True)
    
    # drop tmp column
    portshow_sfp_aggregated_df.drop(columns=interval_readings_tmp_column, inplace=True)