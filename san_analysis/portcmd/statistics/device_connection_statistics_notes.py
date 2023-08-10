"""Module to add notes to device connection statistics based on connection details in DataFrame"""

import re

import numpy as np
import pandas as pd

import utilities.dataframe_operations as dfop


def add_notes(device_connection_statistics_df, fabric_labels_lst, columns_str):
    """Function to add notes to device_connection_statistics_df DataFrame"""

    unique_vc_str, unique_slot_str, unique_port_speed_str, bandwidth_str, director_str = columns_str
    
    # unidentified devices for which combination of device class and WWNN is used 
    mask_name_contains_wwn = device_connection_statistics_df['Device_Host_Name'].str.contains(r'(?:[0-9a-f]{2}:){7}[0-9a-f]{2}', 
                                                                                          regex=True, na=False)
    # row with total values
    mask_not_all = device_connection_statistics_df['Fabric_name'] != 'All'
    # no fabric_connection
    mask_no_fabric_connection = (device_connection_statistics_df[fabric_labels_lst] == 0).any(axis=1)
    # devices with stirage and library device class
    mask_storage_lib = device_connection_statistics_df['deviceType'].isin(['STORAGE', 'LIB'])


    def device_connection_note(device_connection_statistics_df):
        """Function to add Device_connection_note containg notes for unsymmetrically connected devices or
        devices with absent connection to any of Fabric_labels"""

        # FABRIC CONNECTION ABSENCE NOTE
        # temporary columns to tag devices with no connection to fabric (column for each fabric_label)
        fabric_connection_verify_columns = [(fabric_label, 'Fabric_connection_absence_' + fabric_label) for fabric_label in fabric_labels_lst]
        
        # check each identified device(device name doesn't contain wwnn) for fabric connection absence
        for fabric_label, absence_column in fabric_connection_verify_columns:
            # devices with no connection to current fabric_label
            mask_connection_absense = device_connection_statistics_df[fabric_label] == 0
            # if device is not connected to faric_label (connected port quantity is zero) set value in absence_column to fabric_label
            device_connection_statistics_df[absence_column] = np.where(~mask_name_contains_wwn & mask_connection_absense, fabric_label, pd.NA)
        
        # concatenate columns with fabric_label connection absence 
        device_connection_statistics_df = \
            dfop.concatenate_columns(device_connection_statistics_df, summary_column='Device_connection_note', 
                                merge_columns=[column for *_, column in fabric_connection_verify_columns])

        # devices with device_connection note values
        mask_dev_conn_notna = device_connection_statistics_df['Device_connection_note'].notna()
        # add 'no_connection_fabric_' before absent fabric_label connection
        device_connection_statistics_df.loc[mask_dev_conn_notna, 'Device_connection_note'] = \
            'no_connection_fabric_' + device_connection_statistics_df['Device_connection_note']

        # UNSYMMETRIC NOTE 
        # devices with equal number of ports connected to Fabric_labels (number of unique values should be equal to one)
        mask_symmetric_connection = device_connection_statistics_df[fabric_labels_lst].nunique(axis=1).eq(1)
        # note devices with different number of port connected in different Fabric_label
        device_connection_statistics_df['Device_connection_note'] = np.where(~mask_name_contains_wwn & ~mask_dev_conn_notna & ~mask_symmetric_connection & mask_not_all, 
                                                                             'unsymmetric_connection', device_connection_statistics_df['Device_connection_note'])
        return device_connection_statistics_df


    def diversity_note(device_connection_statistics_df, diversity_type):
        """Function to add notes if device ports have different speed"""

        if diversity_type == 'slot':
            unique_str = unique_slot_str
            summary_column = 'Single_slot_note'
            clarifying_string = 'single slot connection in '
        elif diversity_type == 'speed':
            unique_str = unique_port_speed_str
            summary_column = 'Multiple_port_speed_note'
            clarifying_string = 'multiple ports speed in '
        elif diversity_type == 'vc':
            unique_str = unique_vc_str
            summary_column = 'Single_VC_note'
            clarifying_string = 'single VC in '            

        # construct column names which need to be verified and created
        verify_columns = [(fabric_label, unique_str + fabric_label, summary_column + fabric_label) for fabric_label in fabric_labels_lst]
        
        # create note for each Fabric_label with Fabric_label as value if 
        for fabric_label, value_nunique, note_column in verify_columns:
            
            # device has to be connected with more than one port to Fabric_label
            mask_port_number = device_connection_statistics_df[fabric_label] > 1
            # single unique value
            mask_single = device_connection_statistics_df[value_nunique] == 1
            # number of unique values exceed one
            mask_multiple = device_connection_statistics_df[value_nunique] > 1
            if diversity_type == 'slot':
                # mask director
                director_column = fabric_label + '_' + director_str
                if not director_column in device_connection_statistics_df.columns:
                    device_connection_statistics_df[director_column] = 0
                mask_director = device_connection_statistics_df[director_column] > 0
                mask_total = mask_port_number & mask_single & mask_director
            elif diversity_type == 'speed':
                mask_total = mask_port_number & mask_multiple
            elif diversity_type == 'vc':
                mask_total = mask_port_number & mask_single
            # note for each Fabric_label
            device_connection_statistics_df[note_column] = np.where(mask_total, fabric_label, pd.NA)
            
            if diversity_type == 'slot' and (device_connection_statistics_df[director_column] == 0).all():
                device_connection_statistics_df.drop(columns=[director_column], inplace=True)
            
        # concatenate columns with fabric labels for current note
        device_connection_statistics_df = \
            dfop.concatenate_columns(device_connection_statistics_df, summary_column=summary_column, 
                                merge_columns=[column for *_, column in verify_columns])
        
        # add string to summary column
        mask_note_na = device_connection_statistics_df[summary_column].isna()
        device_connection_statistics_df[summary_column] = \
            device_connection_statistics_df[summary_column].where(mask_note_na, clarifying_string + device_connection_statistics_df[summary_column])
        
        return device_connection_statistics_df 

        
    def bandwidth_note(device_connection_statistics_df):
        """Function to add note if device don't have equal bandwidth in Fabric_labels connected"""

        # construct 'Bandwidth_' column names to check for each Fabric_label
        bandwidth_columns = [bandwidth_str + fabric_label for fabric_label in fabric_labels_lst]
        # devices with equal bandwidth for Fabric_labels (number of unique values is one)
        mask_equal_bandwidth = device_connection_statistics_df[bandwidth_columns].nunique(axis=1).eq(1)
        # note devices with different bandwidth (exclude unidentified devices and devices with any absent connection)
        device_connection_statistics_df['Bandwidth_note'] = np.where(~mask_name_contains_wwn & ~mask_no_fabric_connection & mask_not_all & ~mask_equal_bandwidth, 
                                                                             'different_bandwidth', pd.NA)
        return device_connection_statistics_df
    
    
    def speed_note(device_connection_statistics_df, speed_type):
        """Function to add note if port speed is not fixed or port speed is low"""
        
        if speed_type == 'low':
            summary_column = 'Low_speed_note'
            clarifying_string = 'low speed in '
            mask_extra = mask_not_all
        elif speed_type == 'auto':
            summary_column = 'Storage_Lib_speed_note'
            clarifying_string = 'auto speed in '
            mask_extra = mask_storage_lib
            
        # construct column names which need to be verified and created
        verify_columns = [(fabric_label, summary_column + fabric_label) for fabric_label in fabric_labels_lst]

        for fabric_label, note_column in verify_columns:
            if speed_type == 'low':
                regex_pattern = f'^{fabric_label}_N?[124]G?$' # A_1G, A_N1, B_2G, B_N2, A_4G, A_N4
            elif speed_type == 'auto':
                regex_pattern = f'{fabric_label}_N\d+' # A_N8, B_N16, A_N32
                
            regex_columns = [column for column in device_connection_statistics_df.columns if re.search(regex_pattern, column)]
            # if low speed devices exist in fabric 
            device_connection_statistics_df[note_column] = pd.NA
            if regex_columns:
                # devices with (low, auto) speed ports not equal to zero
                mask_regex_columns = (device_connection_statistics_df[regex_columns] != 0).any(axis=1)
                device_connection_statistics_df[note_column] = np.where(mask_regex_columns & mask_extra, fabric_label, pd.NA)
            else:
                device_connection_statistics_df[note_column] = pd.NA
                
        # concatenate columns with fabric labels for current note
        device_connection_statistics_df = \
            dfop.concatenate_columns(device_connection_statistics_df, summary_column=summary_column, 
                                merge_columns=[column for *_, column in verify_columns])
            
        # add string to summary column
        mask_note_na = device_connection_statistics_df[summary_column].isna()
        device_connection_statistics_df[summary_column] = \
            device_connection_statistics_df[summary_column].where(mask_note_na, clarifying_string + device_connection_statistics_df[summary_column])
            
        return device_connection_statistics_df
    
    
    def multiple_fabric_note(device_connection_statistics_df):
        """Function to add note if device have connections to multiple fabrics"""
        
        summary_column = 'Multiple_fabrics_connection_note'
        clarifying_string = 'multiple fabrics connection'
        
        mask_multiple_fabrics = device_connection_statistics_df.duplicated(subset=['Device_Host_Name', 'deviceType', 'deviceSubtype'], keep=False)
        device_connection_statistics_df[summary_column] = \
            np.where(mask_multiple_fabrics, clarifying_string, pd.NA)
            
        return device_connection_statistics_df
    
    # add notes about unsymmetrically connected devices or devices with any absent connection
    device_connection_statistics_df = device_connection_note(device_connection_statistics_df)
    # add notes if device uses single virtual channel only
    device_connection_statistics_df = diversity_note(device_connection_statistics_df, diversity_type='vc')
    # add notes if device have different port speed values
    device_connection_statistics_df = diversity_note(device_connection_statistics_df, diversity_type='speed')
    # add notes if device connected  to a single director slot
    device_connection_statistics_df = diversity_note(device_connection_statistics_df, diversity_type='slot')
    # add notes if device has different bandwidth
    device_connection_statistics_df = bandwidth_note(device_connection_statistics_df)
    # add note if device have 4 Gbps connection speed and lower
    device_connection_statistics_df = speed_note(device_connection_statistics_df, speed_type='low')
    # add note if speed is in auto mode for storages and libraries
    device_connection_statistics_df = speed_note(device_connection_statistics_df, speed_type='auto')
    # add note if device have connections to multiple fabrics
    device_connection_statistics_df = multiple_fabric_note(device_connection_statistics_df)
    
    return device_connection_statistics_df