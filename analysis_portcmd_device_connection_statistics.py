"""Module to count device connection statistics (port numbers, port speeds, virtual cahnnel usage)
and add notes if some criteria are not fullfilled"""


import re
import pandas as pd
import numpy as np


unique_vc_str ='Unique_VC_quantity_'
bandwidth_str = 'Bandwidth_'
device_port_quantity_str = 'Device_port_quantity_'


def device_connection_statistics(portshow_aggregated_df):
    
    portshow_aggregated_modified_df, portshow_aggregated_vc_unique_df, fabric_labels_lst = prior_preparation(portshow_aggregated_df)
    # count statistcics for ports quantity, ports speed, ports vc and device unique vc statistics for each device
    device_connection_statistics_df = count_device_connection_statistics(portshow_aggregated_modified_df, portshow_aggregated_vc_unique_df)
    # add device bandwidth in each Fabric_label
    device_connection_statistics_df = count_device_bandwidth(device_connection_statistics_df, portshow_aggregated_modified_df)
    # add notes
    device_connection_statistics_df = add_notes(device_connection_statistics_df, fabric_labels_lst)
    
    # drop fabric_labels columns and remove 'Unknown' tag from 'Device_Location'
    device_connection_statistics_df.drop(columns=fabric_labels_lst, inplace=True)
    device_connection_statistics_df['Device_Location'] = device_connection_statistics_df['Device_Location'].replace(['Unknown'], np.nan)
    
    # change columns order
    stat_columns = device_connection_statistics_df.columns.tolist()
    device_columns = ['Fabric_name', 'Device_Host_Name', 'Device_Location', 'deviceType', 'deviceSubtype']	
    reorder_stat_columns = [*device_columns, *stat_columns[5:]]
    device_connection_statistics_df = device_connection_statistics_df[reorder_stat_columns]

    device_connection_statistics_df.fillna(np.nan, inplace=True)

    
    return device_connection_statistics_df

def prior_preparation(portshow_aggregated_df):
    """Function to filter required data from portshow_aggregated_df DataFrame
    to count statistics"""

    # AG mode switches dropped to avoid duplicate  connection information
    mask_switch_native = portshow_aggregated_df['switchMode'] == 'Native'
    # drop Switches and Virtual connect modules
    mask_not_switch_vc = ~portshow_aggregated_df['deviceType'].isin(['SWITCH', 'VC'])
    # device class has to be defined
    mask_devicetype = portshow_aggregated_df['deviceType'].notna()
    # drop fabrics which are not part of assessment
    mask_fabric_name_label = portshow_aggregated_df[['Fabric_name', 'Fabric_label']].notna().all(axis=1)
    
    mask_complete = mask_fabric_name_label & mask_switch_native & mask_not_switch_vc & mask_devicetype
    columns_lst = ['Fabric_name', 'deviceType',	'Device_Location', 'deviceSubtype', 'Device_Host_Name', 'Fabric_label', 'speed', 'Virtual_Channel']
    portshow_aggregated_modified_df = portshow_aggregated_df.loc[mask_complete, columns_lst].copy()
    
    # assign 'unknown' tag for na Device_Location for grouping and crosstab operations
    portshow_aggregated_modified_df['Device_Location'].fillna('Unknown', inplace=True)
    # set 'Virtual_Channel', 'speed' with Fabric_label tag
    portshow_aggregated_modified_df['Virtual_Channel'] = portshow_aggregated_modified_df['Fabric_label'] + '_' + portshow_aggregated_modified_df['Virtual_Channel']
    portshow_aggregated_modified_df['speed'] = portshow_aggregated_modified_df['Fabric_label'] + '_' + portshow_aggregated_modified_df['speed']
    # set column name tag for Fabric_labels for later port number count
    portshow_aggregated_modified_df['Fabric_connection'] = device_port_quantity_str + portshow_aggregated_modified_df['Fabric_label']
    
    # create DataFrame with unique VC for each device only to verify if Virtual channel divercitu present for each device
    portshow_aggregated_vc_unique_df = portshow_aggregated_modified_df.drop_duplicates().copy()
    portshow_aggregated_vc_unique_df['Unique_Virtual_Channel'] = unique_vc_str +  portshow_aggregated_vc_unique_df['Fabric_label']

    # fabric_labels used in Fabric
    fabric_labels_lst = portshow_aggregated_modified_df['Fabric_label'].unique()
    fabric_labels_lst.sort()
    
    return portshow_aggregated_modified_df, portshow_aggregated_vc_unique_df, fabric_labels_lst


def count_device_connection_statistics(portshow_aggregated_modified_df, portshow_aggregated_vc_unique_df):
    """Function to count ports quantity, ports speed, ports vc and device unique vc statistics for each device"""

    device_connection_statistics_df = pd.DataFrame()
    
    
    statistics_lst =  [('Fabric_connection', portshow_aggregated_modified_df),
                       ('Fabric_label', portshow_aggregated_modified_df),
                       ('speed', portshow_aggregated_modified_df),
                       ('Virtual_Channel', portshow_aggregated_modified_df),
                       ('Unique_Virtual_Channel', portshow_aggregated_vc_unique_df)]
    
    # count statistcics for each column, DataFRame pair in statistics_lst
    for column, df in statistics_lst:
        margins_flag = True if column == 'Fabric_connection' else False
        # groupby columns
        index_lst = [df.Fabric_name,
                     df.deviceType,
                     df.Device_Location,
                     df.deviceSubtype,
                     df.Device_Host_Name]
        # count statistics for current column
        current_statistics_df = pd.crosstab(index = index_lst,
                                columns = df[column],
                                margins=margins_flag)
        # add current_statistics_df DataFrame to aggregated device_connection_statistics_df DataFRame
        if device_connection_statistics_df.empty:
            device_connection_statistics_df = current_statistics_df.copy()
        else:
            device_connection_statistics_df = device_connection_statistics_df.merge(current_statistics_df, how='left', 
                                                                                    left_index=True, right_index=True)
    # rename 'All' column
    device_connection_statistics_df.rename(columns={'All': device_port_quantity_str + '_Total'}, inplace=True)                

    return device_connection_statistics_df


def count_device_bandwidth(device_connection_statistics_df, portshow_aggregated_modified_df):
    """Function to add total bandwidth for each device in device_connection_statistics_df DataFrame"""

    portshow_speed_df = portshow_aggregated_modified_df.copy()
    # extract speed value from 'speed' column
    portshow_speed_df['speed_extract'] = portshow_speed_df['speed'].str.extract(r'(\d+)')
    portshow_speed_df['speed_extract'] = portshow_speed_df['speed_extract'].astype('int64', errors='ignore')
    # add 'Bandwidth' tag to Fabric_label to create columns later
    portshow_speed_df['Bandwidth_label'] = bandwidth_str + portshow_speed_df['Fabric_label']
    
    # groupby ports for each device in Fabric_label and summarize port speeds to get device bandwidth
    grp_lst = ['Fabric_name', 'deviceType',	'Device_Location', 'deviceSubtype', 'Device_Host_Name', 'Bandwidth_label']
    bandwidth_df = portshow_speed_df.groupby(grp_lst, as_index = False)['speed_extract'].sum()
    # move 'Bandwidth_label' column values as independent column names with values from 'speed_extract' column
    bandwidth_pivot_df = bandwidth_df.pivot_table(values='speed_extract', index=grp_lst[:-1], columns='Bandwidth_label', aggfunc='first')
    # if device has no connection to Fabric_label than bandwidth is zero
    bandwidth_pivot_df.fillna(0, inplace=True)
    
    # add bandwidth info to device_connection_statistics_df DataFrame
    device_connection_statistics_df = device_connection_statistics_df.merge(bandwidth_pivot_df, how='left', left_index=True, right_index=True)
    device_connection_statistics_df.reset_index(inplace=True)

    return device_connection_statistics_df


def add_notes(device_connection_statistics_df, fabric_labels_lst):
    """Function to add notes to device_connection_statistics_df DataFrame"""
    
    # unidentified devices for which combination of device class and WWNN is used 
    mask_name_contains_wwn = device_connection_statistics_df['Device_Host_Name'].str.contains(r'(?:[0-9a-f]{2}:){7}[0-9a-f]{2}', 
                                                                                          regex=True, na=False)
    # row with total values
    mask_not_all = device_connection_statistics_df['Fabric_name'] != 'All'
    # no fabric_connection
    mask_no_fabric_connection = (device_connection_statistics_df[fabric_labels_lst] == 0).any(axis=1)


    def device_connection_note(device_connection_statistics_df):
        """Function to add Device_connection_note containg notes for unsymmetrically connected devices or
        devices with absent connection to any of Fabric_labels"""

        # note devices with absent connection to any of Fabric_labels
        device_connection_statistics_df['Device_connection_note'] = np.where(~mask_name_contains_wwn & mask_no_fabric_connection, 
                                                                             'no_fabric_connection', pd.NA)
    
        # devices with noted absence of Fabric_label connection on prev step
        mask_na_note = device_connection_statistics_df['Device_connection_note'].isna()
        # devices with equal number of ports connected to Fabric_labels (number of unique values should be equal to one)
        mask_symmetric_connection = device_connection_statistics_df[fabric_labels_lst].nunique(axis=1).eq(1)
        # note devices with different number of port connected in different Fabric_label
        device_connection_statistics_df['Device_connection_note'] = np.where(~mask_name_contains_wwn & mask_na_note & ~mask_symmetric_connection & mask_not_all, 
                                                                             'unsymmetric_connection', device_connection_statistics_df['Device_connection_note'])
        return device_connection_statistics_df


    def vc_deversity_note(device_connection_statistics_df):
        """Function to add notes if device uses single virtual channel only"""

        # construct column names which need to be verified and created
        vc_verify_columns = [(fabric_label, unique_vc_str + fabric_label, 'VC_note_' + fabric_label) for fabric_label in fabric_labels_lst]
        
        # create vc_note for each Fabric_label with Fabric_label as value if single virtual channel is used
        for fabric_label, vc_nunique, vc_note in vc_verify_columns:
            # device has to be connected with more than one port to Fabric_label
            mask_port_number = device_connection_statistics_df[fabric_label] > 1
            # at lest one virtual channel has to be used
            mask_notna = device_connection_statistics_df[vc_nunique].notna()
            # all ports use same virtual channel
            mask_vc_nunique = device_connection_statistics_df[vc_nunique] == 1
            # note for each Fabric_label
            device_connection_statistics_df[vc_note] = np.where(mask_port_number & mask_notna & mask_vc_nunique,
                                                                fabric_label, pd.NA)
        # merge vc notes for all Fabric_labels into summary note 
        device_connection_statistics_df['Single_VC_note'] = np.nan
        # column names which contain information about single virtual channel usage if it was founded on prev step
        vc_single_columns = [column for *_, column in vc_verify_columns if device_connection_statistics_df[column].notna().any()]
        # add each VC_note information to summary note ony by one if exist
        for column in vc_single_columns:
            # value in summary column is empty
            mask_summary_note_empty = device_connection_statistics_df['Single_VC_note'].isna()
            # value in current column is empty
            mask_current_note_empty = device_connection_statistics_df[column].isna()
            """if value in summary column is empty take value from column to add (if it's not nan)
            if value in column to add is empty take value from summary note column (if it's not nan)
            if both values are empty use nan value
            if both values in summary note and columns to add exist then cancatenate them"""
            device_connection_statistics_df['Single_VC_note'] = np.select(
                [mask_summary_note_empty, mask_current_note_empty, mask_summary_note_empty & mask_current_note_empty],
                [device_connection_statistics_df[column], device_connection_statistics_df['Single_VC_note'], np.nan],
                default=device_connection_statistics_df['Single_VC_note'] + ', ' + device_connection_statistics_df[column])
        # drop columns with VC_notes for each Fabric_labels
        device_connection_statistics_df.drop(columns=[column for *_, column in vc_verify_columns], inplace=True)
        
        return device_connection_statistics_df

        
    def bandwidth_note(device_connection_statistics_df):
        """Function to add note if device have equal bandwidth in Fabric_labels connected"""

        # construct 'Bandwidth_' column names to check for each Fabric_label
        bandwidth_columns = [bandwidth_str + fabric_label for fabric_label in fabric_labels_lst]
        # devices with equal bandwidth for Fabric_labels (number of unique values is one)
        mask_equal_bandwidth = device_connection_statistics_df[bandwidth_columns].nunique(axis=1).eq(1)
        # note devices with different bandwidth (exclude unidentified devices and devices with any absent connection)
        device_connection_statistics_df['Bandwidth_note'] = np.where(~mask_name_contains_wwn & ~mask_no_fabric_connection & mask_not_all & ~mask_equal_bandwidth, 
                                                                             'different_bandwidth', pd.NA)
        return device_connection_statistics_df
    
    
    def low_speed_note(device_connection_statistics_df):
        """Function to add note if device connected at low speed (1, 2 or 4 Gb/s"""

        low_speed_regex = r'^\w+?_N?[124]G?$' # A_1G, A_N1, B_2G, B_N2, A_4G, A_N4
        low_speed_columns = [column for column in device_connection_statistics_df.columns if re.search(low_speed_regex, column)]
        # # port speeds to verify
        # low_speed_lst = ['1N', 'N2', '2G', '4N', '4G']
        # low_speed_columns = []
        # # construct column names
        # for fabric_label  in fabric_labels_lst:
        #     for low_speed in low_speed_lst:
        #         column = fabric_label + '_' + low_speed
        #         if column in device_connection_statistics_df.columns:
        #             low_speed_columns.append(column)

        device_connection_statistics_df['Low_speed_note'] = pd.NA
        # if low speed devices exist in fabric
        if low_speed_columns:
            # devices with low speed ports not equal to zero
            mask_low_speed = (device_connection_statistics_df[low_speed_columns] != 0).any(axis=1)
            device_connection_statistics_df['Low_speed_note'] = np.where(mask_low_speed & mask_not_all , 'low_speed', pd.NA)
            
        return device_connection_statistics_df
    
    
    def auto_speed_storage_lib_note(device_connection_statistics_df):
        """Function to add note if storage or library port speed is not fixed"""
        
        auto_speed_regex = r'\w+?_N\d+' # A_N8, B_N16, A_N32 
        auto_speed_columns = [column for column in device_connection_statistics_df.columns if re.search(auto_speed_regex, column)]
        device_connection_statistics_df['Storage_Lib_speed_note'] = np.nan
        if auto_speed_columns:
            # devices with stirage and library device class
            mask_storage_lib = device_connection_statistics_df['deviceType'].isin(['STORAGE', 'LIB'])
            # devices with auto speed ports
            mask_auto_speed = device_connection_statistics_df[auto_speed_columns].notna().any(axis=1)
            device_connection_statistics_df['Storage_Lib_speed_note'] = np.where(mask_storage_lib & mask_auto_speed, 'auto_speed', pd.NA)
            
        return device_connection_statistics_df
    
    # add notes about unsymmetrically connected devices or devices with any absent connection
    device_connection_statistics_df = device_connection_note(device_connection_statistics_df)
    # add notes if device uses single virtual channel only
    device_connection_statistics_df = vc_deversity_note(device_connection_statistics_df)
    # add notes if device has different bandwidth
    device_connection_statistics_df = bandwidth_note(device_connection_statistics_df)
    # add note if device have 4 Gbps connection speed and lower
    device_connection_statistics_df = low_speed_note(device_connection_statistics_df)
    # add note if speed is in auto mode for storages and libraries
    device_connection_statistics_df = auto_speed_storage_lib_note(device_connection_statistics_df)
    
    return device_connection_statistics_df