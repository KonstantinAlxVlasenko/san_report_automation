"""Module contains functions related to storage connections statistics"""

import re

import numpy as np
import pandas as pd

import utilities.dataframe_operations as dfop

from .storage_connection_statistics_notes import add_notes


def storage_connection_statistics(portshow_aggregated_df, pattern_dct):
    """Aggregated function to count statistics of storage (3PAR, MSA) port connections to Fabrics and
    verify if storages connected properly (ports distrubuted symmetrically, odd and even port
    indexes are in different Fabrics)"""

    # find storages (3PAR, MSA) with non-empty ports numbers in portshow_aggregated_df
    mask_storage_type = portshow_aggregated_df['deviceSubtype'].str.lower().isin(['3par', 'msa', 'emc', 'infinidat', 'huawei'])
    mask_storage_port = portshow_aggregated_df['Device_Port'].notna()
    storage_columns = ['Fabric_name', 'Fabric_label', 
                       'Device_Host_Name', 'Device_Port', 
                       'deviceSubtype', 'Device_type', 'Connected_portWwn']
    storage_ports_df = portshow_aggregated_df.loc[mask_storage_type & mask_storage_port, storage_columns ].copy()
    storage_ports_df.drop_duplicates(inplace=True)

    if storage_ports_df.empty:
        return pd.DataFrame()

    # extract controller, slot, port indexes
    pattern_columns_lst = [(pattern_dct['3par_ctrl_slot_port'], ['Controller', 'Slot', 'Port']),
                            (pattern_dct['emc_ctrl_slot_port'], ['Controller', 'Slot', 'Port']), 
                            (pattern_dct['msa_ctrl_port'], ['Controller', 'Port']),
                            (pattern_dct['infinidat_ctrl_port'], ['Controller', 'Port']),
                            (pattern_dct['oceanstor_ctrl_iom_port'], ['Controller', 'Slot', 'Port']),
                            (pattern_dct['oceanstor_ctrl_port'], ['Controller', 'Port']),
                            (pattern_dct['oceanstor_ctrl_slot_port'], ['Controller', 'Slot', 'Port']),
                            (pattern_dct['oceanstor_iom_port'], ['Slot', 'Port'])
                            ]
    storage_ports_df = dfop.extract_values_from_column(storage_ports_df, 'Device_Port', pattern_columns_lst)
    
    # identify controller for 4 U Controller Enclosure
    storage_ports_df = identify_huawei_4u_controller(storage_ports_df, pattern_dct)
    
    # drop rows without controller and slot
    mask_controller_port_notna = storage_ports_df[['Controller', 'Port']].notna().all(axis=1)
    storage_ports_df = storage_ports_df.loc[mask_controller_port_notna].copy()

    # create column with even or odd port index tag
    storage_ports_df['Port_parity'] = storage_ports_df['Port'].astype('int')
    mask_parity = storage_ports_df['Port_parity'] % 2 == 0
    storage_ports_df['Port_parity'] = np.where(mask_parity, 'even', 'odd')

    # concatenate Fabric_name and Fabric_label columns
    storage_ports_df['Fabric'] = storage_ports_df['Fabric_name'] + ' - ' + storage_ports_df['Fabric_label']

    # add corresponding tag to index
    component_level_lst = ['Controller', 'Slot', 'Port', 'Port_parity']    
    for column in component_level_lst:
        mask_column_notna = storage_ports_df[column].notna()
        if mask_column_notna.any():
            storage_ports_df.loc[mask_column_notna, column] = column + '_' + storage_ports_df.loc[mask_column_notna, column]
    # create controller slot column to count statistics for each controller's slot individually
    storage_ports_df['Controller_Slot'] = None
    mask_controller_slot = storage_ports_df[['Controller', 'Slot']].notna().all(axis=1)
    if mask_controller_slot.any():
        storage_ports_df.loc[mask_controller_slot, ['Controller_Slot']] = storage_ports_df['Controller'] + ' - ' + storage_ports_df['Slot']
    # REMOVE column created before values concat 
    # else:
    #     storage_ports_df['Controller_Slot'] = np.nan

    # create storage column to count summary statistics for all storage ports
    storage_ports_df['Storage'] = 'Storage ' + storage_ports_df['Device_Host_Name']


    component_level_lst = ['Storage', *component_level_lst, 'Controller_Slot', 'Device_type']
    component_sublevel_suffix_lst = ['physical', 'physical_virtual']
    for level in component_level_lst:
        for sublevel_suffix in component_sublevel_suffix_lst:
            storage_ports_df[level + '_' + sublevel_suffix] = storage_ports_df[level]

    # count connection statistics for each port group level 
    # (Storage, Controller, Slot, Port, Port_parity, Controller_Slot)
    storage_connection_statistics_df = pd.DataFrame()

    for level in component_level_lst:
        for sublevel_suffix in component_sublevel_suffix_lst:
            sublevel = level + '_' + sublevel_suffix
            if not (level == 'Device_type' and sublevel_suffix == component_sublevel_suffix_lst[0]):
                storage_ports_cp_df = storage_ports_df.copy()
                if sublevel_suffix == component_sublevel_suffix_lst[0]:
                    storage_ports_cp_df.drop_duplicates(subset=storage_columns[:-2], inplace=True)
                
                group_columns = ['deviceSubtype', 'Device_Host_Name', sublevel]
                current_df = dfop.count_frequency(storage_ports_cp_df, count_columns=['Fabric'], 
                                            group_columns=group_columns, margin_column_row=[(True, False)])
                # rename current port group level to 'Group_level' to concatenate DataFrames
                current_df.rename(columns={sublevel: 'Group_level'}, inplace=True)
                current_df['Group_type'] = sublevel.lower()
                if storage_connection_statistics_df.empty:
                    storage_connection_statistics_df = current_df.copy()
                else:
                    storage_connection_statistics_df = pd.concat([storage_connection_statistics_df, current_df])

    if not storage_connection_statistics_df.empty:
        # extract group type (storage, controller, slot etc) and connection type (physical only or physiscal + virtual)
        storage_connection_statistics_df[['Group_type', 'FLOGI']] = \
            storage_connection_statistics_df['Group_type'].str.extract(f'^(.+)_({component_sublevel_suffix_lst[0]}|{component_sublevel_suffix_lst[1]})$').values

        # sort port groups levels in corresponding order
        sort_priority = {'storage': 1, 'port_parity': 2, 'port': 3, 'controller': 4, 
                         'controller_slot': 5, 'slot': 6, 'device_type': 7}
        storage_connection_statistics_df.sort_values(by=['deviceSubtype', 'Device_Host_Name', 'Group_type', 'Group_level'], 
                                                    key=lambda col: col.replace(sort_priority), inplace=True)
        storage_connection_statistics_df.reset_index(drop=True, inplace=True)

        # add notes (if connections are symmetrical on each port group level, 
        # odd and even ports are in different fabrics)
        storage_connection_statistics_df = add_notes(storage_connection_statistics_df, storage_ports_df)
            
        # remove fabic_name if there is single fabric only
        fabric_names_lst = storage_ports_df['Fabric_name'].unique()
        if len(fabric_names_lst) == 1:
            fabric_name = fabric_names_lst[0]
            fabric_label_pattern = f'^{fabric_name}[ _-]+(.)'
            existing_fabric_columns = [column for column in storage_connection_statistics_df.columns if fabric_name in column]
            rename_dct = {fabric: re.search(fabric_label_pattern, fabric).group(1) for fabric in existing_fabric_columns}
            # rename_dct = {fabric: fabric.lstrip(fabric_name) for fabric in existing_fabric_columns}
            storage_connection_statistics_df.rename(columns=rename_dct, inplace=True)

        # merge Port_note and Port_parity_note columns
        storage_connection_statistics_df['Port_note'] = storage_connection_statistics_df['Port_note'].fillna(storage_connection_statistics_df['Port_parity_note'])
        # storage_connection_statistics_df['Port_note'].fillna(storage_connection_statistics_df['Port_parity_note'], inplace=True) #depricated method
        storage_connection_statistics_df.drop(columns=['Port_parity_note'], inplace=True)

        # # move Group_type column
        storage_connection_statistics_df = dfop.move_column(storage_connection_statistics_df, cols_to_move=['Group_type', 'FLOGI'], 
                                                        place='after', ref_col='Device_Host_Name')
        # create duplicates free storage name column
        storage_connection_statistics_df = dfop.remove_duplicates_from_column(storage_connection_statistics_df, column='Device_Host_Name', 
                                                                            duplicates_subset=['deviceSubtype', 'Device_Host_Name'])
        # drop physical_virtual ports for all storages except 3par (3par PortPersistent detection)
        # keep device_type row 
        mask_not_3par = storage_connection_statistics_df['deviceSubtype'].str.lower() != '3par'
        mask_physical_virtual = storage_connection_statistics_df['FLOGI'] == 'physical_virtual'
        mask_device_type = storage_connection_statistics_df['Group_type'] == 'device_type'
        mask_valid = ~(mask_not_3par & mask_physical_virtual & ~mask_device_type)
        storage_connection_statistics_df = storage_connection_statistics_df.loc[mask_valid]

        # drop 'physical_virtual' rows for Groups where no virtual port login detected
        storage_stat_columns = storage_connection_statistics_df.columns.tolist()
        for column in ['Device_Host_Name_duplicates_free', 'FLOGI']:
            storage_stat_columns.remove(column)
        storage_connection_statistics_df.drop_duplicates(subset=storage_stat_columns, inplace=True)
        
        # drop rows with empty Group_level (Port group type)
        # if Group_level is empty then this group show stats for strorage level and thus excessive 
        mask_group_type_notna = storage_connection_statistics_df['Group_level'].notna()
        storage_connection_statistics_df = storage_connection_statistics_df.loc[mask_group_type_notna].copy()

    return storage_connection_statistics_df


def identify_huawei_4u_controller(storage_ports_df, pattern_dct):
    """Function to identify controller for OceanStor Dorado 4 U Controller Enclosure.
    Controller is identified from IOM slot address (H2, L2, H11, L11 etc).
    Document 'Owning Controllers of Interface Module Ports on OceanStor Dorado 4 U Controller Enclosure'"""


    mask_4u_enclosure = storage_ports_df['Device_Port'].str.contains(pat=pattern_dct['oceanstor_iom_port'])

    if mask_4u_enclosure.any():
        # extract quadrant location (high or low) and slot number
        storage_ports_df.loc[mask_4u_enclosure, ['Quadrant_location', 'Quadrant_slot']] = \
            storage_ports_df.loc[mask_4u_enclosure, 'Device_Port'].str.extract(pat='(H|L)(\d+)').values
        storage_ports_df['Quadrant_slot'] = storage_ports_df['Quadrant_slot'].astype('float', errors='ignore').astype('int', errors='ignore')

        # identify quadrant labels (controllers)
        # HIGH 0-6 - quadrant A, HIGH 7-13 - quadrant C
        # LOW 0-6 - quadrant B, LOW 7-13 - qudrant D
        mask_high = storage_ports_df['Quadrant_location'] == 'H'
        mask_low = storage_ports_df['Quadrant_location'] == 'L'
        mask_left = storage_ports_df['Quadrant_slot'].between(0, 6, inclusive='both')
        mask_right = storage_ports_df['Quadrant_slot'].between(7, 13, inclusive='both')

        storage_ports_df.loc[mask_high & mask_left, 'Controller'] = 'A'
        storage_ports_df.loc[mask_low & mask_left, 'Controller'] = 'B'
        storage_ports_df.loc[mask_high & mask_right, 'Controller'] = 'C'
        storage_ports_df.loc[mask_low & mask_right, 'Controller'] = 'D'

        storage_ports_df.drop(columns=['Quadrant_location', 'Quadrant_slot'], inplace=True)
    return storage_ports_df


