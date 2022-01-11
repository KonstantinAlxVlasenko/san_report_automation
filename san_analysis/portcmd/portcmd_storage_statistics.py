"""Module contains functions related to storage connections statistics"""

import re

import numpy as np
import pandas as pd
import utilities.dataframe_operations as dfop


def storage_connection_statistics(portshow_aggregated_df, pattern_dct):
    """Aggregated function to count statistics of storage (3PAR, MSA) port connections to Fabrics and
    verify if storages connected properly (ports distrubuted symmetrically, odd and even port
    indexes are in different Fabrics)"""

    # regular expression patterns TO_REMOVE
    # comp_keys, _, comp_dct = re_pattern_lst

    # find storages (3PAR, MSA) with non-empty ports numbers in portshow_aggregated_df
    mask_storage_type = portshow_aggregated_df['deviceSubtype'].str.lower().isin(['3par', 'msa', 'emc'])
    mask_storage_port = portshow_aggregated_df['Device_Port'].notna()
    storage_columns = ['Fabric_name', 'Fabric_label', 'Device_Host_Name', 'Device_Port', 'deviceSubtype', 'Device_type', 'Connected_portWwn']
    storage_ports_df = portshow_aggregated_df.loc[mask_storage_type & mask_storage_port, storage_columns ].copy()
    storage_ports_df.drop_duplicates(inplace=True)

    if storage_ports_df.empty:
        return pd.DataFrame()

    # extract controller, slot, port indexes
    pattern_columns_lst = [(pattern_dct['3par_ctrl_slot_port'], ['Controller', 'Slot', 'Port']),
                            (pattern_dct['emc_ctrl_slot_port'], ['Controller', 'Slot', 'Port']), 
                            (pattern_dct['msa_ctrl_port'], ['Controller', 'Port']),]
    storage_ports_df = dfop.extract_values_from_column(storage_ports_df, 'Device_Port', pattern_columns_lst)
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
        mask_notna = storage_ports_df[column].notna()
        storage_ports_df.loc[mask_notna, column] = column + '_' + storage_ports_df.loc[mask_notna, column]
        
    # create controller slot column to count statistics for each controller's slot individually
    mask_controller_slot = storage_ports_df[['Controller', 'Slot']].notna().all(axis=1)
    storage_ports_df.loc[mask_controller_slot, ['Controller_Slot']] = storage_ports_df['Controller'] + ' - ' + storage_ports_df['Slot']

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
        sort_priority = {'storage': 1, 
                            'port_parity': 2, 'port': 3, 'controller': 4, 'controller_slot': 5, 'slot': 6, 'device_type': 7}
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
        storage_connection_statistics_df['Port_note'].fillna(storage_connection_statistics_df['Port_parity_note'], inplace=True)
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

        # drop 'physical_virtual' rows for Groups where no virtual port ligin detected
        storage_stat_columns = storage_connection_statistics_df.columns.tolist()
        for column in ['Device_Host_Name_duplicates_free', 'FLOGI']:
            storage_stat_columns.remove(column)
        storage_connection_statistics_df.drop_duplicates(subset=storage_stat_columns, inplace=True)

    return storage_connection_statistics_df


def add_notes(storage_connection_statistics_df, storage_ports_df):
    """Function to add notes to storage_connection_statistics_df DataFrame"""

    def verify_connection_symmetry(series, fabric_names_lst):
        """Function to check if ports are connected symmetrically in each Fabric_name and across san.
        Symmetrically connected storage means that each fabric in san have same amount of ports connected. 
        Ports are grouped based on the levels: 
        storage, controllers, slots across each controller, slots across storage"""

        # Fabric_names where storage port connection is unsymmtrical (port quantity differs across the Fabric_labels)
        unsymmetric_fabric_name_lst = []
        # Fabric_names where storage ports are connected to the single fabric_label (nonredundant connection)
        single_fabric_name_lst = []
        # list of Fabric_names where storage is connected at least to one of the Fabric_labels
        detected_fabric_name_lst = []
        # list of Fabrics (concatenation of Fabric_name and Fabric_label) where connection is detected to any
        # of the Fabric_labels of Fabric_name. To be symmetrically connected across san all ports quantity 
        # values have to be equal in all Fabrics of this list
        symmetric_fabric_lst = []
        unsymmetric_fabric_str = ''
        unsymmetric_san_str = ''
        single_fabric_str = ''
        
        if series['Group_type'] in ['storage', 'controller', 'controller_slot', 'slot']:
            for fabric_name in fabric_names_lst:
                # find all fabrics in current fabric_name
                existing_fabric_columns = [column for column in series.index if fabric_name in column]
                # find fabrics to which storage is connected in current fabric_name
                connected_fabric_columns = [column for column in existing_fabric_columns if series[column] != 0]
                # if storage is connected to any of the fabrics in current fabric_name
                if connected_fabric_columns:
                    detected_fabric_name_lst.append(fabric_name)
                    symmetric_fabric_lst.extend(existing_fabric_columns)
                    # if storage is connected to less fabrics then there is in current fabric_name
                    # or if storage connected to all fabrics but ports quantity differs
                    if connected_fabric_columns != existing_fabric_columns:
                        single_fabric_name_lst.append(fabric_name)
                    elif series[connected_fabric_columns].nunique() != 1:
                        unsymmetric_fabric_name_lst.append(fabric_name)

            # if asymetric fabric_names was found
            if unsymmetric_fabric_name_lst:
                unsymmetric_fabric_str = 'unsymmetric_connection'
                # add asymetic fabric_names if storage connected to more then one fabric_names
                if len(detected_fabric_name_lst) > 1:
                    unsymmetric_fabric_str = unsymmetric_fabric_str + ' in ' + ', '.join(unsymmetric_fabric_name_lst)
            # if single fabric_label connected fabric_names was found
            if single_fabric_name_lst:
                single_fabric_str = 'single_fabric_connection'
                # add nonredundant fabric_names if storage connected to more then one fabric_names
                if len(detected_fabric_name_lst) > 1:
                    single_fabric_str = single_fabric_str + ' in ' + ', '.join(single_fabric_name_lst) 
            # if storage connected to more than one fabric_names and 
            # port quantity is not equal across all fabrics which storage is connected to (san)
            if len(detected_fabric_name_lst) > 1 and series[symmetric_fabric_lst].nunique() != 1:
                unsymmetric_san_str =  'unsymmetric_san_connection'
            # join fabric and san unsymmetrical notes
            notes = [unsymmetric_fabric_str, single_fabric_str, unsymmetric_san_str]
            if any(notes):
                notes = [note for note in notes if note]
                return ', '.join(notes)
            

    def verify_port_parity(series, fabric_names_lst):
        """Function to check if ports with odd and even indexes are connected to single
        Fabric_label in Fabric_name"""
        
        # list of Fabric_names where storage is connected at least to one of the Fabric_labels
        detected_fabric_names_lst = []
        # list of Fabric_names where port parity is not observed
        broken_parity_fabric_name_lst = []
        broken_parity_fabric_str = ''
        
        if series['Group_type'] in ['port_parity']:
            for fabric_name in fabric_names_lst:
                # find all fabrics in current fabric_name
                existing_fabric_columns = [column for column in series.index if fabric_name in column]
                # find fabrics to which storage is connected in current fabric_name
                connected_fabric_columns = [column for column in existing_fabric_columns if series[column] != 0]
                # if storage is connected to any of the fabrics in current fabric_name
                if connected_fabric_columns:
                    detected_fabric_names_lst.append(fabric_name)
                    # port parity group is connected to more then one fabrics in the current fabric_name
                    if len(connected_fabric_columns) != 1:
                        broken_parity_fabric_name_lst.append(fabric_name)
            
            if broken_parity_fabric_name_lst:
                broken_parity_fabric_str = 'multiple fabrics connection'
                # add broken port parity fabric_names if storage connected to more then one fabric_names
                if len(detected_fabric_names_lst) > 1:
                    broken_parity_fabric_str = broken_parity_fabric_str + ' in ' + ', '.join(broken_parity_fabric_name_lst)
                return broken_parity_fabric_str

    
    def verify_virtual_port_login(storage_connection_statistics_df):
        """Function to verify if virtual port(s) exist behind storage physical port"""

        storage_connection_statistics_df['Physical_virtual_unique_quantity'] = \
            storage_connection_statistics_df.groupby(by=['deviceSubtype', 'Device_Host_Name', 'Group_type', 'Group_level'])['All'].transform('nunique')
        
        mask_virtual_port_absence = storage_connection_statistics_df['Physical_virtual_unique_quantity'] == 1
        mask_virtual_port = storage_connection_statistics_df['Physical_virtual_unique_quantity'] > 1
        mask_physical_virtual = storage_connection_statistics_df['FLOGI'] == 'physical_virtual'
        storage_connection_statistics_df.loc[mask_virtual_port & mask_physical_virtual, 'Virtual_port_note'] = 'virtual_port_login'

        # storage_connection_statistics_df['Virtual_port_note'] = storage_connection_statistics_df['Physical_virtual_unique_quantity'].where(mask_virtual_port_absence, 'virtual_port_login')
        storage_connection_statistics_df.drop(columns=['Physical_virtual_unique_quantity'], inplace=True)
        return  storage_connection_statistics_df
    
    # add note if all storage ports with the same index connected to single fabric
    # column Fabric contains combination of Fabric_name and Fabric_label columns  
    fabric_lst = storage_ports_df['Fabric'].unique()
    mask_port_level = storage_connection_statistics_df['Group_type'].isin(['port'])
    # if value in All column is equal to value in one of fabrics columns then all ports with current index connected to single fabric
    mask_port_fabric_connection = storage_connection_statistics_df[fabric_lst].isin(storage_connection_statistics_df['All']).any(axis=1)                        
    storage_connection_statistics_df.loc[mask_port_level & ~mask_port_fabric_connection, 'Port_note'] = 'multiple fabrics connection'

    # symmetry and port parity connection are verified for each Fabric_name
    fabric_names_lst = storage_ports_df['Fabric_name'].unique()
    # add symmetric connection note
    storage_connection_statistics_df['Symmetric_note'] = \
        storage_connection_statistics_df.apply(lambda series: verify_connection_symmetry(series, fabric_names_lst), axis = 1)
    # add port parity connection note
    storage_connection_statistics_df['Port_parity_note'] = \
        storage_connection_statistics_df.apply(lambda series: verify_port_parity(series, fabric_names_lst), axis = 1)
    # add virtual port login note
    storage_connection_statistics_df = verify_virtual_port_login(storage_connection_statistics_df)
    return storage_connection_statistics_df
