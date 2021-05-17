"""Module contains functions related to storage connections"""


import re

import numpy as np
import pandas as pd

from common_operations_dataframe import (convert_wwn, count_frequency,
                                         dataframe_fillna,
                                         extract_values_from_column,
                                         sequential_equality_note)


def storage_3par_fillna(portshow_aggregated_df, system_3par_df, port_3par_df):
    """Function to add 3PAR information collected from 3PAR configuration files to
    portshow_aggregated_df"""

    if not port_3par_df.empty and not system_3par_df.empty:
        # system information
        system_columns = ['configname', 'System_Model', 'System_Name', 
                            'Serial_Number', 'IP_Address', 'Location']
        system_3par_cp_df = system_3par_df[system_columns].copy()
        system_3par_cp_df.drop_duplicates(inplace=True)

        # add system information to 3PAR ports DataFrame
        system_port_3par_df = port_3par_df.merge(system_3par_cp_df, how='left', on=['configname'])
        # convert Wwnn and Wwnp to regular represenatation (lower case with colon delimeter)
        system_port_3par_df = convert_wwn(system_port_3par_df, ['NodeName', 'PortName'])
        # rename columns to correspond portshow_aggregated_df
        rename_columns = {'System_Name': 'Device_Name',	'System_Model':	'Device_Model', 
                            'Serial_Number': 'Device_SN', 'Location': 'Device_Location'}
        system_port_3par_df.rename(columns=rename_columns, inplace=True)
        system_port_3par_df['Device_Host_Name'] = system_port_3par_df['Device_Name']


        system_port_3par_df = storage_port_partner(system_port_3par_df, portshow_aggregated_df)


        # add 3PAR information to portshow_aggregated_df
        fillna_wwnn_columns = ['Device_Name', 'Device_Host_Name', 'Device_Model', 'Device_SN', 'IP_Address', 'Device_Location']
        portshow_aggregated_df = \
            dataframe_fillna(portshow_aggregated_df, system_port_3par_df, join_lst=['NodeName'] , filled_lst=fillna_wwnn_columns)

        fillna_wwnp_columns = ['Storage_Port_Partner_Fabric_name', 'Storage_Port_Partner_Fabric_label', 
                                'Storage_Port_Partner', 'Storage_Port_Partner_Wwnp', 
                                'Storage_Port_Mode', 'Storage_Port_Type']
        portshow_aggregated_df = \
            dataframe_fillna(portshow_aggregated_df, system_port_3par_df, join_lst=['PortName'] , filled_lst=fillna_wwnp_columns)

        portshow_aggregated_df = sequential_equality_note(portshow_aggregated_df, 
                                                            columns1=['Fabric_name', 'Fabric_label'], 
                                                            columns2=['Storage_Port_Partner_Fabric_name', 'Storage_Port_Partner_Fabric_label'], 
                                                            note_column='Storage_Port_Partner_Fabric_equal')

    # if 3PAR configuration was not extracted apply reserved name (3PAR model and SN combination)
    if 'Device_Name_reserved' in portshow_aggregated_df.columns:
        portshow_aggregated_df['Device_Host_Name'].fillna(portshow_aggregated_df['Device_Name_reserved'], inplace = True)

    return portshow_aggregated_df


def storage_port_partner(system_port_3par_df, portshow_aggregated_df):
    """Function to add 3PAR port partner (faiolver port) Wwnp and fabric connection information to system_port_3par_df"""

    # add port partner Wwnp to system_port_3par_df
    system_port_partner_3par_df = system_port_3par_df[['configname', 'Storage_Port', 'PortName']].copy()
    system_port_partner_3par_df.rename(columns={'Storage_Port': 'Storage_Port_Partner', 'PortName': 'Storage_Port_Partner_Wwnp'}, inplace=True)
    system_port_3par_df = dataframe_fillna(system_port_3par_df, system_port_partner_3par_df, 
                                            filled_lst=['Storage_Port_Partner_Wwnp'], 
                                            join_lst=['configname', 'Storage_Port_Partner'])

    # DataDrame containing all Wwnp in san
    fabric_wwnp_columns = ['Fabric_name', 'Fabric_label', 'PortName']
    portshow_fabric_wwnp_df = portshow_aggregated_df[fabric_wwnp_columns].copy()
    portshow_fabric_wwnp_df.dropna(subset=fabric_wwnp_columns, inplace=True)
    portshow_fabric_wwnp_df.drop_duplicates(inplace=True)
    
    # rename portshow_fabric_wwnp_df columns to correspond columns in system_port_partner_3par_df DataDrame
    storage_port_partner_columns = ['Storage_Port_Partner_Fabric_name', 'Storage_Port_Partner_Fabric_label', 'Storage_Port_Partner_Wwnp']
    rename_dct = dict(zip(fabric_wwnp_columns, storage_port_partner_columns))
    portshow_fabric_wwnp_df.rename(columns=rename_dct, inplace=True)
    # fill in Fabric connection information of failover ports
    system_port_3par_df = dataframe_fillna(system_port_3par_df, portshow_fabric_wwnp_df, 
                                            join_lst=storage_port_partner_columns[2:], 
                                            filled_lst=storage_port_partner_columns[:2])
    return system_port_3par_df


def storage_connection_statistics(portshow_aggregated_df, re_pattern_lst):
    """Aggregated function to count statistics of storage (3PAR, MSA) port connections to Fabrics and
    verify if storages connected properly (ports distrubuted symmetrically, odd and even port
    indexes are in different Fabrics)"""

    # regular expression patterns
    comp_keys, _, comp_dct = re_pattern_lst

    # find storages (3PAR, MSA) with non-empty ports numbers in portshow_aggregated_df
    mask_storage_type = portshow_aggregated_df['deviceSubtype'].str.lower().isin(['3par', 'msa', 'emc'])
    mask_storage_port = portshow_aggregated_df['Device_Port'].notna()
    storage_columns = ['Fabric_name', 'Fabric_label', 'Device_Host_Name', 'Device_Port', 'deviceSubtype']
    storage_ports_df = portshow_aggregated_df.loc[mask_storage_type & mask_storage_port, storage_columns ].copy()
    storage_ports_df.drop_duplicates(inplace=True)

    # extract controller, slot, port indexes
    pattern_columns_lst = [(comp_dct['3par_ctrl_slot_port'], ['Controller', 'Slot', 'Port']),
                            (comp_dct['emc_ctrl_slot_port'], ['Controller', 'Slot', 'Port']), 
                            (comp_dct['msa_ctrl_port'], ['Controller', 'Port']),]
    storage_ports_df = extract_values_from_column(storage_ports_df, 'Device_Port', pattern_columns_lst)

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

    # count connection statistics for each port group level 
    # (Storage, Controller, Slot, Port, Port_parity, Controller_Slot)
    storage_connection_statistics_df = pd.DataFrame()
    for level in ['Storage', *component_level_lst, 'Controller_Slot']:
        group_columns = ['deviceSubtype', 'Device_Host_Name', level]
        current_df = count_frequency(storage_ports_df, count_columns=['Fabric'], 
                                    group_columns=group_columns, margin_column_row=[(True, False)])
        # rename current port group level to 'Group_level' to concatenate DataFrames
        current_df.rename(columns={level: 'Group_level'}, inplace=True)
        current_df['Group_type'] = level.lower()
        if storage_connection_statistics_df.empty:
            storage_connection_statistics_df = current_df.copy()
        else:
            storage_connection_statistics_df = pd.concat([storage_connection_statistics_df, current_df])

    if not storage_connection_statistics_df.empty:
        # sort port groups levels in corresponding order
        sort_priority = {'storage': 1, 'port_parity': 2, 'port': 3, 'controller': 4, 'controller_slot': 5, 'slot': 6}
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

        # move Group_type column
        group_type_values = storage_connection_statistics_df['Group_type']
        storage_connection_statistics_df.drop(columns=['Group_type'], inplace=True)
        storage_connection_statistics_df.insert(2, 'Group_type', group_type_values)
    return storage_connection_statistics_df


def add_notes(storage_connection_statistics_df, storage_ports_df):
    """Function to add notes to storage_connection_statistics_df DataFrame"""

    def verify_connection_symmetry(series, fabric_names_lst):
        """Function to check if ports are connected symmetrically in each Fabric_name and across san.
        Symmetrically connected storage means that each fabric in san have same amount of ports connected. 
        Ports are grouped baseв on the levels: 
        storage, controllers, slots across each controller, slots across storage"""

        # Fabric_names where storage port connection is unsymmtrical (port quantity differs across the Fabric_labels)
        unsymmetric_fabric_name_lst = []
        # list of Fabric_names where storage is connected at least to one of the Fabric_labels
        detected_fabric_name_lst = []
        # list of Fabrics (concatenation of Fabric_name and Fabric_label) where connection is detected to any
        # of the Fabric_labels of Fabric_name. To be symmetrically connected across san all ports quantity 
        # values have to be equal in all Fabrics of this list
        symmetric_fabric_lst = []
        unsymmetric_fabric_str = ''
        unsymmetric_san_str = ''
        
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
                    # if storage connected to less fabrics then there is in current fabric_name
                    # or if storage connected to all fabrics but ports quantity differs
                    if connected_fabric_columns != existing_fabric_columns or series[connected_fabric_columns].nunique() != 1:
                        unsymmetric_fabric_name_lst.append(fabric_name)

            # if ussymetric fabric_names was found
            if unsymmetric_fabric_name_lst:
                unsymmetric_fabric_str = 'unsymmetric_connection'
                # add assymetic fabric_names if storage connected to more then one fabric_names
                if len(detected_fabric_name_lst) > 1:
                    unsymmetric_fabric_str = unsymmetric_fabric_str + ' in ' + ', '.join(unsymmetric_fabric_name_lst)
            # if storage connected to more than one fabric_names and port quantity is not equal across all fabrics (san)
            if len(detected_fabric_name_lst) > 1 and series[symmetric_fabric_lst].nunique() != 1:
                unsymmetric_san_str =  'unsymmetric_san_connection'
            # join fabric and san unsymmetrical notes
            if unsymmetric_fabric_str or unsymmetric_san_str:
                notes = [unsymmetric_fabric_str, unsymmetric_san_str]
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
    return storage_connection_statistics_df
