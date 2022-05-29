"""Module to add notes to isl_statistics_df DataFrame"""

import re
import numpy as np
import pandas as pd


import utilities.dataframe_operations as dfop
# import utilities.database_operations as dbop
# import utilities.data_structure_operations as dsop
# import utilities.module_execution as meop
# import utilities.servicefile_operations as sfop
# import utilities.filesystem_operations as fsop

# from common_operations_dataframe import сoncatenate_columns

def add_notes(isl_statistics_df, isl_aggregated_modified_df, isl_group_columns, pattern_dct):
    """Function to add notes to isl_statistics_df DataFrame"""

    if not 'LISL' in isl_statistics_df.columns:
        isl_statistics_df['LISL'] = 0
    # inter switch connection have physical links (not logical only)
    # logical link always displayed as single link
    

    def connection_note(isl_statistics_df):
        """Function to verify if any links out of trunk present (if trunking licences are on both switches)
        and if connection is redundant"""
        
        # trunk absence note
        # columns with ISL tag (links quantity in ISL) in isl_statistics_df
        isl_columns = [column for column in isl_statistics_df.columns if ('ISL' in column or 'IFL' in column)]
        # conditions for trunk presence
        # switches connected with more then one ISL (excluding LISL)
        mask_port_quantity = (isl_statistics_df['Port_quantity'] - isl_statistics_df['LISL']) > 1
        # link out of trunk present (any ISL contains single link)
        mask_single_link_isl = (isl_statistics_df[isl_columns] == 1).any(axis=1)
        # trunkimg licence present on both switches
        mask_trunk_lic =  isl_statistics_df['Trunking_lic_both_switches'] == 'Yes'
        mask_trunk_absence = mask_port_quantity & mask_single_link_isl & mask_trunk_lic
        # TO_REMOVE
        # isl_statistics_df['Connection_note'] = np.where(mask_trunk_absence, 'trunk_missing', pd.NA)
        isl_statistics_df.loc[mask_trunk_absence, 'Connection_note'] = 'link(s)_out_of_trunk'
        
        """
        nonredundant connection
        Port_Quantity LISL Difference Status
        1             0    1          nonredundant
        1             1    0          redundant
        2             0    2          redundant
        2             1    1          nonredundant
        3             0    3          redundant
        3             1    2          redundant
        Summary: Difference == 1 is nonredundant
        """
        mask_nonredundant_link = (isl_statistics_df['Port_quantity'] - isl_statistics_df['LISL']) == 1
        # TO_REMOVE
        # isl_statistics_df['Connection_note'] = np.where(mask_nonredundant_link, 'nonredundant_connection', pd.NA)
        isl_statistics_df.loc[mask_nonredundant_link, 'Connection_note'] = 'nonredundant_connection'
        
        return isl_statistics_df
    
    
    def nonuniformity_note(isl_statistics_df, isl_aggregated_modified_df):
        """Function to verify if values of each group of ISL parameters are unique.
        If not then nonuniformity note is added. Group of parameters are portType, Speed_gbps,
        Distance and port settings"""
        
        count_columns = ['portType', 'Bandwidth_Gbps', 'Distance', 
                         'Transceiver_mode', 'Transceiver_speed', 
                         'Speed_Cfg', 'TRUNK', 'Encryption', 'Compression', 'QOS', 
                         'FEC', 'Long_Distance', 'VC_Link_Init', 'ISL_R_RDY_Mode']
        # count quantity of unique values in count_columns
        nonuniformity_notes_df = isl_aggregated_modified_df.groupby(by=isl_group_columns)[count_columns].nunique()
        # rename column in nonuniformity_notes_df DataFrame and count_columns list
        replace_columns_dct = {'Bandwidth_Gbps': 'Speed_Gbps'}
        nonuniformity_notes_df.rename(columns=replace_columns_dct, inplace=True)
        count_columns = [replace_columns_dct.get(x, x) for x in count_columns]
        note_columns = [column + '_nonuniformity_note' for column in count_columns]
        
        for count_column, note_column in zip(count_columns, note_columns):
            # mask if number of unique values in count_columns exceeds one
            mask_nonuniformity = nonuniformity_notes_df[count_column] > 1
            if count_column in count_columns[:3]:
                note = count_column.lower() + '_nonuniformity'
            else:
                note = count_column.lower()
            # add current note_column for parameter in current column 
            nonuniformity_notes_df[note_column] = np.where(mask_nonuniformity, note, pd.NA)
        
        # replace pd.NA with np.nan
        nonuniformity_notes_df.fillna(np.nan, inplace=True)
        # merge logically related note columns
        nonuniformity_notes_df = dfop.concatenate_columns(nonuniformity_notes_df, 
                                                  summary_column='Transceiver_nonuniformity_note', merge_columns=note_columns[3:5])
        nonuniformity_notes_df = dfop.concatenate_columns(nonuniformity_notes_df, 
                                                  summary_column='Portcfg_nonuniformity_note', merge_columns=note_columns[5:])
        # drop columns with unique values quantity
        nonuniformity_notes_df.drop(columns=count_columns, inplace=True)
        nonuniformity_notes_df.reset_index(inplace=True)
        
        # add notes columns to isl_statistics_df DataFrame
        isl_statistics_df = isl_statistics_df.merge(nonuniformity_notes_df, how='left', on=isl_group_columns)
    
        return isl_statistics_df
    
    
    def speed_note(isl_statistics_df, pattern_dct):
        """Function to verify ISL speed value and mode. Adds note if speed is in auto mode,
        speed is low (1-4 Gbps) or reduced (lower then port could provide), speed is not uniform
        for all links between pair of switches"""

        # regular expression patterns
        # comp_keys, _, comp_dct = re_pattern_lst

        # low speed note
        low_speed_regex = pattern_dct['low_speed']
        # low_speed_regex = r'^N?[124]G?$' # 1G, N1, 2G, N2, 4G, N4 # TO_REMOVE
        low_speed_columns = [column for column in isl_statistics_df.columns if re.search(low_speed_regex, column)]
        # if low speed port present 
        isl_statistics_df['Speed_low_note'] = pd.NA
        if low_speed_columns:
            # devices with low speed ports not equal to zero
            mask_low_speed = (isl_statistics_df[low_speed_columns] != 0).any(axis=1)
            isl_statistics_df['Speed_low_note'] = np.where(mask_low_speed, 'low_speed', pd.NA)
            
        # reduced speed note
        isl_statistics_df['Speed_reduced_note'] = pd.NA
        if 'Speed_Reduced' in isl_statistics_df.columns:
            mask_speed_reduced =  isl_statistics_df['Speed_Reduced'].notna() & isl_statistics_df['Speed_Reduced'] != 0
            isl_statistics_df['Speed_reduced_note'] = np.where(mask_speed_reduced, 'reduced_speed', pd.NA)
        
        # auto speed note (xisl excluded)
        isl_statistics_df['Speed_auto_note'] = pd.NA
        if 'Speed_Auto' in isl_statistics_df.columns:
            mask_speed_auto = isl_statistics_df['Speed_Auto'] != 0
            mask_not_xisl_only = isl_statistics_df['Port_quantity'] > isl_statistics_df['LISL']
            isl_statistics_df['Speed_auto_note'] = np.where(mask_speed_auto & mask_not_xisl_only, 'auto_speed', pd.NA)
        
        speed_note_columns = ['Speed_auto_note', 'Speed_low_note', 'Speed_reduced_note', 'Speed_Gbps_nonuniformity_note']
        isl_statistics_df = dfop.concatenate_columns(isl_statistics_df, summary_column='Speed_note', merge_columns=speed_note_columns, drop_merge_columns=True)
        
        return isl_statistics_df
    

    def connection_pair_absent_note(isl_statistics_df):
        """Function to verify if pair connection exist in another Fabric_label"""
    
        mask_connection_pair_absent = isl_statistics_df.groupby(by=['Fabric_name', 'switchPair_id', 'Connected_switchPair_id'])['switchWwn'].transform('count') < 2
        isl_statistics_df.loc[mask_connection_pair_absent , 'Connection_pair_absence_note'] = 'connection_pair_absent'
        isl_statistics_df['Asymmetry_note'].fillna(isl_statistics_df['Connection_pair_absence_note'], inplace=True)
        isl_statistics_df.drop(columns=['Connection_pair_absence_note'], inplace=True)
        return isl_statistics_df

    def single_connection_asymmetry_note(isl_statistics_df):
        """Function to verify if connection is symmetric from both switches point of view.
        Connection might be not symmetric if isl configuration in files mismatch.
        It happens if changes were made between supportsave collection"""
    
        # connection ID is IDs of both switches
        isl_statistics_df['switchPair_id_str'] = isl_statistics_df['switchPair_id'].astype(int).astype(str)
        isl_statistics_df['Connected_switchPair_id_str'] = isl_statistics_df['Connected_switchPair_id'].astype(int).astype(str)
        isl_statistics_df = dfop.merge_columns(isl_statistics_df, summary_column='Connection_ID', 
                                                                        merge_columns=['switchPair_id_str', 'Connected_switchPair_id_str'],
                                                                        sort_summary=True)
        # verify link and port quantity mismatch
        isl_statistics_df = dfop.verify_group_symmetry(isl_statistics_df, symmetry_grp=['Fabric_name', 'Fabric_label', 'Connection_ID'], 
                                                    symmetry_columns=[ 'Logical_link_quantity', 'Physical_link_quantity', 'Port_quantity', 'Bandwidth_Gbps'], 
                                                    summary_column='Single_connection_asymmetry_note')
        return isl_statistics_df


    # add notes to isl_statistics_df DataFrame
    isl_statistics_df = connection_note(isl_statistics_df)
    
    isl_statistics_df = nonuniformity_note(isl_statistics_df, isl_aggregated_modified_df)
    isl_statistics_df = speed_note(isl_statistics_df, pattern_dct)
    # add pair connection asymmetry note
    isl_statistics_df = dfop.verify_group_symmetry(isl_statistics_df, symmetry_grp=['Fabric_name','switchPair_id', 'Connected_switchPair_id'], 
                                                    symmetry_columns=['Logical_link_quantity', 'Physical_link_quantity', 'Port_quantity', 'Bandwidth_Gbps'])
    isl_statistics_df = single_connection_asymmetry_note(isl_statistics_df)
    isl_statistics_df = connection_pair_absent_note(isl_statistics_df)
    isl_statistics_df.fillna(np.nan, inplace=True)

    if (isl_statistics_df['LISL'] == 0).all():
        isl_statistics_df.drop(columns=['LISL'], inplace=True)
    return isl_statistics_df
