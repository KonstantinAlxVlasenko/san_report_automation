"""Module to add notes to npiv_statistics_df DataFrame"""

import re
import numpy as np
import pandas as pd

from common_operations_dataframe import сoncatenate_columns, dataframe_fillna


def add_notes(npiv_statistics_df, portshow_npiv_cp_df, link_group_columns, re_pattern_lst):
    """Function to add notes to npiv_statistics_df DataFrame"""

    def connection_note(npiv_statistics_df):
        """Function to verify if any links out of trunk present 
        (if trunking licences are on both switches) and if connection is redundant"""
        
        npiv_statistics_df['Connection_note'] = np.nan
        # connection have single link only
        mask_single_link_connection = npiv_statistics_df['Port_quantity'] == 1
        if 'F_Trunk' in npiv_statistics_df.columns:
            # mask link out of trunk presence
            mask_fport_exceeds_ftrunk = npiv_statistics_df['Port_quantity'] > npiv_statistics_df['F_Trunk']
            # trunking licence present on both switches
            mask_trunk_lic =  npiv_statistics_df['Trunking_lic_both_switches'] == 'Yes'
            # summary ftrunk absensce mask (at least one link is not set as porttrunkarea when both switches are trunk capable)
            mask_ftrunk_absence = ~mask_single_link_connection & mask_fport_exceeds_ftrunk & mask_trunk_lic
            npiv_statistics_df.loc[mask_ftrunk_absence, 'Connection_note'] = 'link(s)_out_of_trunk'

            # columns with port quantity for each link (Link_1, Link_2 etc)
            link_columns = [column for column in npiv_statistics_df.columns if re.match('Link_\d+', column)]
            # mask all links are set as porttrinkarea
            mask_fport_equals_ftrunk = npiv_statistics_df['Port_quantity'] == npiv_statistics_df['F_Trunk']
            # mask at least one link consists of single port
            mask_single_port_link = (npiv_statistics_df[link_columns] == 1).any(axis=1)
            # mask there are more then one link in connection, all links are porttrunkarea and 
            # at least one link is single port (single link trunk)
            mask_single_link_trunk = ~mask_single_link_connection & mask_fport_equals_ftrunk & mask_single_port_link
            npiv_statistics_df.loc[mask_single_link_trunk, 'Connection_note'] = 'single link trunk(s)'

            # mask there are single link connection and link is porttrunkarea (nonredundat_trunk)
            mask_nonredundat_trunk = mask_single_link_connection & mask_fport_equals_ftrunk
            npiv_statistics_df.loc[mask_nonredundat_trunk, 'Connection_note'] = 'nonredundat_trunk'

        # there are single link in connection and it's not trunk (nonredundant_connection)
        mask_note_empty = npiv_statistics_df['Connection_note'].isna()
        npiv_statistics_df.loc[mask_single_link_connection & mask_note_empty, 'Connection_note'] = 'nonredundant_connection'
        return npiv_statistics_df
    
    def single_vc_note(npiv_statistics_df, portshow_npiv_cp_df):
        """Function to verify if all NPIV links are in the same virtal channel.
        Trunks and single link are excluded"""
        
        # count quantity of unique VC and Links
        vc_link_columns = ['Virtual_Channel', 'Link']
        vc_notes_df = portshow_npiv_cp_df.groupby(by=link_group_columns)[vc_link_columns].nunique()
        # single VC note when there are more then one link between switches
        # but all links are in the same Virtual channel 
        mask_vc_single = vc_notes_df['Virtual_Channel'] == 1
        mask_multiple_links = vc_notes_df['Link'] > 1
        vc_notes_df['Single_VC_note'] = np.where(mask_vc_single & mask_multiple_links, 'single_VC', pd.NA)
        vc_notes_df.reset_index(inplace=True)
        # add note to npiv_statistics_df
        npiv_statistics_df = dataframe_fillna(npiv_statistics_df, vc_notes_df, filled_lst=['Single_VC_note'], 
                                              join_lst=link_group_columns)
        return npiv_statistics_df
    
    def nonuniformity_note(npiv_statistics_df, portshow_npiv_cp_df):
        """Function to verify if values in each group of link parameters are unique
        for Native and AG link groups. If not then parameter is shown. 
        Group of parameters are Speed_Gbps, port settings."""
        
        # drop columns with service status for later use of that columns as settings values
        portshow_npiv_cp_df.drop(columns=['FEC', 'Credit_Recovery'], inplace=True)
        replace_columns_dct = {'Link_speedActual': 'Speed_Gbps', 
                               'FEC_cfg': 'FEC', 'Connected_FEC_cfg': 'Connected_FEC',
                               'Trunk_Port': 'Trunk', 'Connected_Trunk_Port': 'Connected_Trunk',
                               'QOS_Port': 'QOS', 'Connected_QOS_Port': 'Connected_QOS',
                               'Credit_Recovery_cfg': 'Credit_Recovery', 
                               'Connected_Credit_Recovery_cfg': 'Connected_Credit_Recovery'}
        portshow_npiv_cp_df.rename(columns=replace_columns_dct, inplace=True)
           
        # columns to count values for native mode switch 
        count_native_columns = ['Transceiver_speed', 'Transceiver_category',
                                'Speed_Cfg', 'Trunk', 'QOS', 
                                'FEC', '10G/16G_FEC', 'Credit_Recovery']
        # columns to count values for ag mode switch
        count_ag_columns = ['Connected_' + column for column in count_native_columns]
        # columns to count values both for native and ag mode switches (link participants)
        count_columns = ['Speed_Gbps', *count_native_columns, *count_ag_columns]
        count_columns = [column for column in count_columns if column in portshow_npiv_cp_df.columns]
        
        # fillna values with OFF to locate nonuniformity coz na values are not taken into account
        # and nonuniform parameter might be missed
        portcfg_native_columns = count_native_columns[3:].copy()
        portcfg_ag_columns = ['Connected_' + column for column in portcfg_native_columns]
        portcfg_columns = [*portcfg_native_columns, *portcfg_ag_columns]
        portcfg_columns = [column for column in portcfg_columns if column in portshow_npiv_cp_df.columns]
        for column in portcfg_columns:
            portshow_npiv_cp_df[column] = portshow_npiv_cp_df[column].fillna('OFF')
        
        # count unique values in count_columns
        nonuniformity_notes_df = portshow_npiv_cp_df.groupby(by=link_group_columns)[count_columns].nunique()
        
        # columns names with values where nonuniformity is found 
        note_native_columns = ['Native_' + column + '_nonuniformity_note' for column in count_native_columns
                               if column in portshow_npiv_cp_df.columns]
        note_ag_columns = ['AG_' + column + '_nonuniformity_note' for column in count_native_columns 
                           if 'Connected_' + column in portshow_npiv_cp_df.columns]
        note_columns = ['Speed_Gbps_nonuniformity_note', *note_native_columns, *note_ag_columns]
        
        # fill nonuniformity note columns with corresponding column name
        # where nonuniformity is found (number of unique values is greater then 1)
        for count_column, note_column in zip(count_columns, note_columns):
            # mask if number of unique values in count_columns exceeds one
            mask_nonuniformity = nonuniformity_notes_df[count_column] > 1
            note = count_column.lstrip('Connected_').lower()
            # add column name where nonuniformity is found to note column
            nonuniformity_notes_df[note_column] = np.where(mask_nonuniformity, note, pd.NA)
        
        # replace pd.NA with np.nan
        nonuniformity_notes_df.fillna(np.nan, inplace=True)
        # merge logically related note columns
        for tag in ('Native_', 'AG_'):
            # columns with transceiver category and speed
            transeivers_columns = [tag + column + '_nonuniformity_note' for column in count_native_columns[:2]
                                   if column in portshow_npiv_cp_df.columns]
            # columns with port settings
            cfg_note_columns = [tag + column + '_nonuniformity_note' for column in count_native_columns[2:]
                                   if column in portshow_npiv_cp_df.columns]
            # merge transceivers nonuniformity note columns
            nonuniformity_notes_df = сoncatenate_columns(nonuniformity_notes_df, 
                                                      summary_column=tag + 'Transceiver_nonuniformity_note', 
                                                      merge_columns=transeivers_columns)
            # merge port settings nonuniformity note columns
            nonuniformity_notes_df = сoncatenate_columns(nonuniformity_notes_df, 
                                                      summary_column=tag + 'Portcfg_nonuniformity_note', merge_columns=cfg_note_columns)
        # drop columns with unique values quantity
        nonuniformity_notes_df.drop(columns=count_columns, inplace=True)
        nonuniformity_notes_df.reset_index(inplace=True)
        # drop allna columns
        nonuniformity_notes_df.dropna(axis=1, how='all', inplace=True)
        # add notes columns to npiv_statistics_df DataFrame
        npiv_statistics_df = npiv_statistics_df.merge(nonuniformity_notes_df, how='left', on=link_group_columns)
        return npiv_statistics_df
    
    
    # def speed_note(npiv_statistics_df, re_pattern_lst):
    def speed_note(npiv_statistics_df, re_pattern_lst):
        """Function to verify link speed value and mode. Adds note if speed is in auto mode,
        speed is low (1-4 Gbps) or reduced (lower then port could provide), speed is not uniform
        for all links between pair of switches"""

        # regular expression patterns
        *_, comp_dct = re_pattern_lst
        
        # low speed note
        low_speed_regex = comp_dct['low_speed']
        # low_speed_regex = r'^(?:Native_|AG_)?N?[124]G?$' # 1G, N1, 2G, N2, 4G, N4 # TO_REMOVE
        low_speed_columns = [column for column in npiv_statistics_df.columns if re.search(low_speed_regex, column)]
        # if low speed port present 
        npiv_statistics_df['Speed_low_note'] = pd.NA
        if low_speed_columns:
            # devices with low speed ports not equal to zero
            mask_low_speed = (npiv_statistics_df[low_speed_columns] != 0).any(axis=1)
            npiv_statistics_df['Speed_low_note'] = np.where(mask_low_speed, 'low_speed', pd.NA)
        # reduced speed note
        npiv_statistics_df['Speed_reduced_note'] = pd.NA
        if 'Speed_Reduced' in npiv_statistics_df.columns:
            mask_speed_reduced = npiv_statistics_df['Speed_Reduced'].notna() & npiv_statistics_df['Speed_Reduced'] != 0
            npiv_statistics_df['Speed_reduced_note'] = np.where(mask_speed_reduced, 'reduced_speed', pd.NA)
        # auto speed note
        auto_speed_regex = comp_dct['auto_speed']
        # auto_speed_regex = r'^(?:Native_|AG_)?Speed_Auto$' # TO_REMOVE
        auto_speed_columns = [column for column in npiv_statistics_df.columns if re.search(auto_speed_regex, column)]
        npiv_statistics_df['Speed_auto_note'] = pd.NA
        if auto_speed_columns:
            # devices with auto speed ports not equal to zero
            mask_auto_speed = (npiv_statistics_df[auto_speed_columns] != 0).any(axis=1)
            npiv_statistics_df['Speed_auto_note'] = np.where(mask_auto_speed, 'auto_speed', pd.NA)
        # merge speed related notes into single column
        speed_note_columns = ['Speed_auto_note', 'Speed_low_note', 'Speed_reduced_note', 'Speed_Gbps_nonuniformity_note']
        npiv_statistics_df = сoncatenate_columns(npiv_statistics_df, summary_column='Speed_note', 
                                                 merge_columns=speed_note_columns, drop_merge_columns=True)
        return npiv_statistics_df
    
    # add notes to npiv_statistics_df DataFrame
    npiv_statistics_df = connection_note(npiv_statistics_df)
    npiv_statistics_df = single_vc_note(npiv_statistics_df, portshow_npiv_cp_df)
    npiv_statistics_df = nonuniformity_note(npiv_statistics_df, portshow_npiv_cp_df)
    # npiv_statistics_df = speed_note(npiv_statistics_df, re_pattern_lst)
    npiv_statistics_df = speed_note(npiv_statistics_df, re_pattern_lst)
    npiv_statistics_df.fillna(np.nan, inplace=True)
    return npiv_statistics_df