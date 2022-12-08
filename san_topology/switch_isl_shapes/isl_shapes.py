"""Module to create ISL shape related DataFrame. 
ISL shape DataFrame used to drop Native switch connections on Visio page"""

import re
import numpy as np
import pandas as pd

import utilities.dataframe_operations as dfop

from ..shape_details import create_shape_name, count_trunk_links


def create_san_graph_isl(isl_aggregated_df, isl_statistics_df, switch_pair_df, pattern_dct):
    """Function to create isl DataFrame with details required for san diagram"""
    
    if isl_statistics_df.empty:
        return pd.DataFrame()
    
    # extract required columns, drop summary rows
    san_graph_isl_df = get_san_graph_isl_from_isl_statistics(isl_statistics_df, pattern_dct)
    # add front and translate domain connections information
    san_graph_isl_df = add_fd_xd_virtual_links(san_graph_isl_df, switch_pair_df)
    # each link represented from both switches. drop duplicated link rows
    san_graph_isl_df = drop_duplicated_links(san_graph_isl_df, switch_pair_df)
    #  create link description (speed, ls mode, distance, edge fid)
    san_graph_isl_df = add_isl_link_description(san_graph_isl_df, isl_aggregated_df, pattern_dct)
    # create switch, conneceted switch and link shape names
    san_graph_isl_df = create_isl_shape_names(san_graph_isl_df)
    return san_graph_isl_df


def get_san_graph_isl_from_isl_statistics(isl_statistics_df, pattern_dct):
    """Function to initialize san_graph_isl DataFrame with ISL details"""
    
    san_graph_isl_df = isl_statistics_df.copy()
    # drop fabric summary rows
    san_graph_isl_df.dropna(subset=['switchWwn', 'Connected_switchWwn'], inplace=True)
    # count ifl quantity
    count_total_link_type(san_graph_isl_df)
    # drop excessive columns 
    isl_columns = ['Fabric_name', 'Fabric_label', 'SwitchName', 'switchWwn',
                   'Connected_SwitchName', 'Connected_switchWwn', 'Connection_ID',
                   'switchPair_id', 'Connected_switchPair_id',
                   'Logical_link_quantity', 'Physical_link_quantity']
    if not 'LISL' in san_graph_isl_df.columns:
        san_graph_isl_df['LISL'] = 0
    speed_columns = dfop.find_columns(san_graph_isl_df, pattern_dct['speed'])
    distance_columns = dfop.find_columns(san_graph_isl_df, pattern_dct['distance'])
    ls_mode_columns = dfop.find_columns(san_graph_isl_df, pattern_dct['ls_mode'])
    link_quantity_columns = dfop.find_columns(san_graph_isl_df, pattern_dct['link_quantity'])
    
    san_graph_isl_df = san_graph_isl_df[
        [*isl_columns, 'LISL', 'IFL', *speed_columns, *distance_columns, *ls_mode_columns, *link_quantity_columns]
        ].copy()
    san_graph_isl_df.rename(columns={'SwitchName': 'switchName', 'Connected_SwitchName': 'Connected_switchName'}, inplace=True)
    return san_graph_isl_df


def count_total_link_type(san_graph_isl_df):
    """"Function to count total number of IFL links"""

    link_type_pattern = 'IFL_\d+'
    link_type_columns = [column for column in san_graph_isl_df.columns if re.search(link_type_pattern, column)]
    san_graph_isl_df['IFL'] = san_graph_isl_df[link_type_columns].sum(axis=1)


def add_fd_xd_virtual_links(san_graph_isl_df, switch_pair_df):
    """Function to create virtual links between Front Domain and Translate Domains.
    FD present switch from BB fabric to which edge fabric is connected to 
    Number of FD equals to number of connected BB switches. 
    XD represents imported edge fabric. Number of XDs is equal to number of remote edge fabrics.
    Each XD have connection to all FDs."""

    mask_fd = switch_pair_df['LS_type_report'] == 'front_domain'
    mask_xd = switch_pair_df['LS_type_report'] == 'translate_domain'
    
    if not all([mask_fd.any(), mask_xd.any()]):
        return san_graph_isl_df

    fd_xd_columns = ['Fabric_name', 'Fabric_label', 'switchName', 'switchWwn', 'switchPair_id']
    # front domain swithes
    fd_df = switch_pair_df.loc[mask_fd, fd_xd_columns].copy()
    # xlate domain switches
    xd_df = switch_pair_df.loc[mask_xd, fd_xd_columns].copy()
    # xlate domain swithes are connected to front domain swithes
    xd_rename_columns = {column: 'Connected_' + column for column in ['switchName', 'switchWwn', 'switchPair_id']}
    xd_df.rename(columns=xd_rename_columns, inplace=True)
    # find active FD (FDs which are in san_graph_isl_df)
    mask_fd_isl = fd_df['switchWwn'].isin(san_graph_isl_df['Connected_switchWwn'])
    fd_df = fd_df.loc[mask_fd_isl].copy()
    # each FD in fabric is connected to each XD
    fd_xd_link_df = fd_df.merge(xd_df, how='left', on=['Fabric_name', 'Fabric_label'])
    # connection ID is sorted combination if FD ID and XD ID 
    fd_xd_link_df['switchPair_id_str'] = fd_xd_link_df['switchPair_id'].astype(int).astype(str)
    fd_xd_link_df['Connected_switchPair_id_str'] = fd_xd_link_df['Connected_switchPair_id'].astype(int).astype(str)
    fd_xd_link_df = dfop.merge_columns(fd_xd_link_df, summary_column='Connection_ID', 
                                                       merge_columns=['switchPair_id_str', 'Connected_switchPair_id_str'],
                                                       sort_summary=True)
    # number of virtual links is virtual
    fd_xd_link_df['Physical_link_quantity'] = 8
    # add FD XD links to san_graph_isl_df
    san_graph_isl_df = pd.concat([san_graph_isl_df, fd_xd_link_df])
    return san_graph_isl_df


def drop_duplicated_links(san_graph_isl_df, switch_pair_df):
    """Function to drop duplicated Connection_ID rows in sorted san_graph_isl_df.
    Each link is presented from switch and connected switch sides.
    Link switchClass priority order DIR, ENT, MID, ENTRY and then switchType.
    Bigger switch link comes first"""

    # sort links within connection_pair_id
    san_graph_isl_df = dfop.dataframe_fillna(san_graph_isl_df, switch_pair_df, 
                                                join_lst=['switchWwn'], filled_lst=['switchClass', 'switchType'])
    mask_dir = san_graph_isl_df['switchClass'].str.contains('DIR', na=False)
    mask_ent = san_graph_isl_df['switchClass'].str.contains('ENT', na=False)
    mask_mid = san_graph_isl_df['switchClass'].str.contains('MID', na=False)
    mask_entry = san_graph_isl_df['switchClass'].str.contains('ENTRY', na=False)
    san_graph_isl_df['switchWeight'] = np.select([mask_dir, mask_ent, mask_mid, mask_entry], [1, 2, 3, 4], default=5)
    san_graph_isl_df.sort_values(by=['Fabric_name', 'Fabric_label', 'Connection_ID', 
                                     'switchWeight', 'switchType', 'switchName'], 
                                 ascending=[*(True,)*4, False, True],  inplace=True)
    # drop duplicated links
    san_graph_isl_df.drop_duplicates(subset=['Fabric_name', 'Fabric_label', 'Connection_ID'], ignore_index=True, inplace=True)
    return san_graph_isl_df


def add_isl_link_description(san_graph_isl_df, isl_aggregated_df, pattern_dct):
    """Function to create ISL Link description (link speed, ls_mode, distance,
    lisl, ifl details)"""

    # column groups
    speed_columns = dfop.find_columns(san_graph_isl_df, pattern_dct['speed'])
    ls_mode_columns = dfop.rename_columns(san_graph_isl_df, pattern_dct['ls_mode'])
    distance_columns = dfop.rename_columns(san_graph_isl_df, pattern_dct['distance'])
    link_quantity_columns = dfop.find_columns(san_graph_isl_df, pattern_dct['link_quantity'])
    column_grps = [speed_columns, ls_mode_columns, distance_columns,  ['LISL']]
    
    summary_column_names = ['speed_string', 'ls_mode_string', 'distance_string', 'lisl_string']
    
    # create summary string with joined values for each group of columns from column_grps
    for summary_column_name, column_grp in zip(summary_column_names, column_grps):
        san_graph_isl_df[summary_column_name] = san_graph_isl_df.apply(lambda series: dfop.concatenate_row_values_with_headers(series, column_grp), axis=1)
    
    # create IFL summary string
    if 'Connected_Edge_FID' in isl_aggregated_df.columns:
        # add edge FID
        san_graph_isl_df = dfop.dataframe_fillna(san_graph_isl_df, isl_aggregated_df, join_lst=['switchWwn', 'Connected_switchWwn'], 
                                                 filled_lst=['Connected_Edge_FID'])
        mask_ifl = san_graph_isl_df['IFL'] > 0
        # FID tag + Edge FID
        san_graph_isl_df.loc[mask_ifl, 'ifl_string'] = 'IFL FID ' + san_graph_isl_df['Connected_Edge_FID']
        summary_column_names.append('ifl_string')
    
    san_graph_isl_df['trunk_string'] = san_graph_isl_df.apply(lambda series: count_trunk_links(series, link_quantity_columns), axis=1)
    summary_column_names.append('trunk_string')
    
    # merge each group summary strings into 'Link_description'
    san_graph_isl_df = dfop.merge_columns(san_graph_isl_df, summary_column='Link_description', 
                                            merge_columns=summary_column_names, drop_merge_columns=False)
    return san_graph_isl_df


def create_isl_shape_names(san_graph_isl_df):
    """Function to create switch, conneceted switch and link Visio shape names"""
    
    # switch shape name
    create_shape_name(san_graph_isl_df, "switchName", "switchWwn", "shapeName")
    # connected switch shape name
    create_shape_name(san_graph_isl_df, "Connected_switchName", "Connected_switchWwn", "Connected_shapeName")
    # link shape_name
    san_graph_isl_df = dfop.merge_columns(san_graph_isl_df, summary_column='Link_shapeName', 
                                          merge_columns=['shapeName', 'Connected_shapeName'], sep=' - ', 
                                          drop_merge_columns=False)
    return san_graph_isl_df


