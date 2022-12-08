"""Module to create NPIV link shape related DataFrame. 
NPIV link shape DataFrame used to drop VC, switch in NPV or AG mode connections on Visio page"""

import pandas as pd

import utilities.dataframe_operations as dfop

from ..shape_details import count_trunk_links, create_shape_name


def create_san_graph_npiv_links(npiv_statistics_df, pattern_dct):
    """Function to create npiv DataFrame with details required for san diagram"""
    
    if npiv_statistics_df.empty:
        return pd.DataFrame()
    
    # extract required columns, drop summary rows
    san_graph_npiv_df = get_san_graph_npiv_link_from_npiv_statistics(npiv_statistics_df, pattern_dct)
    #  create link description (speed)
    san_graph_npiv_df = add_npiv_link_description(san_graph_npiv_df, pattern_dct)
    # create switch, npv device and link shape names
    san_graph_npiv_df = create_npiv_link_shape_names(san_graph_npiv_df)
    return san_graph_npiv_df


def get_san_graph_npiv_link_from_npiv_statistics(npiv_statistics_df, pattern_dct):
    """Function to initialize san_graph_npv DataFrame with links details"""
    
    san_graph_npiv_df = npiv_statistics_df.copy()
    # drop fabric summary rows
    san_graph_npiv_df.dropna(subset=['switchWwn', 'NodeName'], inplace=True)
    # drop excessive columns
    npiv_stat_columns = ['Fabric_name', 'Fabric_label', 'switchName', 'switchWwn', 
                         'Device_Host_Name', 'NodeName', 'Logical_link_quantity', 'Physical_link_quantity']
    native_speed_columns = dfop.find_columns(san_graph_npiv_df, pattern_dct['native_speed'])
    link_quantity_columns = dfop.find_columns(san_graph_npiv_df, pattern_dct['link_quantity'])
    npiv_stat_columns = [*npiv_stat_columns, *native_speed_columns, *link_quantity_columns]
    san_graph_npiv_df = san_graph_npiv_df[npiv_stat_columns].copy()
    return san_graph_npiv_df
    

def add_npiv_link_description(san_graph_npiv_df, pattern_dct):
    """Function to create NPV Link description (link speed)"""

    # remove native tag from speed columns
    speed_columns =  dfop.rename_columns(san_graph_npiv_df, pattern_dct['native_speed'])
    # convert speed columns to single string
    san_graph_npiv_df['speed_string'] = \
        san_graph_npiv_df.apply(lambda series: dfop.concatenate_row_values_with_headers(series, speed_columns), axis=1)
    
    # check if trunk exist
    link_quantity_columns =  dfop.find_columns(san_graph_npiv_df, pattern_dct['link_quantity'])
    san_graph_npiv_df['trunk_string'] = \
        san_graph_npiv_df.apply(lambda series: count_trunk_links(series, link_quantity_columns), axis=1)
    # merge each group summary strings into 'Link_description'
    san_graph_npiv_df = dfop.merge_columns(san_graph_npiv_df, summary_column='Link_description', 
                                                                merge_columns=['speed_string', 'trunk_string'], 
                                                                drop_merge_columns=True)
    return san_graph_npiv_df


def create_npiv_link_shape_names(san_graph_npiv_df):
    """Function to create switch, conneceted npv device and link Visio shape names"""

    # switch shape names
    create_shape_name(san_graph_npiv_df, "switchName", "switchWwn", "shapeName")
    # npv device shape names
    create_shape_name(san_graph_npiv_df, "Device_Host_Name", "NodeName", "Connected_shapeName")
    # link shape_name
    san_graph_npiv_df = dfop.merge_columns(san_graph_npiv_df, summary_column='Link_shapeName', 
                                           merge_columns=['shapeName', 'Connected_shapeName'], 
                                           sep=' - ', drop_merge_columns=False)
    return san_graph_npiv_df