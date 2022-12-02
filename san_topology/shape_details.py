
import warnings

import numpy as np
import pandas as pd


def calculate_sw_grp_y_axis_coordinate(san_graph_sw_pair_df):
    """Function to calculate y-axis coordinate for switch groups (switches with the sane switchPair_ID).
    Depending on switchClass_weight switch groups are located in three levels.
    DIR - level 1, ENT, MID, ENTRY - level 2, EMB, AG, NPV, VC - level 3.
    Along the same group level neighboring switch groups are spaced along the y-axis 
    to avoid switch groups to be located in one line."""

    # enumerate rows in each group based in switchPair_ID (create switch group levels to apply y-axis offset)
    san_graph_sw_pair_df['group_level'] = san_graph_sw_pair_df.groupby(
        ['Fabric_name', 'switchClass_weight'])['switchClass_weight'].cumcount()
    # replace even and odd with 0 and 1
    mask_even = san_graph_sw_pair_df['group_level'] % 2 == 0
    san_graph_sw_pair_df['group_level'] = np.select([mask_even, ~mask_even], [0, 1], default=0)
    # calculate y-axis coordinate for the switch group
    san_graph_sw_pair_df['y_group_level'] = \
        san_graph_sw_pair_df['y_graph_level'] + san_graph_sw_pair_df['group_level'] * san_graph_sw_pair_df['y_group_offset']


def create_shape_name(df, device_name_column, device_wwn_column, shape_name_column):
    """Function to create device shape name by concatenatinating device_name with device_wwn except
    the case if device_name already contains wwn"""
    
    # concatenate switchName and switchWwn
    warnings.filterwarnings("ignore", 'This pattern is interpreted as a regular expression, and has match groups')
    mask_wwn_in_name = df[device_name_column].str.contains("([0-9a-f]{2}:){7}[0-9a-f]{2}")
    df.loc[~mask_wwn_in_name, shape_name_column] = df[device_name_column] + " " + df[device_wwn_column]
    df[shape_name_column].fillna(df[device_name_column], inplace=True)


def count_trunk_links(series, columns):
    """Function to count number of trunks in the connection.
    Returns string 'Trk: 2x3 links, 1x4 links'"""
    
    if series['Logical_link_quantity'] == 1 and series['Physical_link_quantity']>1:
        return 'Trk'
    if series['Logical_link_quantity'] == series['Physical_link_quantity']:
        return None
    if pd.isna(series['Logical_link_quantity']):
        return None

    link_quantity_sr = series[columns]
    link_quantity_sr = link_quantity_sr.loc[link_quantity_sr>1].astype(int).astype(str) + ' links'
    trunk_links_lst = [str(value) + 'x' + key for key, value in link_quantity_sr.value_counts().to_dict().items()]
    return 'Trk: ' + ', '.join(trunk_links_lst)