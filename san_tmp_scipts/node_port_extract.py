# -*- coding: utf-8 -*-
"""
Created on Wed Jul 12 00:42:25 2023

@author: kavlasenko
"""
import os
import warnings
import numpy as np
import re

import pandas as pd
script_dir = r'C:\Users\kavlasenko\Documents\05.PYTHON\Projects\san_report_automation\san_tmp_scipts'
# Change the current working directory
os.chdir(script_dir)
import general_cmd_module as dfop




# # DataLine OST
# db_path = r"D:\Documents\01.CUSTOMERS\DataLine\SAN NORD\NOV2022\database_DataLine_Nord"
# db_file = r"DataLine_Nord_analysis_database.db"

# DataLine SPb
db_path = r"D:\Documents\01.CUSTOMERS\DataLine\SAN SPB\JUL2023\database_DataLine_SPb"
db_file = r"DataLine_SPb_analysis_database.db"

 

data_names = ['portshow_aggregated']
data_lst = dfop.read_database(db_path, db_file, *data_names)
data_lst = dfop.verify_read_data(20, data_names, *data_lst,  show_status=True)

portshow_aggregated_df, *_ = data_lst


pattern_dct = {'wwn_2nd_octet': '^(?:[0-9a-f]{2}:)+?([0-9a-f])([0-9a-f]):(?:[0-9a-f]{2}:){5}[0-9a-f]{2}', 
               'wwn_8th_octet': '(?:[0-9a-f]{2}:){7}([0-9a-f])([0-9a-f])'}

def filter_empty_storage_ports(portshow_aggregated_df, storage_type: str):
    """Function to filter ports of storages defined with storage_type.
    Filterd ports have no 'Device_port' extracted from switch or storage configuration files"""
    
    mask_storage = (portshow_aggregated_df[['deviceType', 'deviceSubtype']] == ('STORAGE', storage_type)).all(axis=1)
    mask_port_na = portshow_aggregated_df['Device_Port'].isna()
    storage_ports_df = portshow_aggregated_df.loc[mask_storage & mask_port_na].copy()
    return storage_ports_df


# def extract_node_port_from_wwpn(storage_ports_df, wwn_octet_pattern: str):
    
#     storage_ports_df[['Node_extracted', 'Port_extracted']] =  storage_ports_df['Connected_portWwn'].str.extract(pattern_dct['wwn_8th_octet']).values
#     return storage_ports_df


def extract_infinidat_node_port_from_wwn(portshow_aggregated_df, pattern_dct):
    """Function to extract node, port number from WWPN 8th octet.
    Construct Node Port in INFINIDAT format N#FC# from extracted values"""

    # filter INFINIDAT ports
    storage_ports_df = filter_empty_storage_ports(portshow_aggregated_df, storage_type='INFINIDAT')
    # extract node and port numbers
    storage_ports_df[['Node_extracted', 'Port_extracted']] = storage_ports_df['Connected_portWwn'].str.extract(pattern_dct['wwn_8th_octet']).values
    # merge node number and port number with 'FC' tag
    storage_ports_df = dfop.merge_columns(storage_ports_df, summary_column='Node_Port_extracted', 
                                          merge_columns=['Node_extracted', 'Port_extracted'], sep='FC', drop_merge_columns=False)
    # add 'N' tag to the non empty device ports 
    mask_node_notna = storage_ports_df['Node_Port_extracted'].notna()
    storage_ports_df.loc[mask_node_notna, ['Node_tag']] = 'N'
    storage_ports_df = dfop.merge_columns(storage_ports_df, summary_column='Device_Port', 
                                          merge_columns=['Node_tag', 'Node_Port_extracted'], sep='', drop_merge_columns=False)
    # add extacted and constructed 'Device_Port' to the aggregated port DataFrame
    portshow_aggregated_df = dfop.dataframe_fillna(portshow_aggregated_df, storage_ports_df, 
                                                   join_lst=['Fabric_name', 'Fabric_label', 'Connected_portWwn'], 
                                                   filled_lst=['Device_Port'])
    return portshow_aggregated_df



def extract_huawei_node_port_from_wwn(portshow_aggregated_df, pattern_dct):
    """Function to extract node, port number from WWPN 2nd octet.
    Construct Node Port in HUAWEI format CTE0.#.IOM0.P# from extracted values"""

    # filter HUAWEI ports
    storage_ports_df = filter_empty_storage_ports(portshow_aggregated_df, storage_type='HUAWEI')
    # extract node and port numbers
    storage_ports_df[['Node_extracted', 'Port_extracted']] =  storage_ports_df['Connected_portWwn'].str.extract(pattern_dct['wwn_2nd_octet']).values
    # replace controller number with controller name
    controller_dct = {'0': 'A', '1': 'B', '2': 'C', '3': 'D', '8': 'A', '9': 'B'}
    storage_ports_df['Node_extracted'].replace(controller_dct, inplace=True)
    # create node and iom tags for non empty node ports
    mask_node_notna = storage_ports_df['Node_extracted'].notna()
    storage_ports_df.loc[mask_node_notna, ['Node_tag']] = 'CTE0.'
    storage_ports_df.loc[mask_node_notna, ['IOM_tag']] = '.IOM0.'
    # merge node and iom
    storage_ports_df = dfop.merge_columns(storage_ports_df, summary_column='Node_IOM', 
                                          merge_columns=['Node_tag', 'Node_extracted', 'IOM_tag'], sep='', drop_merge_columns=False)
    # merge node_iom and port with port tag
    storage_ports_df = dfop.merge_columns(storage_ports_df, summary_column='Device_Port', 
                                          merge_columns=['Node_IOM', 'Port_extracted'], sep='P', drop_merge_columns=False)
    # add extacted and constructed 'Device_Port' to the aggregated port DataFrame
    portshow_aggregated_df = dfop.dataframe_fillna(portshow_aggregated_df, storage_ports_df, 
                                                   join_lst=['Fabric_name', 'Fabric_label', 'Connected_portWwn'], 
                                                   filled_lst=['Device_Port'])
    return portshow_aggregated_df


portshow_aggregated_df = extract_infinidat_node_port_from_wwn(portshow_aggregated_df, pattern_dct)
portshow_aggregated_df = extract_huawei_node_port_from_wwn(portshow_aggregated_df, pattern_dct)



