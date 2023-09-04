# -*- coding: utf-8 -*-
"""
Created on Mon Aug 14 17:14:37 2023

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


# DataLine VC67
db_path = r"D:\Documents\01.CUSTOMERS\DataLine\SAN NORD\VC67\JUL2023\database_DataLine_VC6-VC7"

db_file = r"DataLine_VC6-VC7_analysis_database.db"
data_names = ['portshow_aggregated', 'switch_params_aggregated']
data_lst = dfop.read_database(db_path, db_file, *data_names)
data_lst = dfop.verify_read_data(20, data_names, *data_lst,  show_status=True)
portshow_aggregated_df, switch_params_aggregated_df, *_ = data_lst


db_file = r"DataLine_VC6-VC7_collection_database.db"
data_names = ['licenseport']
data_lst = dfop.read_database(db_path, db_file, *data_names)
data_lst = dfop.verify_read_data(20, data_names, *data_lst,  show_status=True)
licenseport_df, *_ = data_lst


# available_ports_pattern = r'(\d+) +((?:.+-based +)*ports +are +available.+'


# available_ports_pattern = r'(\d+) +((?:.+-based +)*ports +are +available.+'


# available_assigned_port_num_pattern = r'(\d+) +(.+(?:in +this +switch|to +the +base +switch) *[a-z ]*)'
# license_reservations_pattern = r'(\d+) +(license +(?:reservations|assignments).+)'
# pod_method_pattern = r'(.+?) +(POD +method)'


# patterns = ['(\d+) +(.+(?:in +this +switch|to +the +base +switch +allowance +or +installed licenses) *[a-z ]*)',
#             '(\d+) +(license +(?:reservations|assignments).+)',
#             '(.+?) +POD +method']


# pattern_names = ['available_assigned_port_num',
#                 'license_reservations',
#                 'pod_method']


patterns = ['(\d+) +(.+in +this +switch)',
            '(\d+) +(.+to +the +base +switch +allowance +or +installed licenses *[a-z ]*)',
            '(\d+) +(license +(?:reservations?|assignments?).+)',
            '(.+?) +POD +method']


pattern_names = ['ports_in_this_switch',
                'ports_assigned',
                'license_reservations',
                'pod_method']


    
patterns = [re.compile(fr"{element}", re.IGNORECASE) for element in patterns]
pattern_dct = dict(zip(pattern_names, patterns))


licenseport_extracted_df = licenseport_df.copy()  

# licenseport_extracted_df = dfop.extract_values_from_column(licenseport_extracted_df, extracted_column='licenseport',
#                                 pattern_column_lst=[
#                                     (pattern_dct['available_assigned_port_num'], ['Value', 'Name']),
#                                      (pattern_dct['license_reservations'], ['Value', 'Name']),
#                                      (pattern_dct['pod_method'], ['POD_method']),
#                                     ])

licenseport_extracted_df = dfop.extract_values_from_column(licenseport_extracted_df, extracted_column='licenseport',
                                pattern_column_lst=[
                                    (pattern_dct['ports_in_this_switch'], ['Value', 'Name']),
                                    (pattern_dct['ports_assigned'], ['Value', 'Name']),
                                     (pattern_dct['license_reservations'], ['Value', 'Name']),
                                     (pattern_dct['pod_method'], ['POD_method']),
                                    ])


licenseport_extracted_df.dropna(subset=['Value', 'Name', 'POD_method'], how='all', inplace=True)
licenseport_extracted_df.drop(columns=['licenseport'], inplace=True)




# # replace Port with SFP-based port
# licenseport_extracted_df['Name'] = licenseport_extracted_df['Name'].str.replace(pat='^port', repl='SFP-based port', regex=True)


# capitalize port and licence titles
mask_port_lic = licenseport_extracted_df['Name'].str.contains('^port|^license', na=False)
licenseport_extracted_df.loc[mask_port_lic, 'Name'] = licenseport_extracted_df.loc[mask_port_lic, 'Name'].str.capitalize()



# license port quantity
licenseport_num_df = licenseport_extracted_df.copy()
licenseport_num_df.dropna(subset=['Value'], inplace=True)
licenseport_num_df['Value'] = pd.to_numeric(licenseport_num_df['Value'], errors='ignore')



# licence pod method
licenseport_method_df = licenseport_extracted_df.dropna(subset=['POD_method']).copy()



# replace single to multiple
rplcmnt_dct = {'License assignment is held by an offline port (indicated by *)': 'License assignments are held by offline ports (indicated by *)',
               'License reservation is still available for use by unassigned ports': 'License reservations are still available for use by unassigned ports'}
licenseport_num_df.replace({'Name': rplcmnt_dct}, inplace=True)


# sum license reservations
licenseport_num_grp_df = licenseport_num_df.groupby(['configname', 'chassis_name', 'Name'])['Value'].sum()

licenseport_num_grp_df = licenseport_num_grp_df.reset_index()
licenseport_num_pivot_df = licenseport_num_grp_df.pivot_table('Value', ['configname', 'chassis_name'], 'Name')

licenseport_columns = []

# find total ports number for each group of columns
port_grp_type_lst = ['are available', 'are provisioned', 'are assigned', ]
for port_grp_type in port_grp_type_lst:
    port_grp_columns = []
    port_column = None
    for column in licenseport_num_pivot_df.columns:
        if port_grp_type in column:
            if column.startswith('Port'):
                port_column = column
            else:
                port_grp_columns.append(column)
    
    if not port_column and port_grp_columns:
     port_column = re.search('.+? port(.+)', port_grp_columns[0]).group(1)
     port_column = 'Port' + port_column
    licenseport_columns.append(port_column)
    if port_grp_columns:
        port_grp_columns.sort(reverse=True)
        licenseport_columns.extend(port_grp_columns)
        mask_port_column_isna = licenseport_num_pivot_df[port_column].isna()
        licenseport_num_pivot_df.loc[mask_port_column_isna, port_column] = licenseport_num_pivot_df[port_grp_columns].sum(axis=1)
    

missed_columns = [column for column in licenseport_num_pivot_df.columns if column not in licenseport_columns]

            
licenseport_aggregated_df = licenseport_num_pivot_df[licenseport_columns + missed_columns].reset_index().copy()



# count online ports
portshow_cp_df = portshow_aggregated_df.copy()
portshow_cp_df.drop_duplicates(subset=['configname', 'chassis_name', 'chassis_wwn', 
                                                        'switchName', 'switchWwn', 'slot', 'port'], inplace=True)
portshow_cp_df['Port_quantity'] = 'Total_ports_number'
# remove all status except online
mask_online = portshow_cp_df['portState'] == 'Online'
portshow_cp_df.loc[~mask_online, 'portState'] = None



# portshow_cp_df['Fabric_name'] = 'MetaSAN'

port_online_stats_df = dfop.count_statistics(portshow_cp_df, 
                                             connection_grp_columns=['configname', 'chassis_name', 'chassis_wwn'], 
                                             stat_columns=['Port_quantity', 'portState'])

# join online port stats and licenseport stats
licenseport_stats_df = port_online_stats_df.merge(licenseport_aggregated_df, how='left', on=['configname', 'chassis_name'])

# add switch class
licenseport_stats_df = dfop.dataframe_fillna(licenseport_stats_df, portshow_cp_df, 
                                             join_lst=['configname', 'chassis_name', 'chassis_wwn'], 
                                             filled_lst=['switchClass', 'switchType'])

licenseport_stats_df['Fabric_name'] = 'MetaSAN'

# add fabric label
chassis_flabel_df = portshow_cp_df[['Fabric_label', 'configname', 'chassis_name', 'chassis_wwn']].drop_duplicates()
licenseport_stats_df = dfop.dataframe_fillna(licenseport_stats_df, chassis_flabel_df, 
                                             join_lst=['configname', 'chassis_name', 'chassis_wwn'], 
                                             filled_lst=['Fabric_label'], remove_duplicates=False)


# add switch class weight
dfop.add_swclass_weight(licenseport_stats_df)



# add available lic port quantity for directors
mask_director = licenseport_stats_df['switchClass'].str.contains('DIR', na=False)
licenseport_stats_df.loc[mask_director, 'Port assignments are provisioned for use in this switch'] = licenseport_stats_df['Total_ports_number']



# licenseport_stats_df['Fabric_name'] = 'SAN'

# count summary for fabric_name and fabric_label levels
licenseport_stats_cp_df = licenseport_stats_df.copy()
licenseport_stats_cp_df.drop(columns=['switchClass_weight', 'switchType'], inplace=True)
licenseport_statistics_summary_df = dfop.count_summary(licenseport_stats_cp_df, group_columns=['Fabric_name', 'Fabric_label'])





# concatenate all statistics DataFrames
licenseport_statistics_df = pd.concat([licenseport_stats_df, licenseport_statistics_summary_df], ignore_index=True)
dfop.sort_fabric_swclass_swtype_swname(licenseport_statistics_df, switch_columns=['chassis_name'])


# licenseport_statistics_df.sort_values(by=['Fabric_label', 'switchClass_weight', 'switchType', 'chassis_name'], 
#                                       ascending=[True, True, False, True],
#                                       inplace=True)
licenseport_statistics_df.drop(columns=['Fabric_name', 'switchClass',  'switchClass_weight', 'switchType'], inplace=True)

# mark All Fabric label
licenseport_statistics_df['Fabric_label'].fillna('All', inplace=True)


# # count row All with total values for all fabris
# licenseport_statistics_summary_cp_df = licenseport_statistics_summary_df.copy()
# licenseport_statistics_summary_cp_df['Fabric_label'] = None
# licenseport_statistics_all_df = dfop.count_all_row(licenseport_statistics_summary_cp_df)
# licenseport_statistics_all_df['Fabric_label'] = 'All'
# licenseport_statistics_all_df.drop(columns=['Fabric_name'], inplace=True)




# # concatenate All row so it's at the bottom of statistics DataFrame
# licenseport_statistics_df = pd.concat([licenseport_statistics_df, licenseport_statistics_all_df], ignore_index=True)





# count available ports
licenseport_statistics_df['Available_licensed'] = licenseport_statistics_df['Port assignments are provisioned for use in this switch'] - licenseport_statistics_df['Online']
# count percantage of occupied ports for each switch as ratio of Online ports(occupied) number to Licensed ports number
licenseport_statistics_df['%_occupied'] = round(licenseport_statistics_df['Online'].div(licenseport_statistics_df['Port assignments are provisioned for use in this switch'])*100, 1)


# add pod method
licenseport_statistics_df = dfop.dataframe_fillna(licenseport_statistics_df, licenseport_method_df, 
                                                  join_lst=['configname', 'chassis_name'], 
                                                  filled_lst=['POD_method'])


# move column
licenseport_statistics_df = dfop.move_column(licenseport_statistics_df, cols_to_move='Fabric_label', ref_col='configname', place='before')


licenseport_num_pivot_df.columns.to_list()
portshow_cp_df.columns.to_list()

portshow_cp_df['license']



# def add_swclass_weight(swclass_df):
#     """Function to add switch class weight column based on switch class column.
#     Director has highest weight"""
    
#     swclass_df['switchClass_weight'] = swclass_df['switchClass']
#     switchClass_weight_dct = {'DIR': 1, 'ENTP': 2, 'MID': 3, 'ENTRY': 4, 'EMB': 5, 'EXT': 6}
#     mask_assigned_switch_class = swclass_df['switchClass'].isin(switchClass_weight_dct.keys())
#     swclass_df.loc[~mask_assigned_switch_class, 'switchClass_weight'] = np.nan
#     swclass_df['switchClass_weight'].replace(switchClass_weight_dct, inplace=True)
#     swclass_df['switchClass_weight'].fillna(7, inplace=True)
    

# def sort_fabric_swclass_swtype_swname(switch_df, switch_columns, fabric_columns=['Fabric_name', 'Fabric_label']):
#     """Function to sort swithes in fabric. SwitchType (model) is sorted in descending order
#     so newest models are on the top of the list"""
    
#     sort_columns = fabric_columns + ['switchClass_weight', 'switchType'] + switch_columns
#     ascending_flags = [True] * len(fabric_columns) + [True, False] + [True] * len(switch_columns)
#     switch_df.sort_values(by=sort_columns, ascending=ascending_flags, inplace=True)