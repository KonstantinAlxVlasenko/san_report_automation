# -*- coding: utf-8 -*-
"""
Created on Tue Dec 20 14:22:58 2022

@author: kavlasenko
"""

import os
import pandas as pd
import numpy as np

script_dir = r'C:\Users\kavlasenko\Documents\05.PYTHON\Projects\san_report_automation\san_tmp_scipts'
# Change the current working directory
os.chdir(script_dir)
import general_cmd_module as dfop

# # DataLine Nord
# db_path = r"D:\Documents\01.CUSTOMERS\DataLine\SAN NORD\NOV2022\database_DataLine Nord"
# db_file = r"DataLine Nord_analysis_database.db"


# DataLine OST
db_path = r"D:\Documents\01.CUSTOMERS\DataLine\SAN OST\NOV2022\database_DataLine OST"
db_file = r"DataLine OST_analysis_database.db"


data_names = ['portshow_aggregated']
data_lst = dfop.read_database(db_path, db_file, *data_names)
data_lst = dfop.verify_read_data(20, data_names, *data_lst,  show_status=True)

portshow_aggregated_df, *_ = data_lst


pattern_dct = {'domain_name': '^.+?(\.(?:[\w\d-]+\.)*(?:ru|local|com))'}

# domain_found_sr = portshow_aggregated_df['Device_Host_Name'].str.extract(pattern_dct['domain_name'], expand=False).dropna().drop_duplicates().reset_index(drop=True)
# domain_found_sr.rename('Domain', inplace=True)

host_domain_sr = portshow_aggregated_df['Device_Host_Name'].str.extract(pattern_dct['domain_name'], expand=False).dropna().drop_duplicates().reset_index(drop=True)
host_domain_df = host_domain_sr.to_frame()
host_domain_df['Remove'] = np.nan
host_domain_df.rename(columns={'Device_Host_Name': 'Domain'}, inplace=True)


# for i, domain in enumerate(domain_sr.tolist()):
#     print(i, domain)

# domain_remove_sr = pd.Series(name='Domain', dtype='object')

# domain_found_sr.empty
# domain_remove_sr.empty



if not host_domain_df.empty:
    
    host_domain_bckp = host_domain_df.copy()
    
    domain_num = len(host_domain_df.index)
    print(f'{domain_num} domain{"s" if domain_num>1 else ""} found.')
    # reply = dfop.reply_request('Do you want to choose which domain names should be removed from host names? (y)es/(n)o: ')
    # if reply == 'y':
    #     pass

    domain_indexes_str_lst = [str(i) for i in host_domain_df.index]
    # DataFrame operation options (save, reset, exit)
    opeartion_options_lst = ['s', 'a', 'r', 'c', 'x']
    # aggregated list of available operations
    full_options_lst = domain_indexes_str_lst + opeartion_options_lst


    print(host_domain_df)
    
    print('\nS/s - Save changes\nA/a - Add all domains to the list\nR/r - Reset changes\nC/c - Clear\nX/x - Exit without save')
    print(f"{', '.join(domain_indexes_str_lst)} - Choose domain index to remove\n")
    
    # domain_remove_sr = domain_found_sr.copy()
    
    checkmark_str = 'V'
    
    # for i in host_domain_df.index:
    for i in [1,3,4,5]:
        host_domain_df.loc[i, 'Remove'] = checkmark_str
    
    mask_domain_to_remove = host_domain_df['Remove'] == checkmark_str
    domain_remove_lst = host_domain_df.loc[mask_domain_to_remove, 'Domain'].to_list()
    domain_remove_lst.sort(reverse=True, key=lambda x: len(x))
    
    domain_keep_lst = host_domain_df.loc[~mask_domain_to_remove, 'Domain'].to_list()
    
    
    host_domain_df.loc[mask_domain_to_remove].index.to_list()
    
    

def domain_drop_status(df, hostname_column, domain_drop_status_column, status):
    """Function adds domain remove status to the domain_drop_status_column"""
    
    mask_hostname_filled = df[hostname_column].notna()
    mask_domain_status_na = df[domain_drop_status_column].isna()
    df.loc[mask_hostname_filled & mask_domain_status_na, domain_drop_status_column] = status

    
def remove_domain(df, domain_drop_lst, domain_keep_lst, hostname_column):
    """Function remove domain names from domain_lst in the hostname_column"""
    
    # copy column with hostnames to the new column
    hostname_domain_column = hostname_column + '_w_domain'
    df[hostname_domain_column] = df[hostname_column]
    # clear hostname column to fill column with domain free hostnames
    df[hostname_column] = np.nan
    # create column with drop status
    domain_drop_status_column = 'Domain_drop_status'
    df[domain_drop_status_column] = np.nan
    
    # copy hostnames with domains which are in domain_keep_lst
    for domain in domain_keep_lst:
        keep_domain_pattern = '^[\w-]+?' + domain.replace('.', '\.')
        mask_domain_keep = df[hostname_domain_column].str.contains(keep_domain_pattern, na=False, regex=True)
        df.loc[mask_domain_keep, hostname_column] = df.loc[mask_domain_keep, hostname_domain_column]
    
    domain_drop_status(df, hostname_column, domain_drop_status_column, status='domain_kept')
    
    for domain in domain_drop_lst:
        # pattern extracts domain free hostname
        drop_domain_pattern = '(.+?)' + domain.replace('.', '\.')
        # extract current domain free hostname to the tmp column
        df['Domain_free_tmp'] = df[hostname_domain_column].str.extract(drop_domain_pattern)
        # fill empty values in hostname_column with values in tmp column
        df[hostname_column].fillna(df['Domain_free_tmp'], inplace=True)
    
    domain_drop_status(df, hostname_column, domain_drop_status_column, status='domain_dropped')
    
    # fill empty values in hostname_column with values with no domains or 
    # with domains which are not in the domain_lst
    df[hostname_column].fillna(df[hostname_domain_column], inplace=True)
    domain_drop_status(df, hostname_column, domain_drop_status_column, status='domain_absent')
    
    # remove tmp column
    df.drop(columns=['Domain_free_tmp'], inplace=True)



    

remove_domain(portshow_aggregated_df, domain_remove_lst, domain_keep_lst, hostname_column='Device_Host_Name', )
check_domain_df = portshow_aggregated_df[['Device_Host_Name', 'Device_Host_Name_w_domain', 'Domain_drop_status', 'deviceType']]#.dropna(subset=['Device_Host_Name'])


check_domain_cp_df = check_domain_df.copy()

# check_domain_cp_df['Device_Host_Name'].str.contains('.vdi.dtln.ru', regex=False)


tst_df = pd.DataFrame([[]])