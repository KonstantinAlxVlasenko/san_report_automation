"""Module to remove domains from device hostnames in portshow_aggregated DataFrame based on user input"""


import numpy as np

import utilities.dataframe_operations as dfop
import utilities.module_execution as meop
import utilities.servicefile_operations as sfop

from .portcmd_domain_manual import choose_domain_to_remove, display_domain_name_remove_scheme, get_separator_len

REMOVE_MARK_COLUMN = 'Remove'
REMOVE_MARK_STR = 'V'


def hostname_domain_remove(portshow_aggregated_df, domain_name_remove_df, project_constants_lst):
    """Main function to remove domain names from hostnames"""

    # imported project constants required for module execution
    project_steps_df, max_title, io_data_names_df, *_ = project_constants_lst
    # data titles which module is dependent on (input data)
    analyzed_data_names = dfop.list_from_dataframe(io_data_names_df, 'domain_name_remove_analysis_in')
    # verify if device_rename_form key is on or off
    force_domain_remove_schema_update_flag = project_steps_df.loc[analyzed_data_names[0], 'force_run']
    # list of related DataFrame names requested to change
    force_change_data_lst = [data_name for data_name in analyzed_data_names[1:] if project_steps_df.loc[data_name, 'force_run']]
    # data imported from init file (regular expression patterns) to extract values from data columns
    pattern_dct, *_ = sfop.regex_pattern_import('common_regex', max_title)
    # define domain remove scheme
    domain_name_remove_df = get_domain_to_remove(portshow_aggregated_df, domain_name_remove_df, 
                                                force_domain_remove_schema_update_flag, force_change_data_lst, pattern_dct)
    # current operation information string
    info = f'Removing domains from hostnames'
    print(info, end =" ")

    # if at least one domain is marked to remove apply domain remove scheme
    if domain_name_remove_df[REMOVE_MARK_COLUMN].notna().any():
        # filter domains which need to be removed and which to be kept
        domain_remove_lst, domain_keep_lst = \
            separate_keep_and_remove_domains(domain_name_remove_df, REMOVE_MARK_COLUMN, REMOVE_MARK_STR)
        remove_domain(portshow_aggregated_df, domain_remove_lst, domain_keep_lst, hostname_column='Device_Host_Name')
        meop.status_info('ok', max_title, len(info))
        portshow_aggregated_df, freezed_domain_flag = freeze_domain(portshow_aggregated_df)
        info = f'Freezing domains to the duplicated hostnames'
        print(info, end =" ")
        if freezed_domain_flag:
            meop.status_info('ok', max_title, len(info))
        else:
            meop.status_info('skip', max_title, len(info))
    else:
        portshow_aggregated_df['Device_Host_Name_w_domain'] = portshow_aggregated_df['Device_Host_Name']
        meop.status_info('skip', max_title, len(info))
    return portshow_aggregated_df, domain_name_remove_df


def get_domain_to_remove(portshow_aggregated_df, domain_name_remove_df, 
                            force_domain_remove_schema_update_flag, 
                            force_change_data_lst, pattern_dct):
    """Function to define (create new, return previously saved or return empty) 
    domain removal scheme DataFrame"""

    # existing domains in the fabrics
    domain_name_df = find_domain_names(portshow_aggregated_df, pattern_dct)
    domain_num = len(domain_name_df.index)

    
    if domain_name_df.empty:
        return domain_name_df


    # if domain removal scheme exist and there is no request for change of 
    # domain removal scheme or data its dependent on
    if not any([force_domain_remove_schema_update_flag, 
                force_change_data_lst]) and not domain_name_remove_df is None:
        return domain_name_remove_df

    print('\n')
    # if there is request for change for domain removal scheme
    if force_domain_remove_schema_update_flag:
        print(f"Request for force change of domain removal scheme was received.")
    # show data names with request for change flag is ON which domain removal scheme dependent on
    elif force_change_data_lst:
        print(f"Request to force change of {', '.join(force_change_data_lst)} data was received.\n")
    
    
    # find printed domain_name_remove_df upper border length to create visual separator
    separator_len  = get_separator_len(domain_name_df, remove_mark_column=REMOVE_MARK_COLUMN)
    display_domain_name_remove_scheme(domain_name_df, show_domain_remove_scheme=True, separator_len=separator_len)

    print(f'{domain_num} domain{"s" if domain_num>1 else ""} found.')
    print('\n')

    reply = meop.reply_request(f'Do you want to REMOVE DOMAIN{"S" if domain_num>1 else ""}? (y)es/(n)o: ')
    if reply == 'y':
        # if domain removal scheme doesn't exist (1st iteration)
        if domain_name_remove_df is None:
            # change blank domain names DataFrame
            domain_name_remove_df = choose_domain_to_remove(domain_name_df, REMOVE_MARK_COLUMN, REMOVE_MARK_STR)
        else:
            # if there is request for change for data which domain removal scheme is dependent on
            if force_change_data_lst and not force_domain_remove_schema_update_flag and not domain_name_remove_df.empty:
                print('\nSAVED domain removal scheme')
                display_domain_name_remove_scheme(domain_name_remove_df, show_domain_remove_scheme=True, separator_len=separator_len)
                reply = meop.reply_request('Do you want to APPLY SAVED domain removal scheme? (y)es/(n)o: ')
                print('\n')
                if reply == 'y':
                    # use existing domain removal scheme
                    return domain_name_remove_df
            
            if domain_name_df.index.equals(domain_name_remove_df.index):
                domain_name_remove_df = choose_domain_to_remove(domain_name_remove_df, REMOVE_MARK_COLUMN, REMOVE_MARK_STR)
            else:
                domain_name_remove_df = choose_domain_to_remove(domain_name_df, REMOVE_MARK_COLUMN, REMOVE_MARK_STR)
            
            # # change saved domain removal scheme
            # domain_name_remove_df = choose_domain_to_remove(domain_name_remove_df, REMOVE_MARK_COLUMN, REMOVE_MARK_STR)
    else:
        # if user refuse to remove domains then domain removal scheme is empty (no marks)
        domain_name_remove_df = domain_name_df.copy()
    return domain_name_remove_df
    

def find_domain_names(portshow_aggregated_df, pattern_dct):
    """Function returns DataFrame wth hostname domains found in portshow_aggregated_df"""

    domain_name_sr = portshow_aggregated_df['Device_Host_Name'].str.extract(
        pattern_dct['domain_name'], expand=False).dropna().drop_duplicates().reset_index(drop=True)
    domain_name_df = domain_name_sr.to_frame()
    domain_name_df[REMOVE_MARK_COLUMN] = np.nan
    domain_name_df.rename(columns={'Device_Host_Name': 'Domain'}, inplace=True)
    return domain_name_df


def separate_keep_and_remove_domains(domain_name_remove_df, remove_mark_column, remove_mark_str):
    """Function to filter domains which need to be kept and which need to be removed.
    Returns two domain lists"""

    mask_domain_to_remove = domain_name_remove_df[remove_mark_column] == remove_mark_str
    domain_remove_lst = domain_name_remove_df.loc[mask_domain_to_remove, 'Domain'].to_list()
    # sort domains by so longest domains removed first
    # if shorter domain with the same root removed before longest 
    # then part of longer domain (which contains shorter domain) won't be removed
    domain_remove_lst.sort(reverse=True, key=lambda x: len(x))
    domain_keep_lst = domain_name_remove_df.loc[~mask_domain_to_remove, 'Domain'].to_list()
    return domain_remove_lst, domain_keep_lst


def remove_domain(df, domain_drop_lst, domain_keep_lst, hostname_column):
    """Function removes domain names from domain_lst in the hostname_column"""
    
    # copy column with hostnames to the new column
    hostname_domain_column = hostname_column + '_w_domain'
    df[hostname_domain_column] = df[hostname_column]
    # clear hostname column to fill column with domain free hostnames
    df[hostname_column] = np.nan
    # create column with drop status
    domain_drop_status_column = 'Domain_drop_status'
    df[domain_drop_status_column] = np.nan
    # create column with dropped domain names
    df['Domain_name_dropped'] = np.nan
    
    # copy hostnames with domains which are in domain_keep_lst
    for domain in domain_keep_lst:
        keep_domain_pattern = '^[\w-]+?' + domain.replace('.', '\.')
        mask_domain_keep = df[hostname_domain_column].str.contains(keep_domain_pattern, na=False, regex=True)
        df.loc[mask_domain_keep, hostname_column] = df.loc[mask_domain_keep, hostname_domain_column]
    
    domain_drop_status(df, hostname_column, domain_drop_status_column, status='domain_kept')
    
    for domain in domain_drop_lst:
        # pattern extracts domain free hostname
        drop_domain_pattern = '(.+?)' + domain.replace('.', '\.')
        # pattern extracts domain name
        domain_pattern = '.+?' + '(' + domain.replace('.', '\.') + ')'
        # extract current domain free hostname to the tmp column
        df['Domain_free_tmp'] = df[hostname_domain_column].str.extract(drop_domain_pattern)
        # extract domain
        df['Domain_name_dropped_tmp'] = df[hostname_domain_column].str.extract(domain_pattern)
        # fill empty values in hostname_column with values in tmp column
        df[hostname_column].fillna(df['Domain_free_tmp'], inplace=True)
        df['Domain_name_dropped'].fillna(df['Domain_name_dropped_tmp'], inplace=True)
    
    domain_drop_status(df, hostname_column, domain_drop_status_column, status='domain_dropped')
    # fill empty values in hostname_column with values with no domains or 
    # with domains which are not in the domain_lst
    df[hostname_column].fillna(df[hostname_domain_column], inplace=True)
    domain_drop_status(df, hostname_column, domain_drop_status_column, status='domain_absent')
    # remove tmp column
    df.drop(columns=['Domain_free_tmp', 'Domain_name_dropped_tmp'], inplace=True)


def domain_drop_status(df, hostname_column, domain_drop_status_column, status):
    """Function adds domain remove status to the domain_drop_status_column."""
    
    mask_hostname_filled = df[hostname_column].notna()
    mask_domain_status_na = df[domain_drop_status_column].isna()
    df.loc[mask_hostname_filled & mask_domain_status_na, domain_drop_status_column] = status


def freeze_domain(portshow_aggregated_df):
    """Function detects if there are multiple hostnames with different domains within SAN.
    Domain is freezed for that hostnames"""

    # count domain names removed for each hostname
    portshow_aggregated_df['Domain_name_dropped_quantity'] = \
        portshow_aggregated_df.groupby(['Device_Host_Name'])['Domain_name_dropped'].transform('nunique')
    # leave domain names for ports with the same hostname but different domain names 
    mask_domain_name_freeze = portshow_aggregated_df['Domain_name_dropped_quantity'] > 1
    # set domain name drop  status
    portshow_aggregated_df.loc[mask_domain_name_freeze, 'Domain_drop_status'] = 'domain_freezed'
    # keep hostnames with multiple domains original hostnames 
    portshow_aggregated_df.loc[mask_domain_name_freeze, 'Device_Host_Name'] = \
        portshow_aggregated_df.loc[mask_domain_name_freeze, 'Device_Host_Name_w_domain']
    return portshow_aggregated_df, mask_domain_name_freeze.any()