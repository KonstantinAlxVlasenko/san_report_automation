"""Module to remove domains from device hostnames in portshow_aggregated DataFrame based on user input"""


import numpy as np

import utilities.dataframe_operations as dfop
import utilities.module_execution as meop
import utilities.servicefile_operations as sfop


def hostname_domain_remove_main(portshow_aggregated_df, domain_name_remove_df, project_constants_lst):
    """Main function to rename devices"""

    project_steps_df, max_title, io_data_names_df, *_ = project_constants_lst

    analyzed_data_names = dfop.list_from_dataframe(io_data_names_df, 'domain_name_remove_analysis_in')
    
    force_domain_remove_schema_update_flag = project_steps_df.loc[analyzed_data_names[0], 'force_run']

    # list of related DataFrame names requested to change
    force_change_data_lst = [data_name for data_name in analyzed_data_names[1:] if project_steps_df.loc[data_name, 'force_run']]
    
    pattern_dct, *_ = sfop.regex_pattern_import('common_regex', max_title)

    # create DataFrame with devices required to change names
    domain_name_remove_df = get_domain_to_remove(portshow_aggregated_df, domain_name_remove_df, 
                                                force_domain_remove_schema_update_flag, force_change_data_lst, pattern_dct)
    # current operation information string
    info = f'Removing domains'
    print(info, end =" ")

    remove_mark_column = 'Remove'
    remove_mark_str = 'V'
    
    # if at least one name is changed apply rename schema 
    if domain_name_remove_df[remove_mark_column].notna().any():
        domain_remove_lst, domain_keep_lst = \
            separate_keep_and_remove_domains(domain_name_remove_df, remove_mark_column, remove_mark_str)
        remove_domain(portshow_aggregated_df, domain_remove_lst, domain_keep_lst, hostname_column='Device_Host_Name', )
        meop.status_info('ok', max_title, len(info))
    else:
        meop.status_info('skip', max_title, len(info))

    return portshow_aggregated_df, domain_name_remove_df


def separate_keep_and_remove_domains(domain_name_remove_df, remove_mark_column, remove_mark_str):
    mask_domain_to_remove = domain_name_remove_df[remove_mark_column] == remove_mark_str
    domain_remove_lst = domain_name_remove_df.loc[mask_domain_to_remove, 'Domain'].to_list()
    domain_remove_lst.sort(reverse=True, key=lambda x: len(x))
    domain_keep_lst = domain_name_remove_df.loc[~mask_domain_to_remove, 'Domain'].to_list()
    return domain_remove_lst, domain_keep_lst



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

    if not any([force_domain_remove_schema_update_flag, 
                force_change_data_lst]) and not domain_name_remove_df is None:
        return domain_name_remove_df

    print('\n')
    if force_change_data_lst:
        print(f"Request to force change of {', '.join(force_change_data_lst)} data was received.\n")
    
    print(f'{domain_num} domain{"s" if domain_num>1 else ""} found.')
    reply = meop.reply_request(f'Do you want to REMOVE DOMAIN{"S" if domain_num>1 else ""}? (y)es/(n)o: ')
    if reply == 'y':
        # if domain removal scheme doesn't exist (1st iteration)
        if domain_name_remove_df is None:
            # change blank domain DataFrame
            domain_name_remove_df = choose_domain_to_remove(domain_name_df)
        else:
            if force_change_data_lst:
                reply = meop.reply_request('Do you want to APPLY SAVED domain removal scheme? (y)es/(n)o: ')
                print('\n')
                if reply == 'y':
                    # use existing device rename scheme
                    return domain_name_remove_df

            if force_domain_remove_schema_update_flag:
                print(f"Request for force change of domain removal scheme was received.")
            
            domain_name_remove_df = choose_domain_to_remove(domain_name_remove_df)
    else:
        domain_name_remove_df = domain_name_df.copy()
    
    return domain_name_remove_df
    


def find_domain_names(portshow_aggregated_df, pattern_dct):

    domain_name_sr = portshow_aggregated_df['Device_Host_Name'].str.extract(
        pattern_dct['domain_name'], expand=False).dropna().drop_duplicates().reset_index(drop=True)
    domain_name_df = domain_name_sr.to_frame()
    domain_name_df['Remove'] = np.nan
    domain_name_df.rename(columns={'Device_Host_Name': 'Domain'}, inplace=True)
    return domain_name_df


def choose_domain_to_remove(domain_name_df):
    """Function to manual change Fabric Name and Fabric Label."""
    
    remove_mark_column = 'Remove'
    remove_mark_str = 'V'
    
    # copy of initial fabricshow_summary DataFrame
    # to be able to reset all changes 
    # domain_name_default_df = domain_name_df.copy()
    
    domain_name_remove_df = domain_name_df.copy()

    # DataFrame operation options (save, reset, exit)
    operation_options_lst = ['s', 'a', 'm', 'd', 'r', 'c', 'x']
    if domain_name_df['Remove'].isna().all():
        operation_options_lst.remove('r')
    
    
    separator_len  = get_separator_len(domain_name_remove_df, remove_mark_column)

    # user input from full_options_lst
    # initial value is None to enter while
    input_option = None
    
    show_domain_remove_scheme = True
    display_domain_name_remove_scheme(domain_name_remove_df, show_domain_remove_scheme, separator_len)
    
    # work with DataFrame until it is saved or exit without saving
    while not input_option in ['s', 'x']:

        # printing menu options to choose to work with DataFrame
        # save, reset, exit or fabric index number to change
        if show_domain_remove_scheme:
            print("M/m - Mark single domain\n"
                    "D/d - Delete single mark\n"
                    "A/a - Mark all domains\n"
                    "C/c - Clear all marks")
            if domain_name_df[remove_mark_column].notna().any():
                print("R/r - Reset all changes")
            print("S/s - Save changes\n"
                "X/x - Exit without save\n")

        show_domain_remove_scheme = True

        # reset input_option value after each iteration to enter while loop
        input_option = meop.reply_request("Choose option: ", reply_options = operation_options_lst, show_reply = True)
        
        if input_option == 'a':
            domain_name_remove_df[remove_mark_column] = remove_mark_str
            print('All domains are marked')
        elif input_option == 'c':
            reply = meop.reply_request('Do you want to clear all marks? (y)es/(n)o: ')
            if reply == 'y':
                domain_name_remove_df[remove_mark_column] = np.nan
                print('All marks are cleared')
        # user input is reset current labeling configuration and start labeling from scratch
        elif input_option == 'r':
            reply = meop.reply_request('Do you want to reset domain removal scheme to the original version? (y)es/(n)o: ')
            # for reset option actual fabricshow_summary DataFrame returns back
            # to initial DataFrame version and while loop don't stop
            if reply == 'y':
                domain_name_remove_df = domain_name_df.copy()
                print('Domain removal scheme is reset to the original version')
        # user input is save current labeling configuration and exit   
        elif input_option == 's':
            reply = meop.reply_request('Do you want to save changes and exit? (y)es/(n)o: ')
            # for save option do nothing and while loop stops on next condition check
            if reply == 'y':
                print('Domain removal scheme is saved')
            else:
                input_option = None
            show_domain_remove_scheme = False
        # user input is exit without saving
        elif input_option == 'x':
            reply = meop.reply_request('Do you want to leave without save? (y)es/(n)o: ')
            # for exit option DataFrame returns back to initial version
            # and while loop stops
            if reply == 'y':
                domain_name_remove_df = domain_name_df.copy()
                if domain_name_df[remove_mark_column].notna().any():
                    print('Keeping original domain removal scheme')
            else:
                input_option = None
        elif input_option == 'm':
            if domain_name_remove_df[remove_mark_column].isna().any():
                unmarked_domain_indexes_str_lst = get_domain_indexes(domain_name_remove_df, remove_mark_column, remove_mark_str, return_marked=False)
                show_single_domain_operation_options(unmarked_domain_indexes_str_lst, operation=input_option)
                input_option = meop.reply_request("Choose option: ", reply_options=unmarked_domain_indexes_str_lst + ['c'], show_reply=True)
                if input_option == 'c':
                    input_option = None
                elif input_option in unmarked_domain_indexes_str_lst:
                    domain_name_remove_df.loc[int(input_option), remove_mark_column] = remove_mark_str
                    print(f'Domain index {input_option} is marked')
            else:
                print('There is no domain to mark\n')
                show_domain_remove_scheme = False
        elif input_option == 'd':
            if domain_name_remove_df[remove_mark_column].notna().any():
                marked_domain_indexes_str_lst = get_domain_indexes(domain_name_remove_df, remove_mark_column, remove_mark_str, return_marked=True)
                show_single_domain_operation_options(marked_domain_indexes_str_lst, operation=input_option)
                input_option = meop.reply_request("Choose option: ", reply_options=marked_domain_indexes_str_lst + ['c'], show_reply=True)
                if input_option == 'c':
                    input_option = None
                elif input_option in marked_domain_indexes_str_lst:
                    domain_name_remove_df.loc[int(input_option), remove_mark_column] = np.nan
                    print(f'Domain index {input_option} mark is cleared')
            else:
                print('There is no marked domain')
                show_domain_remove_scheme = False
        
        display_domain_name_remove_scheme(domain_name_remove_df, show_domain_remove_scheme, separator_len)

    display_domain_scheme_set_status(domain_name_remove_df, domain_name_df, remove_mark_column)
    
    return domain_name_remove_df


def display_domain_scheme_set_status(domain_name_remove_df, domain_name_df, remove_mark_column):

    if domain_name_remove_df[remove_mark_column].isna().all():
        print('Domain removal scheme is not set')
    else:
        if domain_name_df[remove_mark_column].notna().any():
            if domain_name_remove_df.equals(domain_name_df):
                print('Domain removal scheme is not changed')
            else:
                print('Domain removal scheme is changed')
        else:
            if domain_name_remove_df[remove_mark_column].notna().any():
                 print('Domain removal scheme is set')
    print('\n')


def get_separator_len(domain_name_remove_df, remove_mark_column):

    if domain_name_remove_df['Domain'].notna().any():
        max_domain_name_length = domain_name_remove_df['Domain'].str.len().max()
    else:
        max_domain_name_length = len('Domain')

    max_domain_name_length = max_domain_name_length if max_domain_name_length > len('Domain') else len('Domain')

    separator_len = max_domain_name_length + len(remove_mark_column) + 5
    return separator_len


def display_domain_name_remove_scheme(domain_name_remove_df, show_domain_remove_scheme, separator_len):
    if show_domain_remove_scheme:
        print(f'\n{"-"*separator_len}')
        print(domain_name_remove_df)
        print('\n')


def show_single_domain_operation_options(domain_indexes_str_lst, operation):
    print(f"\n{', '.join(domain_indexes_str_lst)} - Choose domain index to {'mark' if  operation=='m' else 'unmark'}")
    print('C/c - Cancel\n')


def get_domain_indexes(df, remove_mark_column, remove_mark_str, return_marked=True):
    
    mask_domain_marked = df[remove_mark_column] == remove_mark_str
    if return_marked:
        domain_indexes_int_lst = df.loc[mask_domain_marked].index.to_list()
    else:
        domain_indexes_int_lst = df.loc[~mask_domain_marked].index.to_list()
    return [str(i) for i in domain_indexes_int_lst]


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