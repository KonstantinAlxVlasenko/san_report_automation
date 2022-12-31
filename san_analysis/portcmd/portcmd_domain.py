"""Module to remove domains from device hostnames in portshow_aggregated DataFrame based on user input"""


import numpy as np

import utilities.dataframe_operations as dfop
import utilities.module_execution as meop
import utilities.servicefile_operations as sfop


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
        remove_domain(portshow_aggregated_df, domain_remove_lst, domain_keep_lst, hostname_column='Device_Host_Name', )
        meop.status_info('ok', max_title, len(info))
    else:
        meop.status_info('skip', max_title, len(info))
    return portshow_aggregated_df, domain_name_remove_df


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
    
    print(f'{domain_num} domain{"s" if domain_num>1 else ""} found.')
    reply = meop.reply_request(f'Do you want to REMOVE DOMAIN{"S" if domain_num>1 else ""}? (y)es/(n)o: ')
    if reply == 'y':
        # if domain removal scheme doesn't exist (1st iteration)
        if domain_name_remove_df is None:
            # change blank domain names DataFrame
            domain_name_remove_df = choose_domain_to_remove(domain_name_df)
        else:
            # if there is request for change for data which domain removal scheme is dependent on
            if force_change_data_lst and not force_domain_remove_schema_update_flag:
                reply = meop.reply_request('Do you want to APPLY SAVED domain removal scheme? (y)es/(n)o: ')
                print('\n')
                if reply == 'y':
                    # use existing domain removal scheme
                    return domain_name_remove_df
            # change saved domain removal scheme
            domain_name_remove_df = choose_domain_to_remove(domain_name_remove_df)
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


def choose_domain_to_remove(domain_name_df):
    """Function to mark domains manually. Marked domains is going to be rmoved from hostnames"""
    
    remove_mark_column = 'Remove'
    remove_mark_str = 'V'
    
    # copy domain_name_df to be able to reset all changes 
    domain_name_remove_df = domain_name_df.copy()

    # domain_name_remove_df operation options (save, add all, mark, delete mark, reset, clear, exit)
    operation_options_lst = ['s', 'a', 'm', 'd', 'r', 'c', 'x']
    if domain_name_df[remove_mark_column].isna().all():
        operation_options_lst.remove('r')
    
    # find printed domain_name_remove_df upper border length to create visual separator
    separator_len  = get_separator_len(domain_name_remove_df, remove_mark_column)
    # user input while working with domain_name_remove_df 
    input_option = None
    # depends on user input domain_name_remove_df is shown after operation or not
    show_domain_remove_scheme = True
    # when domain_name_remove_df is loaded from the database
    if domain_name_df is not None and domain_name_df[remove_mark_column].notna().any():
        print('Loaded domain remove scheme')
    display_domain_name_remove_scheme(domain_name_remove_df, show_domain_remove_scheme, separator_len)
    
    # work with domain_name_remove_df until it is saved or exit without saving
    while not input_option in ['s', 'x']:
        # display menu option from operation_options_lst
        if show_domain_remove_scheme:
            display_menu_options(operation_options_lst)
        # default behavior is show domain_name_remove_df after any operation
        # if after user input it's not requred flag is changed to False
        show_domain_remove_scheme = True

        input_option = meop.reply_request("Choose option: ", reply_options=operation_options_lst, show_reply=True)
        
        # mark all domains
        if input_option == 'a':
            domain_name_remove_df[remove_mark_column] = remove_mark_str
            print('All domains are marked')
        # remove all marks (clear)
        elif input_option == 'c':
            reply = meop.reply_request('Do you want to clear all marks? (y)es/(n)o: ')
            if reply == 'y':
                domain_name_remove_df[remove_mark_column] = np.nan
                print('All marks are cleared')
        # reset domain remove scheme to the version loaded from the database
        elif input_option == 'r':
            reply = meop.reply_request('Do you want to reset domain removal scheme to the original version? (y)es/(n)o: ')
            if reply == 'y':
                domain_name_remove_df = domain_name_df.copy()
                print('Domain removal scheme is reset to the original version')
        # save current scheme and exit   
        elif input_option == 's':
            reply = meop.reply_request('Do you want to save changes and exit? (y)es/(n)o: ')
            if reply == 'y':
                print('Domain removal scheme is saved')
            else:
                input_option = None
            show_domain_remove_scheme = False
        # exit without save
        elif input_option == 'x':
            reply = meop.reply_request('Do you want to leave without save? (y)es/(n)o: ')
            if reply == 'y':
                # restore scheme to the initial version (empty or loaded from the database)
                domain_name_remove_df = domain_name_df.copy()
                if domain_name_df[remove_mark_column].notna().any():
                    print('Keeping original domain removal scheme')
            else:
                input_option = None
        # mark single domain
        elif input_option == 'm':
            if domain_name_remove_df[remove_mark_column].isna().any():
                # get list if  domains indexes available to mark
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
        # delete single mark
        elif input_option == 'd':
            if domain_name_remove_df[remove_mark_column].notna().any():
                # get list if  domains indexes available to unmark
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


def display_menu_options(operation_options_lst):
    """Function displays operation options"""

    print("M/m - Mark single domain\n"
            "D/d - Delete single mark\n"
            "A/a - Mark all domains\n"
            "C/c - Clear all marks")
    if 'r' in operation_options_lst:
        print("R/r - Reset all changes")
    print("S/s - Save changes\n"
        "X/x - Exit without save\n")


def display_domain_scheme_set_status(domain_name_remove_df, domain_name_df, remove_mark_column):
    """Function shows summary string of the domain mark to remove"""

    # no domain is marked in new scheme
    if domain_name_remove_df[remove_mark_column].isna().all():
        print('Domain removal scheme is not set')
    else:
        # domain removal scheme loaded from the database contains marked domains 
        if domain_name_df[remove_mark_column].notna().any():
            # new and loaded scheme are equal
            if domain_name_remove_df.equals(domain_name_df):
                print('Domain removal scheme is not changed')
            # new and loaded scheme are different
            else:
                print('Domain removal scheme is changed')
        # there are no marked domains in domain removal scheme loaded from the database
        else:
            # new scheme have at least one marked domain
            if domain_name_remove_df[remove_mark_column].notna().any():
                 print('Domain removal scheme is set')
    print('\n')


def get_separator_len(domain_name_remove_df, remove_mark_column):
    """Function returns length of the printed domain_name_remove_df DataFrame upper border"""

    # get length of longest domain name
    if domain_name_remove_df['Domain'].notna().any():
        max_domain_name_length = domain_name_remove_df['Domain'].str.len().max()
    # if there are no domains use length of the empty 'Domain' column
    else:
        max_domain_name_length = len('Domain')
    # if longest domain name is shorter the 'Domain' column name use column length
    max_domain_name_length = max_domain_name_length if max_domain_name_length > len('Domain') else len('Domain')
    # plus lengths of the 'Remove' index columns
    separator_len = max_domain_name_length + len(remove_mark_column) + 5
    return separator_len


def display_domain_name_remove_scheme(domain_name_remove_df, show_domain_remove_scheme, separator_len):
    "Function prints domain remove scheme with upper border separator"

    if show_domain_remove_scheme:
        print(f'\n{"-"*separator_len}')
        print(domain_name_remove_df)
        print('\n')


def show_single_domain_operation_options(domain_indexes_str_lst, operation):
    """Function prints menu options for single domain operation (mark or unmark).
    Menu options contains available domain indexes for the operation and cancel
    to return to the upper level menu"""

    print(f"\n{', '.join(domain_indexes_str_lst)} - Choose domain index to {'mark' if  operation=='m' else 'unmark'}")
    print('C/c - Cancel\n')


def get_domain_indexes(df, remove_mark_column, remove_mark_str, return_marked=True):
    """Function returns list of marked or unmarked domains"""
    
    mask_domain_marked = df[remove_mark_column] == remove_mark_str
    if return_marked:
        domain_indexes_int_lst = df.loc[mask_domain_marked].index.to_list()
    else:
        domain_indexes_int_lst = df.loc[~mask_domain_marked].index.to_list()
    return [str(i) for i in domain_indexes_int_lst]


def domain_drop_status(df, hostname_column, domain_drop_status_column, status):
    """Function adds domain remove status to the domain_drop_status_column."""
    
    mask_hostname_filled = df[hostname_column].notna()
    mask_domain_status_na = df[domain_drop_status_column].isna()
    df.loc[mask_hostname_filled & mask_domain_status_na, domain_drop_status_column] = status

    
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