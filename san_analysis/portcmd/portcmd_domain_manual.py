"""Mudule to manually mark domains to remove"""

import numpy as np

import utilities.module_execution as meop


def choose_domain_to_remove(domain_name_df, remove_mark_column, remove_mark_str):
    """Function to mark domains manually. Marked domains is going to be rmoved from hostnames"""
    
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
    "Function prints domain remove scheme with borders"

    if show_domain_remove_scheme:
        print(f'\n{"-"*separator_len}')
        print(domain_name_remove_df)
        print(f'{"-"*separator_len}\n')
        # print('\n')


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