
"""Module to check if necessary sshow sections are present in the directory.
There is single switch instance of sshow files in the folder.
There is no files with zero size."""

import os
import sys

import utilities.module_execution as meop

from .sshow_stats import count_ssave_section_files_stats, filter_fault_sections


def validate_sshow_section_files(ssave_sys_section_file, pattern_dct):
    """Function to verify sshow sections with high priority located in folder and there is no files duplication"""

    ssave_sys_section_filename = os.path.basename(ssave_sys_section_file)
    # count sshow sections statistics for the current folder
    sw_ssave_sections_stats_df, ssave_sections_stats_current_df = \
        count_ssave_section_files_stats(ssave_sys_section_file, pattern_dct)
    # find absent sections, duplicated sections or sections with zero size
    sw_fault_sections_df, multiple_sshow_section_files_warning_on = filter_fault_sections(sw_ssave_sections_stats_df)
    if not sw_fault_sections_df.empty:
        input_option = None
        print('\n')
        # user can surpress any warning exept section multiple files
        while multiple_sshow_section_files_warning_on or \
                (not multiple_sshow_section_files_warning_on and not input_option in ['c']):
            display_fault_sections(sw_fault_sections_df, ssave_sys_section_filename)
            operation_options_lst = get_operation_options(multiple_sshow_section_files_warning_on)
            display_menu_options(operation_options_lst)
            input_option = meop.reply_request("Choose option: ", reply_options=operation_options_lst, show_reply=True)
            # if 'rescan' option is chosen after removing files in the ssave folder
            if input_option == 'r':
                # count stats and fault sections again
                sw_ssave_sections_stats_df, ssave_sections_stats_current_df = \
                    count_ssave_section_files_stats(ssave_sys_section_file, pattern_dct)
                sw_fault_sections_df, multiple_sshow_section_files_warning_on = filter_fault_sections(sw_ssave_sections_stats_df)
                print('Directory rescanned')
            elif input_option == 'x':
                print('Stop program execution')
                sys.exit()
            print('\n')
    return ssave_sections_stats_current_df


def display_fault_sections(sw_fault_sections_df, ssave_section_filename):
    """Function shows sections which are not fullfill conditions.
    DataFrame is shown with top and bottom separator"""

    separator_len = get_separator_len(sw_fault_sections_df)
    print('-' * len(ssave_section_filename))
    print(ssave_section_filename)
    print('-' * separator_len)
    print(sw_fault_sections_df)
    print('-' * separator_len)


def get_separator_len(sw_fault_sections_df):
    """Function returns length of the printed sw_fault_sections_df DataFrame top and bottom border"""

    # find length all column names
    column_names_length = len(' '.join(sw_fault_sections_df.columns)) + 1
    # find max index str length
    max_section_name_length = sw_fault_sections_df.index.str.len().max()
    index_name_length = len(sw_fault_sections_df.index.name)
    section_name_length = max_section_name_length if max_section_name_length > index_name_length else index_name_length
    # total separator length
    separator_len = section_name_length + column_names_length
    return separator_len


def get_operation_options(multiple_sshow_section_files_warning_on):
    """Function returns options available for user.
    Ignore and 'continue' option is not available 
    if multiple files for the same sshow section are in the directory"""

    operation_options_lst = ['r', 'c', 'x']
    if multiple_sshow_section_files_warning_on:
        operation_options_lst.remove('c')
    return operation_options_lst


def display_menu_options(operation_options_lst):
    """Function displays operation options"""

    print("\nR/r - Rescan files in folder")
    if 'c' in operation_options_lst:
        print("C/c - Ignore and continue")
    print("X/x - Stop program and exit\n")