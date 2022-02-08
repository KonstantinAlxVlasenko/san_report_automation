"""Module to set Fabric names and labels"""


import os
from datetime import date

import pandas as pd
import utilities.database_operations as dbop
import utilities.dataframe_operations as dfop
import utilities.module_execution as meop
import utilities.servicefile_operations as sfop
import utilities.filesystem_operations as fsop

from .switch_pair_auto import auto_switch_pairing
from .switch_pair_verification import *
from .switch_pair_correction import *


def switch_pair_analysis(switch_params_aggregated_df, portshow_aggregated_df, report_creation_info_lst):
    """Function to set switch pair IDs"""

    # report_steps_dct contains current step desciption and force and export tags
    # report_headers_df contains column titles, 
    # report_columns_usage_dct show if fabric_name, chassis_name and group_name of device ports should be used
    report_constant_lst, report_steps_dct, report_headers_df, report_columns_usage_dct = report_creation_info_lst
    # report_constant_lst contains information: customer_name, project directory, database directory, max_title
    *_, max_title = report_constant_lst

    # names to save data obtained after current module execution
    data_names = ['switch_pair', 'sw_wwn_occurrence_stats']
    # service step information
    print(f'\n\n{report_steps_dct[data_names[0]][3]}\n')
    # read data from database if they were saved on previos program execution iteration
    data_lst = dbop.read_database(report_constant_lst, report_steps_dct, *data_names)
    # unpacking DataFrames from the loaded list with data
    switch_pair_df, _ = data_lst
    # list of data to analyze from report_info table
    analyzed_data_names = ['switch_params_aggregated', 'portshow_aggregated']
    # force run when any data from data_lst was not saved (file not found) or 
    # procedure execution explicitly requested for output data or data used during fn execution  
    force_run = meop.verify_force_run(data_names, data_lst, report_steps_dct, max_title, analyzed_data_names)

    if force_run:             
        print('\nSETTING UP SWITCH PAIRS  ...\n')

        # switchWwn pair was not calculated before (no data in database)
        first_run = False
        # saved switchWwn pair was reset to automatic version
        reset_flag = False
        # switchWwn pair was changed
        change_flag = False
        if switch_pair_df is None:
            first_run = True
            switch_pair_df = auto_switch_pairing(switch_params_aggregated_df, portshow_aggregated_df)    
        
        switch_pair_bckp_df = switch_pair_df.copy()
        
        while True:
            show_switch_wwn_pair_summary(switch_pair_df)
            reply = meop.reply_request('Do you want to CHANGE switch pairs? (y)es/(n)o: ')
            if reply == 'y':
                change_flag = True
                if not first_run:
                    question = 'Do you want to RESET switch pairs or MODIFY EXISTING one? (r)eset/(m)odify: '
                    reply = meop.reply_request(question, reply_options=['r', 'reset', 'm', 'modify'])
                    if reply in ['r', 'reset']:
                        reset_flag = True
                        switch_pair_df = auto_switch_pairing(switch_params_aggregated_df, portshow_aggregated_df)
                        print('Switch pairs have been reset')
                else:
                    first_run = False

                if not reset_flag:
                    switch_pair_df['switchWwn_pair_MANUAL'] = np.nan
                    switch_pair_df = dfop.move_column(switch_pair_df, cols_to_move='switchWwn_pair_MANUAL', ref_col='switchWwn_pair') 
                    # save manual_device_rename_df DataFrame to excel file to use at as form to fill 
                    sheet_title = 'switch_pair_manual'
                    file_path = dfop.dataframe_to_excel(switch_pair_df, sheet_title, report_creation_info_lst, force_flag=True)
                    file_name = os.path.basename(file_path)
                    file_directory = os.path.dirname(file_path)
                    print(f"\nPut REQUIRED to be changed switch wwn to switchWwn_pair_MANUAL column of the '{file_name}' file, '{sheet_title}' sheet in\n'{file_directory}' directory")
                    print('ATTN! CLOSE file after changes were made\n')
                    reply = meop.reply_request("When finish enter 'yes': ", ['yes'])
                    if reply == 'y':
                        while not fsop.check_file_is_closed(file_path):
                            reply = meop.reply_request('Have you closed the file? (y)es/(n)o: ', ['y'])
                        print('\n')
                        switch_pair_df = sfop.dataframe_import(sheet_title, max_title, init_file = file_path, header = 2)
                        copy_unchanged_paired_wwns(switch_pair_df)
                        switch_pair_df = update_switch_pair_dataframe(switch_pair_df)
                        switch_pair_df = dfop.drop_column_if_all_na(switch_pair_df, 'switchName_pair_by_labels')
            else:
                break
                    
        if change_flag:
            reply = meop.reply_request('Do you want to SAVE or CANCEL all changes? (s)ave/(c)ancel: ', ['s', 'save', 'c', 'cancel'])
            if reply in ['c', 'cancel']:
                print('Switch pair changes is cancelled')
                switch_pair_df = switch_pair_bckp_df.copy()
                change_flag = False
            else:
                print('Switch pair changes is saved')
            switch_pair_df = assign_switch_pair_id(switch_pair_df)

        sw_wwn_occurrence_stats_df = count_switch_pairs_match_stats(switch_pair_df)
        
        info = f'Switch pairs setting'
        print(info, end =" ")
        if first_run or change_flag:
            switch_pair_df = assign_switch_pair_id(switch_pair_df)
            meop.status_info('ok', max_title, len(info))
        else:
            meop.status_info('skip', max_title, len(info))
        
        # create list with partitioned DataFrames
        data_lst = [switch_pair_df, sw_wwn_occurrence_stats_df]
        # writing data to sql
        dbop.write_database(report_constant_lst, report_steps_dct, data_names, *data_lst)  
    # verify if loaded data is empty and replace information string with empty DataFrame
    else:
        data_lst = dbop.verify_read_data(report_constant_lst, data_names, *data_lst)
        switch_pair_df, *_ = data_lst
    # save data to excel file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        dfop.dataframe_to_excel(data_frame, data_name, report_creation_info_lst)

    return switch_pair_df 


def show_switch_wwn_pair_summary(switch_pair_df):
    """Function to display status of switchWwn pair match."""

    if all_switch_pairs_matched(switch_pair_df):
        print('ALL switch pairs have MATCHED.\n')
        pairing_type_stat_df = switch_pair_df.groupby(by=['Fabric_name', 'Switch_pairing_type'])['Switch_pairing_type'].count()
        print(pairing_type_stat_df)
    else:
        print('Switch pairs MISMATCH is found.\n')
        print(count_switch_pairs_match_stats(switch_pair_df))
    print('\n')


def copy_unchanged_paired_wwns(switch_pair_df):
    """Function to fill manual switchWwn pair with one derived from auto calculations"""

    switch_pair_df['switchWwn_pair_MANUAL'].fillna(switch_pair_df['switchWwn_pair'], inplace=True)
    switch_pair_df.drop(columns=['switchWwn_pair'], inplace=True)
    switch_pair_df.rename(columns={'switchWwn_pair_MANUAL': 'switchWwn_pair'}, inplace=True)


