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


def switch_pair_analysis(switch_params_aggregated_df, portshow_aggregated_df, fcr_xd_proxydev_df, project_constants_lst):
    """Function to set switch pair IDs"""

    # imported project constants required for module execution
    project_steps_df, max_title, io_data_names_df, *_ = project_constants_lst

    # data titles obtained after module execution (output data)
    # data titles which module is dependent on (input data)
    data_names, analyzed_data_names = dfop.list_from_dataframe(io_data_names_df, 'switch_pair_analysis_out', 'switch_pair_analysis_in')
    
    # module information
    meop.show_module_info(project_steps_df, data_names)
    # read data from database if they were saved on previos program execution iteration
    data_lst = dbop.read_database(project_constants_lst, *data_names)
    switch_pair_df, npv_ag_connected_devices_df, *_ = data_lst
    
    # force run when any output data from data_lst is not found in database or 
    # procedure execution explicitly requested (force_run flag is on) for any output or input data   
    force_run = meop.verify_force_run(data_names, data_lst, project_steps_df, max_title, analyzed_data_names)

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
            switch_pair_df, npv_ag_connected_devices_df = auto_switch_pairing(switch_params_aggregated_df, portshow_aggregated_df, fcr_xd_proxydev_df)    
        
        switch_pair_bckp_df = switch_pair_df.copy()
        
        first_while_run = True

        while True:
            if first_while_run and not first_run:
                print('Summary for switch pairs loaded from database:')
                first_while_run = False
            show_switch_wwn_pair_summary(switch_pair_df)
            reply = meop.reply_request('Do you want to CHANGE switch pairs? (y)es/(n)o: ')
            if reply == 'y':
                change_flag = True
                if not first_run:
                    question = 'Do you want to RESET switch pairs or MODIFY EXISTING one? (r)eset/(m)odify: '
                    reply = meop.reply_request(question, reply_options=['r', 'reset', 'm', 'modify'])
                    if reply in ['r', 'reset']:
                        reset_flag = True
                        switch_pair_df, npv_ag_connected_devices_df = auto_switch_pairing(switch_params_aggregated_df, portshow_aggregated_df, fcr_xd_proxydev_df)
                        print('Switch pairs have been reset')
                    else:
                        reset_flag = False
                else:
                    first_run = False

                if not reset_flag:
                    switch_pair_df['switchWwn_pair_MANUAL'] = np.nan
                    switch_pair_df = dfop.move_column(switch_pair_df, cols_to_move='switchWwn_pair_MANUAL', ref_col='switchWwn_pair') 
                    # save manual_device_rename_df DataFrame to excel file to use at as form to fill 
                    sheet_title = 'switch_pair_manual'
                    file_path = dfop.dataframe_to_excel(switch_pair_df, sheet_title, project_constants_lst, force_flag=True)
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
        
        print('\n')
        info = f'Switch pairs setting'
        print(info, end =" ")
        if first_run or change_flag:
            switch_pair_df = assign_switch_pair_id(switch_pair_df)
            meop.status_info('ok', max_title, len(info))
        else:
            meop.status_info('skip', max_title, len(info))
        
        # create list with partitioned DataFrames
        data_lst = [switch_pair_df, npv_ag_connected_devices_df, sw_wwn_occurrence_stats_df]
        # writing data to sql
        dbop.write_database(project_constants_lst, data_names, *data_lst)  
    # verify if loaded data is empty and replace information string with empty DataFrame
    else:
        data_lst = dbop.verify_read_data(max_title, data_names, *data_lst)
        switch_pair_df, npv_ag_connected_devices_df, *_ = data_lst
    # save data to excel file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        dfop.dataframe_to_excel(data_frame, data_name, project_constants_lst)
    return switch_pair_df, npv_ag_connected_devices_df 


def show_switch_wwn_pair_summary(switch_pair_df):
    """Function to display status of switchWwn pair match."""

    if all_switch_pairs_matched(switch_pair_df):
        print('\nALL switch pairs have MATCHED.\n')
        pairing_type_stat_df = switch_pair_df.groupby(by=['Fabric_name', 'Switch_pairing_type'])['Switch_pairing_type'].count()
        print(pairing_type_stat_df)
    else:
        print('\nSwitch pairs MISMATCH is found.\n')
        print(count_switch_pairs_match_stats(switch_pair_df))
    print('\n')


def copy_unchanged_paired_wwns(switch_pair_df):
    """Function to fill manual switchWwn pair with one derived from auto calculations"""

    switch_pair_df['switchWwn_pair_MANUAL'].fillna(switch_pair_df['switchWwn_pair'], inplace=True)
    switch_pair_df.drop(columns=['switchWwn_pair'], inplace=True)
    switch_pair_df.rename(columns={'switchWwn_pair_MANUAL': 'switchWwn_pair'}, inplace=True)


