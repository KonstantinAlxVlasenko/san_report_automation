"""Module to set fabric names and labels manually"""


import numpy as np
import pandas as pd

import utilities.module_execution as meop


def manual_fabrics_labeling(fabricshow_summary_df, fabricshow_summary_automatic_df, info_labels):
    """Function to manual change Fabric Name and Fabric Label."""
    # copy of initial fabricshow_summary DataFrame
    # to be able to reset all changes 
    fabricshow_summary_default_df = fabricshow_summary_df.copy()
    # convert DataFrame indexes to string versions 
    fabric_indexes_str_lst = [str(i) for i in fabricshow_summary_df.index]
    # DataFrame operation options (save, reset, exit)
    opeartion_options_lst = ['s', 'r', 'v', 'x', 'a']
    # aggregated list of available operations
    full_options_lst = fabric_indexes_str_lst + opeartion_options_lst
    # parameters to change 
    rename_options_dct = {'NAME': 'Fabric_name', 'LABEL': 'Fabric_label'}
    
    # user input from full_options_lst
    # initial value is None to enter while
    input_option = None
    # work with DataFrame until it is saved or exit without saving
    while not input_option in ['s', 'x']:

        fabricshow_summary_df.sort_values(by=['Fabric_name', 'Fabric_label', 'Principal_switch_name', 'Domain_IDs'],
                                                inplace=True, ignore_index=True)
        # no need to print fabric info after verification
        if input_option != 'v':
            # printing actual fabricshow_summary DataFrame
            print('\nCurrent fabric labeling\n')
            print(fabricshow_summary_df.loc[:, info_labels])
        
        # printing menu options to choose to work with DataFrame
        # save, reset, exit or fabric index number to change
        print('\nS/s - Save changes in labeling\nA/a - Atomatic labeling\nR/r - Reset to default labeling\nV/v - Veriify labeling\nX/x - Exit without saving')
        print(f"{', '.join(fabric_indexes_str_lst)} - Choose fabric index to change labeling\n")
        # reset input_option value after each iteration to enter while loop
        input_option = meop.reply_request("Choose option: ", reply_options = full_options_lst, show_reply = True)
        
        # user input is fabric index to change labeling 
        if input_option in fabric_indexes_str_lst:
            print('\n')
            # make copy of actual fabricshow_summary DataFrame
            # to be able to reset current iteration changes
            fabricshow_summary_before_df = fabricshow_summary_df.copy()
            
            # convert user input fabric number to integer
            fabric_num = int(input_option)
            # printing information for current fabric
            print(fabricshow_summary_df.iloc[fabric_num])
            print('\n')
            
            # list to save user input
            reply_lst = []
            # initial user input to check if nan is entered
            value = 'empty'
            # ask user to change labels and names by looping over rename_options_dct 
            for option_name, option_column in rename_options_dct.items():
                if value:
                    reply = meop.reply_request(f'Do you want to change Fabric {option_name}? (y)es/(n)o: ')
                    # save user input to the list
                    reply_lst.append(reply)
                    # if user want to change name or label ask to enter new value
                    if reply == 'y':
                        current_value = fabricshow_summary_df.loc[fabric_num, rename_options_dct[option_name]]
                        value = input(f'\nEnter new Fabric {option_name}. 0 or None will remove fabric from assessment. Current value is {current_value}: ')
                        value = value.strip()
                        # 0 or None means no labeling
                        if value.lower() in ['0', 'none']:
                            value = None
                            fabricshow_summary_df.loc[fabric_num, ['Fabric_name', 'Fabric_label']] = [np.nan, np.nan] 
                        elif len(value) == 0:
                            value = current_value
                            print(f'Fabric {option_name} was not changed')
                        else:
                            # change values in fabricshow_summary DataFrame 
                            fabricshow_summary_df.loc[fabric_num, option_column] = value
            # if user didn't reply "no" two times
            if reply_lst != ['n']*2:
                print('\n')
                # print current fabric information after change
                print(fabricshow_summary_df.iloc[fabric_num])
                print('\n')
                # verify if Fabric names or label is NaN and ask input
                label_name_uniformity = fabricshow_summary_df.loc[fabric_num, rename_options_dct.values()].isna().nunique() == 1
                if not label_name_uniformity:
                    print('Fabric NAME and Fabric LABEL fields BOTH must have values')
                    # verify which label have Nan value
                    nan_option_name, = [key for key, value in rename_options_dct.items() if pd.isna(fabricshow_summary_df.loc[fabric_num, value])]
                    while True:
                        current_value = fabricshow_summary_df.loc[fabric_num, rename_options_dct[nan_option_name]]
                        value = input(f'\nEnter new Fabric {nan_option_name}. Current value is {current_value}: ')
                        value = value.strip()
                        # 0, None or '' means no labeling
                        if value in ['0', 'None', '', 'none']:
                            print(f"Fabric {nan_option_name} must have value.")
                            continue
                        else:
                            fabricshow_summary_df.loc[fabric_num, rename_options_dct[nan_option_name]] = value
                            print('\n')
                            # print current fabric information after change
                            print(fabricshow_summary_df.iloc[fabric_num])
                            print('\n')
                            break

                reply = meop.reply_request('Do you want to keep changes? (y)es/(n)o: ')
                # when user doesn't want to keep data fabricshow_summary DataFrame
                # returns to the state saved bedore current iteration
                if reply == 'n':
                    fabricshow_summary_df = fabricshow_summary_before_df.copy()
        # user input is verify fabric labeling
        elif input_option == 'v':
            count_labels_df = verify_fabric_labels(fabricshow_summary_df)
            print('\n', count_labels_df, '\n')
        # user input is save current labeling configuration and exit   
        elif input_option == 's':
            count_labels_df = verify_fabric_labels(fabricshow_summary_df)
            print('\n', count_labels_df, '\n')
            # check for errors in fabric labeling
            if (count_labels_df['Verification'] == 'ERROR').any():
                print('There is an error in fabric labeling. Please re-label fabrics.')
                input_option = None
            else:
                # if fabric labeling is ok request to save
                reply = meop.reply_request('Do you want to save changes and exit? (y)es/(n)o: ')
                # for save option do nothing and while loop stops on next condition check
                if reply == 'y':
                    print('\nSaved fabric labeling\n')
                else:
                    input_option = None
        # user input is reset current labeling configuration and start labeling from scratch
        elif input_option == 'r':
            reply = meop.reply_request('Do you want to reset fabric labeling to original values? (y)es/(n)o: ')
            # for reset option actual fabricshow_summary DataFrame returns back
            # to initial DataFrame version and while loop don't stop
            if reply == 'y':
                fabricshow_summary_df = fabricshow_summary_default_df.copy()
                print('\nFabric labeling has been reset to original version\n')
        # user input is automatic re-labeling
        elif input_option == 'a':
            reply = meop.reply_request('Do you want to perform automatic fabric labeling? (y)es/(n)o: ')
            # for reset option actual fabricshow_summary DataFrame returns back
            # to initial DataFrame version and while loop don't stop
            if reply == 'y':
                fabricshow_summary_df = fabricshow_summary_automatic_df.copy()
                print('\nAutomatic fabric labeling has been performed\n')
        # user input is exit without saving
        elif input_option == 'x':
            reply = meop.reply_request('Do you want to leave without saving? (y)es/(n)o: ')
            # for exit option DataFrame returns back to initial version
            # and while loop stops
            if reply == 'y':
                fabricshow_summary_df = fabricshow_summary_default_df.copy()
                print('\nKeeping original labeling version\n')
            else:
                input_option = None
    else:
        # show actual version of DataFrame after user inputs save or exit
        print(fabricshow_summary_df.loc[:, info_labels])
        print('\n')
    return fabricshow_summary_df


def verify_fabric_labels(fabricshow_summary_df):
    """Function to count fabric_labels in each fabric to avoid label duplicates causing errors"""

    # count fabric labels for each fabric name
    count_labels_df = pd.crosstab(fabricshow_summary_df.Fabric_name, fabricshow_summary_df.Fabric_label)
    columns = count_labels_df.columns.tolist()
    count_labels_df['Verification'] = np.nan
    # put 'error' tag for fabric_name if number of any label is exceed 1
    count_labels_df['Verification'] = np.where(count_labels_df[columns].gt(1).any(axis=1), 'ERROR', count_labels_df['Verification'])
    # put 'warning' tag if some labels are absent
    count_labels_df['Verification'] = \
        np.where((count_labels_df[columns] == 0).any(axis=1) & count_labels_df[columns].lt(2).all(axis=1), 'WARNING', count_labels_df['Verification'])
    # put 'ok' tag if each label used once
    count_labels_df['Verification'] = np.where((count_labels_df[columns] == 1).all(axis=1), 'OK', count_labels_df['Verification'])
    return count_labels_df
