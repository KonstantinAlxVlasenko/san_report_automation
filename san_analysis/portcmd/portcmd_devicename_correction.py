"""Module to change device names in portshow_aggregated DataFrame based on user input"""

import os

import numpy as np
import pandas as pd

import utilities.database_operations as dbop
import utilities.dataframe_operations as dfop
import utilities.module_execution as meop
import utilities.report_operations as report
import utilities.servicefile_operations as sfop


def devicename_correction_main(portshow_aggregated_df, device_rename_df, project_constants_lst):
    """Main function to rename devices"""

    project_steps_df, max_title, io_data_names_df, _, _, report_columns_usage_sr, *_ = project_constants_lst

    analyzed_data_names = dfop.list_from_dataframe(io_data_names_df, 'devicename_correction_analysis_in')
    
    # check if portshow_aggregated DataFrame is changed or 'device_rename' force flag is on
    force_form_update_flag = any([project_steps_df.loc[data_name, 'force_run'] for data_name in analyzed_data_names])
    # list of related DataFrame names requested to change
    force_change_data_lst = [data_name for data_name in analyzed_data_names[1:] if project_steps_df.loc[data_name, 'force_run']]
    # flag to force change group name usage mode
    force_group_name_usage_update_flag = project_steps_df.loc['report_columns_usage_upd', 'force_run']

    # create DataFrame with devices required to change names
    device_rename_df = define_device_to_rename(portshow_aggregated_df, device_rename_df, max_title, 
                                                    force_form_update_flag, force_change_data_lst, project_constants_lst)
    # current operation information string
    info = f'Applying device rename schema'
    print(info, end =" ")

    # if at least one name is changed apply rename schema 
    if device_rename_df['Device_Host_Name_rename'].notna().any():
        portshow_aggregated_df = device_rename(portshow_aggregated_df, device_rename_df)
        meop.status_info('ok', max_title, len(info))
    else:
        # to ask if group usage required
        force_group_name_usage_update_flag = 1
        meop.status_info('skip', max_title, len(info))

    # check if device Group_Name should be used in report tables (alias group name)
    group_name_usage(report_columns_usage_sr, device_rename_df, max_title, force_group_name_usage_update_flag)
    return portshow_aggregated_df, device_rename_df


def define_device_to_rename(portshow_aggregated_df, device_rename_df, max_title, 
                                force_form_update_flag, force_change_data_lst, project_constants_lst):
    """
    Function to define (create new, return previously saved or return empty) 
    device_rename_df DataFrame to apply device rename schema
    """

    _, max_title, *_ = project_constants_lst

    device_rename_columns = ['Fabric_name', 'Device_Host_Name', 'Group_Name', 
                                'deviceType', 'deviceSubtype', 'Device_Host_Name_rename']
    empty_device_rename_df = pd.DataFrame(columns=device_rename_columns)

    # if device_rename_df DataFrame doesn't exist (1st iteration)
    # or force flag to change device_rename_df DataFrame is on 
    # or some related DataFrames was forcibly changed

    if device_rename_df is None or force_form_update_flag:
        print('\n')
        if force_change_data_lst:
            print(f"Request to force change of {', '.join(force_change_data_lst)} data was received.")
        reply = meop.reply_request('Do you want to CHANGE AUTO assigned device names? (y)es/(n)o: ')
        if reply == 'y':
            # if device_rename_df DataFrame doesn't exist (1st iteration)
            if device_rename_df is None:
                # create new device rename DataFrame
                manual_device_rename_df = create_device_rename_form(portshow_aggregated_df)
            else:
                # if any related DataFrames was forcibly changed ask if device rename form reset required
                if force_change_data_lst:
                    reply = meop.reply_request('Do you want to APPLY SAVED device rename schema? (y)es/(n)o: ')
                    if reply == 'y':
                        print('\n')
                        return device_rename_df
                    else:
                        print('\n')
                        reply = meop.reply_request('Do you want to RESET device rename schema? (y)es/(n)o: ')
                        if reply == 'y':
                            # create new device rename DataFrame
                            manual_device_rename_df = create_device_rename_form(portshow_aggregated_df)
                        else:
                            # use saved device rename DataFrame
                            manual_device_rename_df = device_rename_df.copy()
                else:
                    # if no force change in related DataFrames but device_rename_df DataFrame 
                    # change initiated use saved device rename DataFrame
                    manual_device_rename_df = device_rename_df.copy()

            
            if not manual_device_rename_df.empty:
                # save manual_device_rename_df DataFrame to excel file to use at as form to fill 
                sheet_title = 'device_rename_form'
                file_path = report.dataframe_to_excel(manual_device_rename_df, sheet_title, project_constants_lst, force_flag = True)
                file_name = os.path.basename(file_path)
                file_directory = os.path.dirname(file_path)
                print(f"\nTo rename devices put new names into the '{file_name}' file, '{sheet_title}' sheet in\n'{file_directory}' directory")
                print('ATTN! CLOSE file after changes were made\n')
                # complete the manual_device_rename_df form and import it
                reply = meop.reply_request("When finish enter 'yes': ", ['yes'])
                if reply == 'y':
                    print('\n')
                    device_rename_df = sfop.dataframe_import(sheet_title, max_title, init_file = file_path, header = 2)
            else:
                print('No device to rename found')
                device_rename_df = empty_device_rename_df

        else:
            # if don't change auto assigned names save empty device_rename_df DataFrame
            device_rename_df = empty_device_rename_df
    # else:
    #     # check loaded device_rename_df DataFrame (if it's empty)
    #     device_rename_df, = dbop.verify_read_data(report_constant_lst, ['device_rename'], device_rename_df,  show_status=False)
    return device_rename_df


def create_device_rename_form(portshow_aggregated_df):
    """
    Auxiliary function for define_device_to_rename function. 
    Creates manual_device_rename_df DataFrame from portshow_aggregated_df DataFrame
    to use it as form to complete with new device names
    """

    mask_columns_lst = ['Fabric_name', 'Device_Host_Name', 'Group_Name', 
                        'deviceType', 'deviceSubtype', 'Host_Name', 
                        'alias', 'Connected_portWwn']
    # no need to change server host name extracted from name server and fdmi
    mask_empty_host_name =  portshow_aggregated_df['Host_Name'].isna()
    # storage, library and server name can be changed only
    mask_device_class = ~portshow_aggregated_df['deviceType'].isin(['SWITCH', 'VC'])
    # if no Device_Host_Name defined then no device connected
    mask_device_host_name = portshow_aggregated_df['Device_Host_Name'].notna()
    
    # no need to rename 3PARs with parsed configs
    mask_3par = portshow_aggregated_df['deviceSubtype'].str.lower() == '3par'
    mask_device_name = portshow_aggregated_df['Device_Name'].notna()
    mask_3par_parsed = mask_3par & mask_device_name

    # join all masks
    mask_complete =  mask_device_class & mask_empty_host_name & mask_device_host_name & ~mask_3par_parsed

    manual_device_rename_df = portshow_aggregated_df.loc[mask_complete , mask_columns_lst].copy()
    manual_device_rename_df.drop_duplicates(inplace=True)

    # drop unnecessary column
    manual_device_rename_df.drop(columns=['Host_Name'], inplace=True)

    '''When device has different wwnn then Group_Name might differ for each wwnn
    and one device can have several Group_Names thus generating duplication in tables.
    To avoid this perform gropuby based on Device_Host_Name reqiured'''

    # add quantity of ports in each 'Device_Host_Name'
    manual_device_rename_df['Port_quantity'] = \
        manual_device_rename_df.groupby(['Fabric_name', 'Device_Host_Name'], as_index=False).Connected_portWwn.transform('count')

    manual_device_rename_df['Group_Name'].fillna('nan', inplace=True)

    # join group names, aliases and pWwns for each Device_Host_Name
    manual_device_rename_df = manual_device_rename_df.groupby(['Fabric_name', 'Device_Host_Name'], as_index = False).\
                                                                agg({'Group_Name': lambda x : ', '.join(y for y in x if y==y), 
                                                                        'alias': lambda x : ', '.join(y for y in x if y==y), 
                                                                        'Connected_portWwn': lambda x : ', '.join(y for y in x if y==y),
                                                                        'deviceType': 'first', 'deviceSubtype': 'first', 
                                                                        'Port_quantity': 'first'})

    # drop duplicated values from cell
    for column in ['Group_Name', 'alias']:
        manual_device_rename_df[column] = manual_device_rename_df[column].str.split(', ').apply(set).str.join(', ')
    # replace '' (empty string) with np.nan
    manual_device_rename_df = manual_device_rename_df.replace(r'^\s*$', np.NaN, regex=True)
    # create new column for new device names manual imput 
    manual_device_rename_df['Device_Host_Name_rename'] = np.nan
    # sorting DataFrame
    sort_columns = ['Fabric_name', 'deviceType', 'deviceSubtype', 'Device_Host_Name']
    manual_device_rename_df.sort_values(by=sort_columns, inplace=True)
    manual_device_rename_df.reset_index(drop=True, inplace=True)

    return manual_device_rename_df


def device_rename(portshow_aggregated_df, device_rename_df):
    """
    Function to rename devices in portshow_aggregated_df DataFrame if
    device_rename_df contains new name(s)
    """

    device_rename_columns = ['Fabric_name', 'Device_Host_Name', 'deviceType', 
                                'deviceSubtype', 'Device_Host_Name_rename']
    device_rename_join_df = device_rename_df.loc[:, device_rename_columns].copy()
    # add new manual device names based on automatic device names and device class in each fabric 
    portshow_aggregated_df = \
        portshow_aggregated_df.merge(device_rename_join_df, how = 'left', on = device_rename_columns[:4])
    
    portshow_aggregated_df['Device_Host_Name_w_domain_rename'] = portshow_aggregated_df['Device_Host_Name_rename']
    
    # copy all unchanged names and rename new column name
    portshow_aggregated_df['Device_Host_Name_rename'] = \
        portshow_aggregated_df['Device_Host_Name_rename'].fillna(portshow_aggregated_df['Device_Host_Name'])
    
    portshow_aggregated_df['Device_Host_Name_w_domain_rename'] = \
        portshow_aggregated_df['Device_Host_Name_w_domain_rename'].fillna(portshow_aggregated_df['Device_Host_Name_w_domain'])
    
    portshow_aggregated_df.rename(columns={'Device_Host_Name': 'Device_Host_Name_old', 
                                            'Device_Host_Name_rename': 'Device_Host_Name',
                                            'Device_Host_Name_w_domain': 'Device_Host_Name_w_domain_old', 
                                            'Device_Host_Name_w_domain_rename': 'Device_Host_Name_w_domain'}, inplace=True)

    return portshow_aggregated_df


def group_name_usage(report_columns_usage_sr, device_rename_df, max_title, force_group_name_usage_update_flag):
    """Function to determine if device alias Group_Name column required in report tables.
    Value (True or False) written to the dictonary so no need to return value."""

    info = f'Device Group Name usage'

    # if all devices were renamed and force flag is off column is dropped from report tables 
    if device_rename_df['Device_Host_Name_rename'].notna().all() and not force_group_name_usage_update_flag:
        report_columns_usage_sr['group_name_usage'] = 0
        print(info, end =" ")
        meop.status_info('off', max_title, len(info))
    # if no information in report_columns_usage_sr or force flag is on or any device keep old name
    # ask user input 
    elif report_columns_usage_sr.get('group_name_usage') is None or \
            force_group_name_usage_update_flag or \
                device_rename_df['Device_Host_Name_rename'].isna().any():
        print('\n')
        reply = meop.reply_request('Do you want to use Alias Group Device names? (y)es/(n)o: ')
        print('\n')
        if reply == 'y':
            report_columns_usage_sr['group_name_usage'] = 1
            print(info, end =" ")
            meop.status_info('on', max_title, len(info))
        else:
            report_columns_usage_sr['group_name_usage'] = 0
            print(info, end =" ")
            meop.status_info('off', max_title, len(info))









        


