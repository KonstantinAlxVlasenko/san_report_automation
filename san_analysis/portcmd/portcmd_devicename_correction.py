
"""Module to change device names in portshow_aggregated DataFrame based on user input"""

import os

import numpy as np
import pandas as pd

import utilities.dataframe_operations as dfop
import utilities.database_operations as dbop
# import utilities.data_structure_operations as dsop
import utilities.module_execution as meop
import utilities.servicefile_operations as sfop
# import utilities.filesystem_operations as fsop




def devicename_correction_main(portshow_aggregated_df, device_rename_df, report_creation_info_lst):
    """Main function to rename devices"""

    # report_steps_dct contains current step desciption and force and export tags
    # report_headers_df contains column titles, 
    # report_columns_usage_dct show if fabric_name, chassis_name and group_name of device ports should be used
    report_constant_lst, report_steps_dct, report_headers_df, report_columns_usage_dct = report_creation_info_lst
    # report_constant_lst contains information: customer_name, project directory, database directory, max_title
    *_, max_title = report_constant_lst

    analyzed_data_names = ['device_rename', 'device_rename_form', 'portshow_aggregated', 'portcmd', 'switchshow_ports', 
                            'switch_params_aggregated', 'switch_parameters', 'chassis_parameters', 
                            'fdmi', 'nscamshow', 'nsshow', 'alias', 'blade_servers', 'synergy_servers', 'system_3par', 'fabric_labels']

    
    
    # check if portshow_aggregated DataFrame is changed or 'device_rename' force flag is on
    force_form_update_flag = any([report_steps_dct[data_name][1] for data_name in analyzed_data_names])
    # list of related DataFrame names requested to change 
    force_change_data_lst = [data_name for data_name in analyzed_data_names[1:] if report_steps_dct[data_name][1]]
    # flag to force change group name usage mode
    force_group_name_usage_update_flag = report_steps_dct['report_columns_usage_upd'][1]

    # create DataFrame with devices required to change names
    device_rename_df = define_device_to_rename(portshow_aggregated_df, device_rename_df, max_title, 
                                                    force_form_update_flag, force_change_data_lst, report_creation_info_lst)
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
    group_name_usage(report_columns_usage_dct, device_rename_df, max_title, force_group_name_usage_update_flag)

    return portshow_aggregated_df, device_rename_df


def define_device_to_rename(portshow_aggregated_df, device_rename_df, max_title, 
                                force_form_update_flag, force_change_data_lst, report_creation_info_lst):
    """
    Function to define (create new, return previously saved or return empty) 
    device_rename_df DataFrame to apply device rename schema
    """

    report_constant_lst, *_ = report_creation_info_lst
    # report_constant_lst contains information: customer_name, project directory, database directory, max_title
    *_, max_title = report_constant_lst

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
                file_path = dfop.dataframe_to_excel(manual_device_rename_df, sheet_title, report_creation_info_lst, force_flag = True)
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
        manual_device_rename_df.groupby(['Fabric_name', 'Device_Host_Name'], as_index = False).Connected_portWwn.transform('count')

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

    # TO_REMOVE
    # # fill empty values with 'no_grp_name' tag to perform Group_Name join aggregation
    # manual_device_rename_df['Group_Name'].fillna('no_grp_name', inplace=True)
    # manual_device_rename_df = \
    #     manual_device_rename_df.groupby(['Fabric_name', 'Device_Host_Name'], as_index = False).\
    #         agg({'Group_Name': ', '.join, 
    #                 'deviceType': 'first', 'deviceSubtype': 'first'})
    # manual_device_rename_df.reset_index(inplace=True, drop=True)
    # # remove 'no_grp_name' tag for devices with no Group_Name
    # mask_no_grp_name = manual_device_rename_df['Group_Name'].str.contains('no_grp_name')
    # manual_device_rename_df['Group_Name'] = manual_device_rename_df['Group_Name'].where(~mask_no_grp_name, np.nan)
    
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
    # copy all unchanged names and rename new column name
    portshow_aggregated_df['Device_Host_Name_rename'] = \
        portshow_aggregated_df['Device_Host_Name_rename'].fillna(portshow_aggregated_df['Device_Host_Name'])
    portshow_aggregated_df.rename(columns={'Device_Host_Name': 'Device_Host_Name_old', 
                                            'Device_Host_Name_rename': 'Device_Host_Name'}, inplace=True)

    return portshow_aggregated_df


def group_name_usage(report_columns_usage_dct, device_rename_df, max_title, force_group_name_usage_update_flag):
    """
    Function to determine if device alias Group_Name column required in report tables.
    Value (True or False) written to the dictonary so no need to return value.
    """

    info = f'Device Group Name usage'

    # if all devices were renamed and force flag is off column is dropped from report tables 
    if device_rename_df['Device_Host_Name_rename'].notna().all() and not force_group_name_usage_update_flag:
        report_columns_usage_dct['group_name_usage'] = False
        print(info, end =" ")
        meop.status_info('off', max_title, len(info))
    # if no information in report_columns_usage_dct or force flag is on or any device keep old name
    # ask user input 
    elif report_columns_usage_dct.get('group_name_usage') is None or \
            force_group_name_usage_update_flag or \
                device_rename_df['Device_Host_Name_rename'].isna().any():
        print('\n')
        reply = meop.reply_request('Do you want to use Alias Group Device names? (y)es/(n)o: ')
        print('\n')
        if reply == 'y':
            report_columns_usage_dct['group_name_usage'] = True
            print(info, end =" ")
            meop.status_info('on', max_title, len(info))
        else:
            report_columns_usage_dct['group_name_usage'] = False
            print(info, end =" ")
            meop.status_info('off', max_title, len(info))









        


