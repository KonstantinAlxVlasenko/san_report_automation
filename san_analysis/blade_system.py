"""
Module to create Blade IO modules report table
and add Device_Location column to blade modules DataFrame
"""


import pandas as pd


import utilities.dataframe_operations as dfop
import utilities.database_operations as dbop
# import utilities.data_structure_operations as dsop
import utilities.module_execution as meop
# import utilities.servicefile_operations as sfop
# import utilities.filesystem_operations as fsop


def blade_system_analysis(blade_module_df, synergy_module_df, report_creation_info_lst):
    """Main function to add connected devices information to portshow DataFrame"""
    
    # report_steps_dct contains current step desciption and force and export tags
    # report_headers_df contains column titles, 
    # report_columns_usage_dct show if fabric_name, chassis_name and group_name of device ports should be used
    report_constant_lst, report_steps_dct, report_headers_df = report_creation_info_lst
    # report_constant_lst contains information: customer_name, project directory, database directory, max_title
    *_, max_title = report_constant_lst

    # names to save data obtained after current module execution
    # data_names = ['blade_module_loc', 'Blade_шасси']
    data_names = ['blade_module_loc']
    # service step information
    print(f'\n\n{report_steps_dct[data_names[0]][3]}\n')
    
    # load data if they were saved on previos program execution iteration
    # data_lst = load_data(report_constant_lst, *data_names)
    # reade data from database if they were saved on previos program execution iteration
    data_lst = dbop.read_database(report_constant_lst, report_steps_dct, *data_names)
    
    # # unpacking DataFrames from the loaded list with data
    # # pylint: disable=unbalanced-tuple-unpacking
    # blade_module_loc_df, blade_module_report_df = data_lst

    # list of data to analyze from report_info table
    analyzed_data_names = ['blade_interconnect', 'blade_servers', 'blade_vc', 'synergy_interconnect', 'Blade_шасси']

    # force run when any data from data_lst was not saved (file not found) or 
    # procedure execution explicitly requested for output data or data used during fn execution  
    force_run = meop.verify_force_run(data_names, data_lst, report_steps_dct, 
                                            max_title, analyzed_data_names)
    if force_run:
        # current operation information string
        info = f'Generating blade modules location table'
        print(info, end =" ") 
        blade_module_df.drop_duplicates(inplace=True)
        # create DataFrame with Device_Location column
        blade_module_loc_df = blademodule_location(blade_module_df, synergy_module_df)



        # add VC device name if empty
        blade_module_loc_df = vc_name_fillna(blade_module_loc_df)
        # after finish display status
        meop.status_info('ok', max_title, len(info))
        # # create Blade chassis report table
        # blade_module_report_df = blademodule_report(blade_module_loc_df, report_headers_df, data_names)
        # blade_module_report_df = blademodule_report(blade_module_df, data_names, max_title)
        # create list with partitioned DataFrames
        # data_lst = [blade_module_loc_df, blade_module_report_df]

        data_lst = [blade_module_loc_df]

        # saving data to json or csv file
        # save_data(report_constant_lst, data_names, *data_lst)
        # writing data to sql
        dbop.write_database(report_constant_lst, report_steps_dct, data_names, *data_lst)  
    # verify if loaded data is empty and replace information string with empty DataFrame
    else:
        # blade_module_loc_df, blade_module_report_df = \
        #     verify_data(report_constant_lst, data_names, *data_lst)
        # data_lst = [blade_module_loc_df, blade_module_report_df]


        # blade_module_loc_df = \
        #     verify_data(report_constant_lst, data_names, *data_lst)
        # data_lst = [blade_module_loc_df]

        data_lst = dbop.verify_read_data(report_constant_lst, data_names, *data_lst)
        blade_module_loc_df, *_ = data_lst

    # save data to service file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        dfop.dataframe_to_excel(data_frame, data_name, report_creation_info_lst)
    return blade_module_loc_df


# auxiliary lambda function to combine two columns in DataFrame
# it combines to columns if both are not null and takes second if first is null
# str1 and str2 are strings before columns respectively (default is whitespace between columns)
wise_combine = lambda series, str1='', str2=' ': \
    str1 + str(series.iloc[0]) + str2 + str(series.iloc[1]) \
        if pd.notna(series.iloc[[0,1]]).all() else series.iloc[1]


def blademodule_location(blade_module_df, synergy_module_df):
    """Function to add Device_Location column to Blade chassis DataFrame"""

    # add Device_Location column to DataFrame
    columns_lst = [*blade_module_df.columns.to_list(), 'Device_Location', 'FW_Supported', 'Recommended_FW']
    blade_module_loc_df = blade_module_df.reindex(columns=columns_lst)

    if not blade_module_df.empty:
        # combine 'Enclosure_Name' and 'Bay' columns
        blade_module_loc_df['Device_Location'] = \
            blade_module_loc_df[['Enclosure_Name', 'Interconnect_Bay']].apply(wise_combine, axis=1, args=('Enclosure ', ' bay '))

    if not synergy_module_df.empty:
        synergy_module_df = synergy_module_df.reindex(columns=columns_lst)
        if not blade_module_df.empty:
            blade_module_loc_df = pd.concat([blade_module_loc_df, synergy_module_df], ignore_index=True)
        else:
            blade_module_loc_df = synergy_module_df

    return blade_module_loc_df


# def blademodule_report(blade_module_loc_df, report_headers_df, data_names):
#     """Function to create Blade IO modules report table"""

#     report_columns_usage_dct = {'fabric_name_usage': False, 'chassis_info_usage': False}

#     # pylint: disable=unbalanced-tuple-unpacking
#     # blade_module_report_df, = dataframe_segmentation(blade_module_loc_df, data_names[1:], report_columns_usage_dct, max_title)
#     blade_module_report_df = dfop.generate_report_dataframe(blade_module_loc_df, report_headers_df, report_columns_usage_dct, data_names[1])
#     return blade_module_report_df


def vc_name_fillna(blade_module_loc_df):
    """Function to combine 'VC' and module serial number to fill in empty VC name value"""

    if not blade_module_loc_df.empty:
        mask_vc = blade_module_loc_df['Interconnect_Model'].str.contains(r'VC Flex|Virtual Connect|VC.+FC Module', regex=True)
        mask_sn = blade_module_loc_df['Interconnect_SN'].notna()
        mask_modulename_empty = blade_module_loc_df['Interconnect_Name'].isna()
        mask_complete = mask_vc & mask_sn & mask_modulename_empty
        blade_module_loc_df.loc[mask_complete, 'Interconnect_Name'] = 'VC' + blade_module_loc_df['Interconnect_SN']
    return blade_module_loc_df