"""
Module to create Blade IO modules report table
and add Device_Location column to blade modules DataFrame
"""


import pandas as pd

from common_operations_dataframe import dataframe_segmentation
from common_operations_filesystem import load_data, save_data, save_xlsx_file
from common_operations_miscellaneous import force_extract_check, status_info
from common_operations_servicefile import dataframe_import


def blademodule_analysis(blade_module_df, report_data_lst):
    """Main function to add connected devices information to portshow DataFrame"""
    
    # report_data_lst contains information: 
    # customer_name, dir_report, dir to save obtained data, max_title, report_steps_dct
    *_, max_title, report_steps_dct = report_data_lst

    # names to save data obtained after current module execution
    data_names = ['blade_module_loc', 'Blade_шасси']
    # service step information
    print(f'\n\n{report_steps_dct[data_names[0]][3]}\n')
    
    # load data if they were saved on previos program execution iteration
    data_lst = load_data(report_data_lst, *data_names)
    # unpacking DataFrames from the loaded list with data
    # pylint: disable=unbalanced-tuple-unpacking
    blade_module_loc_df, blade_module_report_df, = data_lst
    # nsshow_unsplit_df = pd.DataFrame()

    # list of data to analyze from report_info table
    analyzed_data_names = ['blade_interconnect']

    # data force extract check 
    # list of keys for each data from data_lst representing if it is required 
    # to re-collect or re-analyze data even they were obtained on previous iterations 
    force_extract_keys_lst = [report_steps_dct[data_name][1] for data_name in data_names]
    # list with True (if data loaded) and/or False (if data was not found and None returned)
    data_check = force_extract_check(data_names, data_lst, force_extract_keys_lst, max_title)

    # check force extract keys for data passed to main function as parameters and fabric labels
    # if analyzed data was re-extracted or re-analyzed on previous steps then data from data_lst
    # need to be re-checked regardless if it was analyzed on prev iterations
    analyzed_data_change_flags_lst = [report_steps_dct[data_name][1] for data_name in analyzed_data_names]

    # when no data saved or force extract flag is on or data passed as parameters have been changed then 
    # analyze extracted config data  
    if not all(data_check) or any(force_extract_keys_lst) or any(analyzed_data_change_flags_lst):
        # information string if data used have been forcibly changed
        if any(analyzed_data_change_flags_lst) and not any(force_extract_keys_lst) and all(data_check):
            info = f'Force data processing due to change in collected or analyzed data'
            print(info, end =" ")
            status_info('ok', max_title, len(info))
         
        # current operation information string
        info = f'Generating blade modules location table'
        print(info, end =" ") 

        # create DataFrame with Device_Location column
        blade_module_loc_df = blademodule_location(blade_module_df)
        # after finish display status
        status_info('ok', max_title, len(info))
        # create Blade chassis report table
        blade_module_report_df = blademodule_report(blade_module_df, data_names, max_title)
        # create list with partitioned DataFrames
        data_lst = [blade_module_loc_df, blade_module_report_df]
        # saving data to json or csv file
        save_data(report_data_lst, data_names, *data_lst)
    # save data to service file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        save_xlsx_file(data_frame, data_name, report_data_lst)

    return blade_module_loc_df


# auxiliary lambda function to combine two columns in DataFrame
# it combines to columns if both are not null and takes second if first is null
# str1 and str2 are strings before columns respectively (default is whitespace between columns)
wise_combine = lambda series, str1='', str2=' ': \
    str1 + str(series.iloc[0]) + str2 + str(series.iloc[1]) \
        if pd.notna(series.iloc[[0,1]]).all() else series.iloc[1]


def blademodule_location(blade_module_df):
    """Function to add Device_Location column to Blade chassis DataFrame"""

    # add Device_Location column to DataFrame
    columns_lst = [*blade_module_df.columns.to_list(), 'Device_Location']
    blade_module_loc_df = blade_module_df.reindex(columns = columns_lst)

    if not blade_module_df.empty:
        # combine 'Enclosure_Name' and 'Bay' columns
        blade_module_loc_df['Device_Location'] = \
            blade_module_loc_df[['Enclosure_Name', 'Interconnect_Bay']].apply(wise_combine, axis=1, args=('Enclosure ', ' bay '))

    return blade_module_loc_df


def blademodule_report(blade_module_df, data_names, max_title):
    """Function to create Blade IO modules report table"""

    report_columns_usage_dct = {'fabric_name_usage': False, 'chassis_info_usage': False}

    # columns_lst = [*blade_module_df.columns.to_list(), 'FW_Supported', 'Recommended_FW'] # remove
    # blade_modules_prep_df = blade_module_df.reindex(columns = columns_lst) # remove

    blade_module_report_df, = dataframe_segmentation(blade_module_df, data_names[1:], report_columns_usage_dct, max_title)

    return blade_module_report_df
