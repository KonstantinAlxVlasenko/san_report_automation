"""Module to create storage hosts DataFrame"""

import numpy as np
import pandas as pd

import utilities.database_operations as dbop
import utilities.dataframe_operations as dfop
import utilities.module_execution as meop
import utilities.report_operations as report
from .storage_host_aggregation import storage_host_aggregation


def storage_host_analysis(host_3par_df, system_3par_df, port_3par_df,
                        system_oceanstor_df, port_oceanstor_df, host_oceanstor_df, 
                        host_id_name_oceanstor_df, host_id_fcinitiator_oceanstor_df, 
                        hostid_ctrlportid_oceanstor_df,
                        portshow_aggregated_df, zoning_aggregated_df, 
                        project_constants_lst):
    """Main function to analyze storage port configuration"""

    # imported project constants required for module execution
    project_steps_df, max_title, io_data_names_df, _, report_headers_df, report_columns_usage_sr, *_ = project_constants_lst

    # data titles obtained after module execution (output data)
    # data titles which module is dependent on (input data)
    data_names, analyzed_data_names = dfop.list_from_dataframe(io_data_names_df, 'storage_host_analysis_out', 'storage_host_analysis_in')
    # module information
    meop.show_module_info(project_steps_df, data_names)
    # read data from database if they were saved on previos program execution iteration
    data_lst = dbop.read_database(project_constants_lst, *data_names)
    
    # force run when any output data from data_lst is not found in database or 
    # procedure execution explicitly requested (force_run flag is on) for any output or input data  
    force_run = meop.verify_force_run(data_names, data_lst, project_steps_df, 
                                            max_title, analyzed_data_names)
    if force_run:
        # current operation information string
        info = f'Generating storage hosts table'
        print(info, end =" ") 
        
        storage_host_aggregated_df = \
            storage_host_aggregation(host_3par_df, system_3par_df, port_3par_df, 
                                    system_oceanstor_df, port_oceanstor_df, host_oceanstor_df, 
                                    host_id_name_oceanstor_df, host_id_fcinitiator_oceanstor_df, 
                                    hostid_ctrlportid_oceanstor_df,
                                    portshow_aggregated_df, zoning_aggregated_df)
        # after finish display status
        meop.status_info('ok', max_title, len(info))
        # report tables
        storage_host_report_df, storage_host_compare_report_df = \
            storage_host_report(storage_host_aggregated_df, data_names, report_headers_df, report_columns_usage_sr)
        # create list with partitioned DataFrames
        data_lst = [storage_host_aggregated_df, storage_host_report_df, storage_host_compare_report_df]
        # writing data to sql
        dbop.write_database(project_constants_lst, data_names, *data_lst)  
    # verify if loaded data is empty and replace information string with empty DataFrame
    else:
        data_lst = dbop.verify_read_data(max_title, data_names, *data_lst)
        storage_host_aggregated_df, *_ = data_lst
    # save data to service file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        report.dataframe_to_excel(data_frame, data_name, project_constants_lst)
    return storage_host_aggregated_df


def storage_host_report(storage_host_aggregated_df, data_names, report_headers_df, report_columns_usage_sr):
    """Function to create storage_host and storage_host fabric_label comparision DataFrames"""

    if storage_host_aggregated_df.empty:
        return pd.DataFrame(), pd.DataFrame()
    
    storage_host_report_df = storage_host_aggregated_df.copy()
    # dataframe where hosts and storage port are in the same fabric or host imported to storage fabric
    mask_local_imported = storage_host_aggregated_df['Fabric_host_status'].isin(['local', 'remote_imported'])
    storage_host_valid_df = storage_host_aggregated_df.loc[mask_local_imported].copy()

    # drop uninformative columns 
    storage_host_report_df = clean_storage_host(storage_host_report_df)
    storage_host_valid_df = clean_storage_host(storage_host_valid_df)
    
    storage_host_valid_df = dfop.remove_duplicates_from_column(storage_host_valid_df, 'System_Name',
                                                                duplicates_subset=['configname', 'System_Name'])   
    # slice required columns and translate header
    storage_host_report_df = report.generate_report_dataframe(storage_host_report_df, report_headers_df, report_columns_usage_sr, data_names[1])
    report.drop_slot_value(storage_host_report_df, report_columns_usage_sr)
    storage_host_valid_df = report.generate_report_dataframe(storage_host_valid_df, report_headers_df, report_columns_usage_sr, data_names[1])
    report.drop_slot_value(storage_host_valid_df, report_columns_usage_sr)
    # translate values in columns
    storage_host_report_df = report.translate_values(storage_host_report_df)
    storage_host_valid_df = report.translate_values(storage_host_valid_df)
    # create comparision storage_host DataFrame based on Fabric_labels
    slice_column = 'Подсеть' if 'Подсеть' in storage_host_valid_df.columns else 'Подсеть порта массива'
    storage_host_compare_report_df = dfop.dataframe_slice_concatenate(storage_host_valid_df, column=slice_column)
    return storage_host_report_df, storage_host_compare_report_df


def clean_storage_host(df):
    """Function to clean storage_host and storage_host_valid (storage port and host are in the same fabric)"""

    # drop second column in each tuple of the list if values in columns of the tuple are equal
    df = dfop.drop_equal_columns(df, columns_pairs=[('Host_Wwnp', 'Host_Wwn'), 
                                                ('Device_Host_Name_per_fabric_name_and_label', 'Device_Host_Name_per_fabric_label'),
                                                ('Device_Host_Name_total_fabrics', 'Device_Host_Name_per_fabric_name')])
    # drop empty columns
    df = dfop.drop_column_if_all_na(df, ['Device_Port', 'Device_Location', 'LUN_quantity'])
    # drop columns where all values are equal to the item value
    columns_values = {'Host_Storage_Fabric_equal': 'Yes', 'Persona_correct': 'Yes', 'Fabric_host_status': 'local'}
    df = dfop.drop_all_identical(df, columns_values, dropna=True)
    # drop second pair of Fabric_name, Fabric_label if the columns are respectively equal 
    df = dfop.drop_equal_columns_pairs(df, columns_main=['Storage_Fabric_name', 'Storage_Fabric_label'], 
                                        columns_droped=['Host_Fabric_name', 'Host_Fabric_label'], dropna=False)
    # rename first pair of Fabric_name, Fabric_label if second one was droped in prev step
    if not 'Host_Fabric_name' in df.columns:
        df.rename(columns={'Storage_Fabric_name': 'Fabric_name', 'Storage_Fabric_label': 'Fabric_label'}, inplace=True)
    return df


        


