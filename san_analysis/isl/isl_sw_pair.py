"""Module to add switch pair id information to  'InterSwitch links', 'InterFabric links' customer report tables"""


import pandas as pd

import utilities.database_operations as dbop
import utilities.dataframe_operations as dfop
import utilities.module_execution as meop
import utilities.report_operations as report
import utilities.servicefile_operations as sfop

from .isl_statistics import isl_statistics


def isl_sw_pair_update(isl_aggregated_df, fcredge_aggregated_df, switch_pair_df, project_constants_lst):
    """Main function to add switch pair ID to ISL and IFR tables"""

    # imported project constants required for module execution
    project_steps_df, max_title, io_data_names_df, _, report_headers_df, report_columns_usage_sr, *_ = project_constants_lst

    # data titles obtained after module execution (output data)
    # data titles which module is dependent on (input data)
    data_names, analyzed_data_names = dfop.list_from_dataframe(io_data_names_df, 'isl_sw_pair_analysis_out', 'isl_sw_pair_analysis_in')
    # module information
    meop.show_module_info(project_steps_df, data_names)
    # read data from database if they were saved on previos program execution iteration
    data_lst = dbop.read_database(project_constants_lst, *data_names)
    
    # force run when any output data from data_lst is not found in database or 
    # procedure execution explicitly requested (force_run flag is on) for any output or input data
    force_run = meop.verify_force_run(data_names, data_lst, project_steps_df, 
                                            max_title, analyzed_data_names)
    if force_run:
        # data imported from init file (regular expression patterns) to extract values from data columns
        pattern_dct, _ = sfop.regex_pattern_import('common_regex', max_title)
        
        # current operation information string
        info = f'Updating ISL and IFL tables'
        print(info, end =" ")

        isl_aggregated_df = dfop.dataframe_join(isl_aggregated_df, switch_pair_df,  ['switchWwn', 'switchPair_id'], 1)
        if not fcredge_aggregated_df.empty:
            fcredge_aggregated_df = dfop.dataframe_join(fcredge_aggregated_df, switch_pair_df,  ['switchWwn', 'switchPair_id'], 1)

        isl_statistics_df = isl_statistics(isl_aggregated_df, pattern_dct)
        # after finish display status
        meop.status_info('ok', max_title, len(info))      

        isl_report_df = report.generate_report_dataframe(isl_aggregated_df, report_headers_df, report_columns_usage_sr, data_names[3]) 
        isl_report_df = dfop.translate_values(isl_report_df, translate_dct={'Yes': 'Да', 'No': 'Нет'})
        isl_report_df = dfop.drop_column_if_all_na(isl_report_df, columns=['Идентификатор транка', 'Deskew', 'Master', 'Идентификатор IFL'])
        # check if IFL table required
        if not fcredge_aggregated_df.empty:
            # ifl_report_df, = dataframe_segmentation(fcredge_df, [data_names[3]], report_columns_usage_sr, max_title)
            ifl_report_df = report.generate_report_dataframe(fcredge_aggregated_df, report_headers_df, report_columns_usage_sr, data_names[4]) 
        else:
            ifl_report_df = fcredge_aggregated_df.copy()

        isl_statistics_report_df = isl_statistics_report(isl_statistics_df, report_headers_df, report_columns_usage_sr)

        # create list with partitioned DataFrames
        data_lst = [isl_aggregated_df, isl_statistics_df, fcredge_aggregated_df, isl_report_df, ifl_report_df, isl_statistics_report_df]
        # writing data to sql
        dbop.write_database(project_constants_lst, data_names, *data_lst)  
    # verify if loaded data is empty and replace information string with empty DataFrame
    else:
        data_lst = dbop.verify_read_data(max_title, data_names, *data_lst)
        isl_aggregated_df, isl_statistics_df, *_ = data_lst

    # save data to service file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        report.dataframe_to_excel(data_frame, data_name, project_constants_lst)
    return isl_aggregated_df, isl_statistics_df


def isl_statistics_report(isl_statistics_df, report_headers_df, report_columns_usage_sr):
    """Function to create report table out of isl_statistics_df DataFrame"""

    isl_statistics_report_df = pd.DataFrame()

    if not isl_statistics_df.empty:
        chassis_column_usage = report_columns_usage_sr.get('chassis_info_usage')
        isl_statistics_report_df = isl_statistics_df.copy()
        # identify columns to drop and drop columns
        drop_columns = ['switchWwn', 'Connected_switchWwn', 'sort_column_1', 'sort_column_2', 'Connection_ID']
        if not chassis_column_usage:
            drop_columns.append('chassis_name')
        drop_columns = [column for column in drop_columns if column in isl_statistics_df.columns]
        isl_statistics_report_df.drop(columns=drop_columns, inplace=True)

        # translate values in columns and headers
        translated_columns = [column for column in isl_statistics_df.columns if 'note' in column and isl_statistics_df[column].notna().any()]
        translated_columns.extend(['Fabric_name', 'Trunking_lic_both_switches'])
        isl_statistics_report_df = dfop.translate_dataframe(isl_statistics_report_df, report_headers_df, 
                                                            'Статистика_ISL_перевод', translated_columns)
        # drop empty columns
        isl_statistics_report_df.dropna(axis=1, how='all', inplace=True)
        # remove zeroes to clean view
        dfop.drop_zero(isl_statistics_report_df)
    return isl_statistics_report_df