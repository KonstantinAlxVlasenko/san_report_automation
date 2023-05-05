"""Module to count Fabric statistics (quantity of port_types)"""


import utilities.database_operations as dbop
import utilities.dataframe_operations as dfop
import utilities.module_execution as meop
import utilities.report_operations as report

from .port_statistics_aggregation import port_statisctics_aggregated


def port_statistics_analysis(portshow_aggregated_df, project_constants_lst):
    """Main function to count Fabrics statistics"""

    # imported project constants required for module execution
    project_steps_df, max_title, io_data_names_df, _, report_headers_df, report_columns_usage_sr, *_ = project_constants_lst
    
    # data titles obtained after module execution (output data)
    # data titles which module is dependent on (input data)
    data_names, analyzed_data_names = dfop.list_from_dataframe(io_data_names_df, 'port_statistics_analysis_out', 'port_statistics_analysis_in')

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
        info = f'Counting up Fabrics statistics'
        print(info, end =" ")  

        port_statistics_df = port_statisctics_aggregated(portshow_aggregated_df)
        # after finish display status
        meop.status_info('ok', max_title, len(info))
        # get report DataFrame
        port_statistics_report_df = port_statistics_report(port_statistics_df, report_headers_df, report_columns_usage_sr)
        # create list with partitioned DataFrames
        data_lst = [port_statistics_df, port_statistics_report_df]
        # writing data to sql
        dbop.write_database(project_constants_lst, data_names, *data_lst)      
    # verify if loaded data is empty and replace information string with empty DataFrame
    else:
        data_lst = dbop.verify_read_data(max_title, data_names, *data_lst)
        port_statistics_df, *_ = data_lst
    # save data to service file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        report.dataframe_to_excel(data_frame, data_name, project_constants_lst)
    return port_statistics_df


def port_statistics_report(port_statistics_df, report_headers_df, report_columns_usage_sr):
    """Function to create report table out of statistics_df DataFrame"""

    port_statistics_report_df = port_statistics_df.copy()
    port_statistics_report_df.drop(columns = ['switchWwn', 'N:E_int', 'N:E_bw_int'], inplace=True)
    # drop Not Licensed ports column if there are no any
    port_statistics_report_df = dfop.drop_all_identical(port_statistics_report_df, 
                                            columns_values={'Not_licensed': 0, 'Disabled': 0, 'Offline': 0}, dropna=False)
    # drop Licensed column if all ports are licensed
    port_statistics_report_df = dfop.drop_equal_columns(port_statistics_report_df, columns_pairs = [('Total_ports_number', 'Licensed')])
    # drop column 'chassis_name' if it is not required
    if not report_columns_usage_sr['chassis_info_usage']:
        port_statistics_report_df.drop(columns = ['chassis_name'], inplace=True)
    # port_statistics_report_df = dfop.translate_header(port_statistics_report_df, report_headers_df, 'Статистика_портов')
    port_statistics_report_df = dfop.translate_dataframe(port_statistics_report_df, report_headers_df, 'Статистика_портов')
    # drop visual uninformative zeroes
    dfop.drop_zero(port_statistics_report_df)
    return port_statistics_report_df
