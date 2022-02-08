"""Module to set Fabric names and labels"""


from datetime import date

import pandas as pd
import utilities.database_operations as dbop
import utilities.dataframe_operations as dfop
import utilities.module_execution as meop

from .fabric_label_auto import auto_fabrics_labeling
from .fabric_label_manual import manual_fabrics_labeling


def fabric_label_analysis(switchshow_ports_df, switch_params_df, fabricshow_df, ag_principal_df, report_creation_info_lst):
    """Function to set Fabric labels"""

    # report_steps_dct contains current step desciption and force and export tags
    # report_headers_df contains column titles, 
    # report_columns_usage_dct show if fabric_name, chassis_name and group_name of device ports should be used
    report_constant_lst, report_steps_dct, report_headers_df = report_creation_info_lst
    # report_constant_lst contains information: customer_name, project directory, database directory, max_title
    customer_name, report_path, *_, max_title = report_constant_lst

    # names to save data obtained after current module execution
    data_names = ['fabric_labels', 'fabricshow_summary']
    # service step information
    print(f'\n\n{report_steps_dct[data_names[0]][3]}\n')

    # load data if they were saved on previos program execution iteration
    # data_lst = load_data(report_constant_lst, *data_names)
    # reade data from database if they were saved on previos program execution iteration
    data_lst = dbop.read_database(report_constant_lst, report_steps_dct, *data_names)

    # unpacking DataFrames from the loaded list with data
    # pylint: disable=unbalanced-tuple-unpacking
    fabricshow_ag_labels_df, fabricshow_summary_df = data_lst

    # list of data to analyze from report_info table
    analyzed_data_names = []
    # force run when any data from data_lst was not saved (file not found) or 
    # procedure execution explicitly requested for output data or data used during fn execution  
    force_run = meop.verify_force_run(data_names, data_lst, report_steps_dct, max_title, analyzed_data_names)

    if force_run:             
        print('\nSETTING UP FABRICS NAMES AND LABELS  ...\n')

        fabricshow_summary_automatic_df = auto_fabrics_labeling(switchshow_ports_df, switch_params_df, fabricshow_df, report_creation_info_lst)

        if not isinstance(fabricshow_summary_df, pd.DataFrame):
            fabricshow_summary_df = fabricshow_summary_automatic_df.copy()

        # display automatic fabric labeling
        info_labels = ['Fabric_name', 'Fabric_label', 'chassis_name', 'Principal_switch_name', 'Fabric_ID', 
                    'FC_Route', 'Total_switch', 'Domain_IDs', 'Switch_names', 'Device_ports', 'Online_ports', 'LS_type', 'Fabric_Name']
        # service file name for detailed information
        current_date = str(date.today())
        file_name = customer_name + '_' + report_steps_dct['fabricshow_summary'][2] + '_' + current_date + '.xlsx' 
        print('\nCurrent fabrics labeling\n')
        # set option to show all columns
        with pd.option_context('display.max_columns', None, 'display.expand_frame_repr', False):
            print(fabricshow_summary_df.loc[:, info_labels])
        print(f"\nFor detailed switch port types and numbers statistic in each fabric check '{file_name}' file 'fabricshow_statistics' sheet in")
        print(f'{report_path} directory')
        print('ATTN! CLOSE file after check\n')
        
        # ask user if Automatic Fabric labeling need to be corrected
        query = 'Do you want to change Fabrics Names or Labels? (y)es/(n)o: '
        reply = meop.reply_request(query)
        if reply == 'y':
            # saving DataFrame to Excel to check during manual labeling if required
            dfop.dataframe_to_excel(fabricshow_summary_df, 'fabricshow_summary', report_creation_info_lst, force_flag=True)
            fabricshow_summary_df = manual_fabrics_labeling(fabricshow_summary_df, fabricshow_summary_automatic_df, info_labels)
        
        # takes all switches working in Native and AG switches
        # merge the in one DataFrame and identify which Fabrics they belong too with fabricshow_summary DataFrame
        fabricshow_ag_labels_df = native_ag_labeling(fabricshow_df, ag_principal_df, fabricshow_summary_df)
        fabricshow_ag_labels_df['Fabric_name'].fillna('x', inplace=True)
        fabricshow_ag_labels_df['Fabric_label'].fillna('x', inplace=True)
        
        info = f'Fabric name and label setting'
        print(info, end =" ")
        meop.status_info('ok', max_title, len(info))
        
        # create list with partitioned DataFrames
        data_lst = [fabricshow_ag_labels_df, fabricshow_summary_df]
        # saving data to json or csv file
        # save_data(report_constant_lst, data_names, *data_lst)
        # writing data to sql
        dbop.write_database(report_constant_lst, report_steps_dct, data_names, *data_lst)  
    # verify if loaded data is empty and replace information string with empty DataFrame
    else:
    #    fabricshow_ag_labels_df, fabricshow_summary_df = dbop.verify_read_data(report_constant_lst, data_names, *data_lst)
    #    data_lst = [fabricshow_ag_labels_df, fabricshow_summary_df]

        data_lst = dbop.verify_read_data(report_constant_lst, data_names, *data_lst)
        fabricshow_ag_labels_df, *_ = data_lst
       
    # save data to excel file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        dfop.dataframe_to_excel(data_frame, data_name, report_creation_info_lst)

    return fabricshow_ag_labels_df 


def native_ag_labeling(fabricshow_df, ag_principal_df, fabricshow_summary_df):
    """
    Fabricshow doesn't include AG switches. To have comprehensive picture
    of switches in all fabrics need concatenate information from both DataFrames.
    Function to concatenate fabricshow and ag_principal DataFrames.
    After labels all switches in concatenated DataFrame.
    Returns DataFrame all switches in Fabric with labels
    """
    
    # removing duplicates switches(wwns) from 
    ag_concat_df = ag_principal_df.drop_duplicates(subset=['AG_Switch_WWN'])
    
    # align columns in both DataFrames to concatenate
    fabricshow_columns = ['configname','chassis_name', 'chassis_wwn', 'Principal_switch_index', 
                          'Principal_switch_name', 'Principal_switch_wwn', 'Fabric_ID', 
                          'FC_Route', 'Domain_ID', 'Worldwide_Name', 'Enet_IP_Addr', 'Name']
    
    ag_columns = ['configname','chassis_name', 'chassis_wwn', 'Principal_switch_index', 
                  'Principal_switch_name', 'Principal_switch_wwn', 'Fabric_ID', 
                  'AG_Switch_WWN', 'AG_Switch_IP_Address', 'AG_Switch_Name']
    
    # align DataFrame columns
    fabricshow_concat_df = fabricshow_df.loc[:, fabricshow_columns]
    ag_concat_df = ag_concat_df.loc[:, ag_columns]
    
    # Adding two columns to ag DataFrame
    # These parameters is not applicable to AG switches but required
    ag_concat_df['FC_Route'] = None
    ag_concat_df['Domain_ID'] = None
    
    # rename columns in ag DataFrame to make identical with fabricshow DataFrame 
    ag_concat_df.rename(columns={'AG_Switch_WWN': 'Worldwide_Name', 
                              'AG_Switch_IP_Address': 'Enet_IP_Addr', 
                              'AG_Switch_Name': 'Name'}, 
                     inplace=True)
    
    # set ag DataFrame columns order same as fabricshow columns order 
    ag_concat_df = ag_concat_df[fabricshow_columns]

    
    # concatenating ag and fabricshow DataFrames
    # set label Native and AG mode
    fabricshow_ag_df = pd.concat([fabricshow_concat_df, ag_concat_df], ignore_index=False, keys=['Native', 'Access Gateway Mode'])
    # reset index and set name for the new column
    fabricshow_ag_df = fabricshow_ag_df.reset_index(level = 0).rename(columns={'level_0': 'SwitchMode'})
    # relocating SwitchMode column to the end
    fabricshow_columns.append('SwitchMode')
    fabricshow_ag_df = fabricshow_ag_df[fabricshow_columns]
    

    # remove columns with multi values from fabricshow_summary DataFrame
    fabricshow_summary_columns = ['chassis_name', 'Principal_switch_name', 
                                  'Principal_switch_wwn', 'Fabric_ID', 
                                  'Fabric_name', 'Fabric_label']
    fabricshow_summary_join_df = fabricshow_summary_df[fabricshow_summary_columns].copy()
    
    # left join on fabricshow_ag and fabricshow_summary_join DataFrames
    fabricshow_summary_join_df['Fabric_ID'] = fabricshow_summary_join_df['Fabric_ID'].astype('str')

    fabricshow_ag_labels_df = fabricshow_ag_df.merge(fabricshow_summary_join_df, how='left', 
                                                     on = ['chassis_name', 'Principal_switch_name', 
                                                           'Principal_switch_wwn', 'Fabric_ID'])
    
    return fabricshow_ag_labels_df
