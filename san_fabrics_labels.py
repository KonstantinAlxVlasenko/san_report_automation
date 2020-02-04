import pandas as pd
from datetime import date
# from files_operations import columns_import, status_info, data_extract_objects, load_data, save_data 
# from files_operations import line_to_list, force_extract_check, update_dct

from files_operations import load_data, save_data, force_extract_check
from files_operations import save_xlsx_file

"""Module to set Fabric names and labels"""

# auxiliary global variables for auto_fabrics_labeling function
# variables changed globally each time function called
fabric_bb = False
fabric_num = 0
fabric_label = False
called = False


def fabriclabels_main(switchshow_ports_df, fabricshow_df, ag_principal_df, report_data_lst):
    """Function to set Fabric labels
    """
    # report_data_lst contains [customer_name, dir_report, dir_data_objects, max_title]
    
    print('\n\nSTEP 15. FABRIC NAMES AND LABELS IDENTIFICATION...\n')
    
    customer_name, report_path, _, max_title, report_steps_dct = report_data_lst
    # check if data already have been extracted
    data_names = ['fabric_labels']
    data_lst = load_data(report_data_lst, *data_names)  
    fabricshow_ag_labels_df, = data_lst

    # data force extract check. 
    # if data have been extracted already but extract key is ON then data re-extracted
    force_extract_keys_lst = [report_steps_dct[data_name][1] for data_name in data_names]
    data_check = force_extract_check(data_names, data_lst, force_extract_keys_lst, max_title)
    
    # when no data saved or force extract flag is on than extract data from configurtion files  
    if not all(data_check) or any(force_extract_keys_lst):             
        print('\nSETTING UP FABRICS NAMES AND LABELS  ...\n')
    
        fabricshow_porttype_state_df = fabricshow_porttype_state(switchshow_ports_df, fabricshow_df)

        # saving DataFrame to Excel if manual labeling required
        save_xlsx_file(fabricshow_porttype_state_df, 'fabricshow_statistics', report_data_lst, report_type = 'analysis')
        # removing front domain and translate domain switches from DataFrame
        fabricshow_porttype_state_df = fabricshow_porttype_state_df.loc[fabricshow_porttype_state_df.Enet_IP_Addr != '0.0.0.0']
        
        # dividing fabricshow_porttype_state_df into groups. One group for each fabric
        fabricshow_grp = fabricshow_porttype_state_df.groupby(
            by=['chassis_name', 'Principal_switch_name', 'Principal_switch_wwn', 'Fabric_ID', 'FC_Route'])
        # applying faricshow_summary for each fabric to summarize fabricshow_porttype_state_df DataFrame
        fabricshow_summary_df = fabricshow_grp.apply(faricshow_summary)
        
        # sorting data in such way that two rows (odd and even) are pair fabrics
        fabricshow_summary_df = fabricshow_summary_df.reset_index().sort_values(
            by=['FC_Route', 'Total_switch', 'Domain_IDs', 'Switch_names'], 
            ascending=[False, False, True, True]).reset_index(drop=True)
        
        # labeling fabrics with auto_fabrics_labeling fanction
        fabricshow_summary_df[['Fabric_name', 'Fabric_label']] = \
            fabricshow_summary_df.apply(lambda row: pd.Series(auto_fabrics_labeling(row)), axis=1)
        
        # fabricshow_summary_df columns to present current fabric labeling
        info_labels = ['Fabric_name', 'Fabric_label', 'chassis_name', 'Principal_switch_name', 'Fabric_ID', 
                    'FC_Route', 'Total_switch', 'Domain_IDs', 'Switch_names', 'Total_Online_ports']

        # service file name for detailed information
        current_date = str(date.today())
        file_name = customer_name + '_service_report_' + current_date + '.xlsx'
        print('\nAutomatic fabrics labeling\n')
        print(fabricshow_summary_df.loc[:, info_labels])
        print(f"\nFor detailed switch port types and numbers statistic in each fabric check '{file_name}' service file 'fabricshow_statistics' sheet in\n{report_path} directory")
        print('WARNING! CLOSE file after check\n')
        
        # ask user if Automatic Fabric labeling need to be corrected 
        input_yn = None                
        while not input_yn in ['y', 'yes', 'n', 'no']:
            input_yn = input(f'Do you want to change Fabrics Names or Labels? (y)es/(n)o: ').lower()
        # when manual correction required call function    
        if input_yn[0] == 'y':
            fabricshow_summary_df = manual_fabrics_labeling(fabricshow_summary_df, info_labels)
        # saving DataFrame to Excel to check during manual labeling if required
        save_xlsx_file(fabricshow_summary_df, 'fabricshow_summary', report_data_lst, report_type = 'analysis')
        
        # takes all switches working in Native and AG switches
        # merge the in one DataFrame and identify which Fabrics they belong too with fabricshow_summary DataFrame
        fabricshow_ag_labels_df = native_ag_labeling(fabricshow_df, ag_principal_df, fabricshow_summary_df)

        
        # saving fabricshow_ag_labels DataFrame to csv file
        save_data(report_data_lst, data_names, fabricshow_ag_labels_df)

    # saving fabricshow_ag_labels DataFrame to Excel
    save_xlsx_file(fabricshow_ag_labels_df, 'fabric_labels', report_data_lst, report_type = 'analysis')

    return fabricshow_ag_labels_df
    
    
def fabricshow_porttype_state(switchshow_ports_df, fabricshow_df):
    """Function adding to fabricshow DataFrame summary
    about port type for each switch and total number of ports
    and online ports by crosstab switchshow DataFrame and
    joining with fabricshow DataFrame
    """
    # switchshow DataFrame contains Online switches operating in Native mode
    switchshow_df = switchshow_ports_df.loc[(switchshow_ports_df.switchState == 'Online') & 
                                         (switchshow_ports_df.switchMode == 'Native')
                                         ]
    
    # crosstab DataFrame contains summary for port states for switches from switchshow
    port_state_df = pd.crosstab(index = [switchshow_df.chassis_name, switchshow_df.switchName, 
                                         switchshow_df.switchWwn], columns = switchshow_df.state, margins = True)
    # DataFrame index need to be sorted before loc operation
    port_state_df.sort_index(inplace=True)
    # only total and online ports number are required
    port_state_df = port_state_df.loc[:, ['Online', 'All']]
    # drop row with summary for all ports in fabrics
    port_state_df.drop(index=('All'), inplace=True)
    
    # crosstab DataFrame contains summary for port types for switches from switchshow
    port_type_df = pd.crosstab(index = [switchshow_df.chassis_name, switchshow_df.switchName, 
                                        switchshow_df.switchWwn], columns = switchshow_df.portType)
    
    # concatenating port_type port_state DataFrames by rightjoin
    porttype_state_df = port_type_df.merge(port_state_df, how='right', on = ['chassis_name', 'switchName', 'switchWwn'])
    # fill None values with 0
    porttype_state_df.fillna(0, inplace=True)
    # converting all values to integer
    porttype_state_df = porttype_state_df.astype('int64', errors = 'ignore')
    # sorting index
    porttype_state_df.sort_index(inplace=True)
    
    # concatenating fabricshow and porttype_state DataFrames by leftjoin
    fabricshow_porttype_state_df = fabricshow_df.merge(porttype_state_df, how='left', 
                                                       left_on = ['Worldwide_Name'], right_on=['switchWwn'])
    # fill None values with 0
    fabricshow_porttype_state_df.fillna(0, inplace=True)
    # converting all values to integer
    fabricshow_porttype_state_df = fabricshow_porttype_state_df.astype('int64', errors='ignore')
    
    return fabricshow_porttype_state_df    

def native_ag_labeling(fabricshow_df, ag_principal_df, fabricshow_summary_df):
    """Fabricshow doesn't include AG switches. To have comprehensive picture
    of switches in all fabrics need concatenate information from both DataFrames.
    Function to concatenate fabricshow and ag_principal DataFrames.
    After labels all switches in concatenated DataFrame.
    Returns DataFrame all switches in Fabric with labels
    """
    
    # removing duplicates switches(wwns) from 
    ag_concat_df = ag_principal_df.drop_duplicates(subset=['AG_Switch_WWN'])
    
    # align columns in both DataFrames to concatenate
    fabricshow_columns = ['configname','chassis_name', 'Principal_switch_index', 
                          'Principal_switch_name', 'Principal_switch_wwn', 'Fabric_ID', 
                          'FC_Route', 'Domain_ID', 'Worldwide_Name', 'Enet_IP_Addr', 'Name']
    
    ag_columns = ['configname','chassis_name', 'Principal_switch_index', 
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
    fabricshow_ag_df = pd.concat([fabricshow_concat_df, ag_concat_df], ignore_index=False, keys=['Native', 'AG'])
    # reset index and set name for the new column
    fabricshow_ag_df = fabricshow_ag_df.reset_index(level = 0).rename(columns={'level_0': 'SwitchMode'})
    # relocating SwitchMode column to the end
    fabricshow_columns.append('SwitchMode')
    fabricshow_ag_df = fabricshow_ag_df[fabricshow_columns]
    

    # remove columns with multi values from fabricshow_summary DataFrame
    fabricshow_summary_columns = ['chassis_name', 'Principal_switch_name', 
                                  'Principal_switch_wwn', 'Fabric_ID', 
                                  'Fabric_name', 'Fabric_label']
    fabricshow_summary_join_df = fabricshow_summary_df[fabricshow_summary_columns]
    
    # left join on fabricshow_ag and fabricshow_summary_join DataFrames
    fabricshow_ag_labels_df = fabricshow_ag_df.merge(fabricshow_summary_join_df, how='left', 
                                                     on = ['chassis_name', 'Principal_switch_name', 
                                                           'Principal_switch_wwn', 'Fabric_ID'])
    
    return fabricshow_ag_labels_df
        
        
def manual_fabrics_labeling(fabricshow_summary_df, info_labels):
    """Function to manual change Fabric Name and Fabric Label.
    """
    # copy of initial fabricshow_summary DataFrame
    # to be able to reset all changes 
    fabricshow_summary_default_df = fabricshow_summary_df.copy()
    # convert DataFrame indexes to string versions 
    fabric_indexes_str_lst = [str(i) for i in fabricshow_summary_df.index]
    # DataFrame operation options (save, reset, exit)
    opeartion_options_lst = ['s', 'r', 'x']
    # aggregated list of available operations
    full_options_lst = fabric_indexes_str_lst + opeartion_options_lst
    # parameters to change 
    rename_options_dct = {'Name': 'Fabric_name', 'Label': 'Fabric_label'}
    
    # user input from full_options_lst
    # initial value is None to enter while
    input_option = None
    # work with DataFrame until it is saved or exit without saving
    while not input_option in ['s', 'x']:
        # printing actual fabricshow_summary DataFrame
        print('\nCurrent fabric labeling\n')
        print(fabricshow_summary_df.loc[:, info_labels])
        # printing menu options to choose to work with DataFrame
        # save, reset, exit or fabric index number to change
        print('\nS/s - Save changes in labeling\nR/r - Reset to default labeling\nX/x - Exit without saving')
        print(f"{', '.join(fabric_indexes_str_lst)} - Choose fabric index to change labeling\n")

        # reset input_option value after each iteration to enter while loop
        input_option = None
        # ask user until user input is in allowed list
        while not input_option in full_options_lst:
            input_option = input("Choose option: ").lower()
        else:
            print(f'Your choice: {input_option}')
        
        # user input is fabric index to change labeling 
        if input_option in fabric_indexes_str_lst:
            print('\n')
            # make copy of actual fabricshow_summary DataFrame
            # to be able to reset current iteration changes
            fabricshow_summary_before_df = fabricshow_summary_df.copy()
            
            # convert user input fabric number to integer
            fubric_num = int(input_option)
            # printing information for current fabric
            print(fabricshow_summary_df.iloc[fubric_num])
            print('\n')
            
            # list to save user input
            input_yn_lst = []
            # ask user to change labels and names by looping over rename_options_dct 
            for option_name, option_column in rename_options_dct.items():
                # initial user input to enter while loop
                input_yn = None                
                while not input_yn in ['y', 'yes', 'n', 'no']:
                    input_yn = input(f'Do you want to change Fabric {option_name}? (y)es/(n)o: ').lower()
                # save user input to the list
                input_yn_lst.append(input_option[0])
                # if user want to change name or label ask to enter new value
                if input_yn in ['y', 'yes']:
                    value = input(f'Enter new Fabric {option_name}: ')
                    # 0 or None means no labeling
                    if value in ['0', 'None']:
                        value = None
                    # change values in fabricshow_summary DataFrame 
                    fabricshow_summary_df.loc[fubric_num, option_column] = value
            # if user didn't reply "no" two times
            if input_yn_lst != ['n']*2:
                print('\n')
                # print current fabric information after change
                print(fabricshow_summary_df.iloc[fubric_num])
                print('\n')
                # initial user input to enter while loop
                input_yn = None
                # ask if user wants to keep values changed
                while not input_yn in ['y', 'yes', 'n', 'no']:
                    input_yn = input('Do you want to keep changes? (y)es/(n)o: ').lower()
                # when user don't want to keep data fabricshow_summary DataFrame
                # returns to the state saved bedore current iteration
                if input_yn in ['n', 'no']:
                    fabricshow_summary_df = fabricshow_summary_before_df.copy()        
        
        # user input is save current labeling configuration and exit   
        elif input_option == 's':
            # initial user input to enter while loop
            input_yn = None
            while not input_yn in ['y', 'yes', 'n', 'no']:
                input_yn = input('Do you really want to save changes and exit? (y)es/(n)o: ').lower()
            # for save option do nothing and while loop stops on next condition check
            if input_yn in ['y', 'yes']:
                print('\nSaved fabric labeling\n')
                # print(fabricshow_summary_df.loc[:, info_labels])
        # user input is reset current labeling configuration and start labeling from scratch
        elif input_option == 'r':
            # initial user input to enter while loop
            input_yn = None
            while not input_yn in ['y', 'yes', 'n', 'no']:
                input_yn = input('Do you really want to reset fabric labeling to default values? (y)es/(n)o: ').lower()
            # for reset option actual fabricshow_summary DataFrame returns back
            # to initial DataFrame version and while loop don't stop
            if input_yn in ['y', 'yes']:
                fabricshow_summary_df = fabricshow_summary_default_df.copy()
                print('\nFabric labeling has been reset to initial version\n')
        # user input is exit without saving
        elif input_option == 'x':
            # initial user input to enter while loop
            input_yn = None
            while not input_yn in ['y', 'yes', 'n', 'no']:
                input_yn = input('Do you really want to leave without saving? (y)es/(n)o: ').lower()
            # for exit option DataFrame returns back to initial version
            # and while loop stops
            if input_yn in ['y', 'yes']:
                fabricshow_summary_df = fabricshow_summary_default_df.copy()
                print('\nKeeping original labeling version\n')
                # print(fabricshow_summary_df.loc[:, info_labels])
    else:
        # show actual version of DataFrame after user inputs save or exit
        print(fabricshow_summary_df.loc[:, info_labels])
        print('\n')
        
    return fabricshow_summary_df


def faricshow_summary(group):
    """Function takes group of switches in fabric.
    Summarizes DomainIDs, Switchnames for each fabric.
    Calculates total switch and online port numbers for each fabric
    Return Series 
    """
    # calculates total switch number in fabric
    switch_nums = group.Name.count()
    # calculates total port number in fabric
    online_sum = group.Online.sum()
    # collects domain ids
    domain_ids = ', '.join(str(i) for i in group.Domain_ID.tolist())
    # collects switch names
    names = ', '.join(group.Name.tolist())
    # column names for Series
    columns_names = ['Total_switch', 'Domain_IDs', 'Switch_names', 'Total_Online_ports']    
    
    return pd.Series([switch_nums, domain_ids, names, online_sum], index= columns_names)


def auto_fabrics_labeling(row):
    """Function labeling fabric in sorted DataFrame
    Each row is one fabric. 
    Returns fabric_name (BB or number) and fabric_label (A or B)
    """
    # global variables changing its values each time function has been called called  
    global fabric_bb # Flag if fabric is Backbone fabric 
    global fabric_num # Number of current Edge Fabric 
    global fabric_label # if True then label A has been already assigned and current fabric should get B label
    # apply method calls function on first row two times
    # flag to check if function has been already called 
    global called
    
    # called function flag is on
    if called:
        # Online ports present in the fabric
        if row.loc['Total_Online_ports'] != 0:
            # Backbone fabric
            if row.loc['FC_Route'] == 'ON':
                fabric_num_current = 'Fabric_BB'
                # first BB fabric row
                if not fabric_bb:
                    # label A assigned
                    fabric_label_current = 'A'
                    fabric_bb = True
                # second BB fabric row
                else:
                    # label B assigned
                    fabric_label_current = 'B'
            # Edge fabrics
            else:
                # second and more Edge fabric row
                if fabric_num:
                    fabric_num_current = 'Fabric_' + str(fabric_num)
                    # if before 'A' label has been assigned
                    if fabric_label:
                        fabric_label = False
                        fabric_label_current = 'B'
                        fabric_num += 1
                    # if before 'B' label has been assigned
                    else:
                        fabric_label_current = 'A'
                        fabric_label = True
                # first Edge fabric row
                else:
                    fabric_num += 1
                    fabric_label_current = 'A'
                    fabric_label = True
                    fabric_num_current = 'Fabric_' + str(fabric_num)
        # if there are no Online ports in Fabric labels are not assigned
        else:
            fabric_num_current = None
            fabric_label_current = None

        return [fabric_num_current, fabric_label_current]
    # called function flag is off
    # first time function is called do nothing
    else:
        called = True