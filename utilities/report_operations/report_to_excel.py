"""Module to export dataframes and table of contents to report excel file"""

import os
import sys
from datetime import date

import openpyxl
import pandas as pd

import utilities.filesystem_operations as fsop
from utilities.module_execution import status_info

from .worksheet_operations import format_workbook, hyperlink_content


def dataframe_to_excel(df, sheet_title, project_constants_lst, 
                        current_date=str(date.today()), force_flag = False, freeze_column='A'):
    """Check if excel file exists, write DataFrame, create or update table of contents,
    change DataFrame text and presentation format."""
    
    project_steps_df, max_title, _, report_requisites_sr, *_ = project_constants_lst
    report_type, export_flag, df_decription = project_steps_df.loc[sheet_title, ['report_type', 'export_to_excel', 'description']].values
    
    # check DataFrame report type to save
    if report_type == 'report':
        report_mark = report_requisites_sr['project_title'] + '_tables'
    else:
        report_mark = report_type
          
    # construct excel filename
    file_name = report_requisites_sr['customer_name'] + '_' + report_mark + '_' + current_date + '.xlsx'

    # information string
    info = f'Exporting {sheet_title} table to {report_mark} file'
    print(info, end =" ")
    file_path = os.path.join(report_requisites_sr['today_report_folder'], file_name)
    
    # save DataFrame to excel file if export_to_excel trigger is ON
    # and DataFrame is not empty
    if (force_flag or export_flag) and not df.empty:
        fsop.create_folder(report_requisites_sr['today_report_folder'], max_title, display_status=False)
        file_mode = 'a' if os.path.isfile(file_path) else 'w'
        df = df.apply(pd.to_numeric, errors='ignore')
        try:
            if_sheet_exists_param = 'replace' if file_mode == 'a' else None
            content_df, item_exist = generate_table_of_contents(file_path, file_mode, sheet_title, df_decription)
            df_flat = drop_multindex(df)
            # write table of contents and data dataframe to the excel file
            with pd.ExcelWriter(file_path, mode=file_mode, if_sheet_exists=if_sheet_exists_param, engine='openpyxl') as writer:
                if file_mode == 'w' or not item_exist:
                    content_df.to_excel(writer, sheet_name='Содержание', index=False)
                df_flat.to_excel(writer, sheet_name=sheet_title,  startrow=2, index=False)
            # format table of contents and data worksheets
            workbook = openpyxl.load_workbook(file_path)
            format_workbook(workbook, sheet_title, df_decription, freeze_column)
            workbook.save(file_path)
        except PermissionError:
            status_info('fail', max_title, len(info))
            print('\nPermission denied. Close the file.\n')
            sys.exit()
        else:
            status_info('ok', max_title, len(info))
        return file_path        
    else:
        # if save key is on but DataFrame empty
        if project_steps_df.loc[sheet_title, 'export_to_excel'] and df.empty:
            status_info('no data', max_title, len(info))
        else:            
            status_info('skip', max_title, len(info))
        return None
    

def generate_table_of_contents(file_path, file_mode, sheet_title, df_decription):
    """"Function to create or recreate table of contents if file is newly created or new sheet added"""

    item_exist = None 
    # if file exists and there is no sheet_title in contents add new item to the existing content
    if  file_mode == 'a':
        existing_content_df = pd.read_excel(file_path, sheet_name='Содержание')
        item_exist = sheet_title in existing_content_df['Закладка'].values
        if not item_exist:
            # content item to add
            content_df = pd.DataFrame([[sheet_title, df_decription]], columns=['Закладка', 'Название таблицы'])
            # concatenate existing items to the current
            content_df = pd.concat([existing_content_df, content_df])
        else:
            content_df = existing_content_df
    # if file is newly created then create contents with single sheet_title
    elif file_mode == 'w':
        content_df = pd.DataFrame([[sheet_title, df_decription]], columns=['Закладка', 'Название таблицы'])
    return content_df, item_exist


def drop_multindex(df):
    """Function to drop index If DataFrame index is multiIndex."""

    if isinstance(df.index, pd.MultiIndex):
        df_flat = df.reset_index()
    # keep index if False
    else:
        df_flat = df.copy()
    return df_flat


def report_format_completion(project_constants_lst, current_date=str(date.today())):
    """Function to reorder sheets and items in table of contents"""

    project_steps_df, max_title, _, report_requisites_sr, *_ = project_constants_lst
    
    # verify if any report DataFrame need to be saved
    mask_report = project_steps_df['report_type'] == 'report'
    mask_save = project_steps_df['export_to_excel'] == 1

    if not project_steps_df.loc[mask_report & mask_save].empty:

        print('\n')
        info = f'Completing the report'.upper()
        print(info, end =" ")

        file_name = report_requisites_sr['customer_name'] + '_' + report_requisites_sr['project_title'] + '_tables_' + current_date + '.xlsx'
        file_path = os.path.join(report_requisites_sr['today_report_folder'], file_name)
        try:
            with pd.ExcelWriter(file_path, mode='a', if_sheet_exists= 'replace', engine='openpyxl') as writer: 
                # import table of contents
                content_df = pd.read_excel(writer, sheet_name='Содержание')
                # sort table of contents items
                content_df.sort_values(by=['Закладка'], key=lambda menu_sr: project_steps_df.loc[menu_sr, 'sort_weight'], inplace=True)
                # write content to report file
                content_df.to_excel(writer, sheet_name='Содержание', index = False)
            workbook = openpyxl.load_workbook(file_path)
            # create hyperlinks for all items of table of contents
            hyperlink_content(workbook)
            # sort worksheets
            workbook._sheets.sort(key=lambda ws: project_steps_df.loc[ws.title, 'sort_weight'])
            workbook.save(file_path)
        except PermissionError:
            status_info('fail', max_title, len(info))
            print('\nPermission denied. Close the file.\n')
            exit()
        else:
            status_info('ok', max_title, len(info)) 