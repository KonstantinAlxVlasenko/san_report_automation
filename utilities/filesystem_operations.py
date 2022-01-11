"""Module to perform operations with files (create folder, create files list in folder etc)"""


import os
import re
import sys

from utilities.module_execution import status_info


def create_folder(path, max_title):
    """Function to create any folder with path"""

    info = f'Making directory {os.path.basename(path)}'
    print(info, end = ' ')
    # if folder not exist create 
    if not os.path.exists(path):        
        try:
            os.makedirs(path)
        except OSError:
            status_info('fail', max_title, len(info))
            print(f'Not possible create directory {path}.')
            print('Code execution finished')
            sys.exit()
        else:
            status_info('ok', max_title, len(info))
    # otherwise print 'SKIP' status
    else:
        status_info('skip', max_title, len(info))
        
     
def check_valid_path(path):
    """Function to check if folder exist"""

    path = os.path.normpath(path)
    if not os.path.isdir(path):
        print(f"{path} folder doesn't exist")
        print('Code execution exit')
        sys.exit()
        

def find_files(folder, max_title, filename_contains='', filename_extension=''):
    """
    Function to create list with files. Takes directory, regex_pattern to verify if filename
    contains that pattern (default empty string) and filename extension (default is empty string)
    as parameters. If filename extension is None then filename shouldn't contain any extension. 
    Returns list of files with the extension deteceted in root folder defined as
    folder parameter and it's nested folders. If both parameters are default functions returns
    list of all files in directory
    """

    info = f'Checking {os.path.basename(folder)} folder for configuration files'
    print(info, end =" ") 

    # check if ssave_path folder exist
    check_valid_path(folder)
   
    # list to save configuration data files
    files_lst = []

    # going through all directories inside ssave folder to find configuration data
    
    # for root, _, files in os.walk(folder):
    #     for file in files:
    #         if re.search(filename_contains, file):
    #             file_path = os.path.normpath(os.path.join(root, file))
    #             if filename_extension and file.endswith(filename_extension):
    #                 files_lst.append(file_path)
    #             elif not filename_extension and not re.search('\.', file):
    #                 files_lst.append(file_path)

    for root, _, files in os.walk(folder):
        for file in files:
            if re.search(filename_contains, file):
                file_path = os.path.normpath(os.path.join(root, file))
                if filename_extension and file.endswith(filename_extension):
                    files_lst.append(file_path)
                # when file extension flag is None and file name doesn't contain extension (.7zip, .exe, .log, .3g2)
                elif filename_extension is None and not re.search('.+\.(\d)?[A-Za-z]+(\d)?$', file):
                    files_lst.append(file_path)
                elif filename_extension=='':
                    files_lst.append(file_path)


    if len(files_lst) == 0:
        status_info('no data', max_title, len(info))
    else:
        status_info('ok', max_title, len(info))        
    return files_lst


         
