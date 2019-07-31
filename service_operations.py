import os.path
import sys
# from os import mkdir
import os
import pandas as pd


# function to create any folder with 'path' 
def create_folder(path):    
    if not os.path.exists(path):        
        try:
            os.mkdir(path)
        except OSError:
            print(f'Not possible create directory {path}.')
            print('Code execution finished')
            sys.exit()
        else:
            print(f'Successfully created directory {path}.')
    else:
        print(f'Directory {path} already exists.')
        
        
# function to check if folder exist
def check_valid_path(path):
    path = os.path.normpath(path)
    if not os.path.exists(path):
        print(f"{path} folder doesn't exist")
        print('Code execution finished')
        sys.exit()
        

# saving data to excel file
def save_xlsx_file(data_frame, sheet_title, file_name, file_path):
    print(f'Exporting data to {file_name}')
    with pd.ExcelWriter(file_path, engine='openpyxl',  mode='a') as writer:
        data_frame.to_excel(writer, sheet_name=sheet_title, index=False)
