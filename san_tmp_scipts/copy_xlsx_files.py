# -*- coding: utf-8 -*-
"""
Created on Fri Feb  4 12:43:23 2022

@author: vlasenko
"""

import os
import shutil

source_dir = r"C:\ProgramData\HP StorageTools\SANXpert\Customers\Megafon\SanProjects\3PAR Perf\FEB2022\Tools\3PARDT"
dest_dir = r"C:\Users\vlasenko\OneDrive - Hewlett Packard Enterprise\Documents\02.DOCUMENTATION\Procedures\3Par Perf\Megafon\feb22\reports"

def copy_xlsx_files(source_dir, dest_dir):

    for root, _, files in os.walk(source_dir):
        for file in files:
            if file.endswith(".xlsx"):
                file_path_source = os.path.normpath(os.path.join(root, file))
                file_path_dest = os.path.join(dest_dir, file)
                print(file)
                shutil.copyfile(file_path_source, file_path_dest)