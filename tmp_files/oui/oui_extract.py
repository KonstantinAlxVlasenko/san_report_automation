# Filename: oui_extract.py

"""OUI data extract"""

import re
import pandas as pd

oui_file = 'oui.txt'
pattern = re.compile(
    r'^([A-F0-9]{2}-[A-F0-9]{2}-[A-F0-9]{2})\s+\(hex\)\s+([\w &.,()-]+)'
    )

oui_dct = {'oui': [], 'decription': []}

with open(oui_file, encoding='utf-8', errors='ignore') as file:
    while True:
        line = file.readline()
        match_pattern = re.search(pattern, line)
        if match_pattern:
            
            oui_dct['oui'].append(match_pattern.group(1).replace('-', ':'))
            oui_dct['decription'].append(match_pattern.group(2).rstrip())

        if not line:
            break


oui_df = pd.DataFrame(oui_dct)

with pd.ExcelWriter('oui.xlsx', engine='openpyxl', mode='w') as writer:
    oui_df.to_excel(writer, sheet_name='oui', index=False)
