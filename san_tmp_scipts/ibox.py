# -*- coding: utf-8 -*-
"""
Created on Wed Dec 27 16:21:51 2023

@author: kavlasenko
"""

import requests
import urllib3
import base64
import json

urllib3.disable_warnings()

fqdn = 'ibox3156-nord.dtln.ru'
username = 'test_api'
password = 'Qwerty123'
credentials = base64.b64encode(f'{username}:{password}'.encode()).decode()

def rack_id(fqdn, credentials):
    url = f"https://{fqdn}/api/rest/components/"
    headers = {'Authorization': f'Basic {credentials}', 'Content-Type': 'application/json'}
    response = requests.get(url, headers=headers, verify=False)
    
    return response.json()
    

    # records = json.loads(response.text)
    # print(json.dumps(records, indent=4))
    # return json.dumps(records, indent=4)
records = rack_id(fqdn, credentials)

wwpn = []
for elem in records['result']['nodes']:
    for port in elem['fc_ports']:
        if port['enabled']:
            wwpn.append(port['wwpn'])
            print(port['wwpn'])