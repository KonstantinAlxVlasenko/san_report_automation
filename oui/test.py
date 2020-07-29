import re

regex_exp = r'[\da-f]{16}|[\da-f]{8}-([\da-f]{4}-){3}[\da-f]{8}|AG-FDMI|ESXHost'
regex_storeonce = r'StoreOnce +S/N-([\w]+)'

line1 = '55363e27-58d7-d7ac-4bb8-f0921c00'
line2 = 'AG-FDMI'
line3 = 'HP StoreOnce S/N-CZ3636M301 HP StoreOnce Catalyst Over Fibre Channel 0 Port 1'

# print(re.match(regex_exp, line2).group())
# print(re.compile(regex_storeonce).search(line3).group(1))


# a = "        ".strip()
# print(len(a))

regex_vc = r'^ *\w+:(\d+):(\d+) +[\w]+ +([\w]+) +((?:[0-9a-f]{2}:){7}[0-9a-f]{2}) +((?:[0-9a-f]{2}:){7}[0-9a-f]{2}) *$'
line_vc = 'enc0:3:1  OK      4Gb    20:00:F4:CE:46:AE:38:EB  10:00:00:05:33:21:21:00  '

print(re.compile(regex_vc).search(line_vc)