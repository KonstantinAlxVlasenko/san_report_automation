
# software release number
RELEASE = '0.7'

# information string
LEFT_INDENT = 10
MIDDLE_SPACE = 80
RIGHT_INDENT = 10

# visio diagram steps descrition
SWITCH_DESC = 'Switch pairs'
ISL_DESC = 'ISLs, ICLs, IFLs'
NPIV_DESC = 'NPIV links'
STORAGE_DESC = 'Storages'
SERVER_DESC = 'Servers'
SWITCH_GROUPS_DESC = 'Switch groups'

# colours for Visiso scheme
HPE_PALETTE = {
    'green': 'RGB(0,169,103)', 
    'red': 'RGB(255,141,109)', 
    'blue': 'RGB(42,210,201)', 
    'purple': 'RGB(97,71,103)'
    }

RT_PALETTE = {
    'grey': 'RGB(123,147,155)',
    'orange': 'RGB(255,79,18)',
    'purple': 'RGB(119,0,255)',
    'black': 'RGB(16,24,40)' 
    }

COLOUR_PALETTE = HPE_PALETTE

# min connected device match ratio for the switch and the pair switch
MIN_DEVICE_NUMBER_MATCH_RATIO = 0.5
# min switch name match ratio for switch and the pair switch 
MIN_SW_NAME_MATCH_RATIO = 0.8

# Direcotors with 4 slots
DIR_4SLOTS_TYPE = [77, 121, 165, 179]
# All directors (8-slots, 4-slots)
DIRECTOR_TYPE = [42, 62, 77, 120, 121, 165, 166, 179, 180]