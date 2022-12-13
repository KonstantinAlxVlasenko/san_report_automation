


import pandas as pd

LINE_PATTERN = {'2': 'USE("Parallel Lines x2")', 
                '3': 'USE("Parallel Lines x3")', 
                '4': 'USE("Parallel Lines x4")', 
                '4+': 'USE("Parallel Lines x4+")'}



def drop_connector_shape(link_sr, page, stn, fabric_label_colours_dct, link_font_size, source, destination):
    """Function to connect source and destination shapes from link_sr with connector_master shape.
    source and destination parameters are links_sr Series row titles with source and destination shapeNames accordingly"""

    source_shape_name = link_sr[source]
    destination_shape_name =  link_sr[destination]
    connector_master = stn.Masters('Link - HPE')
    
    # verify if source and desrinataion exist
    page_shape_name_lst = [page.Shapes.ItemU(i).Name  for i in range(1, page.Shapes.count + 1)]
    absent_shape_name_lst = [shape_name for shape_name in [source_shape_name, destination_shape_name] if shape_name not in page_shape_name_lst]
    
    if not absent_shape_name_lst:
        # sorce and destination shape objects
        source_shape = page.Shapes.ItemU(link_sr[source])
        destination_shape = page.Shapes.ItemU(link_sr[destination])
        # connect source and destionation shape objects (0 - connect without relocating the shapes)
        source_shape.AutoConnect(destination_shape, 0, connector_master)
        # last shape in the page shapes list is connector added above
        connector = page.Shapes.ItemU(page.Shapes.count)
        set_connector_attributes(connector, link_sr, fabric_label_colours_dct, link_font_size)
    else:
        print(f"Can't create link between {source_shape_name} and {destination_shape_name}. "
              f"{', '.join(absent_shape_name_lst)} missing on page {page.Name}")


def set_connector_attributes(connector, link_sr, fabric_label_colours_dct, link_font_size):
    """Function to set connector shape name, text, colour and lines number"""
    
    # connection shape colour
    fabric_label_colour = fabric_label_colours_dct[link_sr['Fabric_label']]
    link_quantity = link_sr['Physical_link_quantity']


    # set shape connector name
    connector.Name = link_sr['Link_shapeName']
    # set shape connector colour
    connector.Cells('LineColor' ).FormulaU = fabric_label_colour
    # set shape connector text (links number, trunk, npiv etc) if exist
    if pd.notna(link_sr['Link_description']):
        connector.Text = link_sr['Link_description']
    else:
        connector.Text = ""
    
    shape_font_change(connector, link_font_size)
    
    # if link_font_size != default_font_size:
    #     connector.Cells("Char.Size").FormulaU = link_font_size
    
    # set shape connector text colour
    connector.Cells('Char.Color' ).FormulaU = fabric_label_colour
    
    # show lines number for connector with links quantity up to four
    if  1 < link_quantity <= 4:
        connector.Cells("LinePattern").FormulaU = LINE_PATTERN[str(link_quantity)]
    # for connector with links quantity greater than 4 show multiline connector
    elif link_quantity > 4:
        connector.Cells("LinePattern").FormulaU = LINE_PATTERN['4+']


def shape_font_change(shape, font_size):

    default_font_size = '12 pt'
    font_size = str(font_size) + ' pt'

    if font_size != default_font_size:
        shape.Cells("Char.Size").FormulaU = font_size