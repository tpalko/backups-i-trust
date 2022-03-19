import logging 
import subprocess

FOREGROUND_COLOR_PREFIX = '\033[38;2;'
FOREGROUND_COLOR_SUFFIX = 'm'
FOREGROUND_COLOR_RESET = '\033[0m'

COLOR_TABLE = {
    'white': '255;255;255',
    'red': '255;0;0',
    'green': '0;255;0',
    'orange': '255;165;0',
    'gray': '192;192;192',
    'darkgray': '128;128;128',
    'yellow': '165:165:0'
}

def colorwrapper(text, color):
    return f'{FOREGROUND_COLOR_PREFIX}{COLOR_TABLE[color]}{FOREGROUND_COLOR_SUFFIX}{text}{FOREGROUND_COLOR_RESET}'

class Columnizer(object):

    TAB_STD_INTERVAL = "tabs -8"

    logger = None 
    tabs = None 
    cell_padding = None 
    alignment = None 
    header_color = None 
    row_color = None 
    cell_padding_default = 5
    header_color_default = 'white'
    row_color_default = 'orange'
    quiet = False 
    headers = True 

    def __init__(self, *args, **kwargs):
        
        self.cell_padding = self.cell_padding_default
        self.header_color = self.header_color_default
        self.row_color = self.row_color_default

        if 'logger' in kwargs:
            self.logger = kwargs['logger']

        for k in kwargs:
            self.__setattr__(k, kwargs[k])
            
        if not self.logger:            
            self.logger = logging.getLogger(__name__)

    def _pad_tabs(self, data):

        # -- initialize tabs array 
        if not self.tabs and len(data) > 0:
            self.tabs = [ 1 for c in data[0] ]
            self.tabs.append(1)
        
        self.logger.debug(self.tabs)

        for rix, row in enumerate(data):
            # -- we're calculating the space from the start of each cell to the start of the next
            # -- don't bother looking at the last element 
            for cix, col in enumerate(data[rix]):
                curr_tab = self.tabs[cix+1] - self.tabs[cix]
                cell_value = str(data[rix][cix])
                cell_width = len(cell_value) + self.cell_padding
                extra = cell_width - curr_tab
                
                # if cix < len(self.tabs):
                #     self.tabs[cix] = self.tabs[cix] + extra 

                self.tabs = [ m + extra if (i >= cix+1 and extra > 0) else m for i,m in enumerate(self.tabs) ]

                self.logger.debug(f'{cell_value} (p {self.cell_padding}) -> {",".join([ str(t) for t in self.tabs ])}')
                # self.logger.debug(f'curr_tab: {curr_tab}, cell_value: {cell_value}, cell_width: {cell_width}, extra: {extra}, cix_tabs: {self.tabs[cix]}')

    def _align_spaces(self, value, cell_width, alignment):
        '''Calculate cell whitespace and place it to the left or right of cell value to align it to the right or left of the cell'''
        # self.logger.debug(f'{value} {cell_width} {alignment}')
        if alignment == 'r':
            return f'{"".join([ " " for i in range(cell_width - self.cell_padding - len(value)) ])}{value}'
        return value

    def _align_on_type(self, value):
        try:
            tried = float(value)
            return 'r'
        except:
            return 'l'

    def _align_table(self, data):
        return [ [ self._align_spaces(str(r), cell_width=self.tabs[i+1] - self.tabs[i], alignment=self.alignment[i] if self.alignment else self._align_on_type(r)) if i < len(row) else str(r) for i,r in enumerate(row) ] for row in data ]

    # def _table_data(self, table):
    #     return "\n".join([ "\t".join([ str(v) for v in v in row ]) for row in table ])

    def _printf_command(self, table, color):
        tabs_cmd = f'tabs {",".join([ str(c) for c in self.tabs ])}'
        print_data = "\n\"; printf \"".join([ colorwrapper("\t".join([ str(v) for v in row ]), color) for row in table ])
        return "%s; printf \"%s\n\"; %s;" % (tabs_cmd, print_data, self.TAB_STD_INTERVAL)
        
    def print(self, table, header, data=False):

        if not data and self.quiet:
            return 
            
        if header and self.headers:
            self._pad_tabs([header])
        self._pad_tabs(table)
        
        # self.logger.info(self._table_data(header), tabs=self.tabs, color=self.header_color)
        # self.logger.info(self._table_data(table), tabs=self.tabs, color=self.row_color)
        
        self.logger.debug(self.tabs)
        printout = ""
        if header and self.headers:
            header = [header]
            header = self._align_table(header)
            printout += self._printf_command(header, self.header_color)
        
        table = self._align_table(table)
        printout += self._printf_command(table, self.row_color)

        subprocess.run(printout, shell=True)
