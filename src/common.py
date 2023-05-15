import subprocess 
import math
from enum import Enum 

FOREGROUND_COLOR_PREFIX = '\033[38;2;'
FOREGROUND_COLOR_SUFFIX = 'm'
FOREGROUND_COLOR_RESET = '\033[0m'

class Color(Enum):
    WHITE = 'white' # '255;255;255'
    RED = 'red' # '255;0;0'
    GREEN = 'green' # '0;255;0'
    ORANGE = 'orange' # '255;165;0'
    GRAY = 'gray' # '192;192;192'
    DARKGRAY = 'darkgray' # '128;128;128'
    YELLOW = 'yellow' # '165:165:0'

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

# class FrankLogger(object):
    
#     logger = None 

#     quiet = False 
#     headers = True 
#     log_level = None 
#     context = None 
#     dry_run = False 
    
#     def __init__(self, *args, **kwargs):
#         for k in [ p for p in kwargs if p in dir(self) ]:
#             self.__setattr__(k, kwargs[k])

#         self.logger = cowpy.getLogger() # logging.getLogger(__file__)
#         # self.logger.setLevel(logging._nameToLevel[self.log_level.upper()])
#         # self.logger.addHandler(logging.StreamHandler())
#         # log_filename = datetime.now()
#         # self.logger.addHandler(logging.FileHandler(f'/var/log/bckpsitrst/{log_filename}', mode='a'))
   
#     def clear_context(self):
#         self.context = None 
    
#     def set_context(self, context):
#         self.context = context 
    
#     def wrap_context(self, message):
#         if self.dry_run:
#             message = f'[ DRY RUN ] {message}'
#         if self.context:
#             message = f'[ {self.context} ] {message}'
#         return message 
    
#     def _wrap(self, call, message, color=None):
#         if not self.quiet:
#             message = self.wrap_context(message)
#             if color:
#                 call(colorwrapper(message, color))
#             else:
#                 call(message)
    
#     def text(self, message):
#         if not self.quiet:
#             self.logger.warning(message)
            
#     def debug(self, message):
#         self._wrap(self.logger.debug, message, 'darkgray')
    
#     def info(self, message):
#         self._wrap(self.logger.info, message, 'white')
    
#     def warning(self, message):
#         self._wrap(self.logger.warning, message, 'orange')

#     def success(self, message):
#         self._wrap(self.logger.info, message, 'green')
    
#     def error(self, message):
#         self._wrap(self.logger.error, message, 'red')

#     def exception(self, data=False):
#         stack_summary = traceback.extract_tb(sys.exc_info()[2])
#         self.logger.error(stack_summary)
#         if logging._nameToLevel[self.log_level.upper()] <= logging.ERROR:
#             self.logger.error(sys.exc_info()[0])
#             self.logger.error(sys.exc_info()[1])
#             for line in stack_summary.format():
#                 self.logger.error(line)

UNITS = [
    {
        'unit': 'day',
        'duration': 60*24
    },
    {
        'unit': 'hour',
        'duration': 60
    },
    {
        'unit': 'minute',
        'duration': 1
    }  
]

def smart_precision(float_value):
    
    float_value_string = str(float_value)
    
    places = 3
    
    # -- a hyphen will indicate the negative exponent of a python-formatted exponentially small number 2e-3 = 0.002 
    if float_value_string.find('-') >= 0:
        places = float_value_string.split('-')[1]
        
    return f'{float_value}:.{places}f'

def time_since(minutes):

    display = []

    for unit in UNITS:
        if minutes >= unit['duration']:
            val = math.floor(minutes / unit["duration"])
            display.append(f'{val} {unit["unit"]}{"s" if val > 1 else ""}')
            minutes -= val*unit['duration']
            if len(display) >= 1:
                break 
    
    return " ".join(display)

def stob(val):
    return str(val).lower() in ['1', 'true', 'yes', 'y']

def get_folder_free_space(folder):
    '''Folder free space in kilobytes'''
    cp = subprocess.run("df -k %s | grep -v Used | awk '{ print $4 }'" % folder, shell=True, text=True, capture_output=True)
    return int(cp.stdout.replace('\n', ''))

class Frequency(Enum):
    NEVER = 'never'
    HOURLY = 'hourly'
    DAILY = 'daily'
    WEEKLY = 'weekly'
    MONTHLY = 'monthly'

FREQUENCY_TO_MINUTES = {
    Frequency.NEVER.value: 0,
    Frequency.HOURLY.value: 60,
    Frequency.DAILY.value: 60*24,
    Frequency.WEEKLY.value: 60*24*7,
    Frequency.MONTHLY.value: 60*24*30
}

def frequency_to_minutes(frequency_value):
    return FREQUENCY_TO_MINUTES[frequency_value]
    
def get_path_uncompressed_size_kb(path, excludes):

    # TODO: use target excludes to more accurately compute size
    # TODO: estimate compressed size to more accurately compute size 
    cp = subprocess.run("du -kd 0 %s | awk '{ print $1 }'" % path, shell=True, text=True, capture_output=True)
    path_size = int(cp.stdout.replace('\n', ''))
    
    #excludes = excludes.split(':')

    # build:aws_backup/working:thirdparty:*.box:rpi/images:node_modules:clients/riproad:*.log:boxes:minecraft/worlds:minecraft/server/world
    # for path in Path(path).rglob('*'):
    #     if path.is_dir() and path in excludes:
            # pass # maybe exclude it
    
    return path_size 


def human(value, initial_units='b'):
    return_val = value 
    units = [ 'b', 'kb', 'mb', 'gb', 'tb', 'pb' ]    
    unit_index = units.index(initial_units)
    if type(value).__name__ in ['int', 'float'] or value.isnumeric():
        while value >= 1024:
            value = value / 1024.0
            unit_index += 1
        return_val = "%.1f %s" % (value, units[unit_index])
    
    return return_val
