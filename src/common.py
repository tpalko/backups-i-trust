import os
import re
import subprocess 
import math
from enum import Enum 
from datetime import datetime 
import traceback 
import cowpy 
from pathlib import Path 
from cache import Cache, CacheType

# FOREGROUND_COLOR_PREFIX = '\033[38;2;'
# FOREGROUND_COLOR_SUFFIX = 'm'
# FOREGROUND_COLOR_RESET = '\033[0m'

home_folder = os.path.expanduser(f'~{os.getenv("USER")}')
LOCAL_STATS_CACHE_FILE = os.path.join(home_folder, '.bckt-local-stats-cache')

logger = cowpy.getLogger()

class Color(Enum):
    WHITE = 'white' # '255;255;255'
    RED = 'red' # '255;0;0'
    GREEN = 'green' # '0;255;0'
    ORANGE = 'orange' # '255;165;0'
    GRAY = 'gray' # '192;192;192'
    DARKGRAY = 'darkgray' # '128;128;128'
    YELLOW = 'yellow' # '165:165:0'

# COLOR_TABLE = {
#     'white': '255;255;255',
#     'red': '255;0;0',
#     'green': '0;255;0',
#     'orange': '255;165;0',
#     'gray': '192;192;192',
#     'darkgray': '128;128;128',
#     'yellow': '165:165:0'
# }

# def colorwrapper(text, color):
#     return f'{FOREGROUND_COLOR_PREFIX}{COLOR_TABLE[color]}{FOREGROUND_COLOR_SUFFIX}{text}{FOREGROUND_COLOR_RESET}'

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
        places = float_value_string.split("-")[1]
    
    # -- TODO: make this dynamic
    return f'{float_value:.3f}'

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

def _slugify_target_name(target_name):
    return target_name.replace("/", "_")

def calculate_archive_digest(filename):
    md5_cmd = "md5sum %s | awk '{ print $1 }'" % filename
    logger.debug(f'md5sum for {filename}: {md5_cmd}')

    cp = subprocess.run(md5_cmd, text=True, shell=True, capture_output=True)
    digest = str([ line for line in cp.stdout.splitlines() if line ][0])

    return digest

def generate_archive_target_filename(target, pre_timestamp):
    return f'{_slugify_target_name(target["name"])}_{datetime.strftime(pre_timestamp, "%Y%m%d_%H%M%S")}.tar.gz'

def archive_filename_match(target_name):
    return f'.*\/{_slugify_target_name(target_name)}\_[0-9]+_[0-9]+\.tar\.gz'    

def target_name_from_archive_filename(archive_filename):
    matches = re.findall("(.+)_[0-9]+_[0-9]+\.tar\.gz", archive_filename)
    if len(matches) > 0:
        return matches[0]
    return "-"

def pre_marker_timestamp_from_archive_filename(archive_filename):
    matches = re.findall(".+_([0-9]+_[0-9]+)\.tar\.gz", archive_filename)
    if len(matches) > 0:
        return datetime.strptime(matches[0], "%Y%m%d_%H%M%S")
    return "-"

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

def get_filesystem_coverage(mount_point, targets):

    # relevant_target_paths = [ t['path'] for t in targets if re.match(f'${mount_point}', t['path']) ]

    target_paths = [ t['path'] for t in targets ]

    for root, dirs, files in os.walk(mount_point):

        dirs_to_remove = []
        for dir in dirs:
            # exact matches we don't need to recurse into, they're covered
            if dir in target_paths:
                dirs_to_remove.append(dir)
                continue 
            
            possible = False
            for target_path in target_paths:
                if re.fullmatch(f'${dir}', target_path):
                    possible = True
                    break 
            if not possible:
                dirs_to_remove.append(dir)

        for dir in dirs_to_remove:
            dirs.remove(dir)


def get_new_files_since_timestamp(target_name, path, pre_marker_date, no_cache=False):

    logger.info(f'Checking new files for {target_name} at {path} since {pre_marker_date} (no_cache={no_cache})')

    local_stats_cache = Cache(context='local', cache_file=LOCAL_STATS_CACHE_FILE)

    cache_id = local_stats_cache.get_cache_id(CacheType.NewFiles, target_name)

    new_file_output = local_stats_cache.cache_fetch(cache_id)

    if new_file_output is None or no_cache:
            
        since_pre_minutes = (datetime.now() - pre_marker_date).total_seconds() / 60.0    
        
        logger.info(f'New file check for {target_name} at {path} since {pre_marker_date} is for {since_pre_minutes} minutes ago')

        find_cmd = f'find {path} -type f -mmin -{since_pre_minutes}'
        # self.logger.debug(find_cmd)
        cp = subprocess.run(find_cmd.split(' '), check=True, capture_output=True)

        # cp = subprocess.run(f'find {path} -newer {pre_marker}'.split(' '), check=True, capture_output=True)

        new_file_output = cp.stdout.splitlines()
        new_file_output = [ l.decode('utf-8') for l in new_file_output ]
        local_stats_cache.cache_store(cache_id, new_file_output)

    return new_file_output 

def get_path_excluded_files(target_name, path, excludes, no_cache=False):

    local_stats_cache = Cache(context='local', cache_file=LOCAL_STATS_CACHE_FILE)
    
    cache_id = local_stats_cache.get_cache_id(CacheType.ExcludedFiles, target_name)

    flat_excludes = local_stats_cache.cache_fetch(cache_id)
    
    if flat_excludes is None or no_cache:

        flat_excludes = []

        if excludes is not None:
                
            excludes = [ e for e in excludes.split(':') if e and e.strip() != "" ]
            this_path = Path(path)
            
            exclude_file_map = { e: [] for e in excludes }

            for exclude in excludes:      
                # logger.debug(f'looking at exclude {exclude}')  
                for found in this_path.rglob(exclude):
                    if found.is_dir():
                        cp = subprocess.run("find \"%s\" -type f" % found, shell=True, text=True, capture_output=True)
                        exclude_file_map[exclude].extend(cp.stdout.split('\n'))
                    else:
                        exclude_file_map[exclude].append(str(found))
            
            flat_excludes = [ f for e in exclude_file_map.keys() for f in exclude_file_map[e] if f.strip() != "" ]
            local_stats_cache.cache_store(cache_id, flat_excludes)
    
    return flat_excludes

def get_path_uncompressed_size_kb(target_name, path, excludes, no_cache=False):

    local_stats_cache = Cache(context='local', cache_file=LOCAL_STATS_CACHE_FILE)

    # TODO: use target excludes to more accurately compute size
    # TODO: estimate compressed size to more accurately compute size 
    cp = subprocess.run("du -kxd 0 %s | awk '{ print $1 }'" % path, shell=True, text=True, capture_output=True)
    path_size = int(cp.stdout.replace('\n', ''))
    
    cache_id = local_stats_cache.get_cache_id(CacheType.ExcludedSize, target_name)

    excluded_size = local_stats_cache.cache_fetch(cache_id)
    
    if excluded_size is None or no_cache:

        if excludes is not None:
                
            excludes = [ e for e in excludes.split(':') if e and e.strip() != "" ]
            this_path = Path(path)
            
            exclude_sizes = { e: 0 for e in excludes }

            for exclude in excludes:      
                logger.debug(f'looking at exclude {exclude}')  
                for found in this_path.rglob(exclude):
                    # logger.debug(f'looking at exclude {exclude} -> {found}')
                    found_excluded_size_bytes = 0
                    try:
                        if found.is_dir():
                            cp = subprocess.run("du -kxd 0 \"%s\" | awk '{ print $1 }'" % found, shell=True, text=True, capture_output=True)                
                            found_excluded_size_bytes = int(cp.stdout.replace('\n', '')) * 1024
                        else:
                            found_stat = found.stat()
                            found_excluded_size_bytes = int(found_stat.st_size)
                    except:
                        logger.exception()
                    # logger.debug(f'looking at exclude {exclude} -> {found} -> {human(found_excluded_size_bytes, "b")} bytes')
                    exclude_sizes[exclude] += found_excluded_size_bytes
                logger.debug(f'{exclude} => {1.0*exclude_sizes[exclude]/(1024*1024*1024):.2f} GB')
        
            excluded_size = sum([ exclude_sizes[e] for e in exclude_sizes.keys() ]) / 1024.0

            local_stats_cache.cache_store(cache_id, excluded_size)

    if excluded_size is not None:
        path_size = path_size - excluded_size

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
