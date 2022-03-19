import subprocess 
from glob import glob 
from pathlib import Path 

def stob(val):
    return str(val).lower() in ['1', 'true', 'yes', 'y']

def get_folder_free_space(folder):
    cp = subprocess.run("df -k %s | grep -v Used | awk '{ print $4 }'" % folder, shell=True, text=True, capture_output=True)
    return int(cp.stdout.replace('\n', ''))

def get_path_uncompressed_size_kb(path, excludes):

    # TODO: use target excludes to more accurately compute size
    # TODO: estimate compressed size to more accurately compute size 
    cp = subprocess.run("du -kd 0 %s | awk '{ print $1 }'" % path, shell=True, text=True, capture_output=True)
    path_size = int(cp.stdout.replace('\n', ''))

    # excludes = excludes.split(':')

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