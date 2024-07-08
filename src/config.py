import os 
import sys 
import re
from common import stob 
import cowpy 

logger = cowpy.getLogger() 

# -- this is the default template to be updated by matching input below 
# -- it will be passed as keyword args to logging and the main backup class 
#TODO: keep 'du' commands honest in common.py - we default to one file system, du includes 'x' -- need to make dynamic if we implement a flag
FLAGS = {
    'quiet': False,
    'headers': True,
    'show_would_push': False,
    'show_size_on_disk': False,
    'show_has_new_files': False,
    'verbose': False,
    'sort_targets': False,    
    'dry_run': False,    
    'force_push_latest': False,
    'exclude_vcs_ignores': False,
    'one_file_system': True,
    'no_cache': False,
    'ignore_schedule': False
}

NAMED_PARAMETER_DEFAULTS = {
    'order_by': 'name',
    'log_level': 'info'
}

# -- input matching these will update flags with the corresponding dict
FLAGS_OPTIONS = {
    '-q': 'quiet',
    '--no-headers': 'headers',
    '-p': 'show_would_push',
    '--show-size': 'show_size_on_disk',
    '-n': 'show_has_new_files',
    '-v': 'verbose',
    '-s': 'sort_targets',
    '-d': 'dry_run',
    '-f': 'force_push_latest',
    '--no-cache': 'no_cache',
    '--ignore-schedule': 'ignore_schedule'
}

# -- input matching these will become keyword args passed to the command
# -- flags will steal (take precedence) for matching keywords
NAMED_PARAMETER_OPTIONS = {
    '--name': 'target_name',
    '--freq': 'frequency',
    '--budget': 'budget',
    '--path': 'path',
    '-l': 'log_level',
    '-o': 'order_by',
    '--excludes': 'excludes'
}

class Config(object):

    s3_bucket = None 

    database_file = None 
    database_type = None 
    database_user = None 
    database_host = None 
    database_name = None 
    database_password = None 
        
    is_no_solicit = None 
    is_exclude_vcs_ignores = None 
    is_one_file_system = None 
    is_dry_run = None 
    
    working_folder = None 
    log_folder = None 

    cache_filename = None 

    def __init__(self, *args, **kwargs):        

        home_folder = os.path.expanduser(f'~{os.getenv("USER")}')

        def boolifbool(name, val):
            return stob(val) if name.find('is_') == 0 else val

        rc_file = os.getenv('FRANKBACK_RC_FILE', os.path.join(home_folder, '.frankbackrc'))

        if len(args) > 0:
            rc_file = args[0]

        if os.path.exists(rc_file):                
            rc_contents = []
            with open(rc_file, 'r') as f:
                rc_contents = f.readlines()
            logger.info(f'Reading {len(rc_contents)} lines from {rc_file}')
            for line in rc_contents:
                (envvar, val,) = line.strip('\n').split('=')
                varname_match = "^([A-Z0-9_]+)$"
                matches = re.findall(varname_match, envvar)
                if len(matches) > 0:
                    thisvar = matches[0].lower()
                    val = boolifbool(thisvar, val)
                    logger.debug(f'RC: setting {envvar} -> {thisvar} -> {val}')
                    self.__setattr__(thisvar, val)
                else:
                    logger.debug(f'No match found for {envvar} (pattern: {varname_match})')
        else:
            logger.warning(f'RC {rc_file} provided, but does not exist')

        # -- pull from environment
        for d in [ d for d in self.__class__.__dict__.keys() if d[0] != "_" ]:
            envvar = f'__FBCK_{d.upper()}'
            val = os.getenv(envvar)
            if val:
                val = boolifbool(d, val)
                logger.debug(f'ENV: setting {d} -> {envvar} -> {val}')
                self.__setattr__(d, val)
        
        # -- override as requested
        for k in kwargs:
            val = boolifbool(k, kwargs[k])
            logger.debug(f'ARG: setting {k} -> {val}')
            self.__setattr__(k, val)
        
        if not self.cache_filename:
            self.cache_filename = os.path.join(home_folder, '.bckt-target-cache')

        if not self.log_folder:
            self.log_folder = os.path.join(home_folder, 'bcktlog')

    def __repr__(self):
        return str(self.__dict__)
   
    def parse_flags(self):
        ''' Separate sys.argv into flags updates, actually make those updates and return everything else '''

        named_parameters = {**NAMED_PARAMETER_DEFAULTS}
        positional_parameters = []
        skip_next = False 
        flag_args = {**FLAGS}        
        
        for i, arg in enumerate(sys.argv[1:], 1):
            
            if skip_next:
                skip_next = False 
                continue 

            if arg in FLAGS_OPTIONS:
                opt = FLAGS_OPTIONS[arg]
                flag_args[opt] = not flag_args[opt]                
            elif arg in NAMED_PARAMETER_OPTIONS:                
                named_parameters.update({NAMED_PARAMETER_OPTIONS[arg]: sys.argv[i+1]})            
                skip_next = True
            else:
                positional_parameters.append(arg)
        
        return flag_args, positional_parameters, named_parameters