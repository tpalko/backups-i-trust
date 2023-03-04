#!/usr/bin/env python3

import cowpy 
from datetime import datetime
from enum import Enum 
import json
import copy
import os 
import shutil
import sys
import traceback 
import math
# import hashlib
import sqlite3
import subprocess 
import inspect 
import logging
import time 
from common import smart_precision, get_folder_free_space, get_path_uncompressed_size_kb, human, stob, time_since, frequency_to_minutes, Frequency, FrankLogger, Color
from awsclient import AwsClient 
from columnizer import Columnizer
from database import Database 

MARKER_PLACEHOLDER_TEXT = f'this is a backup timestamp marker. its existence is under the control of {os.path.realpath(__file__)}'

EXCLUDE_VCS_IGNORES_DEFAULT = False 
ORDER_BY_DEFAULT = 'name'

# -- this is the default template to be updated by matching input below 
# -- it will be passed as keyword args to logging and the main backup class 
FLAGS = {
    'quiet': False,
    'headers': True,
    'verbose': False,
    'sort_targets': False,
    'order_by': ORDER_BY_DEFAULT,
    'dry_run': False,
    'log_level': 'info',
    'force_push_latest': False,
    'exclude_vcs_ignores': EXCLUDE_VCS_IGNORES_DEFAULT
}

# -- input matching these will update flags with the corresponding dict
FLAGS_OPTIONS = {
    '-q': {'quiet': True},
    '--no-headers': {'headers': False},
    '-v': {'verbose': True},
    '-s': {'sort_targets': True},
    '-d': {'dry_run': True},
    '-f': {'force_push_latest': True}
}

# -- input matching these will become keyword args passed to the command
# -- flags will steal (take precedence) for matching keywords
NAMED_PARAMETER_OPTIONS = {
    '--name': 'name',
    '--freq': 'frequency',
    '--budget': 'budget',
    '-l': 'log_level',
    '-o': 'order_by',
    '--excludes': 'excludes'
}

BACKUP_HOME = f'{os.path.dirname(os.path.realpath(__file__))}'

S3_BUCKET = None 
DATABASE_FILE = os.path.join(BACKUP_HOME, 'backups.db')
WORKING_FOLDER = os.path.join(BACKUP_HOME, 'working')
NO_SOLICIT = False 
EXCLUDE_VCS_IGNORES = EXCLUDE_VCS_IGNORES_DEFAULT

# -- config from file overwrites defaults 
RCFILE = os.getenv('FRANKBACK_RC_FILE') or os.path.join(os.path.expanduser(f'~{os.getenv("USER")}'), '.frankbackrc')

if RCFILE and os.path.exists(RCFILE) and os.path.isfile(RCFILE):
    from configparser import ConfigParser 
    p = ConfigParser()
    p.read(RCFILE)
    if p.has_section('default'):
        S3_BUCKET = p['default']['s3_bucket'] if 's3_bucket' in p['default'] else None 
        DATABASE_FILE = p['default']['database_file'] if 'database_file' in p['default'] else DATABASE_FILE
        WORKING_FOLDER = p['default']['working_folder'] if 'working_folder' in p['default'] else WORKING_FOLDER
        NO_SOLICIT = stob(p['default']['no_solicit']) if 'no_solicit' in p['default'] else NO_SOLICIT
        EXCLUDE_VCS_IGNORES = stob(p['default']['exclude_vcs_ignores']) if 'exclude_vcs_ignoers' in p['default'] else EXCLUDE_VCS_IGNORES_DEFAULT
else:
    RCFILE = None 

# -- env overwrites anything set so far 
S3_BUCKET = os.getenv('S3_BUCKET', S3_BUCKET)
DATABASE_FILE = os.getenv('DATABASE_FILE', DATABASE_FILE)
WORKING_FOLDER = os.getenv('WORKING_FOLDER', WORKING_FOLDER)
NO_SOLICIT = stob(os.getenv('FRANKBACK_NO_SOLICIT', NO_SOLICIT))
EXCLUDE_VCS_IGNORES = stob(os.getenv('EXCLUDE_VCS_IGNORES', EXCLUDE_VCS_IGNORES))
DRY_RUN = stob(os.getenv('DRY_RUN', False))

class Location(Enum):
    LOCAL_AND_REMOTE = 'local_and_remote'
    LOCAL_ONLY = 'local_only'
    REMOTE_ONLY = 'remote_only'
    DOES_NOT_EXIST = 'does_not_exist'
    LOCAL_REMOTE_UNKNOWN = 'local_remote_unknown'

# LOG_FILE = '/var/log/frankback/frankback.log'

#######################
#
# operation

class Results(object):
    
    _results = None 
    
    def __init__(self, *args, **kwargs):
        self._results = {
            'no_new_files': 0,
            'insufficient_space': 0,
            'archive_created': 0,
            'not_active': 0,
            'not_scheduled': 0,
            'other_failure': 0
        }
    
    def log(self, reason):
        if reason not in self._results:
            self._results[reason] = 0
        self._results[reason] += 1

    def print(self):
        print('\n\nResults:')
        print(json.dumps(self._results, indent=4))

if not os.path.exists("/var/log/bckt"):
    os.makedirs(os.path.join(os.path.sep, 'var', 'log', 'bckt'))

logger = cowpy.getLogger()
user_logger = cowpy.getLogger('user')

class Backup(object):
    
    logger = None 
    user_logger = None

    db = None 
    awsclient = None 

    dry_run = None 
    verbose = False 
    sort_targets = False 
    
    current_target = None 

    def __init__(self, *args, **kwargs):
        
        self.logger = kwargs['logger'] if 'logger' in kwargs else cowpy.getLogger()
        self.user_logger = kwargs['user_logger'] if 'logger' in kwargs else cowpy.getLogger('user')

        if 'bucket_name' not in kwargs or kwargs['bucket_name'] == '':
            raise Exception("bucket_name must be supplied to Backup")

        self.db = Database(logger=self.logger, db_file=DATABASE_FILE)
        self.awsclient = AwsClient(bucket_name=kwargs['bucket_name'], db=self.db, logger=self.logger)

        for k in kwargs:
            setattr(self, k, kwargs[k])
        
        if DRY_RUN:
            self.dry_run = DRY_RUN
        
    def init_commands(self):
        '''
        bckt db init
        bckt info 
        bckt target add <path> [-n NAME] [-f FREQUENCY] [-b BUDGET] [-e EXCLUDES]
        bckt target edit <TARGET NAME> [-f FREQUENCY] [-b BUDGET] [-e EXCLUDES]
        bckt target pause|unpause <TARGET NAME>
        bckt target list [TARGET NAME]
        bckt run [TARGET NAME]
        bckt target run <TARGET NAME>
        bckt target push <TARGET NAME>
        bckt archive list [TARGET NAME]
        bckt archive prune 
        bckt archive aggressive-prune 
        bckt archive restore <ID>
        globals:
            set log level:  -l <LOG LEVEL>
            set quiet:      -q
            set verbose:    -v
            set dry run:    -d
            set no headers: --no-headers
        '''
        return {
            'db init': self.initialize_database,
            'info': self.print_header,
            'target add': self.create_target,
            'target edit': self.edit_target,
            'target pause': self.pause_target,
            'target unpause': self.unpause_target,
            'target list': self.print_targets,
            'run': self.run,
            'target run': self.add_archive,
            'target push': self.push_target_latest,
            'archive list': self.print_archives,
            'archive last': self.print_last_archive,
            'archive prune': self.prune_archives,
            'archive aggressive-prune': self.prune_archives_aggressively,
            'archive restore': self.restore_archive,
            'fixarchives': self.db.fix_archive_filenames,
            'help': self.print_help
        }

    def print_help(self):
        '''
        Print this help
        '''

        for command in self.init_commands().keys():
            fn = self.init_commands()[command]
            sig = (' '.join(inspect.signature(fn).parameters.keys())).upper()
            # fn_name = fn.__code__.co_name 
            doc = fn.__doc__
            if doc:
                while doc.find('DOCDEFER') == 0:
                    doc_location = doc.split(':')[1]
                    doc = eval(doc_location).__doc__
            print(f'{command} {sig}')
            print(f'\t{doc}\n' if doc else '')
    
    def command(self, positional_parameters, named_parameters):

        if len(sys.argv) < 2:
            self.print_help()
            exit(1)

        # -- starting from the first post-executable token and progressively 
        # -- including subsequent tokens 
        # -- find the first full command match in provided 'commands' keys 
        # -- note that this implies more complex commands must come earlier 
        # -- i.e. "run program" would match "run" if tested, even if it was meant 
        # -- to match "run program", so "run program" must come before "run" in 'commands'

        # -- increment until we match a command 
        tokens = 1
        command = ""
        while True:
            if tokens > len(positional_parameters):
                self.user_logger.error(f'The command {command} is not supported.')
                exit(1)    
            command = " ".join(positional_parameters[0:tokens])
            if command in self.init_commands().keys():
                break 
            tokens += 1
        
        fn = self.init_commands()[command]

        # -- any arguments left over after matching the command are compiled here 
        args = positional_parameters[tokens:] if len(positional_parameters) > tokens else []
        
        self.print_header()

        try:
            fn(*args, **named_parameters)
        except:
            # self.logger.error(f'Please refer to help for "{command}"')
            # self.logger.error('Some common errors:')
            # self.logger.error('- unquoted strings with spaces')
            self.logger.exception()
        
    def print_header(self):
        self.logger.info(f'\n\n\
Date:\t\t{datetime.now()}\n\
User:\t\t{os.getenv("USER", "unknown")}\n\
Database file:\t{DATABASE_FILE}\n\
RC File:\t{RCFILE}\n\
Command:\t{" ".join(sys.argv)}\n\
Working folder:\t{WORKING_FOLDER}\n\
\n\
*********begin output***********\n')
    
    def targets(self, target_name=None):
         targets = []

         if target_name:
             target = self.db.get_target(name=target_name)
             if target:
                 targets.append(target)
         else:
             targets = self.db.get_targets()            
         
         self.logger.debug(f'fetching remote stats on {len(targets)} targets')
         remote_stats = self.awsclient.get_remote_stats(targets)
         self.logger.debug(f'stats fetched')
         
         for target in targets:
             self.logger.set_context(target['name'])
             yield target, remote_stats[target['name']]
             self.logger.clear_context()
            
    def initialize_database(self):
        '''DOCDEFER:Database.init_db'''
        self.db.init_db()

    def create_target(self, path, name=None, frequency=Frequency.DAILY.value, budget=0.01, excludes=''):

        if not name:
            name = path.replace('/', '-').lstrip('-').rstrip('-')

        self.db.create_target(path, name, frequency, budget=budget, excludes=excludes)

    def edit_target(self, target_name, frequency=None, budget=None, excludes=None):
        '''Sets target parameters'''
        if frequency is not None:
            frequency_choices = [ m.lower() for m in Frequency.__members__ ]
            if frequency not in frequency_choices:
                raise Exception(f'"{frequency}" is not a valid frequency (choose: {",".join(frequency_choices)})')

        self.db.update_target(target_name, frequency=frequency, budget_max=budget, excludes=excludes)

    def pause_target(self, target_name):
        '''Sets target inactive'''
        self.db.update_target(target_name, is_active=False)
    
    def unpause_target(self, target_name):
        '''Sets target active'''
        self.db.update_target(target_name, is_active=True)

    def remove_marker_files(self, target):        
        os.unlink(self.get_marker_path(target, 'pre'))
        os.unlink(self.get_marker_path(target, 'post'))

    def get_marker_path(self, target, place):
        return os.path.join(os.path.realpath(os.path.join(target['path'], '..')), f'{target["name"]}_{place}_backup_marker')

    def recreate_marker_file(self, marker_path, timestamp=None):
        if not os.path.exists(marker_path):
            with open(marker_path, 'w') as f:
                f.write(MARKER_PLACEHOLDER_TEXT)

        if timestamp:
            subprocess.run(f'touch {marker_path} -t {datetime.strftime(timestamp, "%Y%m%d%H%M.%S")}'.split(' '))
        else:
            subprocess.run(f'touch {marker_path}'.split(' '))

    def update_markers(self, target, pre_marker_timestamp):
        # TODO: moving pre/post should be atomic
        pre_marker = self.get_marker_path(target, 'pre')
        self.recreate_marker_file(pre_marker, pre_marker_timestamp)
        post_marker = self.get_marker_path(target, 'post')
        self.recreate_marker_file(post_marker)

    def target_has_new_files(self, target, log=True):
        
        has_new_files = False 

        if log:
            self.logger.debug(f'Determining if new files exist')

        try:
            
            pre_marker_date = target["pre_marker_at"]

            if not pre_marker_date:
                pre_marker_file = self.get_marker_path(target, 'pre')
                pre_marker_stat = shutil.os.stat(pre_marker_file)            
                pre_marker_date = datetime.fromtimestamp(pre_marker_stat.st_mtime)

                if pre_marker_date and not target["pre_marker_at"]:
                    self.db.update_target(target["name"], pre_marker_at=pre_marker_date)
                    self.remove_marker_files(target)

            if pre_marker_date:

                pre_marker_stamp = datetime.strftime(pre_marker_date, "%c")

                # -- verify an archive actually exists corresponding to this pre-marker file
                marker_archive = self.db.get_archive_for_pre_timestamp(target['id'], pre_marker_date)
                if not marker_archive:
                    if log:
                        self.logger.warning(f'No archive exists corresponding to the existing pre-marker. This marker is invalid, and all files are considered new.')
                    has_new_files = True 
                else:

                    since_pre_minutes = (datetime.utcnow() - pre_marker_date).total_seconds() / 60.0    
                    find_cmd = f'find {target["path"]} -mmin -{since_pre_minutes}'
                    cp = subprocess.run(find_cmd.split(' '), check=True, capture_output=True)

                    # cp = subprocess.run(f'find {target["path"]} -newer {pre_marker}'.split(' '), check=True, capture_output=True)

                    new_file_output = cp.stdout.splitlines()
                    new_file_count = len(new_file_output)
                    has_new_files = new_file_count > 0
                    if log:                        
                        pre_marker_stamp = datetime.strftime(pre_marker_date, "%c")
                        self.logger.info(f'{new_file_count} new files found since {pre_marker_stamp}')
                        self.logger.debug(new_file_output)
            else:
                has_new_files = True 
                if log:
                    self.logger.warning(f'No pre-marker date found, all files considered new')        

        except subprocess.CalledProcessError as cpe:
            self.logger.error(cpe.stderr)   
            traceback.print_tb(sys.exc_info()[2])
        
        return has_new_files

    def cleanup_local_archives(self, target=None, aggressive=False, dry_run=True):
        '''
        baseline: keep a minimum number of recent versions, delete anything over and/or older than a margin
            keep 3 latest

        cleanup:
            shrink minimum number of recent versions and/or pull up expiration margin 
            delete anything local that has a copy remote 
        '''

        targets = []
        if target:
            targets = [target]
        else:
            targets = self.db.get_targets()
        
        archives = self.get_archives()

        if dry_run and not target:
            self.logger.warning(f'Examining {len(targets)} targets to determine possible space made available by cleanup ({ "not " if not aggressive else "" }aggressive).')
        
        self.logger.debug(f'creating cleanup-by-target tracking dict from {len(targets)} targets')
        
        cleaned_up_by_target = { t["name"]: 0 for t in targets }

        for t in targets:        

            target_archives = [ a for a in archives if a['target_id'] == t['id'] ]        
            
            archives_newest_first = sorted(target_archives, key=lambda a: a['created_at'])
            archives_newest_first.reverse()     
            minimum_to_keep = 1 if aggressive else 3
            found = 0

            if dry_run and target:
                self.logger.warning(f'Examining {len(archives_newest_first)} ({target["name"]}) archives to determine possible space made available by cleanup ({ "not " if not aggressive else "" }aggressive).')
        
            for archive in archives_newest_first:

                cleaned_up_aggressively = False 
                
                # -- two places aggressive comes into play 
                # -- 1. only aggressive will remove _all_ local archives for a target 
                # -- as long as there is also a remote copy (non-aggressive will only 
                # -- clean up to within "minimum to keep", allowing local copies even 
                # -- if a remote copy exists)
                # -- 2. aggressive will lower the "minimum to keep" from 3 to 1
                # -- so non-aggressive will allow up to 3 local archives for each target 
                # -- aggressive will potentially remove every local archive, as long 
                # -- as at least one archive exists remotely 
                # -- TODO: check that aggressive will in fact remove all local archives 
                # -- it may be keeping one 
                
                if aggressive:
                    if archive['location'] == Location.LOCAL_AND_REMOTE:
                        cleaned_up_by_target[t['name']] += archive['size_kb']
                        cleaned_up_aggressively = True 
                        message = f'Archive {archive["name"]}/{archive["filename"]} is both remote and local. Deleting local copy.'
                        if dry_run:
                            self.logger.warning(f'[ DRY RUN ] {message}')
                        else:
                            self.logger.warning(message)
                            os.unlink(os.path.join(WORKING_FOLDER, archive["filename"]))            

                if not cleaned_up_aggressively and os.path.exists(os.path.join(WORKING_FOLDER, archive["filename"])):                
                    if found < minimum_to_keep:
                        message = f'Keeping newer file {archive["filename"]}'
                        if dry_run:
                            self.logger.info(f'[ DRY RUN ] {message}')
                        else:
                            self.logger.info(message)
                        found += 1
                    else:
                        cleaned_up_by_target[t['name']] += archive['size_kb']
                        message = f'Deleting file {os.path.join(WORKING_FOLDER, archive["filename"])}'
                        if dry_run:
                            self.logger.error(f'[ DRY RUN ] {message}')
                        else:
                            self.logger.error(message)
                            os.unlink(os.path.join(WORKING_FOLDER, archive["filename"]))
        
        return cleaned_up_by_target[target['name']] if target else cleaned_up_by_target
    
    def _create_working_folder(self):
        if not os.path.isdir(WORKING_FOLDER):
            os.makedirs(WORKING_FOLDER)
            self.logger.success(f'Created working folder: {WORKING_FOLDER}')
            
    def add_archive(self, target_name, results=None):
        '''
            Creates a new archive for the provided target name, assuming 
            precursors (frequency, active status), however does account for delta-on-disk and 
            honors budget constraints for remote storage
        '''
        
        target = self.db.get_target(name=target_name)
        
        if not results:
            results = Results()

        if not self.target_has_new_files(target):
            self.logger.warning(f'No new files. Skipping archive creation.')
            results.log('no_new_files')
            return
            
        target_size = get_path_uncompressed_size_kb(target['path'], target['excludes'])        
        last_archive = self.db.get_last_archive(target['id'])
        
        increase = float("%.f" % (target_size*100.0 / int(last_archive['size_kb'])))
        if increase > 0:
            self.logger.warning(f'This target increased in size by {increase}% since the last archive ({smart_precision(target_size/(1024*1024))} GB over {smart_precision(int(last_archive["size_kb"])/(1024*1024))} GB)')
            # -- TODO: can put a limiter in here 
        
        self._create_working_folder()
        
        free_space = get_folder_free_space(WORKING_FOLDER)

        if target_size > free_space:
            
            additional_space_needed = target_size - free_space

            self.logger.error(f'This target is {human(additional_space_needed, "kb")} bigger than what is available on the filesystem.')

            space_freed_by_cleanup = self.cleanup_local_archives(aggressive=False, dry_run=True)
            space_sum = sum([ space_freed_by_cleanup[t] for t in space_freed_by_cleanup.keys() ])

            if space_sum < additional_space_needed:

                self.logger.error(f'Even after cleaning up local archives, an additional {human(additional_space_needed - space_sum, "kb")} is still needed. Please free up space and reschedule this target as soon as possible.')

                space_freed_by_cleanup = self.cleanup_local_archives(aggressive=True, dry_run=True)
                space_sum = sum([ space_freed_by_cleanup[t] for t in space_freed_by_cleanup.keys() ])
                
                if space_sum < additional_space_needed:
                    self.logger.error(f'Even after aggressively cleaning up local archives, an additional {human(additional_space_needed - space_sum, "kb")} is still needed. Please free up space and reschedule this target as soon as possible.')
                    results.log('insufficient_space')
                    return 
                else:
                    self.logger.warning(f'Cleaning up old local archives aggressively will free {human(space_sum, "kb")}. Proceeding with cleanup.')
                    space_freed_by_cleanup = self.cleanup_local_archives(aggressive=True, dry_run=self.dry_run)
            else:

                self.logger.warning(f'Cleaning up old local archives will free {human(space_sum, "kb")}. Proceeding with cleanup.')
                self.cleanup_local_archives(aggressive=False, dry_run=self.dry_run)
        
        new_archive_id = None 

        try:
                
            target_file = f'{WORKING_FOLDER}/{target["name"]}_{datetime.strftime(datetime.now(), "%Y%m%d_%H%M%S")}.tar.gz'
            self.logger.info(f'Creating archive for {target["name"]}: {target_file}')

            excludes = ""
            if target["excludes"] and len(target["excludes"]) > 0:
                excludes = f'--exclude {" --exclude ".join(target["excludes"].split(":"))}'
            
            archive_command = f'tar {excludes} -czf {target_file} {target["path"]}'

            if self.exclude_vcs_ignores:
                archive_command = f'tar {excludes} --exclude-vcs-ignores -czf {target_file} {target["path"]}'
                
            self.logger.info(f'Running archive command: {archive_command}')

            # -- strip off microseconds as this is lost when creating the marker file and will prevent the assocation with the archive record
            pre_timestamp = datetime.strptime(datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")

            cp = subprocess.run(archive_command.split(' '), capture_output=True)
            self.logger.warning(cp.args)            
            self.logger.warning(f'Archive returncode: {cp.returncode}')
            if cp.stdout:
                self.logger.warning(cp.stdout)
            if cp.stderr:
                self.logger.error(cp.stderr)
            archive_errors = str(cp.stderr)

            cp = subprocess.run(f'tar --test-label -f {target_file}'.split(' '), capture_output=True)
            self.logger.warning(cp.args)
            self.logger.warning(f'Archive test returncode: {cp.returncode}')
            if cp.stderr:
                self.logger.error(cp.stderr)
            
            cp.check_returncode()

            if archive_errors.find("No space left on device") >= 0:
                results.log('insufficient_space')
                raise Exception("Insufficient space while archiving. Archive target file (assumed partial) will be deleted. Please clean up the disk and reschedule this target as soon as possible.")
            
            target_file_stat = shutil.os.stat(target_file)
            
            new_archive_id = self.db.create_archive(
                target_id=target['id'], 
                size_kb=target_file_stat.st_size/1024.0, 
                filename=target_file, 
                returncode=cp.returncode, 
                errors=archive_errors, 
                pre_marker_timestamp=pre_timestamp)
            
            self.update_markers(target, pre_timestamp)
            results.log('archive_created')
            
            self.logger.success(f'Archive record {new_archive_id} created for {target_file}')
            
            if target_file:
                self.logger.success(f'Created {target["name"]} archive: {target_file}')
            else:
                self.logger.warning(f'No {target["name"]} archive created')

        except:
            # -- maybe roll back any changes if not past a certain point 
            # -- group tasks into milestones
            # -- i.e. milestone 1: create archive, write archive record, move marker files
            # -- rollback: 
            # --    if marker file move fails, delete archive record, delete archive file 
            # --    if archive record fails, delete archive file 
            # -- milestone 2: push to s3, update archive record
            #   rollback:
            #       if archive record fails, don't delete from s3 because the remaining days will only be prorated 

            self.logger.exception()

            if new_archive_id:
                self.logger.error(f'Removing archive record {new_archive_id}')
                self.db.delete_archive(new_archive_id)
            if os.path.exists(target_file):
                self.logger.error(f'Removing archive file {target_file}')
                os.unlink(target_file)
    
    def push_target_latest(self, target_name):
        '''Pushes latest target archive remotely, if not already remote. Honors budget/time constraints by default, so usually used with -p (force push latest). If target name not provided, acts on all targets.'''
        
        for target, remote_stats in self.targets(target_name):
            
            last_archive = self.db.get_last_archive(target['id'])
            if last_archive and not last_archive['is_remote']:
                self.logger.warning(f'Last archive is not pushed remotely')

                if target['is_active']:
                    # -- no, we do not want to automatically delete anything older than 6 months
                    # -- before confirming if anything will be pushed up to replace it, ever
                    self.awsclient.cleanup_remote_archives(target_name, remote_stats, dry_run=True)
                else:
                    self.logger.warning(f'Not cleaning remote archives (is_active={target["is_active"]})')

                if self.force_push_latest or self.awsclient.is_push_due(target, remote_stats=remote_stats):
                    try:
                        if target['is_active']:
                            archive_full_path = os.path.join(WORKING_FOLDER, last_archive["filename"])
                            self.logger.success(f'Pushing {archive_full_path} ({human(last_archive["size_kb"], "kb")})')
                            if not self.dry_run:
                                self.awsclient.push_archive(last_archive["name"], last_archive["filename"], archive_full_path)
                            self.logger.success(f'Last archive has been pushed remotely')                        
                            if not self.dry_run:
                                self.db.set_archive_remote(last_archive)
                        else:
                            self.logger.warning(f'Not pushing remote (is_active={target["is_active"]})')
                    except:
                        self.logger.error(f'The last archive failed to push remotely')
                        self.logger.error(sys.exc_info()[1])
                        traceback.print_tb(sys.exc_info()[2])
            elif not last_archive:
                self.logger.warning(f'No last archive is available for this target')
            elif last_archive['is_remote']:
                self.logger.info(f'The last archive is already pushed remotely')
    
    def restore_archive(self, archive_id):
        '''Unpacks the archive identified by the ID provided into WORKING_FOLDER/restore/<target name>/<archive filename base>'''
        
        archive_record = self.db.get_archive(archive_id)
        if not archive_record:
            self.logger.warning(f'Archive {archive_id} was not found')
            return 

        location = self.get_archive_location(archive_record)
        if location in [Location.LOCAL_AND_REMOTE, Location.LOCAL_ONLY, Location.LOCAL_REMOTE_UNKNOWN]:
            self.logger.info(f'Archive {archive_record["filename"]} is local, proceeding to unarchive.')
            filenamebase = archive_record["filename"].split('.')[0]
            archive_path = f'{WORKING_FOLDER}/{archive_record["filename"]}'
            unarchive_folder = f'{WORKING_FOLDER}/restore/{archive_record["name"]}/{filenamebase}'
            self.logger.info(f'Unarchiving into {unarchive_folder}')
            os.makedirs(unarchive_folder)
            unarchive_command = f'tar -xzf {archive_path} -C {unarchive_folder}'
            cp = subprocess.run(unarchive_command.split(' '), capture_output=True)
            self.logger.warning(cp.args)
            self.logger.warning(f'Archive returncode: {cp.returncode}')
            self.logger.warning(cp.stdout)
            self.logger.error(cp.stderr)

    ### other operations 

    def get_archives(self, target_name=None):
        db_records = self.db.get_archives(target_name)        
        s3_objects = self.awsclient.get_remote_archives(target_name)
        self.logger.debug(f'Have {len(db_records)} database records and {len(list(s3_objects))} S3 objects')
        s3_objects_by_filename = { os.path.basename(obj['key']): obj for obj in s3_objects }    
        for record in db_records:
            record['location'] = self.get_archive_location(record, s3_objects_by_filename)
        return db_records

    def target_is_scheduled(self, target):
        '''Reports true/false based on target.frequency and existence/timestamp of last archive, NOT existence of new files'''

        frequency = target['frequency']
        frequency_minutes = frequency_to_minutes(frequency)
        last_archive = self.db.get_last_archive(target['id'])
        is_scheduled = False 
        if last_archive:
            since_minutes = (datetime.now() - last_archive['created_at']).total_seconds() / 60
            is_scheduled = since_minutes >= frequency_minutes
        else:
            is_scheduled = True 
        return is_scheduled
    
    def get_blank_target(self):
        return {
            'name': '-',
            'is_active': '-',
            'path': '-',
            'excludes': '-',
            'uncompressed_kb': '-',
            'last_archive_size': '-',
            'frequency': '-',
            'cycles_behind': '-',
            'last_archive_at': '-',
            'has_new_files': '-',
            'would_push': '-',
            'push_strategy': '-',
            'monthly_cost': '-',
            'budget_max': '-',
            'local_archive_count': '-',
            'remote_archive_count': '-'
        }

    def print_targets(self, target_name=None):

        '''for each target:
                how many cycles behind is it?
                are there files not backed up?
                are there archives not pushed? (implies there are files not pushed)
                monthly cost in s3?'''

        now = datetime.now()

        target_print_items = []
        archives_by_target_and_location = {}
        total_last_archive_size_kb = 0

        for target_print_item, remote_stats in self.targets(target_name):
            
            # -- target name, path, budget max, frequency, total archive count, % archives remote, last archive date/days, next archive date/days
            time_out = datetime.now()
            archives = self.get_archives(target_print_item['name'])
            time_in = datetime.now()
            self.logger.debug(f'archive fetch time: {"%.1f" % (time_in - time_out).total_seconds()} seconds')

            self.logger.debug(f'have {len(archives)} archives for {target_print_item["name"]}')
            
            if target_print_item['id'] not in archives_by_target_and_location:
                archives_by_target_and_location[target_print_item['id']] = {'local': [], 'remote': [] }
            
            archives_by_target_and_location[target_print_item['id']]['local'] = [ a for a in archives if a['location'] in [ Location.LOCAL_AND_REMOTE, Location.LOCAL_ONLY ] ]
            archives_by_target_and_location[target_print_item['id']]['remote'] = [ a for a in archives if a['location'] in [ Location.LOCAL_AND_REMOTE, Location.REMOTE_ONLY ] ]

            # for archive in archives:
            #     if archive['location'] in [ Location.LOCAL_AND_REMOTE, Location.LOCAL_ONLY ]:
            #         archives_by_target_and_location[archive['target_id']]['local'].append(archive)
            #     if archive['location'] in [ Location.LOCAL_AND_REMOTE, Location.REMOTE_ONLY ]:
            #         archives_by_target_and_location[archive['target_id']]['remote'].append(archive)
                    
            # target_print_item = copy.copy(target)

            self.logger.debug(f'analyzing {target_print_item["name"]}')
            
            target_print_item['has_new_files'] = '-'

            if self.verbose and target_print_item['is_active']:
                target_print_item['has_new_files'] = self.target_has_new_files(target_print_item, log=True)

            target_archives_by_created_at = { a['created_at']: a for a in archives if a['target_id'] == target_print_item['id'] }
            
            target_print_item['last_archive_at'] = '-'
            target_print_item['last_archive_pushed'] = '-'
            target_print_item['last_archive_size'] = '-'
            target_print_item['cycles_behind'] = '-'
            target_print_item['would_push'] = '-'
            target_print_item['uncompressed_kb'] = '-'
            
            # -- if no archives, we set some defaults and skip the remaining analysis 
            if len(target_archives_by_created_at) == 0:
                target_print_item['last_archive_pushed'] = 'n/a'
                if self.verbose and target_print_item['is_active']:
                    target_print_item['would_push'] = target_print_item['has_new_files']
            else:
                last_archive_created_at = max(target_archives_by_created_at.keys())
                last_archive = target_archives_by_created_at[last_archive_created_at]
                
                target_print_item['cycles_behind'] = 0
                frequency = target_print_item['frequency']
                minutes_since_last_archive = (now - last_archive['created_at']).total_seconds() / 60.0
                
                frequency_minutes = frequency_to_minutes(frequency)
                if frequency_minutes != 0:            
                    target_print_item['cycles_behind'] = math.floor(minutes_since_last_archive / frequency_minutes)

                target_print_item['last_archive_at'] = time_since(minutes_since_last_archive)
                target_print_item['last_archive_pushed'] = last_archive['is_remote']
                target_print_item['last_archive_size'] = "%.2f" % (last_archive['size_kb'] / (1024*1024))
                total_last_archive_size_kb += last_archive['size_kb']
            
            
            if self.verbose and target_print_item['is_active']:
                push_due = self.awsclient.is_push_due(target_print_item, remote_stats=remote_stats, print=False)
                target_print_item['would_push'] = push_due and (not target_print_item['last_archive_pushed'] or target_print_item['has_new_files'])
                target_print_item['uncompressed_kb'] = get_path_uncompressed_size_kb(target_print_item['path'], target_print_item['excludes'])

            target_print_item['local_archive_count'] = len(archives_by_target_and_location[target_print_item['id']]['local'])
            target_print_item['remote_archive_count'] = len(archives_by_target_and_location[target_print_item['id']]['remote'])
            target_storage_cost_sum = sum([ self.awsclient.get_object_storage_cost_per_month(a['size_kb']*1024) for a in archives_by_target_and_location[target_print_item['id']]['remote'] ])
            self.logger.debug(f'{target_print_item["name"]} storage cost sum: {target_storage_cost_sum}')
            target_print_item['monthly_cost'] = smart_precision(target_storage_cost_sum)

            target_print_items.append(target_print_item)
                
        # self.logger.debug(json.dumps(target_print_items, indent=4))
        
        target_columns = [
            {
                'key': 'name',
                'trunc': False
            },
            {
                'key': 'is_active',
                'header': 'active?'
            },
            {
                'key': 'path',
                'trunc': False
            },
            {
                'key': 'excludes'
            },
            {
                'key': 'uncompressed_kb',
                'header': 'KB on disk',
                'full': True
            },
            {
                'key': 'last_archive_size',
                'header': 'last archive GB'
            },
            {
                'key': 'frequency'
            },  
            {
                'key': 'cycles_behind'
            },
            {
                'key': 'last_archive_at',
                'header': 'last archive',
                'trunc': False
            },
            {
                'key': 'has_new_files',
                'header': 'new files?',
                'full': True
            },
            {
                'key': 'would_push',
                'header': 'would push?',
                'full': True
            },
            {
                'key': 'push_strategy'
            },
            {
                'key': 'monthly_cost'
            },
            {
                'key': 'budget_max'
            },
            {
                'key': 'local_archive_count',
                'header': '# local archives'
            },
            {
                'key': 'remote_archive_count',
                'header': '# remote archives'
            },
        ]

        # -- remove columns based on the verbose flag and whether the column specifies full: True 
        trimmed_target_columns = [ c for c in target_columns if 'full' not in c or (self.verbose and c['full']) ]
        
        sorted_target_print_items = sorted(target_print_items, key=lambda t: t['path'])

        if self.sort_targets:
            if self.verbose:
                sorted_target_print_items = sorted(target_print_items, key=lambda t: not stob(t['would_push']))
                sorted_target_print_items = sorted(sorted_target_print_items, key=lambda t: not stob(t['has_new_files']))
            sorted_target_print_items = sorted(sorted_target_print_items, key=lambda t: not stob(t['is_active']))
            # sorted_target_print_items.extend([ t for t in target_print_items if t['would_push'] == True ])
            # sorted_target_print_items.extend([ t for t in target_print_items if t['would_push'] == False and t['has_new_files'] == True ])
            # sorted_target_print_items.extend([ t for t in target_print_items if t['would_push'] == False and t['has_new_files'] == False ])

        highlight_template = [ Color.DARKGRAY if not t['is_active'] else Color.GREEN if t['would_push'] == True else Color.WHITE if t['has_new_files'] == True else None for t in sorted_target_print_items ]
        
        sorted_target_print_items = [ { k: t[k] for k in t.keys() if k not in ['is_active'] } for t in sorted_target_print_items ]
        trimmed_target_columns = [ c for c in trimmed_target_columns if c['key'] not in ['is_active'] ]
        
        # -- from the trimmed columns, generate the header row
        header = [ c['header'] if 'header' in c else c['key'] for c in trimmed_target_columns ]
        
        # -- this appears to blank values in the table data if not str(value)?
        sorted_target_print_items = [ { k: str(v) or '' for k,v in t.items() } for t in sorted_target_print_items ]

        # -- using the target columns as a guide
        # -- cycle through the table data and trim values to the length of the header
        # -- unless the column specifies trunc: False 
        table = [ [ f'{str(t[c["key"]])[0:len(header[i])-2]}..' if len(str(t[c['key']])) > len(header[i]) and not self.verbose and ('trunc' not in c or c['trunc']) else t[c['key']] for i,c in enumerate(trimmed_target_columns) ] for t in sorted_target_print_items ]

        c = Columnizer(logger=self.logger, **flag_args)
        c.print(table, header, highlight_template=highlight_template, data=True)
        print(f'Total current backup size: {(total_last_archive_size_kb/(1024*1024)):.2f} GB')

    def get_archive_location(self, archive, remote_file_map=None):
        basename = os.path.basename(archive["filename"])
        local_file_exists = os.path.exists(os.path.join(WORKING_FOLDER, archive["filename"]))
        remote_file_exists = remote_file_map and basename in remote_file_map
        location = Location.DOES_NOT_EXIST

        if local_file_exists and remote_file_exists:
            location = Location.LOCAL_AND_REMOTE
        elif local_file_exists and remote_file_map:
            location = Location.LOCAL_ONLY
        elif remote_file_exists:
            location = Location.REMOTE_ONLY
        elif local_file_exists and not remote_file_map:
            location = Location.LOCAL_REMOTE_UNKNOWN
        
        # self.logger.debug(f'Location: {location}')
        return location 

    def _get_archives_for_target(self, target_name=None):
        
        db_records = self.db.get_archives(target_name)

        self.logger.debug(db_records)

        s3_objects = self.awsclient.get_remote_archives(target_name)
        s3_objects_by_filename = { os.path.basename(obj['key']): obj for obj in s3_objects }    

        archive_display = []

        for db_record in db_records:

            db_record['target_name'] = db_record['name']
            db_record['size_mb'] = "%.1f" % (db_record['size_kb'] / 1024.0)
            db_record['location'] = self.get_archive_location(db_record, s3_objects_by_filename)    

            basename = os.path.basename(db_record['filename'])

            db_record['s3_cost_per_month'] = "%.4f" % 0.00
            if basename in s3_objects_by_filename:
                db_record['s3_cost_per_month'] = "%.4f" % self.awsclient.get_object_storage_cost_per_month(s3_objects_by_filename[basename].size)
            
            archive_display.append(db_record)

            if basename in s3_objects_by_filename:
                del s3_objects_by_filename[basename]
        
        # -- at this point, s3_objects_by_filename has been cleaned of everything with a DB representation
        # -- only orphans left 
        for orphaned_s3_object_filename in s3_objects_by_filename:
            obj = s3_objects_by_filename[orphaned_s3_object_filename]

            archive_display.append({ 
                'id': None, 
                'filename': obj['key'], 
                'created_at': obj['last_modified'], 
                'size_mb': "%.1f" % (obj['size']/(1024.0*1024.0)), 
                'location': Location.REMOTE_ONLY, 
                's3_cost_per_month': "%.4f" % self.awsclient.get_object_storage_cost_per_month(obj['size']) 
            })

            if not target_name:
                archive_display[-1]['target_name'] = '-'
        
        #total_cost = sum([ float(a['s3_cost_per_month'])  for a in archive_display ])
        
        # -- filter out DNE
        archive_display = [ a for a in archive_display if a['location'] != Location.DOES_NOT_EXIST ]
        
        archive_list_filter = {
            #'location': Location.LOCAL_ONLY
        }

        archive_list_sort = {
            'size_mb': {
                'fn': lambda a: float(a['size_mb']),
                'dir': 'desc'
            }
        }

        for f_key in archive_list_filter:
            archive_display = [ a for a in archive_display if a[f_key] == archive_list_filter[f_key] ]
        
        for s_key in archive_list_sort:
            archive_display = sorted(archive_display, key=archive_list_sort[s_key]['fn'], reverse=archive_list_sort[s_key]['dir'] == 'desc')
        
        table = []
        header = []
        
        if len(archive_display) == 0:
            self.logger.warning(f'No archives found for target.')
        else:
            # -- when not showing for a specific target, include target name
            if not target_name:
                table = [ [ archive["id"], archive["target_name"], archive["filename"], archive["created_at"], archive["size_mb"], archive["location"], archive["s3_cost_per_month"] ] for archive in archive_display ]
                header = ['id','target_name', 'filename','created_at','size_mb','location','$/month']
            # -- when showing for a specific target, no need for target name 
            else:
                table = [ [ archive["id"], archive["filename"], archive["created_at"], archive["size_mb"], archive["location"], archive["s3_cost_per_month"] ] for archive in archive_display ]
                header = ['id','filename','created_at','size_mb','location','$/month']
        
        return table, header 

    def print_last_archive(self, target_name=None):
        '''Prints the last archive created, filtered by target name if provided'''
        
        table, header = self._get_archives_for_target(target_name=target_name)
        rows = [ dict(zip(header, row)) for row in table ]
        for row in rows:
            if row['location'] != Location.DOES_NOT_EXIST:
                logger.text(row['filename'])
                break 

    def print_archives(self, target_name=None):
        '''Prints all archives, filtered by target name if provided'''

        table, header = self._get_archives_for_target(target_name)

        c = Columnizer(logger=self.logger, cell_padding=5, header_color='white', row_color='orange', **flag_args)
        c.print(table, header, data=True)

    def prune_archives(self, target_name=None):

        target = None 
        if target_name:
            target = self.db.get_target(name=target_name)

        self.cleanup_local_archives(target=target, aggressive=False, dry_run=self.dry_run)
    
    def prune_archives_aggressively(self, target_name=None):

        target = None 
        if target_name:
            target = self.db.get_target(name=target_name)

        self.cleanup_local_archives(target=target, aggressive=True, dry_run=self.dry_run)

    def run(self, target_name=None):
        '''
        Executes the full backup workflow for all targets.
        If TARGET_NAME provided, executes the full backup workflow only for that target.
        1. pull all targets
        2. for each:
            a. check schedule against last run time and proceed 
            b. check existence of new files and proceed 
            c. generate a new archive 
            d. check status of S3 objects and push latest if not pushed 
        '''

        start = datetime.now()

        self.logger.info(f'\nBackup run: {datetime.strftime(start, "%c")}')

        results = Results()

        for target, remote_stats in self.targets(target_name):
            
            try:
                
                is_scheduled = self.target_is_scheduled(target)
                if target['is_active'] and is_scheduled:                    
                    self.add_archive(target["name"], results)
                else:
                    self.logger.warning(f'Not running {target["name"]} (scheduled={is_scheduled}, active={target["is_active"]})')
                    if not target["is_active"]:
                        results.log('not_active')
                    if not is_scheduled:
                        results.log('not_scheduled')
            except:
                self.logger.exception()

            try:
                '''
                push frequency is
                    - by the budget (budget priority)
                        - allow some margin on the budget depending on how soon age-outs will occur
                    - by the calendar (schedule priority)
                        - may still set a max budget with either a "do not exceed" or "warn if exceeded" flag
                    - by any new content (content priority)
                        - i.e. any new archive is get pushed 
                        - allow some threshold required number of new files to consider a new archive for pushing
                in the case of budget or schedule priority, if no new archive at the time of calculated push time, the next new archive is pushed regardless and the next period is based from there
                
                '''
                self.push_target_latest(target['name'])
            except:
                self.logger.exception()
            
            # -- check S3 status (regardless of schedule)
            # -- check target budget (calculate )
            # -- clean up S3 / push latest archive if not pushed 
            # -- update archive push status/time
            
        
        end = datetime.now()

        results.print()

        self.logger.info(f'\n\nBackup run completed: {datetime.strftime(end, "%c")}\n')

def parse_flags():
    ''' Separate sys.argv into flags updates, actually make those updates and return everything else '''

    named_parameters = {}
    positional_parameters = []
    skip_next = False 
    flag_args = {**FLAGS}
    
    for i, arg in enumerate(sys.argv[1:], 1):
        
        if skip_next:
            skip_next = False 
            continue 

        if arg in FLAGS_OPTIONS:
            flag_args.update(FLAGS_OPTIONS[arg])
        elif arg in NAMED_PARAMETER_OPTIONS:
            named_parameter_name = NAMED_PARAMETER_OPTIONS[arg]
            new_parameter = {named_parameter_name: sys.argv[i+1]}
            skip_next = True
            # -- a named parameter will first try to set FLAGS with whatever is passed 
            if named_parameter_name in flag_args.keys():
                flag_args.update(new_parameter)
            else:
                # -- but if no flag matches, we get a named parameter
                named_parameters.update(new_parameter)            
        else:
            positional_parameters.append(arg)
    
    return flag_args, positional_parameters, named_parameters

def solicit():
    if NO_SOLICIT:
        return 
    import random 
    sol_chance_low = 1
    sol_chance_high = 5
    sol_chance_val = random.randint(sol_chance_low, sol_chance_high)
    if sol_chance_val == 1:        
        logger.error(f'WOW! This is annoying!')
        time.sleep(.5)
        logger.error('but if you are enjoying this backup solution')
        time.sleep(.5)
        logger.error('and you find it reduces your anxiety')
        time.sleep(.5)
        logger.error('all while being ridiculously easy to use')
        time.sleep(.5)
        logger.info('please consider throwing me a few bucks on PayPal @ timpalko79@yahoo.com')
        time.sleep(.5)
        logger.info('that\'s.. timpalko79@yahoo.com!')
        time.sleep(2)
        logger.warning('(You can turn this off')
        logger.warning('by adding no_solicit = true in your FRANKBACK_RC_FILE')
        logger.warning('or by setting env FRANKBACK_NO_SOLICIT=1)')


if __name__ == "__main__":
    
    flag_args, positional_parameters, named_parameters = parse_flags()
        
    # logger = FrankLogger(**flag_args)

    if not S3_BUCKET or not DATABASE_FILE or not WORKING_FOLDER:
        logger.error(f'S3_BUCKET, DATABASE_FILE and WORKING_FOLDER must all be provided')
    
    solicit()

    b = Backup(bucket_name=S3_BUCKET, logger=logger, user_logger=user_logger, **flag_args)
    b.command(positional_parameters, named_parameters)
    
