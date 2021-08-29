#!/usr/bin/env python3

from datetime import datetime #, timedelta
from enum import Enum 
from contextlib import contextmanager
import base64
import json
#from json import JSONEncoder
import os 
import shutil
#from statistics import stdev, mean
import sys
import traceback 
#import PyGnuplot
import math
import hashlib
import sqlite3
import boto3 
#from botocore.config import Config 
from pytz import timezone 
import subprocess 

UTC = timezone('UTC')
now = UTC.localize(datetime.now())

REMOTE_STORAGE_COST_GB_PER_MONTH = 0.00099
DATABASE_FILE = 'backups.db'
WORKING_FOLDER = '/media/storage/tmp'
MARKER_PLACEHOLDER_TEXT = f'this is a backup timestamp marker. its existence is under the control of {os.path.realpath(__file__)}'

class Location(Enum):
    LOCAL_AND_REMOTE = 'local_and_remote'
    LOCAL_ONLY = 'local_only'
    REMOTE_ONLY = 'remote_only'
    DOES_NOT_EXIST = 'does_not_exist'

class Strategy(Enum):
    BUDGET_PRIORITY = 'budget_priority'
    SCHEDULE_PRIORITY = 'schedule_priority'
    CONTENT_PRIORITY = 'content_priority'

######################
#
# display 

FOREGROUND_COLOR_PREFIX = '\033[38;2;'
FOREGROUND_COLOR_SUFFIX = 'm'
FOREGROUND_COLOR_RESET = '\033[0m'

COLOR_TABLE = {
    'white': '255;255;255',
    'red': '255;0;0',
    'green': '0;255;0',
    'orange': '255;165;0'
}

def colorwrapper(text, color):
    return f'{FOREGROUND_COLOR_PREFIX}{COLOR_TABLE[color]}{FOREGROUND_COLOR_SUFFIX}{text}{FOREGROUND_COLOR_RESET}'

def printwhite(text):
    print(colorwrapper(text, 'white'))

def printred(text):
    print(colorwrapper(text, 'red'))

def printgreen(text):
    print(colorwrapper(text, 'green'))

def printorange(text):
    print(colorwrapper(text, 'orange'))

def human(value):
    return_val = value 
    units = [ 'kb', 'mb', 'gb', 'tb', 'pb' ]
    unit_index = 0
    if type(value).__name__ == 'int' or value.isnumeric():
        while value >= 1024:
            value = value / 1024.0
            unit_index += 1
        return_val = "%.1f %s" % (value, units[unit_index])
    
    return return_val

def column(table):

    row_lengths = set([ len(table[i]) for i in range(len(table)) ])
    if len(row_lengths) > 1:
        # -- not every row has the same number of columns
        pass 
    
    val_widths = []
    col_max_widths = {}
    for rix, row in enumerate(table):
        for cix, col in table[rix]:
            val_widths[rix][cix] = len(table[rix][cix])
    col_max_widths = [ max(r) for r in val_widths ]

    for c, max_width in enumerate(col_max_widths):
        while max_width % 4 != 0:
            max_width += 1
        for row in table:
            printval = row[c]
            while len(printval) < max_width:
                printval += '\t'


#####################
#
# database 

class Database(object):

    conn = None 
     
    def __init__(self, *args, **kwargs):
      pass 
        #self.init_db()
    
    def parse_type(self, column_name, value):
        if value is not None:
            if column_name[-3:] == '_at':
                return datetime.strptime(value, "%Y-%m-%d %H:%M:%S.%f")
            elif column_name[0:3] == 'is_':
                return bool(value)
        return value 

    def dict_factory(self, cursor, row):
        return { col[0]: self.parse_type(col[0], row[idx]) for idx,col in enumerate(cursor.description) }
        '''
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d
        '''
        
    @contextmanager 
    def cursor(self):

        self.conn = sqlite3.connect(DATABASE_FILE)
        self.conn.row_factory = self.dict_factory
        c = self.conn.cursor()
        try:
            yield c
        except:
            printred(sys.exc_info()[0])
            printred(sys.exc_info()[1])
            traceback.print_tb(sys.exc_info()[2])
        finally:
            self.conn.close()

    def init_db(self, tables):

        with self.cursor() as c:
            for table in tables:
                try:
                    c.execute(f'select sql from sqlite_master where name = ?', (table,))
                    sql = c.fetchone()['sql']
                    printgreen(f'Captured {table} schema')
                    schema_in_code = f'CREATE TABLE {table} {tables[table]}'
                    if sql != schema_in_code:
                        printred(f'WARNING: {table} schema in database does not match schema in code')
                        printred(f'Database:\t{sql}')
                        printred(f'Code:\t\t{schema_in_code}')
                    else:
                        printgreen(f'Table schema OK')
                except sqlite3.OperationalError as oe:
                    printred(f'Failed to read from table {table}')
                    printred(oe)
                    printred(f'Creating table {table}..')
                    # c = self.conn.cursor()
                    c.execute(f'CREATE TABLE {table} {tables[table]}')
                    self.conn.commit()
                except:
                    printred(f'Something else failed testing table {table}')
                    printred(sys.exc_info()[0])  
                    printred(sys.exc_info()[1])  
                    traceback.print_tb(sys.exc_info()[2])

def initialize_database():
    
    _TABLES = {
        'archives': '(id integer primary key autoincrement, target_id int, created_at datetime, size_kb int, is_remote bool, remote_push_at datetime, filename char(255), returncode int, errors text, pre_marker_timestamp datetime, md5 char(32))',
        'targets': '(id integer primary key autoincrement, path text, name char(255), excludes text, budget_max float, schedule char(32), push_strategy char(32), push_period int)',
        'runs': '(id integer primary key autoincrement, start_at datetime, end_at datetime, run_stats_json text)'
    }

    db = Database()
    db.init_db(_TABLES)

ARCHIVE_TARGET_JOIN_SELECT = 'a.id, a.target_id, a.created_at, a.size_kb, a.is_remote, a.remote_push_at, a.filename, a.returncode, a.errors, a.pre_marker_timestamp, a.md5, t.name, t.path'
ARCHIVE_TARGET_JOIN = 'from archives a inner join targets t on t.id = a.target_id'

def get_archive_for_pre_timestamp(target_id, timestamp):
    db = Database()
    line = None 
    with db.cursor() as c:
        c.execute(f'select {ARCHIVE_TARGET_JOIN_SELECT} {ARCHIVE_TARGET_JOIN} where a.target_id = ? and a.pre_marker_timestamp = ? order by created_at desc limit 1', (target_id, timestamp))
        line = c.fetchone()
    return line

def get_last_archive(target_id):
    db = Database()
    line = None 
    with db.cursor() as c:
        c.execute(f'select {ARCHIVE_TARGET_JOIN_SELECT} {ARCHIVE_TARGET_JOIN} where a.target_id = ? order by created_at desc limit 1', (target_id,))
        line = c.fetchone()
    return line

def get_archives(target_name=None):

    target = None 
    if target_name:
        target = get_target(target_name)

    db_records = []
    db = Database()
    with db.cursor() as c:
        if target:
            c.execute(f'select {ARCHIVE_TARGET_JOIN_SELECT} {ARCHIVE_TARGET_JOIN} where a.target_id = ? order by created_at desc', (target['id'],))
        else:
            c.execute(f'select {ARCHIVE_TARGET_JOIN_SELECT} {ARCHIVE_TARGET_JOIN} order by created_at desc')
        db_records = c.fetchall()
    
    return db_records

def delete_archive(archive_id):

    db = Database()    
    with db.cursor() as c:        
        c.execute('delete from archives where id = ?', (archive_id,))
        db.conn.commit()
        printgreen(f'Archive {archive_id} deleted')

def create_archive(target_id, size_kb, filename, returncode, errors, pre_marker_timestamp):

    new_archive_id = None 

    cp = subprocess.run("md5sum %s | awk '{ print $1 }'" % filename, text=True, shell=True, capture_output=True)
    digest = str([ line for line in cp.stdout.splitlines() if line ][0])

    # with open(filename, 'rb') as f:
    #     contents = f.read()
    #     digest = hashlib.md5(contents).hexdigest()

    db = Database()    
    with db.cursor() as c:        
        params = (target_id, datetime.now(), size_kb, False, None, filename, returncode, errors, pre_marker_timestamp, digest,)
        c.execute('insert into archives (target_id, created_at, size_kb, is_remote, remote_push_at, filename, returncode, errors, pre_marker_timestamp, md5) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', params)
        db.conn.commit()
        new_archive_id = c.lastrowid
        printgreen(f'Archive {new_archive_id} added')
    
    return new_archive_id

def get_targets():
    db = Database()
    all_targets = None 
    with db.cursor() as c:
        c.execute('select * from targets')
        all_targets = c.fetchall()
    return all_targets        

def get_target(name):
    db = Database()
    line = None 
    with db.cursor() as c:
        c.execute('select id, path, name, excludes, budget_max, schedule from targets where name = ?', (name,))
        line = c.fetchone()
    return line

def create_target(path, name, schedule, excludes=None):
    existing_target = get_target(name)
    if not existing_target:
        db = Database()
        with db.cursor() as c:        
            c.execute('insert into targets (path, name, excludes, schedule) values(?, ?, ?, ?)', (path, name, excludes, schedule,))
            db.conn.commit()
            printgreen(f'Target {name} added')
    else:
        printorange(f'Target {name} already exists')

def set_archive_remote(archive):

    db = Database()
    with db.cursor() as c:
        c.execute('update archives set is_remote = 1, remote_push_at = ? where id = ?', (datetime.now(), archive['id'],))
        db.conn.commit()
        printgreen(f'Archive {archive["id"]} set as remote')

def target_is_scheduled(target):
    schedule = target['schedule']
    last_archive = get_last_archive(target['id'])
    is_scheduled = False 
    if last_archive:
        since_minutes = (datetime.now() - last_archive['created_at']).total_seconds() / 60
        is_scheduled = since_minutes >= int(schedule) if schedule.isnumeric() else False 
    else:
        is_scheduled = True 
    return is_scheduled

def get_archive_stats(target_name):
    
    pass 

def print_targets():
    # -- target name, path, budget max, schedule, total archive count, % archives remote, last archive date/days, next archive date/days
    targets = get_targets()

    printwhite(f'name\tpath\tbudget_max\tschedule\tarchive_count')
    for target in targets:
        archives = get_archives(target["name"])
        printorange(f'{target["name"]}\t{target["path"]}\t{target["budget_max"]}\t{target["schedule"]}\t{len(archives)}')

def get_archive_location(archive, remote_file_map):
    basename = os.path.basename(archive['filename'])
    local_file_exists = os.path.exists(archive['filename'])
    remote_file_exists = basename in remote_file_map
    
    if local_file_exists and remote_file_exists:
        return Location.LOCAL_AND_REMOTE
    elif local_file_exists:
        return Location.LOCAL_ONLY
    elif remote_file_exists:
        return Location.REMOTE_ONLY
    return Location.DOES_NOT_EXIST

def print_archives(target_name=None, print_headers=True):

    db_records = get_archives(target_name)

    s3_objects = []
    with archivebucket() as bucket:
        # TODO: improve the matching here 
        s3_objects = [ obj for obj in bucket.objects.all() if (target_name and obj.key.find(f'{target_name}_') == 0) or not target_name ]
    s3_objects_by_filename = { obj.key: obj for obj in s3_objects }    

    archive_display = []

    for db_record in db_records:

        basename = os.path.basename(db_record['filename'])

        db_record['location'] = get_archive_location(db_record, s3_objects_by_filename)    

        db_record['s3_cost_per_month'] = "%.2f" % 0.00
        if basename in s3_objects_by_filename:
            db_record['s3_cost_per_month'] = "%.2f" % (REMOTE_STORAGE_COST_GB_PER_MONTH*(s3_objects_by_filename[basename].size / (1024.0*1024.0*1024.0)))

        db_record['size_mb'] = "%.1f" % (db_record['size_kb'] / 1024.0)
        
        archive_display.append(db_record)
        if basename in s3_objects_by_filename:
            del s3_objects_by_filename[basename]
    
    for orphaned_s3_object_filename in s3_objects_by_filename:
        obj = s3_objects_by_filename[orphaned_s3_object_filename]

        archive_display.append({ 'id': None, 'filename': obj.key, 'created_at': obj.last_modified, 'size_mb': "%.1f" % (obj.size/(1024.0*1024.0)), 'location': 'remote only', 's3_cost_per_month': "%.2f" % (REMOTE_STORAGE_COST_GB_PER_MONTH*(obj.size / (1024.0*1024.0*1024.0))) })
    
    #total_cost = sum([ float(a['s3_cost_per_month'])  for a in archive_display ])
    
    if print_headers:
        if len(archive_display) > 0:
            printwhite(f'id\tfilename\t\t\t\t\tcreated_at\t\t\tsize_mb\tlocation\t$/month')
        else:
            printorange(f'No archives found for target.')

    for archive in archive_display:
        # id, filename, created_at, size_mb, is_remote, location
        printorange(f'{archive["id"]}\t{archive["filename"]}\t{archive["created_at"]}\t{archive["size_mb"]}\t\t{archive["location"]}\t\t\t{archive["s3_cost_per_month"]}')


#################
#
# aws

@contextmanager
def archivebucket():
    s3 = boto3.resource('s3')
    archive_bucket = s3.Bucket('frankenarchive')
    yield archive_bucket

def is_push_due(target):
    
    archives = get_archives(target['name'])
    push_due = False 
    message = 'No calculation was performed to determine push eligibility. The default is no.'

    objs = get_remote_archives(target['name'])
    objs_by_last_modified = { obj.last_modified: obj for obj in objs }
    last_modified = max(objs_by_last_modified.keys())
    now = UTC.localize(datetime.utcnow())
    since_last_remote_object = now - last_modified
    minutes_since_last_object = (since_last_remote_object.total_seconds()*1.0) / 60
    
    if target['push_strategy'] == Strategy.BUDGET_PRIORITY:

        average_size = 0
        max_s3_objects = 0
        if len(archives) > 0:
            average_size = sum([ a['size_kb'] / (1024.0*1024.0) for a in archives ]) / len(archives)
        else:
            average_size = get_target_uncompressed_size_kb(target) / (1024.0*1024.0)
        lifetime_cost = average_size * REMOTE_STORAGE_COST_GB_PER_MONTH * 6
        max_s3_objects = math.floor(target['budget_max'] / lifetime_cost)
        minutes_per_push = (180.0*24*60) / max_s3_objects

        push_due = minutes_since_last_object > minutes_per_push
        message = f'Given a calculated size of {average_size} GB and a budget of ${target["budget_max"]}, a push can be accepted every {minutes_per_push} minutes and it has been {minutes_since_last_object} minutes'
    
    elif target['push_strategy'] == Strategy.SCHEDULE_PRIORITY:
        
        push_due = minutes_since_last_object > target['push_period']
        message = f'The push period is {target["push_period"]} minutes and it has been {minutes_since_last_object} minutes'

    if push_due:
        printwhite(message)
    else:
        printorange(message)

    return push_due

    #   bucket_name
    #   copy_from
    #   delete
    #   e_tag
    #   get
    #   get_available_subresources
    #   initiate_multipart_upload
    #   key
    #   last_modified
    #   load
    #   meta
    #   owner
    #   put
    #   restore_object
    #   size
    #   storage_class
    #   wait_until_exists
    #   wait_until_not_exists'

def get_archive_bytes(filename):
    b = None 
    with open(filename, 'rb') as f:
        b = f.read()
    return b

def push_archive_to_bucket(archive):
    # print(archive)
    object = None 
    with archivebucket() as bucket:
        b64_md5 = base64.b64encode(bytes(archive['md5'], 'utf-8')).decode()
        # printwhite(f'{b64_md5}')
        object = bucket.put_object(
            Body=get_archive_bytes(archive['filename']),
            #ContentMD5=b64_md5,
            Key=os.path.basename(archive['filename'])
        )
    return object

def get_remote_archives(target_name):
        
    S3_BACKUP_BUCKET = 'frankenback'
    S3_ARCHIVE_BUCKET = 'frankenarchive'
    WORKING_FOLDER = 'tmp'
    SKIP_COPY = 0
    SKIP_ARCHIVE = 0

    # my_config = Config(
    #   region_name = 'us-east-1',
    #   signature_version = 's3v4',
    #   retries = {
    #     'max_attempts': 10,
    #     'mode': 'standard'
    #   }
    # )

    # client = boto3.client('s3', config=my_config)
    # buckets_response = client.list_buckets()
    # buckets = [ bucket['Name'] for bucket in buckets_response['Buckets'] ]
    # print(f'buckets: {buckets}')
    # print(f'frankenarchive objects (via client): {client.list_objects(Bucket="frankenarchive")}')

    objects = []
    #print(dir(archive_bucket.objects))
    #print(dir(archive_bucket.objects.all()))
    with archivebucket() as bucket:
        objects = [ obj for obj in bucket.objects.all() if obj.key.find(f'{target_name}_') == 0 ]
    return objects 

def cleanup_remote_archives(dry_run=True):
    now = UTC.localize(datetime.utcnow())
    aged_out = []
    for obj in s3_objects:
        print(f'subtracting {datetime.strftime(now, "%Y-%m-%d %H:%M:%S")} {now.tzinfo} and {datetime.strftime(obj.last_modified, "%Y-%m-%d %H:%M:%S")} {obj.last_modified.tzinfo}')
        days_old = (now - obj.last_modified).total_seconds() / (60*60*24)
        if days_old >= 180:
            aged_out.append(obj.key)

#################
#
# filesystem 

def get_working_folder_free_space():
    cp = subprocess.run("df -k %s | grep -v Used | awk '{ print $4 }'" % WORKING_FOLDER, shell=True, text=True, capture_output=True)
    return int(cp.stdout.replace('\n', ''))

def get_target_uncompressed_size_kb(target):

    cp = subprocess.run("du -kd 0 %s | awk '{ print $1 }'" % target['path'], shell=True, text=True, capture_output=True)
    return int(cp.stdout.replace('\n', ''))

def marker_path(target, place):
    return os.path.join(os.path.realpath(os.path.join(target['path'], '..')), f'{target["name"]}_{place}_backup_marker')

def recreate_marker_file(marker_path, timestamp=None):
    if not os.path.exists(marker_path):
        with open(marker_path, 'w') as f:
            f.write(MARKER_PLACEHOLDER_TEXT)

    if timestamp:
        subprocess.run(f'touch {marker_path} -t {datetime.strftime(timestamp, "%Y%m%d%H%M.%S")}'.split(' '))
    else:
        subprocess.run(f'touch {marker_path}'.split(' '))

def update_markers(target, pre_marker_timestamp):
    # TODO: moving pre/post should be atomic
    pre_marker = marker_path(target, 'pre')
    recreate_marker_file(pre_marker, pre_marker_timestamp)
    post_marker = marker_path(target, 'post')
    recreate_marker_file(post_marker)

def target_has_new_files(target):

    pre_marker = marker_path(target, 'pre')
    has_new_files = False 

    if os.path.exists(pre_marker):
        try:
            pre_marker_stat = shutil.os.stat(pre_marker)
            pre_marker_date = datetime.fromtimestamp(pre_marker_stat.st_mtime)
            pre_marker_stamp = datetime.strftime(pre_marker_date, "%c")

            # -- verify an archive actually exists corresponding to this pre-marker file
            marker_archive = get_archive_for_pre_timestamp(target['id'], pre_marker_date)
            if not marker_archive:
                printorange(f'No archive exists corresponding to the existing pre-marker. This marker is invalid, and all files are considered new.')
                has_new_files = True 
            else:
                
                cp = subprocess.run(f'find {target["path"]} -newer {pre_marker}'.split(' '), check=True, capture_output=True)
                new_file_count = len(cp.stdout.splitlines())
                has_new_files = new_file_count > 0
                printwhite(f'{new_file_count} new files found since {pre_marker_stamp}')
        except subprocess.CalledProcessError as cpe:
            printred(cpe.stderr)   
    else:
        has_new_files = True 
        printwhite(f'No marker file found, all files considered new')
    
    return has_new_files

def cleanup_local_archives(dry_run=True):
    '''
    assertive
        - if remote and local delete local copy
        - delete all but latest local copies
    aggressive
        no regard for local presence, required remote retrieval popsicle
        leaves only the latest archive for a target, separately accounting local and remote
    '''

def add_archive(target, results):

    none_response = (None, None, None, None, )

    if not target_has_new_files(target):
        printorange(f'No new files. Skipping archive creation.')
        results.log('no_new_files')
        return none_response

    target_size = get_target_uncompressed_size_kb(target)
    free_space = get_working_folder_free_space()

    if target_size > free_space:


        printred(f'This target may be too big ({target_size} kb) for the available space on the working folder filesystem ({free_space} kb). Please free up {human(target_size - free_space)} and reschedule this target as soon as possible.')
        results.log('insufficient_space')
        return none_response
        
    try:

        target_file = f'{WORKING_FOLDER}/{target["name"]}_{datetime.strftime(datetime.now(), "%Y%m%d_%H%M%S")}.tar.gz'

        excludes = ""
        if target["excludes"] and len(target["excludes"]) > 0:
            excludes = f'--exclude {" --exclude ".join(target["excludes"].split(":"))}'

        archive_command = f'tar {excludes} -czf {target_file} {target["path"]}'
        printwhite(f'Running archive command: {archive_command}')

        # -- strip off microseconds as this is lost when creating the marker file and will prevent the assocation with the archive record
        pre_timestamp = datetime.strptime(datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")

        cp = subprocess.run(archive_command.split(' '), capture_output=True)
        printorange(cp.args)
        returncode = cp.returncode
        printorange(f'Archive returncode: {cp.returncode}')
        printorange(cp.stdout)
        printred(cp.stderr)
        archive_errors = str(cp.stderr)

        cp = subprocess.run(f'tar --test-label -f {target_file}'.split(' '), capture_output=True)
        printorange(cp.args)
        printorange(f'Archive test returncode: {cp.returncode}')
        printred(cp.stderr)
        
        cp.check_returncode()

        if archive_errors.find("No space left on device") >= 0:
            report = "Insufficient space while archiving. Archive target file (assumed partial) will be deleted. Please clean up the disk and reschedule this target as soon as possible."
            if os.path.exists(target_file):
                os.unlink(target_file)
            results.log('insufficient_space')
            raise Exception(report)
        
        return target_file, returncode, archive_errors, pre_timestamp

    except subprocess.CalledProcessError as cpe:
        printred(f'Archive process failed')
        printred(cpe.stderr)
    
    except OSError as ose:
        printred(sys.exc_info()[1])
        traceback.print_tb(sys.exc_info()[2])
    
    return none_response


#######################
#
# operation

class Results(object):
    _results = None 
    def __init__(self, *args, **kwargs):
        self._results = {
            'archive_created': 0,
            'insufficient_space': 0,
            'not_scheduled': 0,
            'no_new_files': 0,
            'archive_failed': 0,
            'other_failure': 0
        }
    def log(self, reason):
        self._results[reason] += 1
    def print(self):
        print(json.dumps(self._results, indent=4))

def run(requested_target=None):
    '''
    1. pull all targets
    2. for each:
        a. check schedule against last run time and proceed 
        b. check existence of new files and proceed 
        c. generate a new archive 
        d. check status of S3 objects and push latest if not pushed 
    '''

    start = datetime.now()

    printwhite(f'\nBackup run: {datetime.strftime(start, "%c")}')

    targets = []

    if requested_target:
        target = get_target(name=requested_target)
        if target:
            targets.append(target)
    else:
        targets = get_targets()

    printwhite(f'{len(targets)} targets identified')

    results = Results()

    for target in targets:
        archive_file = None 
        new_archive_id = None 
        try:
            printwhite(f'\n*************************************************\n')
            printwhite(f'Backup target: {target["name"]} ({target["path"]})')
            if target_is_scheduled(target):
                printwhite(f'Creating archive for {target["name"]}')
                (archive_file, returncode, errors, pre_marker_timestamp) = add_archive(target, results)
                if archive_file:
                    printgreen(f'Archive created: {archive_file}')
                    archive_file_stat = shutil.os.stat(archive_file)
                    new_archive_id = create_archive(target_id=target['id'], size_kb=archive_file_stat.st_size/1024.0, filename=archive_file, returncode=returncode, errors=errors, pre_marker_timestamp=pre_marker_timestamp)
                    update_markers(target, pre_marker_timestamp)
                    results.log('archive_created')
                else:
                    printorange(f'No archive created')
                    results.log('other_failure')
            else:
                printorange(f'{target["name"]} is not scheduled')
                results.log('not_scheduled')
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
            if new_archive_id:
                delete_archive(new_archive_id)
            if archive_file:
                os.unlink(archive_file)

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
            last_archive = get_last_archive(target['id'])
            if last_archive and not last_archive['is_remote']:
                printorange(f'Last archive is not pushed remotely')
                if is_push_due(target):
                    obj = push_archive_to_bucket(last_archive)
                    if obj:
                        printgreen(f'Last archive has been pushed remotely')                        
                        set_archive_remote(last_archive)
                    else:
                        printred(f'The last archive failed to push remotely')
            elif not last_archive:
                printorange(f'No last archive is available for this target')
            elif last_archive['is_remote']:
                printwhite(f'The last archive is already pushed remotely')
        except:
            print(sys.exc_info()[1])
            traceback.print_tb(sys.exc_info()[2])
        
        # -- check S3 status (regardless of schedule)
        # -- check target budget (calculate )
        # -- clean up S3 / push latest archive if not pushed 
        # -- update archive push status/time
        
    
    end = datetime.now()

    results.print()

    printwhite(f'\n\nBackup run completed: {datetime.strftime(end, "%c")}\n')



if __name__ == "__main__":

    if sys.argv[1] == 'db':
        if sys.argv[2] == 'init':
            initialize_database()
        elif sys.argv[2] == 'targets':
            if sys.argv[3] == 'list':
                all_targets = get_targets()
                for target in all_targets:
                    print(target)
            elif sys.argv[3] == 'add':
                print(len(sys.argv))
                create_target(*sys.argv[4:])
    elif sys.argv[1] == 's3':
        if sys.argv[2] == 'archives':

            target_name = sys.argv[4]
            target = get_target(target_name)
            
            if sys.argv[3] == 'list':
                list_remote_archives(target_name)
            elif sys.argv[3] == 'add':
            
                (target_file, returncode, errors, pre_marker_timestamp) = add_archive(target)
            
                if target_file:
                    print(f'Created {target_name} archive: {target_file}')
                else:
                    print(f'No {target_name} archive created')
    elif sys.argv[1] == 'get':
        if sys.argv[2] == 'archives':
            target_name = None 
            if len(sys.argv) > 3:
                target_name = sys.argv[3]
            print_archives(target_name)
        elif sys.argv[2] == 'targets':
            print_targets()
        elif sys.argv[2] == 's3info':
            target_name = None 
            if len(sys.argv) > 3:
                target_name = sys.argv[3]
                target = get_target(target_name)
                if not target:
                    print(f'target {target_name} not found')
                    exit(1)
            archives = get_archives(target_name)
            average_size = 0
            max_s3_objects = 0
            if len(archives) > 0:
                average_size = sum([ a['size_kb'] / (1024.0*1024.0) for a in archives ]) / len(archives)
            else:
                average_size = get_target_uncompressed_size_kb(target) / (1024.0*1024.0)
            lifetime_cost = average_size * REMOTE_STORAGE_COST_GB_PER_MONTH * 6
            max_s3_objects = math.floor(target['budget_max'] / lifetime_cost)
            print(f'{target["budget_max"]} will support {max_s3_objects} {average_size} archives over 180 days')
            s3_objects = get_remote_archives(target_name)
            print(s3_objects)
    elif sys.argv[1] == 'run':
        target = None 
        if len(sys.argv) > 2:
            target = sys.argv[2]
        run(target)
