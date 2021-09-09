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
    'orange': '255;165;0',
    'gray': '192;192;192'
}

def colorwrapper(text, color):
    return f'{FOREGROUND_COLOR_PREFIX}{COLOR_TABLE[color]}{FOREGROUND_COLOR_SUFFIX}{text}{FOREGROUND_COLOR_RESET}'

def flushprint(text):
    print(text, flush=True)

def printwhite(text):
    flushprint(colorwrapper(text, 'white'))

def printred(text):
    flushprint(colorwrapper(text, 'red'))

def printgreen(text):
    flushprint(colorwrapper(text, 'green'))

def printorange(text):
    flushprint(colorwrapper(text, 'orange'))

def printgray(text):
    flushprint(colorwrapper(text, 'gray'))

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

class Columnizer(object):

    tabs = None 
    cell_padding = None 
    alignment = None 
    header_color = None 
    row_color = None 
    cell_padding_default = 5
    header_color_default = 'white'
    row_color_default = 'orange'

    def __init__(self, *args, **kwargs):
        for k in kwargs:
            self.__setattr__(k, kwargs[k])
        if not self.cell_padding:
            self.cell_padding = self.cell_padding_default
        if not self.header_color:
            self.header_color = self.header_color_default
        if not self.row_color:
            self.row_color = self.row_color_default

    def pad_tabs(self, data):

        if not self.tabs and len(data) > 0:
            self.tabs = [ 1 for c in data[0] ]

        for rix, row in enumerate(data):
            for cix, col in enumerate(data[rix][0:-1]):
                curr_tab = self.tabs[cix+1] - self.tabs[cix]
                cell_value = str(data[rix][cix])
                cell_width = len(cell_value)
                extra = cell_width + self.cell_padding - curr_tab
                self.tabs = [ m + extra if (i >= cix+1 and extra > 0) else m for i,m in enumerate(self.tabs) ]

    def align_spaces(self, value, cell_width, alignment):
        if alignment == 'r':
            return f'{"".join([ " " for i in range(cell_width - self.cell_padding - len(value)) ])}{value}'
        return value

    def align_table(self, data):
        return [ [ self.align_spaces(str(r), cell_width=self.tabs[i+1] - self.tabs[i], alignment=self.alignment[i]) if i < len(row) - 1 else str(r) for i,r in enumerate(row) ] for row in data ]

    def assemble_table_print(self, table, color):
        tabs_cmd = f'tabs {",".join([ str(c) for c in self.tabs ])}'
        tab_reset = "tabs -8"
        print_data = "\n\"; printf \"".join([ colorwrapper("\t".join([ str(v) for v in row ]), color) for row in table ])
        return "%s; printf \"%s\n\"; %s;" % (tabs_cmd, print_data, tab_reset)
        
    def print(self, table, header):
        if header:
            self.pad_tabs([header])
        self.pad_tabs(table)

        printout = ""
        if header:
            header = [header]
            if self.alignment:
                header = self.align_table(header)
            printout += self.assemble_table_print(header, self.header_color)
        
        if self.alignment:
            table = self.align_table(table)
        printout += self.assemble_table_print(table, self.row_color)

        subprocess.run(printout, shell=True)


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
TARGETS_SELECT = 't.id, t.path, t.name, t.excludes, t.budget_max, t.schedule, t.push_strategy, t.push_period'

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

def get_archives(target_name=None, with_location=False):

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
    
    if with_location:
        s3_objects = get_remote_archives(bucket_name)
        s3_objects_by_filename = { os.path.basename(obj.key): obj for obj in s3_objects }    
        for record in db_records:
            record['location'] = get_archive_location(record, s3_objects_by_filename)
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
        c.execute(f'select {TARGETS_SELECT} from targets t')
        all_targets = c.fetchall()
    return all_targets        

def get_target(name):
    db = Database()
    line = None 
    with db.cursor() as c:
        c.execute(f'select {TARGETS_SELECT} from targets t where t.name = ?', (name,))
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

    '''
    for each target:
        how many cycles behind is it?
        are there files not backed up?
        are there archives not pushed? (implies there are files not pushed)
        monthly cost in s3?
    '''

    # -- target name, path, budget max, schedule, total archive count, % archives remote, last archive date/days, next archive date/days
    targets = get_targets()
    archives = get_archives(with_location=True)
    remote_stats = get_remote_stats(targets)

    now = datetime.now()

    for target in targets:
        target['has_new_files'] = target_has_new_files(target, log=False)
        target_archives_by_created_at = { a['created_at']: a for a in archives if a['target_id'] == target['id'] }
        if len(target_archives_by_created_at) == 0:
            target['cycles_behind'] = -1
            target['last_archive_pushed'] = 'n/a'
            continue 
        last_archive_created_at = max(target_archives_by_created_at.keys())
        last_archive = target_archives_by_created_at[last_archive_created_at]
        target['cycles_behind'] = math.floor(int(target['schedule']) / ((now - last_archive['created_at']).total_seconds() / 60.0))        
        target['last_archive_pushed'] = last_archive['is_remote']
        
        push_due = is_push_due(target, remote_stats=remote_stats, print=False)
        target['would_push'] = push_due and (not target['last_archive_pushed'] or target['has_new_files'])

    archives_by_target_and_location = { t['id']: {'local': [], 'remote': []} for t in targets }
    for archive in archives:
        if archive['location'] in [ Location.LOCAL_AND_REMOTE, Location.LOCAL_ONLY ]:
            archives_by_target_and_location[archive['target_id']]['local'].append(archive)
        if archive['location'] in [ Location.LOCAL_AND_REMOTE, Location.REMOTE_ONLY ]:
            archives_by_target_and_location[archive['target_id']]['remote'].append(archive)

    header = ['name', 'path', 'schedule', 'push_strategy', 'push_period', 'budget_max', 'local_archives', 'remote_archives', 'cycles behind', 'new_files', 'would_push']
    table = [ [ 
            target['name'], 
            target['path'],
            target['schedule'], 
            target['push_strategy'], 
            target['push_period'], 
            target['budget_max'], 
            len(archives_by_target_and_location[target['id']]['local']), 
            len(archives_by_target_and_location[target['id']]['remote']), 
            target['cycles_behind'], 
            target['has_new_files'], 
            target['would_push']
        ] for target in targets 
    ]

    c = Columnizer()
    c.print(table, header)

def get_archive_location(archive, remote_file_map={}):
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

def get_object_storage_cost_per_month(obj):
    return REMOTE_STORAGE_COST_GB_PER_MONTH*(obj.size / (1024 ** 3))

def print_archives(target_name):

    db_records = get_archives(target_name)

    s3_objects = get_remote_archives(bucket_name, target_name)
    s3_objects_by_filename = { os.path.basename(obj.key): obj for obj in s3_objects }    

    archive_display = []

    for db_record in db_records:

        db_record['size_mb'] = "%.1f" % (db_record['size_kb'] / 1024.0)
        db_record['location'] = get_archive_location(db_record, s3_objects_by_filename)    

        basename = os.path.basename(db_record['filename'])

        db_record['s3_cost_per_month'] = "%.4f" % 0.00
        if basename in s3_objects_by_filename:
            db_record['s3_cost_per_month'] = "%.4f" % get_object_storage_cost_per_month(s3_objects_by_filename[basename])
        
        archive_display.append(db_record)

        if basename in s3_objects_by_filename:
            del s3_objects_by_filename[basename]
    
    for orphaned_s3_object_filename in s3_objects_by_filename:
        obj = s3_objects_by_filename[orphaned_s3_object_filename]

        archive_display.append({ 'id': None, 'filename': obj.key, 'created_at': obj.last_modified, 'size_mb': "%.1f" % (obj.size/(1024.0*1024.0)), 'location': Location.REMOTE_ONLY, 's3_cost_per_month': "%.4f" % get_object_storage_cost_per_month(obj) })
    
    #total_cost = sum([ float(a['s3_cost_per_month'])  for a in archive_display ])
    
    if len(archive_display) == 0:
        printorange(f'No archives found for target.')
    else:
        table = [ [ archive["id"], archive["filename"], archive["created_at"], archive["size_mb"], archive["location"], archive["s3_cost_per_month"] ] for archive in archive_display ]
        header = ['id','filename','created_at','size_mb','location','$/month']

        c = Columnizer(cell_padding=5, alignment=['l', 'r', 'l', 'r', 'l', 'r'], header_color='white', row_color='orange')
        c.print(table, header)


#################
#
# aws

@contextmanager
def archivebucket(bucket_name):
    s3 = boto3.resource('s3')
    archive_bucket = s3.Bucket(bucket_name)
    printorange(f'S3 bucket yield out')
    time_out = datetime.now()    
    yield archive_bucket    
    time_in = datetime.now()
    printorange(f'S3 bucket yield in')
    printorange(f'S3 bucket calculation time: {"%.1f" % (time_in - time_out).total_seconds()} seconds')

def is_push_due(target, remote_stats=None, print=True):
    
    archives = get_archives(target['name'])
    push_due = False 
    message = 'No calculation was performed to determine push eligibility. The default is no.'
    minutes_since_last_object = None 

    if not remote_stats:
        remote_stats = get_remote_stats([target])

    last_modified = remote_stats[target['name']]['max_last_modified']
    current_s3_objects = remote_stats[target['name']]['count']

    if last_modified:
        now = UTC.localize(datetime.utcnow())
        since_last_remote_object = now - last_modified
        minutes_since_last_object = (since_last_remote_object.total_seconds()*1.0) / 60
    else:
        push_due = True 
        message = 'No remote objects were found, this may be the first?'
    
    if minutes_since_last_object:

        if target['push_strategy'] == Strategy.BUDGET_PRIORITY.value:

            average_size = 0
            max_s3_objects = 0
            if len(archives) > 0:
                average_size = sum([ a['size_kb'] / (1024.0*1024.0) for a in archives ]) / len(archives)
            else:
                average_size = get_target_uncompressed_size_kb(target) / (1024.0*1024.0)
            lifetime_cost = average_size * REMOTE_STORAGE_COST_GB_PER_MONTH * 6
            max_s3_objects = math.floor(target['budget_max'] / lifetime_cost)
            if max_s3_objects == 0:
                message = f'One archive has a lifetime cost of {lifetime_cost}. At a max budget of {target["budget_max"]}, no archives can be stored in S3'
            else:
                minutes_per_push = (180.0*24*60) / max_s3_objects
                push_due = current_s3_objects < max_s3_objects and minutes_since_last_object > minutes_per_push
                message = f'Given a calculated size of {average_size} GB and a budget of ${target["budget_max"]}, a push can be accepted every {minutes_per_push} minutes for max {max_s3_objects} objects. It has been {minutes_since_last_object} minutes and there are {current_s3_objects} objects.'
        
        elif target['push_strategy'] == Strategy.SCHEDULE_PRIORITY.value:
            
            push_due = minutes_since_last_object > target['push_period']
            message = f'The push period is {target["push_period"]} minutes and it has been {minutes_since_last_object} minutes'
        
        else:
            message = f'No identifiable push strategy ({target["push_strategy"]}) has been defined for {target["name"]}.'

    if print:
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

def push_archive_to_bucket(archive, bucket_name):
    printgreen(f'Pushing {archive["filename"]} ({human(archive["size_kb"], "kb")})')
    object = None 
    with archivebucket(bucket_name) as bucket:
        b64_md5 = base64.b64encode(bytes(archive['md5'], 'utf-8')).decode()
        # printwhite(f'{b64_md5}')
        
        method = 'upload_file'
        #method = 'put_object'

        key = f'{archive["name"]}/{os.path.basename(archive["filename"])}'
        if method == 'upload_file':
            from boto3.s3.transfer import TransferConfig
            uploadconfig = TransferConfig(multipart_threshold=4*1024*1024*1024)
            object = bucket.upload_file(archive['filename'], key, Config=uploadconfig)
        elif method == 'put_object':
            object = bucket.put_object(
                Body=get_archive_bytes(archive['filename']),
                #ContentLength=int(archive['size_kb']*1024),
                #ContentMD5=b64_md5,
                Key=key
            )
    return object

def delete_objects(objs, bucket_name):
    with archivebucket(bucket_name) as bucket:
        delete_resp = bucket.delete_objects(
            Delete={
                'Objects': [ { 'Key': obj.key } for obj in objs ],
                'Quiet': False
            }
        )
        if len(delete_resp['Errors']) > 0:
            printred(f'Delete errors: {",".join([ "%s: %s" % (o["Key"], o["Code"], o["Message"]) for o in delete_resp["Errors"] ])}')
        else:
            printgreen(f'Delete confirmed: {",".join([ o["Key"] for o in delete_resp["Deleted"] ])}')

def object_is_target(obj, target_name):
    return (obj.key.find(f'{target_name}/{target_name}_') == 0 or obj.key.find(f'{target_name}_') == 0)

def get_remote_stats(targets):
    s3_objects = get_remote_archives(bucket_name)
    remote_stats = {}
    for target in targets:        
        archives_by_last_modified = { obj.last_modified: obj for obj in s3_objects if object_is_target(obj, target['name']) }
        now = UTC.localize(datetime.utcnow())
        aged = [ archives_by_last_modified[last_modified] for last_modified in archives_by_last_modified if (now - last_modified).total_seconds() / (60*60*24) >= 180 ]
        current_count = [ last_modified for last_modified in archives_by_last_modified if (now - last_modified).total_seconds() / (60*60*24) < 180 ]
        remote_stats[target['name']] = { 
            'max_last_modified': max(archives_by_last_modified.keys()) if len(archives_by_last_modified.keys()) > 0 else None, 
            'count': len(archives_by_last_modified),
            'aged': aged,
            'current_count': current_count
        }
    return remote_stats 

def get_remote_archives(bucket_name, target_name=None):
        
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

    objects = []
    #print(dir(archive_bucket.objects))
    #print(dir(archive_bucket.objects.all()))

    # TODO: improve the matching here 
    with archivebucket(bucket_name) as bucket:
        all_objects = bucket.objects.all()
        objects = [ obj for obj in all_objects if (target_name and object_is_target(obj, target_name)) or not target_name ]
    return objects 

def cleanup_remote_archives(bucket_name, remote_stats, dry_run=True):
    if remote_stats['current_count'] > 0:
        printorange(f'Deleting remote archives aged out: {",".join([ obj.key for obj in remote_stats["aged"] ])}')
        if dry_run:
            printred(f'DRY RUN -- skipping remote deletion')
        else:
            delete_objects(remote_stats["aged"], bucket_name)


#################
#
# filesystem 

def get_working_folder_free_space():
    cp = subprocess.run("df -k %s | grep -v Used | awk '{ print $4 }'" % WORKING_FOLDER, shell=True, text=True, capture_output=True)
    return int(cp.stdout.replace('\n', ''))

def get_target_uncompressed_size_kb(target):

    # TODO: use target excludes to more accurately compute size
    # TODO: estimate compressed size to more accurately compute size 
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

def target_has_new_files(target, log=True):

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
                if log:
                    printorange(f'No archive exists corresponding to the existing pre-marker. This marker is invalid, and all files are considered new.')
                has_new_files = True 
            else:
                
                cp = subprocess.run(f'find {target["path"]} -newer {pre_marker}'.split(' '), check=True, capture_output=True)
                new_file_count = len(cp.stdout.splitlines())
                has_new_files = new_file_count > 0
                if log:
                    printwhite(f'{new_file_count} new files found since {pre_marker_stamp}')
        except subprocess.CalledProcessError as cpe:
            printred(cpe.stderr)   
    else:
        has_new_files = True 
        if log:
            printwhite(f'No marker file found, all files considered new')
    
    return has_new_files

def cleanup_local_archives(bucket_name, target=None, aggressive=False, dry_run=True):
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
        targets = get_targets()
    
    archives = get_archives(with_location=True)

    if dry_run and not target:
        printorange(f'Examining {len(targets)} targets to determine possible space made available by cleanup ({ "not " if not aggressive else "" }aggressive).')
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            
    cleaned_up_by_target = { t["name"]: 0 for t in targets }
    for t in targets:        

        target_archives = [ a for a in archives if a['target_id'] == t['id'] ]        
        
        archives_newest_first = sorted(target_archives, key=lambda a: a['created_at'])
        archives_newest_first.reverse()     
        minimum_to_keep = 1 if aggressive else 3
        found = 0

        if dry_run and target:
            printorange(f'Examining {len(archives_newest_first)} ({target["name"]}) files to determine possible space made available by cleanup ({ "not " if not aggressive else "" }aggressive).')
    
        for archive in archives_newest_first:

            marked = False 

            if aggressive:
                if archive['location'] == Location.LOCAL_AND_REMOTE:
                    cleaned_up_by_target[t['name']] += archive['size_kb']
                    marked = True 
                    if not dry_run:
                        printorange(f'Archive {archive["name"]}/{archive["filename"]} is both remote and local. Deleting local copy.')
                        os.unlink(archive["filename"])            

            if not marked and os.path.exists(archive['filename']):                
                if found < minimum_to_keep:
                    if not dry_run:
                        printwhite(f'Keeping newer file {archive["filename"]}')
                    found += 1
                else:
                    cleaned_up_by_target[t['name']] += archive['size_kb']
                    if not dry_run:
                        printred(f'Deleting file {archive["filename"]}')
                        os.unlink(archive['filename'])
    
    return cleaned_up_by_target[target['name']] if target else cleaned_up_by_target

def add_archive(target, bucket_name, results=None):

    if not results:
        results = Results()

    none_response = None

    if not target_has_new_files(target):
        printorange(f'No new files. Skipping archive creation.')
        results.log('no_new_files')
        return none_response

    target_size = get_target_uncompressed_size_kb(target)
    free_space = get_working_folder_free_space()

    if target_size > free_space:
        
        additional_space_needed = target_size - free_space

        printred(f'This target is {human(additional_space_needed, "kb")} bigger than what is available on the filesystem.')

        space_freed_by_cleanup = cleanup_local_archives(bucket_name, aggressive=False, dry_run=True)
        space_sum = sum([ space_freed_by_cleanup[t] for t in space_freed_by_cleanup.keys() ])

        if space_sum < additional_space_needed:

            printred(f'Even after cleaning up local archives, an additional {human(additional_space_needed - space_sum, "kb")} is still needed. Please free up space and reschedule this target as soon as possible.')

            space_freed_by_cleanup = cleanup_local_archives(bucket_name, aggressive=True, dry_run=True)
            space_sum = sum([ space_freed_by_cleanup[t] for t in space_freed_by_cleanup.keys() ])
            
            if space_sum < additional_space_needed:
                printred(f'Even after aggressively cleaning up local archives, an additional {human(additional_space_needed - space_sum, "kb")} is still needed. Please free up space and reschedule this target as soon as possible.')
                results.log('insufficient_space')
                return none_response
            else:
                printorange(f'Cleaning up old local archives aggressively will free {human(space_sum, "kb")}. Proceeding with cleanup.')
                space_freed_by_cleanup = cleanup_local_archives(bucket_name, aggressive=True, dry_run=False)
        else:

            printorange(f'Cleaning up old local archives will free {human(space_sum, "kb")}. Proceeding with cleanup.')
            cleanup_local_archives(bucket_name, aggressive=False, dry_run=False)
    
    new_archive_id = None 

    try:

        target_file = f'{WORKING_FOLDER}/{target["name"]}_{datetime.strftime(datetime.now(), "%Y%m%d_%H%M%S")}.tar.gz'

        excludes = ""
        if target["excludes"] and len(target["excludes"]) > 0:
            excludes = f'--exclude {" --exclude ".join(target["excludes"].split(":"))}'

        archive_command = f'tar {excludes} --exclude-vcs-ignores -czf {target_file} {target["path"]}'
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
        
        target_file_stat = shutil.os.stat(target_file)
        new_archive_id = create_archive(target_id=target['id'], size_kb=target_file_stat.st_size/1024.0, filename=target_file, returncode=returncode, errors=archive_errors, pre_marker_timestamp=pre_timestamp)
        update_markers(target, pre_timestamp)
        results.log('archive_created')
        
        printgreen(f'Archive record {new_archive_id} created for {target_file}')
        
        if target_file:
            printgreen(f'Created {target["name"]} archive: {target_file}')
        else:
            printorange(f'No {target["name"]} archive created')
            
        return target_file

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

        printred(sys.exc_info()[1])
        traceback.print_tb(sys.exc_info()[2])

        if new_archive_id:
            printred(f'Removing archive record {new_archive_id}')
            delete_archive(new_archive_id)
        if os.path.exists(target_file):
            printred(f'Removing archive file {target_file}')
            os.unlink(target_file)
            
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

class Backup(object):
    bucket_name = None 

    def __init__(self, *args, **kwargs):
        self.bucket_name = kwargs['bucket_name']
    
    def run(self, requested_target=None):
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
        
        remote_stats = get_remote_stats(targets)

        printwhite(f'{len(targets)} targets identified')

        results = Results()

        for target in targets:
            try:
                printwhite(f'\n*************************************************\n')
                printwhite(f'Backup target: {target["name"]} ({target["path"]})')
                if target_is_scheduled(target):
                    printwhite(f'Creating archive for {target["name"]}')
                    archive_file = add_archive(target, self.bucket_name, results)
                else:
                    printorange(f'{target["name"]} is not scheduled')
                    results.log('not_scheduled')
            except:
                printred(sys.exc_info()[1])
                traceback.print_tb(sys.exc_info()[2])

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
                    cleanup_remote_archives(bucket_name, remote_stats[target['name']])
                    if is_push_due(target, remote_stats=remote_stats):
                        try:
                            push_archive_to_bucket(last_archive, self.bucket_name)
                            printgreen(f'Last archive has been pushed remotely')                        
                            set_archive_remote(last_archive)
                        except:
                            printred(f'The last archive failed to push remotely')
                            printred(sys.exc_info()[1])
                            traceback.print_tb(sys.exc_info()[2])
                elif not last_archive:
                    printorange(f'No last archive is available for this target')
                elif last_archive['is_remote']:
                    printwhite(f'The last archive is already pushed remotely')
            except:
                printred(sys.exc_info()[1])
                traceback.print_tb(sys.exc_info()[2])
            
            # -- check S3 status (regardless of schedule)
            # -- check target budget (calculate )
            # -- clean up S3 / push latest archive if not pushed 
            # -- update archive push status/time
            
        
        end = datetime.now()

        results.print()

        printwhite(f'\n\nBackup run completed: {datetime.strftime(end, "%c")}\n')



if __name__ == "__main__":

    bucket_name = os.getenv('S3_BUCKET')

    if not bucket_name:
        printred('Set S3_BUCKET prior to running this program.')
        exit(1)

    b = Backup(bucket_name=bucket_name)

    commands = {
        'db init': initialize_database,
        'targets list': print_targets,
        'targets add': create_target,
        'archives list': print_archives,
        'archives add': add_archive,
        'run': b.run        
    }

    tokens = 1

    while tokens < len(sys.argv) and " ".join(sys.argv[1:tokens+1]) not in commands:
        tokens += 1
    
    command = " ".join(sys.argv[1:tokens+1])

    if command not in commands:
        printred(f'The command {command} is not supported.')
        exit(1)

    fn = commands[command]
    args = []
    if len(sys.argv) > tokens + 1:
        args = sys.argv[tokens+1:]
    print(f'calling {command} {args}')
    fn(*args) 

    exit(0)

    if sys.argv[1] == 'targets':
        if sys.argv[2] == 'list':

            all_targets = get_targets()
            for target in all_targets:
                printgray(target)
        elif sys.argv[2] == 'add':
            create_target(*sys.argv[3:])
    elif sys.argv[1] == 'archives':

        target_name = None 
        
        if len(sys.argv) > 3:
            target_name = sys.argv[3]
            target = get_target(target_name)
        if sys.argv[2] == 'add':  
            target_file = add_archive(target, bucket_name)            
        elif sys.argv[2] == 'get':
            print_archives(bucket_name, target_name)

    elif sys.argv[1] == 'run':
        target = None 
        if len(sys.argv) > 2:
            target = sys.argv[2]
        run(bucket_name, target)
