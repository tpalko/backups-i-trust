import os
from datetime import datetime 
import sqlite3 
import logging 
from contextlib import contextmanager
import subprocess

from awsclient import PushStrategy 

_TABLES = {
    'archives': '(id integer primary key autoincrement, target_id int, created_at datetime, size_kb int, is_remote bool, remote_push_at datetime, filename char(255), returncode int, errors text, pre_marker_timestamp datetime, md5 char(32))',
    'targets': '(id integer primary key autoincrement, path text, name char(255), excludes text, budget_max float, frequency char(32), push_strategy char(32), push_period int, is_active bool)',
    'runs': '(id integer primary key autoincrement, start_at datetime, end_at datetime, run_stats_json text)'
}
ARCHIVE_TARGET_JOIN_SELECT = 'a.id, a.target_id, a.created_at, a.size_kb, a.is_remote, a.remote_push_at, a.filename, a.returncode, a.errors, a.pre_marker_timestamp, a.md5, t.name, t.path, t.is_active'
ARCHIVE_TARGET_JOIN = 'from archives a inner join targets t on t.id = a.target_id'
TARGETS_SELECT = 't.id, t.path, t.name, t.excludes, t.budget_max, t.frequency, t.push_strategy, t.push_period, t.is_active'

class Database(object):

    conn = None 
    logger = None 
    db_file = None 

    def __init__(self, *args, **kwargs):

        if 'db_file' not in kwargs:
            raise Exception("db_file is required for Database")

        for k in kwargs:
            setattr(self, k, kwargs[k])

        if not self.logger:
            self.logger = logging.getLogger(__name__)
     
    def parse_type(self, column_name, value):
        if value is not None:
            if column_name[-3:] == '_at':
                return datetime.strptime(value, "%Y-%m-%d %H:%M:%S.%f")
            elif column_name[0:3] == 'is_':
                return bool(value)
        return value 

    def dict_factory(self, cursor, row):
        return { col[0]: self.parse_type(col[0], row[idx]) for idx,col in enumerate(cursor.description) }
        
    @contextmanager 
    def cursor(self):

        self.conn = sqlite3.connect(self.db_file)
        self.conn.row_factory = self.dict_factory
        c = self.conn.cursor()
        try:
            yield c
        except:
            self.logger.exception()
        finally:
            self.conn.close()

    def init_db(self):
        '''Checks database table schema against table schema definition, creating missing tables'''
        with self.cursor() as c:
            for table in _TABLES:
                try:
                    c.execute(f'select sql from sqlite_master where name = ?', (table,))
                    firstrow = c.fetchone()
                    if not firstrow:
                        raise sqlite3.OperationalError("fetchone returned nothing")
                    sql = firstrow['sql']
                    self.logger.success(f'Captured {table} schema')
                    schema_in_code = f'CREATE TABLE "{table}" {_TABLES[table]}'
                    if sql != schema_in_code:
                        self.logger.error(f'WARNING: {table} schema in database does not match schema in code')
                        self.logger.error(f'Database:\t{sql}')
                        self.logger.error(f'Code:\t\t{schema_in_code}')
                    else:
                        self.logger.success(f'Table schema OK')
                except sqlite3.OperationalError as oe:
                    self.logger.error(f'Failed to read from table {table}')
                    self.logger.error(oe)
                    self.logger.error(f'Creating table {table}..')
                    # c = self.conn.cursor()
                    c.execute(f'CREATE TABLE {table} {_TABLES[table]}')
                    self.conn.commit()
                except:
                    self.logger.error(f'Something else failed testing table {table}')
                    self.logger.exception()

    def fix_archive_filenames(self):
        ''' Replaces archive filename with basename(filename) '''
        
        db_records = []
        
        with self.cursor() as c:
            c.execute(f'select id, filename from archives')
            db_records = c.fetchall()
        
        with self.cursor() as c:
            for record in db_records:
                c.execute(f'update archives set filename = ? where id = ?', (os.path.basename(record['filename']), record['id'], ))
                self.conn.commit()

    def get_archive(self, archive_id):
        archive_record = None         
        with self.cursor() as c:
            c.execute(f'select {ARCHIVE_TARGET_JOIN_SELECT} {ARCHIVE_TARGET_JOIN} where a.id = ?', (archive_id,))
            archive_record = c.fetchone()
        return archive_record

    def get_archives(self, target_name=None):
        target = None 
        if target_name:
            target = self.get_target(target_name)

        db_records = []

        with self.cursor() as c:
            if target:
                c.execute(f'select {ARCHIVE_TARGET_JOIN_SELECT} {ARCHIVE_TARGET_JOIN} where a.target_id = ? order by created_at desc', (target['id'],))
            else:
                c.execute(f'select {ARCHIVE_TARGET_JOIN_SELECT} {ARCHIVE_TARGET_JOIN} order by created_at desc')
            db_records = c.fetchall()
            
        return db_records

    def get_archive_for_pre_timestamp(self, target_id, timestamp):
        line = None 
        with self.cursor() as c:
            c.execute(f'select {ARCHIVE_TARGET_JOIN_SELECT} {ARCHIVE_TARGET_JOIN} where a.target_id = ? and a.pre_marker_timestamp = ? order by created_at desc limit 1', (target_id, timestamp))
            line = c.fetchone()
        return line

    def get_last_archive(self, target_id):
        line = None 
        with self.cursor() as c:
            c.execute(f'select {ARCHIVE_TARGET_JOIN_SELECT} {ARCHIVE_TARGET_JOIN} where a.target_id = ? order by created_at desc limit 1', (target_id,))
            line = c.fetchone()
        return line

    def delete_archive(self, archive_id):

        with self.cursor() as c:        
            c.execute('delete from archives where id = ?', (archive_id,))
            self.conn.commit()
            self.logger.success(f'Archive {archive_id} deleted')

    def create_archive(self, target_id, size_kb, filename, returncode, errors, pre_marker_timestamp):

        new_archive_id = None 

        cp = subprocess.run("md5sum %s | awk '{ print $1 }'" % filename, text=True, shell=True, capture_output=True)
        self.logger.debug(f'md5sum output: {cp.stdout}')
        digest = str([ line for line in cp.stdout.splitlines() if line ][0])

        # with open(filename, 'rb') as f:
        #     contents = f.read()
        #     digest = hashlib.md5(contents).hexdigest()

        with self.cursor() as c:        
            params = (target_id, datetime.now(), size_kb, False, None, os.path.basename(filename), returncode, errors, pre_marker_timestamp, digest,)
            c.execute('insert into archives (target_id, created_at, size_kb, is_remote, remote_push_at, filename, returncode, errors, pre_marker_timestamp, md5) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', params)
            self.conn.commit()
            new_archive_id = c.lastrowid
        
        return new_archive_id

    def get_targets(self):
        all_targets = None 
        with self.cursor() as c:
            db_command = f'select {TARGETS_SELECT} from targets t'
            self.logger.debug(db_command)
            c.execute(db_command)
            all_targets = c.fetchall()
        return all_targets        

    def get_target(self, name):
        line = None 
        with self.cursor() as c:
            c.execute(f'select {TARGETS_SELECT} from targets t where t.name = ?', (name,))
            line = c.fetchone()
        return line

    def create_target(self, path, name, frequency, budget, excludes, is_active=True, push_strategy=PushStrategy.BUDGET_PRIORITY):
        '''Creates a new target'''
        existing_target = self.get_target(name)
        if not existing_target:
            # -- if enum, use value 
            if type(push_strategy).__name__ == 'PushStrategy':
                push_strategy = push_strategy.value 
            with self.cursor() as c:        
                c.execute('insert into targets (path, name, budget_max, excludes, frequency, is_active, push_strategy) values(?, ?, ?, ?, ?, ?, ?)', (path, name, budget, excludes, frequency, is_active, push_strategy,))
                self.conn.commit()
                self.logger.success(f'Target {name} added')
        else:
            self.logger.warning(f'Target {name} already exists')

    def update_target(self, target_name, **kwargs): #, frequency=frequency, budget=budget, excludes=excludes):

        setters = ','.join([ f'{k} = ?' for k in kwargs if kwargs[k] is not None ])
        vals = [ kwargs[k] for k in kwargs if kwargs[k] is not None ]

        with self.cursor() as c:
            c.execute(f'select {TARGETS_SELECT} from targets t where t.name = ?', (target_name,))
            line = c.fetchone()
            if line:
                vals.append(line['id'])
                c.execute(f'update targets set {setters} where id = ?', tuple(vals))
                self.conn.commit()

    # def update_target(self, target_name, is_active):
    #     with self.cursor() as c:
    #         c.execute(f'select {TARGETS_SELECT} from targets t where t.name = ?', (target_name,))
    #         line = c.fetchone()
    #         if line:
    #             c.execute(f'update targets set is_active = ? where id = ?', (is_active, line['id'],))
    #             self.conn.commit()

    def set_archive_remote(self, archive):

        with self.cursor() as c:
            c.execute('update archives set is_remote = 1, remote_push_at = ? where id = ?', (datetime.now(), archive['id'],))
            self.conn.commit()
            self.logger.success(f'Archive {archive["id"]} set as remote')
