# import sys 
import cowpy
# from enum import Enum 
import os
from datetime import datetime 
import sqlite3 
import mariadb
from contextlib import contextmanager
import subprocess
from frank.database.database import Database
from frank.database.model import BaseModel, JsonColumn, StringColumn, IntColumn, FloatColumn, BoolColumn, DateTimeColumn

DBCONFIG = {    
    'base': {
        'primary_key': 'id',
        'timestamps': []
    },    
    'models': {
        'archives': [
            { 'name': 'target_id', 'type': int }, 
            { 'name': 'created_at', 'type': datetime.date },             
            { 'name': 'size_kb', 'type': int }, 
            { 'name': 'is_remote', 'type': bool }, 
            { 'name': 'remote_push_at', 'type': datetime.date }, 
            { 'name': 'filename', 'type': str, 'size': 255 },
            { 'name': 'returncode', 'type': int }, 
            { 'name': 'errors', 'type': str }, 
            { 'name': 'pre_marker_timestamp', 'type': datetime.date }, 
            { 'name': 'md5', 'type': str, 'size': 32 },
            { 'name': 'uncompressed_size_kb', 'type': int }
        ],
        'targets': [
            { 'name': 'path', 'type': str }, 
            { 'name': 'name', 'type': str, 'size': 255 }, 
            { 'name': 'excludes', 'type': str }, 
            { 'name': 'budget_max', 'type': float }, 
            { 'name': 'frequency', 'type': str, 'size': 32 }, 
            { 'name': 'push_strategy', 'type': str, 'size': 32 }, 
            { 'name': 'push_period', 'type': int }, 
            { 'name': 'is_active', 'type': bool }, 
            { 'name': 'pre_marker_at', 'type': datetime.date, 'null': True }, 
            { 'name': 'post_marker_at', 'type': datetime.date, 'null': True }, 
            { 'name': 'last_reason', 'type': str },
            { 'name': 'created_at', 'type': datetime.date }
        ],
        'runs': [
            { 'name': 'start_at', 'type': datetime.date }, 
            { 'name': 'end_at', 'type': datetime.date }, 
            { 'name': 'run_stats_json', 'type': str }
        ]
    },
    'foreign_keys': {
        'archives': {
            'targets': 'id'
        }
    }
}

from awsclient import PushStrategy 

### CUT vvv 

# class Dialect(Enum):
#     AUTO_INCREMENT = 0

# db_dialect = {
#     'sqlite': {
#         Dialect.AUTO_INCREMENT: 'autoincrement',            
#     },
#     'mariadb': {
#         Dialect.AUTO_INCREMENT: 'auto_increment',            
#     }
# }

# db_providers = {
#     'sqlite': lambda config: sqlite3.connect(config.db_file),
#     'mariadb': lambda config: mariadb.connect(host=config.db_host, user=config.db_user, password=config.db_password, database=config.db_name)
# }

# class BcktDatabaseException(Exception):
#     pass 

### CUT ^^^ 

# TABLES = {
#     'archives': lambda config: f'(id integer primary key {get_db_dialect(config.database_type)[Dialect.AUTO_INCREMENT]}, target_id int, created_at datetime, size_kb int, is_remote bool, remote_push_at datetime, filename char(255), returncode int, errors text, pre_marker_timestamp datetime, md5 char(32))',
#     'targets': lambda config: f'(id integer primary key {get_db_dialect(config.database_type)[Dialect.AUTO_INCREMENT]}, path text, name char(255), excludes text, budget_max float, frequency char(32), push_strategy char(32), push_period int, is_active bool, pre_marker_at datetime, post_marker_at datetime, last_reason text)',
#     'runs': lambda config: f'(id integer primary key {get_db_dialect(config.database_type)[Dialect.AUTO_INCREMENT]}, start_at datetime, end_at datetime, run_stats_json text)'
# }

ARCHIVE_TARGET_JOIN_SELECT = 'a.id, a.target_id, a.created_at, a.size_kb, a.is_remote, a.remote_push_at, a.filename, a.returncode, a.errors, a.pre_marker_timestamp, a.md5, t.name, t.path, t.is_active'
ARCHIVE_TARGET_JOIN = 'from archives a inner join targets t on t.id = a.target_id'
TARGETS_SELECT = 't.id, t.path, t.name, t.excludes, t.budget_max, t.frequency, t.push_strategy, t.push_period, t.is_active, t.pre_marker_at, t.post_marker_at'

# class DatabaseConfig(object):

#     # - sqlite3 or mariadb
#     db_type = None

#     # -- if sqlite3
#     db_file = None
    
#     # -- if mariadb
#     db_user = None
#     db_password = None
#     db_name = None
#     db_host = None

#     @staticmethod
#     def New(db_type, file=None, user=None, password=None, name=None, host=None):
#         c = DatabaseConfig()
#         c.db_type = db_type 
#         c.db_file = file 
#         c.db_host = host 
#         c.db_name = name 
#         c.db_password = password 
#         c.db_user = user 
#         return c    

class Run(BaseModel):
    start_at = DateTimeColumn()
    end_at = DateTimeColumn()
    run_stats_json = JsonColumn()

class Archive(BaseModel):
    target_id = IntColumn()
    size_kb = IntColumn()
    is_remote = BoolColumn()
    remote_push_at = DateTimeColumn()
    filename = StringColumn()
    returncode = IntColumn()
    errors = StringColumn()
    pre_marker_timestamp = DateTimeColumn()
    md5 = StringColumn()
    uncompressed_size_kb = IntColumn()

class Target(BaseModel):
    path = StringColumn()
    name = StringColumn()
    excludes = StringColumn()
    budget_max = FloatColumn()
    frequency = StringColumn()
    push_strategy = StringColumn()
    push_period = IntColumn()
    is_active = BoolColumn()
    pre_marker_at = DateTimeColumn()
    post_marker_at = DateTimeColumn()
    last_reason = StringColumn()
    
class BcktDb(object):

    # conn = None 
    logger = None 
    user_logger = None 

    config = None 

    sqliteDb = None  
    mariaDb = None 
    
    def __init__(self, *args, **kwargs):

        self.logger = cowpy.getLogger()        

        # dbConfig = DatabaseConfig.New(host=kwargs['host'], user=kwargs['user'], password=kwargs['password'], name=kwargs['database'])

        self.config = kwargs['config']
        self.user_logger = kwargs['user_logger']

        self.logger.debug(self.config)
        
        Database.createInstance(config={'filename': self.config.database_file, 'dbType': self.config.database_type})

        # self.sqliteDb = Database(
        #     config=DatabaseConfig.NewSqlite(
        #         filename=self.config.database_file             
        #     ),
        #     tables=DBCONFIG
        # )

        # self.mariaDb = Database(
        #     config=DatabaseConfig.NewMariadb(
        #         host=self.config.database_host,
        #         user=self.config.database_user,
        #         password=self.config.database_password,
        #         name=self.config.database_name
        #     ),
        #     tables=DBCONFIG
        # )

        # if 'db_file' not in kwargs:
        #     raise Exception("db_file is required for Database")

        # if 'config' in kwargs:
        #     config = kwargs['config']
        #     for k in config.__dict__:
        #         setattr(self, k, config.__dict__[k])
        #     del kwargs['config']

        # for k in kwargs:
        #     setattr(self, k, kwargs[k])

        # if not self.logger:
        
        # self.logger.debug('Database wrapper created')
     
    def __repr__(self):
        return str(self.__dict__)
    
    ### CUT vvv 

    # def parse_type(self, column_name, value):
    #     if value is not None:
    #         if column_name[-3:] == '_at':
    #             parsed = value 
    #             try:
    #                 parsed = datetime.strptime(value, "%Y-%m-%d %H:%M:%S.%f")
    #             except:
    #                 parsed = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    #             return parsed
    #         elif column_name[0:3] == 'is_':
    #             return bool(value)
    #     return value 

    # def dict_factory(self, cursor, row):
    #     return { col[0]: self.parse_type(col[0], row[idx]) for idx,col in enumerate(cursor.description) }

    # @contextmanager
    # def get_cursor(self):
    #     '''Generic cursor munging, dialect fallback, nothing else'''
    #     try:
    #         # -- some cursors will have their own context 
    #         # -- e.g. mariadb
    #         with self.conn.cursor() as c:
    #             yield c 
    #     except AttributeError as ae:
    #         # -- there is a particular case where self.conn.cursor() will fail with sqlite 
    #         # -- and simply yieldling self.conn.cursor() is the answer 
    #         # -- no context will manage the transaction or connection for us
    #         try:
    #             yield self.conn.cursor()
    #             self.conn.commit()
    #         finally:
    #             self.conn.close()
    #     except:          
    #         # -- but if anything else goes wrong, kick
    #         self.logger.exception()  
    #         raise

    # @contextmanager 
    # def cursor(self):

    #     self.conn = db_providers[self.config.db_type](self.config)
    #     self.conn.row_factory = self.dict_factory

    #     with self.get_cursor() as c:
    #         try:
    #             yield c 
    #         except:
    #             # -- wrap all errors simply for backup callers
    #             self.logger.exception()
    #             raise BcktDatabaseException(sys.exc_info()[1])
    ### CUT ^^^ 
    
    def dump(self):
        '''DOCDEFER:Database.dump'''
        return self.sqliteDb.dump()

    def init(self):
        '''DOCDEFER:Database.init_db'''        
        self.sqliteDb.init_db()

    # def init_db(self):
    #     '''Checks database table schema against table schema definition, creating missing tables'''        

    #     for table in DBCONFIG['models'].keys():
            
    #         create_table_cmd = f'CREATE TABLE {table} {self.sqliteDb.create_table(table)}{get_db_dialect(self.config.database_type)[Dialect.ENGINE]}' 
    #         # f'CREATE TABLE "{table}" {TABLES[table](self.config)}'

    #         try:
    #             sql = None 
    #             # -- schemachecker 
    #             with self.sqliteDb.cursor() as c:
    #                 c.execute(f'select sql from sqlite_master where name = ?', (table,))
    #                 firstrow = c.fetchone()
    #                 if not firstrow:
    #                     raise sqlite3.OperationalError("fetchone returned nothing")
    #                 sql = firstrow['sql']
    #             if sql:
    #                 self.logger.success(f'Captured {table} schema: {sql}')

    #                 sql = " ".join([ s.strip() for s in sql.split(' ') if s.strip() != '' ]).replace('\'', '').replace('"', '').replace('  ', ' ').lower()
    #                 create_table_cmd = " ".join([ s.strip() for s in create_table_cmd.split(' ') if s.strip() != '' ]).replace('\'', '').replace('"', '').replace('  ', ' ').lower()

    #                 if sql != create_table_cmd:
    #                     self.user_logger.error(f'WARNING: {table} schema in database does not match schema in code')
    #                     self.user_logger.error(f'Database:\t{sql}')
    #                     self.user_logger.error(f'Code:\t\t{create_table_cmd}')
    #                 else:
    #                     self.logger.success(f'Table schema OK')
    #         except (BcktDatabaseException, sqlite3.OperationalError, mariadb.ProgrammingError) as oe:
    #             self.logger.error(f'Failed to read from table {table}')
    #             self.logger.error(oe)
    #             self.logger.error(f'Creating table {table}..')
    #             with self.cursor() as c:                    
    #                 c.execute(create_table_cmd)
    #         except:
    #             self.logger.error(f'Something else failed testing table {table}')
    #             self.logger.exception()
    #             raise 

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
        records = self.sqliteDb.raw(f'select {ARCHIVE_TARGET_JOIN_SELECT} {ARCHIVE_TARGET_JOIN} where a.id = ?', (archive_id,))
        return records[0] if len(records) > 0 else None
        
    def get_archives(self, target_name=None):
        
        target = None 
        if target_name:
            target = self.get_target(target_name)

        self.logger.debug(f'getting archives for target: {target}')

        resp = {}

        if target:
            resp = self.sqliteDb._select('archives', joins=['targets'], join_cols=False, where={'a.target_id': target['id']}, order_by='a.created_at desc')
            # c.execute(f'select {ARCHIVE_TARGET_JOIN_SELECT} {ARCHIVE_TARGET_JOIN} where a.target_id = ? order by created_at desc', (target['id'],))
        else:
            resp = self.sqliteDb._select('archives', joins=['targets'], join_cols=False, order_by='a.created_at desc')
            # c.execute(f'select {ARCHIVE_TARGET_JOIN_SELECT} {ARCHIVE_TARGET_JOIN} order by created_at desc')
        
        return resp['data']

    def get_archive_for_pre_timestamp(self, target_id, timestamp):
        
        resp = self.sqliteDb._select('archives', where={'target_id': target_id, 'pre_marker_timestamp': timestamp}, order_by='a.created_at desc')
        if len(resp['data']) > 0:
            return resp['data'][0]

        return None 
        
    def get_last_archive(self, target_id):

        resp = self.sqliteDb._select('archives', where={'target_id': target_id}, order_by='a.created_at desc')
        if len(resp['data']) > 0:
            return resp['data'][0]
        return None 
    
    def delete_archive(self, archive_id):

        self.sqliteDb._delete('archives', archive_id)
        self.logger.success(f'Archive {archive_id} deleted')           

    def create_archive(self, target_id, size_kb, filename, pre_marker_timestamp, digest=None, returncode=0, errors="", uncompressed_size_kb=None):

        # why no hashlib? ^^^
        # with open(filename, 'rb') as f:
        #     contents = f.read()
        #     digest = hashlib.md5(contents).hexdigest()

        params = (target_id, datetime.now(), size_kb, False, None, os.path.basename(filename), returncode, errors, pre_marker_timestamp, digest, uncompressed_size_kb)
        resp = self.sqliteDb._insert('archives', *params)
        self.logger.debug(f'insert to archives ({params}) response: {resp}')

        insert_id = None 

        try:
            insert_id = resp['data']['insert_id']
        except:
            self.logger.error(f'failed to get insert ID from archives insert')
            self.logger.exception()

        return insert_id

    def get_targets(self):
        fake_target = Target()
        return Target.all()
        # resp = self.sqliteDb._select('targets')
        # return resp['data']        

    def get_target(self, name):
        return Target.get(name=name)
        # resp = self.sqliteDb._select('targets', where={'t.name': name})
        # if len(resp['data']) > 0:
        #     return resp['data'][0]
        # return None 

    def create_target(self, path, name, frequency, budget, excludes, is_active=True, push_strategy=PushStrategy.BUDGET_PRIORITY):
        '''Creates a new target'''
        existing_target = self.get_target(name)
        if not existing_target:
            # -- if enum, use value 
            if type(push_strategy).__name__ == 'PushStrategy':
                push_strategy = push_strategy.value 
            #path, name, excludes, budget_max, frequency, push_strategy, push_period, is_active, pre_marker_at, post_marker_at, last_reason, created_at
            params = (path, name, excludes, budget, frequency, push_strategy, "", is_active, None, None, None, datetime.now())
            self.sqliteDb._insert('targets', *params)
            self.logger.success(f'Target {name} added')                
        else:
            self.logger.warning(f'Target {name} already exists')

    def update_archive(self, archive_id, **kwargs):
        self.sqliteDb._update('archives', set=kwargs, where={'id': archive_id})

    def update_target(self, target_name, **kwargs): #, frequency=frequency, budget=budget, excludes=excludes):

        # setters = ','.join([ f'{k} = ?' for k in kwargs if kwargs[k] is not None ])
        # vals = [ kwargs[k] for k in kwargs if kwargs[k] is not None ]

        target = self.get_target(name=target_name)
        self.sqliteDb._update('targets', set=kwargs, where={'id': target['id']})

        # with self.cursor() as c:
        #     c.execute(f'select {TARGETS_SELECT} from targets t where t.name = ?', (target_name,))
        #     line = c.fetchone()
        #     if line:
        #         vals.append(line['id'])
        #         c.execute(f'update targets set {setters} where id = ?', tuple(vals))
        #         self.conn.commit()

    # def update_target(self, target_name, is_active):
    #     with self.cursor() as c:
    #         c.execute(f'select {TARGETS_SELECT} from targets t where t.name = ?', (target_name,))
    #         line = c.fetchone()
    #         if line:
    #             c.execute(f'update targets set is_active = ? where id = ?', (is_active, line['id'],))
    #             self.conn.commit()

    def set_target_last_reason(self, target_name, last_reason):        
        target = self.get_target(target_name)
        self.logger.warning(f'setting target last reason for {target}')
        resp = self.sqliteDb._update('targets', {'last_reason': last_reason.value}, {'id': target['id']})
        
    def set_archive_remote(self, archive):

        self.sqliteDb._update('archives', set={'is_remote': 1, 'remote_push_at': datetime.now()}, where={'id': archive['id']})
        self.logger.success(f'Archive {archive["id"]} set as remote')
            
