import cowpy 
import boto3
import os
import math
from enum import Enum 
import json 
import base64
from contextlib import contextmanager
from datetime import datetime 
from pytz import timezone 
from common import get_path_uncompressed_size_kb, human, frequency_to_minutes, time_since
from cache import Cache, CacheType

UTC = timezone('UTC')
TARGET_CACHE_FILE = f'/tmp/bckt.cache'

REMOTE_STORAGE_COST_GB_PER_MONTH = 0.00099

class PushStrategy(Enum):
    BUDGET_PRIORITY = 'budget_priority' # -- cost setting ultimately drives whether an archive is pushed remotely 
    SCHEDULE_PRIORITY = 'schedule_priority'
    CONTENT_PRIORITY = 'content_priority'

class AwsClient:

    bucket_name = None 
    db = None 
    logger = None 
    target_cache = None 

    def __init__(self, *args, **kwargs):

        if 'bucket_name' not in kwargs or kwargs['bucket_name'] == '':
            raise Exception('bucket_name must be supplied to AwsClient')
        
        if 'db' not in kwargs or not kwargs['db']:
            raise Exception('db must be supplied to AwsClient')
        
        self.logger = cowpy.getLogger()
        
        self.bucket_name = kwargs['bucket_name']
        self.db = kwargs['db']
        
        self.cache_file = TARGET_CACHE_FILE
        if 'cache_filename' in kwargs:
            self.cache_file = kwargs['cache_filename'] 
        
        self.target_cache = Cache(context=self.bucket_name, cache_file=self.cache_file)

    @contextmanager
    def archivebucket(self, bucket_name):
        s3 = boto3.resource('s3')
        archive_bucket = s3.Bucket(bucket_name)
        self.logger.debug(f'S3 bucket yield out')
        time_out = datetime.now()    
        yield archive_bucket    
        time_in = datetime.now()
        self.logger.debug(f'S3 bucket yield in')
        self.logger.debug(f'S3 bucket calculation time: {"%.1f" % (time_in - time_out).total_seconds()} seconds')

    def get_object_storage_cost_per_month(self, size_bytes):
        return REMOTE_STORAGE_COST_GB_PER_MONTH*(size_bytes / (1024 ** 3))

    def is_push_due(self, target, remote_stats=None, last_archive=None, aged_archives=0, print=True):
        '''According to the target push strategy, budget, and the objects already remotely stored, could an(y) archive be pushed?'''
        
        archives = self.db.get_archives(target.name)

        push_due = False 
        message = 'No calculation was performed to determine push eligibility. The default is no.'
        minutes_since_last_object = None 

        if not remote_stats:
            remote_stats = self.get_remote_stats([target])
            remote_stats = remote_stats[target.name]
            
        last_modified = UTC.localize(datetime.strptime(remote_stats['max_last_modified'], '%c')) if remote_stats['max_last_modified'] else None 
        current_s3_objects = remote_stats['count']

        if last_modified:
            now = UTC.localize(datetime.utcnow())
            since_last_remote_object = now - last_modified
            minutes_since_last_object = (since_last_remote_object.total_seconds()*1.0) / 60
        else:
            push_due = True 
            message = 'No remote objects were found, this may be the first?'
        
        if minutes_since_last_object:

            if target.push_strategy == PushStrategy.BUDGET_PRIORITY.value:
                
                max_s3_objects = 0

                average_size = 0
                if last_archive:
                    average_size = last_archive['size_kb'] / (1024.0*1024.0)
                else:
                    if len(archives) > 0:
                        average_size = sum([ a['size_kb'] / (1024.0*1024.0) for a in archives ]) / len(archives)
                    else:
                        # -- yes, we're using uncompressed size to estimate S3 object size in the absence of actual archives to look at, not great
                        average_size = get_path_uncompressed_size_kb(target.path, target.excludes) / (1024.0*1024.0)

                lifetime_cost = average_size * REMOTE_STORAGE_COST_GB_PER_MONTH * 6
                max_s3_objects = math.floor(target.budget_max / lifetime_cost)
                if max_s3_objects == 0:
                    push_due = False 
                    message = f'One archive has a lifetime cost of {lifetime_cost}. At a max budget of {target.budget_max}, no archives can be stored in S3'
                else:
                    minutes_per_push = (180.0*24*60) / max_s3_objects
                    push_due = (current_s3_objects - aged_archives) < max_s3_objects and minutes_since_last_object > minutes_per_push
                    message = f'Given a calculated size of {average_size:.1f} GB and a budget of ${target.budget_max:.2f}, a push can be accepted every {time_since(minutes_per_push)} for max {max_s3_objects} objects. It has been {time_since(minutes_since_last_object)} and there are {current_s3_objects} objects.'
            
            elif target.push_strategy == PushStrategy.SCHEDULE_PRIORITY.value:
                
                frequency_minutes = frequency_to_minutes(target.frequency)
                push_due = minutes_since_last_object >= frequency_minutes
                message = f'The push period is {target.frequency} ({frequency_minutes} minutes) and it has been {time_since(minutes_since_last_object)}'
            
            elif target.push_strategy == PushStrategy.CONTENT_PRIORITY.value:
                
                push_due = True 
                message = f'Content push strategy: any new content justifies remote storage'
                
            else:
                message = f'No identifiable push strategy ({target.push_strategy}) has been defined for {target.name}.'

        if print:
            if push_due:
                self.logger.info(message)
            else:
                self.logger.warning(message)

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

    def _get_archive_bytes(self, filename):
        b = None 
        with open(filename, 'rb') as f:
            b = f.read()
        return b

    def push_archive(self, target_name, archive_filename, archive_path):

        object = None 

        with self.archivebucket(self.bucket_name) as bucket:
            
            method = 'upload_file'

            key = f'{target_name}/{os.path.basename(archive_filename)}'

            if method == 'upload_file':
                from boto3.s3.transfer import TransferConfig
                uploadconfig = TransferConfig(multipart_threshold=4*1024*1024*1024)
                object = bucket.upload_file(archive_path, key, Config=uploadconfig)
            elif method == 'put_object':
                # b64_md5 = base64.b64encode(bytes(archive['md5'], 'utf-8')).decode()
                # self.logger.info(f'{b64_md5}')
                object = bucket.put_object(
                    Body=self._get_archive_bytes(archive_path),
                    #ContentLength=int(archive['size_kb']*1024),
                    #ContentMD5=b64_md5,
                    Key=key
                )
            
            self.target_cache.cache_invalidate(target_name)

        return object

    def _delete_objects(self, keys):
        if len(keys) > 0:
            with self.archivebucket(self.bucket_name) as bucket:
                delete_resp = bucket.delete_objects(
                    Delete = {
                        'Objects': [ { 'Key': key } for key in keys ],
                        'Quiet': False
                    }
                )
                if 'Errors' in delete_resp and len(delete_resp['Errors']) > 0:
                    self.logger.error(f'Delete errors: {",".join([ "%s: %s" % (o["Key"], o["Code"], o["Message"]) for o in delete_resp["Errors"] ])}')
                if 'Deleted' in delete_resp and len(delete_resp['Deleted']) > 0:
                    self.logger.success(f'Delete confirmed: {",".join([ o["Key"] for o in delete_resp["Deleted"] ])}')

    def _object_is_target(self, obj, target_name):
        '''Does this object have a prefix of the target name, in a folder of the target name or in the root of the bucket?'''
        return (obj['key'].find(f'{target_name}/{target_name}_') == 0 or obj['key'].find(f'{target_name}_') == 0)

    def _get_remote_stats_for_target(self, target, s3_objects):

        self.logger.debug(f'Fetching remote stats for {target.name} / {len(list(s3_objects))} archives')

        object_by_last_modified = { datetime.strptime(obj['last_modified'], "%c"): obj for obj in s3_objects if self._object_is_target(obj, target.name) }
        object_name_by_last_modified = { datetime.strptime(obj['last_modified'], "%c"): obj['key'] for obj in s3_objects if self._object_is_target(obj, target.name) }
        
        last_object = None 

        if len(s3_objects) > 0:
            last_object = object_by_last_modified[max(object_by_last_modified.keys())]

        now = UTC.localize(datetime.utcnow())
        
        # -- s3 objects modified before (aged) or after (current) the six month window
        aged = [ 
            object_name_by_last_modified[last_modified] 
            for last_modified in object_name_by_last_modified 
            if ((now - UTC.localize(last_modified)).total_seconds() / (60*60*24)) >= 180 
        ]
        current = [ 
            object_name_by_last_modified[last_modified]
            for last_modified in object_name_by_last_modified 
            if ((now - UTC.localize(last_modified)).total_seconds() / (60*60*24)) < 180 
        ]
        
        return { 
            'max_last_modified': datetime.strftime(max(object_name_by_last_modified.keys()), "%c") if len(object_name_by_last_modified.keys()) > 0 else None, 
            'last_size': human(last_object['size'], 'b') if last_object else None,
            'total': len(object_name_by_last_modified),
            'current': current,
            'count': len(current),
            'aged': aged,
        }

    def get_remote_stats(self, targets, no_cache=False):
        
        remote_stats = {}

        for target in targets:
            cache_id = self.target_cache.get_cache_id(CacheType.RemoteStats, target.name)
            target_stats = self.target_cache.cache_fetch(cache_id)
            if not target_stats or no_cache:
                target_stats = self._get_remote_stats_for_target(target, self.get_remote_archives(target.name))
                self.target_cache.cache_store(cache_id, target_stats)
            remote_stats[target.name] = target_stats

        return remote_stats 

    def get_remote_archives(self, target_name=None, no_cache=False):
            
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
        cache_id = self.target_cache.get_cache_id(CacheType.Archives, target_name)
        
        objects = self.target_cache.cache_fetch(cache_id)

        if not objects or no_cache:
            with self.archivebucket(self.bucket_name) as bucket:            
                if target_name:
                    objects = bucket.objects.filter(Prefix=f'{target_name}/{target_name}_')
                    # self.logger.debug(f'going through each S3 object to test against target name {target_name}')
                    # objects if (target_name and self._object_is_target(obj, target_name)) or not target_name ]
                else:
                    objects = bucket.objects.all()
                    self.logger.debug(f'skipping target name filter for {len(list(objects))} S3 objects')                
            
            objects = [ { 
                'last_modified': datetime.strftime(obj.last_modified, "%c"), 
                'size': obj.size, 
                'key': obj.key 
            } for obj in objects ]

            self.target_cache.cache_store(cache_id, objects)

        return objects 

    def cleanup_remote_archives(self, target_name, remote_stats, dry_run=True):
        if remote_stats['count'] > 0:
            self.logger.warning(f'Deleting remote archives aged out: {",".join([ key for key in remote_stats["aged"] ])}')
            if dry_run:
                self.logger.error(f'DRY RUN -- skipping remote deletion')
            else:
                self._delete_objects(remote_stats["aged"])
                self.target_cache.cache_invalidate(target_name)
