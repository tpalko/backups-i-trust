import boto3
import os
import math
from enum import Enum 
import logging
import base64
from contextlib import contextmanager
from datetime import datetime 
from pytz import timezone 
from common import get_path_uncompressed_size_kb, human, frequency_to_minutes, time_since

UTC = timezone('UTC')

REMOTE_STORAGE_COST_GB_PER_MONTH = 0.00099

class PushStrategy(Enum):
    BUDGET_PRIORITY = 'budget_priority' # -- cost setting ultimately drives whether an archive is pushed remotely 
    SCHEDULE_PRIORITY = 'schedule_priority'
    CONTENT_PRIORITY = 'content_priority'

class AwsClient:

    bucket_name = None 
    db = None 
    logger = None 

    def __init__(self, *args, **kwargs):
        if 'bucket_name' not in kwargs or kwargs['bucket_name'] == '':
            raise Exception('bucket_name must be supplied to AwsClient')
        
        if 'db' not in kwargs or not kwargs['db']:
            raise Exception('db must be supplied to AwsClient')
        
        self.bucket_name = kwargs['bucket_name']
        self.db = kwargs['db']
        self.logger = kwargs['logger'] if 'logger' in kwargs else logging.getLogger(__name__)

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

    def get_object_storage_cost_per_month(self, obj):
        return REMOTE_STORAGE_COST_GB_PER_MONTH*(obj.size / (1024 ** 3))

    def is_push_due(self, target, remote_stats=None, print=True):
        
        archives = self.db.get_archives(target['name'])

        push_due = False 
        message = 'No calculation was performed to determine push eligibility. The default is no.'
        minutes_since_last_object = None 

        if not remote_stats:
            remote_stats = self.get_remote_stats([target])

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

            if target['push_strategy'] == PushStrategy.BUDGET_PRIORITY.value:

                average_size = 0
                max_s3_objects = 0
                if len(archives) > 0:
                    average_size = sum([ a['size_kb'] / (1024.0*1024.0) for a in archives ]) / len(archives)
                else:
                    average_size = get_path_uncompressed_size_kb(target['path'], target['excludes']) / (1024.0*1024.0)
                lifetime_cost = average_size * REMOTE_STORAGE_COST_GB_PER_MONTH * 6
                max_s3_objects = math.floor(target['budget_max'] / lifetime_cost)
                if max_s3_objects == 0:
                    push_due = False 
                    message = f'One archive has a lifetime cost of {lifetime_cost}. At a max budget of {target["budget_max"]}, no archives can be stored in S3'
                else:
                    minutes_per_push = (180.0*24*60) / max_s3_objects
                    push_due = current_s3_objects < max_s3_objects and minutes_since_last_object > minutes_per_push
                    message = f'Given a calculated size of {average_size} GB and a budget of ${target["budget_max"]}, a push can be accepted every {time_since(minutes_per_push)} for max {max_s3_objects} objects. It has been {time_since(minutes_since_last_object)} and there are {current_s3_objects} objects.'
            
            elif target['push_strategy'] == PushStrategy.SCHEDULE_PRIORITY.value:
                
                frequency_minutes = frequency_to_minutes(target['frequency'])
                push_due = minutes_since_last_object >= frequency_minutes
                message = f'The push period is {target["frequency"]} ({frequency_minutes} minutes) and it has been {time_since(minutes_since_last_object)}'
            
            elif target['push_strategy'] == PushStrategy.CONTENT_PRIORITY.value:
                
                push_due = True 
                message = f'Content push strategy: any new content justifies remote storage'
                
            else:
                message = f'No identifiable push strategy ({target["push_strategy"]}) has been defined for {target["name"]}.'

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
        return object

    def _delete_objects(self, objs):
        if len(objs) > 0:
            with self.archivebucket(self.bucket_name) as bucket:
                delete_resp = bucket.delete_objects(
                    Delete = {
                        'Objects': [ { 'Key': obj.key } for obj in objs ],
                        'Quiet': False
                    }
                )
                if 'Errors' in delete_resp and len(delete_resp['Errors']) > 0:
                    self.logger.error(f'Delete errors: {",".join([ "%s: %s" % (o["Key"], o["Code"], o["Message"]) for o in delete_resp["Errors"] ])}')
                if 'Deleted' in delete_resp and len(delete_resp['Deleted']) > 0:
                    self.logger.success(f'Delete confirmed: {",".join([ o["Key"] for o in delete_resp["Deleted"] ])}')

    def _object_is_target(self, obj, target_name):
        return (obj.key.find(f'{target_name}/{target_name}_') == 0 or obj.key.find(f'{target_name}_') == 0)

    def _get_remote_stats_for_target(self, target, s3_objects):
        archives_by_last_modified = { obj.last_modified: obj for obj in s3_objects if self._object_is_target(obj, target['name']) }
            
        now = UTC.localize(datetime.utcnow())
        aged = [ archives_by_last_modified[last_modified] for last_modified in archives_by_last_modified if (now - last_modified).total_seconds() / (60*60*24) >= 180 ]
        current_count = len([ last_modified for last_modified in archives_by_last_modified if (now - last_modified).total_seconds() / (60*60*24) < 180 ])

        return { 
            'max_last_modified': max(archives_by_last_modified.keys()) if len(archives_by_last_modified.keys()) > 0 else None, 
            'count': len(archives_by_last_modified),
            'aged': aged,
            'current_count': current_count
        }

    def get_remote_stats(self, targets):
        
        s3_objects = self.get_remote_archives()
        remote_stats = { target['name']: self._get_remote_stats_for_target(target, s3_objects) for target in targets }
        
        return remote_stats 

    def get_remote_archives(self, target_name=None):
            
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
        with self.archivebucket(self.bucket_name) as bucket:
            all_objects = bucket.objects.all()
            objects = [ obj for obj in all_objects if (target_name and self._object_is_target(obj, target_name)) or not target_name ]
        return objects 

    def cleanup_remote_archives(self, remote_stats, dry_run=True):
        if remote_stats['current_count'] > 0:
            self.logger.warning(f'Deleting remote archives aged out: {",".join([ obj.key for obj in remote_stats["aged"] ])}')
            if dry_run:
                self.logger.error(f'DRY RUN -- skipping remote deletion')
            else:
                self._delete_objects(remote_stats["aged"])
