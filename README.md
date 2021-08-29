## aws backup 

Generate local backup archives for a target. Strategically sync archives to an S3 bucket while factoring in cost, changeset (are there files to backup), and sensitivity (impact of unarchived files).

The finer-grained the targets, the more control is given to the above considerations. A single target for an entire system will not benefit from optimizing cost for more valuable files (and having more recent changes in a remote location) and avoiding the generation of and paying storage for new archives when close to all of the content is duplicated in older archives.

not every archive goes to S3, but some archive may be pushed to S3 on any given run based on budget and recency
Whether an archive is generated on a run and whether an archive is pushed to S3 on a run are independent.

A backup run is performed for each target at a steady frequency. This frequency varies per target and will be 
something like hourly, daily, weekly, etc.

On each run, two separate, independent actions are performed:
* determine if an archive is to be generated and if so, generate an archive
* determine if an archive should be pushed to S3 and if so, push an archive to S3

### scheduling 

The executable program expects to be backup central for all backup targets and be aware of them 
primarily for budget considerations. Aside from a shared financial pool from which the targets 
partake, they operate independently: any decisions made about when or how to generate an archive
can be made in isolation. This means that ignoring global budget considerations, this program can be 
the top-level callable from any scheduling system where each target has its own
entry and completely independent scheduling, such as cron-entry-per-target. If global budgeting
is to work, however, this program should be the sole entry on a fairly frequent schedule, hourly
or daily at most. When it runs, it runs for all targets, but not all will actually do something
on each run. Disabling global budgeting also removes some other dependencies as described later.

change    change 
impact    frequency 
---------------------
low       low       : weekly 
low       high      : daily 
high      low       : daily 
high      high      : hourly 

### determining if an archive will be generated 

Despite backup run frequency, not every run results in an archive.
All created archives go to spare local disk.

* find files newer than pre-archive marker, if not, bail 
* log new files and their mod timestamps along with pre- and post- archive marker timestamps
* touch pre-archive marker 
* archive files for backup 
* touch post-archive marker 

### determining if a push to S3 will occur

An S3 push depends on several factors:

* if there are unpushed archives (content not remotely backed up)
* if there are dollars to support additional archive storage in S3 after archives older than 180 days are deleted 

To account for these considerations, we do some calculations:

* calculate the average archive size 
* determine GDA 180-day storage cost for the archive (OHEDSC, "o-HED-sic")
* calculate budget dollars allocated to this backup target based on its archive's proportional size over all archive sizes (BDABT, "BEE-dab")
* determine number of allowed archives in S3 for this target by: BDABT $ / OHEDSC $ (AAIT, "AYY-it")
* calculate the backup period (180 * OHEDSC $) / BDABT $ or 180 / AIIT
* if there is at least one archive < 180 days old present for a target, delete all archives >= 180 days old 
* determine if the number of existing archives in S3 is less than AAIT
* determine if there is content not yet remotely backed up (an unpushed local archive newer than the latest archive in S3)

After all this, if there is space (budget) in S3 and we have a newer archive to push, we push the latest.
If there is no space in S3 but we have a newer archive to push, it gets a little hairy, but we can determine on a per-target basis 
how important this is. Because we should only be pushing as frequently as the budget says we should, there 
will generally be space available _at that frequency_. i.e. If the budget says every 30 days, then we should push every 30 days,
and every 30 days an archive will fall off the 180-day expiration. That said, if a new archive is generated that really should 
go into S3 before the target period is reached, an exception can be made for one more archive spot, depending on the cost. But
generally there will always be a tail of some content not backed up remotely, just as there is always a tail of some content
not yet captured by a backup at all.

### cost research

s3
  storage: 0.023*50 + 0.022*450 + 0.021*2200
glacier 
  storage: 0.004 / GB / month, 90 days minimum
  
glacier deep archive  
  
  storage: 0.00099 / GB / month, 180 days minimum
  requests: 
    0.05 / 1000 PUT, COPY, POST, LIST 
    0.0004 / 1000 GET, SELECT
  retrieval: 
    0.02 / GB standard
    0.0025 / GB bulk

2700 GB 

S3: $57.25 / month  

GDA
  single full backup:                2.67
  one backup per month in 180 days: 16.04 

G: $10.80 / month
 x one backup per month in 90 days = $32.40 / month 

# Appendix A: old notes and code 

exit 0


echo "Backing up development projects.."
ARCHIVE_FILENAME=development_projects_$(date +%Y%m%d_%H%M%S).tar.gz
EXCLUDE_INCLUDE="--exclude \"*/minecraft/server/world/*\" --exclude \"*/node_modules/*\""
ARCHIVE_CMD="tar ${EXCLUDE_INCLUDE} -czf ${WORKING_FOLDER}/${ARCHIVE_FILENAME} /media/storage/development/projects"
echo "${ARCHIVE_CMD}"
time ${ARCHIVE_CMD} \
  && aws s3 cp ${WORKING_FOLDER}/${ARCHIVE_FILENAME} s3://${S3_ARCHIVE_BUCKET} 
  #&& rm ${WORKING_FOLDER}/${ARCHIVE_FILENAME}

exit 0

#aws glacier list-vaults --account-id - --region us-east-1
#aws s3 rm --recursive s3://${S3_BACKUP_BUCKET}

#48000 files
#600 G uncompressed
for YEAR in {2015..2021}; do 
  echo "Backing up ${YEAR}.."
  ARCHIVE_FILENAME=pics_${YEAR}_$(date +%Y%m%d_%H%M%S).tar.gz
  time tar -czvf ${ARCHIVE_FILENAME} -C ${WORKING_FOLDER} /media/storage/pics/${YEAR} \
    && aws s3 cp ${WORKING_FOLDER}/${ARCHIVE_FILENAME} s3://${S3_ARCHIVE_BUCKET} \
    && rm ${WORKING_FOLDER}/${ARCHIVE_FILENAME}
  # EXCLUDE_INCLUDE="--exclude \"*\" --include \"${YEAR}/*\""
  # time aws s3 sync --no-follow-symlinks --delete --force-glacier-transfer \
  #   ${EXCLUDE_INCLUDE} \
  #   /media/storage/pics s3://${S3_BACKUP_BUCKET}/pics
done 

# 22270 files/folders 
# 1.1 G compressed size 
# 1.6 G uncompressed size 
# 
# echo "Backing up home.."
# ARCHIVE_FILENAME=home_tpalko_$(date +%Y%m%d_%H%M%S).tar.gz
# SOURCE_FOLDER=/home/debian/tpalko
# EXCLUDE_INCLUDE="--exclude .steam \
# --exclude .adobe \
# --exclude Android \
# --exclude .asdf \
# --exclude .atom \
# --exclude .AndroidStudio3.3 \
# --exclude .BuildServer \
# --exclude .cache \
# --exclude .config \
# --exclude Downloads \
# --exclude go \
# --exclude .google \
# --exclude .kde \
# --exclude .local \
# --exclude .minikube \
# --exclude .npm \
# --exclude .npm-global \
# --exclude .thumbnails \
# --exclude .vagrant.d \
# --exclude .virtualenv"
# time tar ${EXCLUDE_INCLUDE} -czvf ${WORKING_FOLDER}/${ARCHIVE_FILENAME} ${SOURCE_FOLDER} \
#   && aws s3 cp ${WORKING_FOLDER}/${ARCHIVE_FILENAME} s3://${S3_ARCHIVE_BUCKET} \
#   && rm ${WORKING_FOLDER}/${ARCHIVE_FILENAME}
# time aws s3 sync --no-follow-symlinks --delete --force-glacier-transfer \
#   ${EXCLUDE_INCLUDE} \ 
#   /home/debian/tpalko s3://${S3_BACKUP_BUCKET}/home/tpalko



# 128840 files/folders 
# 7.2 G compressed size 
echo "Backing up development.."
ARCHIVE_FILENAME=development_$(date +%Y%m%d_%H%M%S).tar.gz
EXCLUDE_INCLUDE="--exclude \"rpi/images/*\" \
--exclude \"*/node_modules/*\" \
--exclude \"clients/riproad/*\" \
--exclude \"thirdparty/exclude/*\" \
--exclude \"*.log\" \
--exclude \"boxes/*\""
time tar ${EXCLUDE_INCLUDE} -czvf ${WORKING_FOLDER}/${ARCHIVE_FILENAME} /media/storage/development \
  && aws s3 cp ${WORKING_FOLDER}/${ARCHIVE_FILENAME} s3://${S3_ARCHIVE_BUCKET} \
  && rm ${WORKING_FOLDER}/${ARCHIVE_FILENAME}
# time aws s3 sync --no-follow-symlinks --delete --force-glacier-transfer \
#   ${EXCLUDE_INCLUDE} \
#   /media/storage/development s3://${S3_BACKUP_BUCKET}/development 


# many files created once 
# few files updated or created daily  
# 817 G total uncompressed 
# 240K objects 
# 
# S3 standard 
# S3 IA 
# Glacier 
# Glacier Deep Archive 
# 
# 3/8
#   initial upload 
#     March 8, 2021, 23:48:09 (UTC-05:00) - March 9, 2021, 01:09:33 (UTC-05:00)
#     bucket lifecycle added to transfer to Glacier Deep Archive, however the additional cost/scope confirmations were not accepted 
# 3/10
#   second sync (took approx. <1 hr)
#     March 10, 2021, 00:11:03 (UTC-05:00)
#     - included --force-glacier-transfer 
# 3/11
#   all objects confirmed as Glacier Deep Archive storage class 
# 3/12 
#   archive upload of home.tar.gz 
#     March 12, 2021, 20:51:53 (UTC-05:00)
# 
# S3 Standard 
# upload  = 0.005 per 1K requests 
# storage = 0.023 per GB per month (first 50 TB)
# 
# Glacier Deep Archive 
# upload  = 0.05 per 1K PUT/transition requests 
# 0.00099 per GB per month 
# 
# upload to S3    = 0.005*240             =  1.20
# storage in S3   = (0.023 * 817) / 30    =  0.63
# transfer to DA  = 0.05*240              = 12.00
# storage in DA   = (0.00099 * 817) / 30  =  0.03
#                                           13.86 + 0.03 daily 
# 
# 3/8 = 0.68
#   usage
#   - requests tier 1   = 0.68
#   operation 
#   - putobject         = 0.32
#   - uploadpart        = 0.33 
#   - initiatemultipart = 0.02
#   - completemultipart = 0.01
# 3/9 = 3.29
#   usage 
#   - requests tier 1   = 1.02
#   - requests tier 3   = 2.24
#   operation 
#   - s3-gdatransition  = 2.24
#   - putobject         = 0.87
#   - uploadpart        = 0.14
#   - da-Storage        = 0.03 
# 3/10 = 6.94
#   usage 
#   - requests tier 1   = 0.00
#   - requests tier 3   = 6.91
#   - timedstorage-gda  = 0.03
#   operation 
#   - s3-gdatransition  = 6.91
#   - da-storage        = 0.03
# 3/11 = 0.05
#   usage 
#   - requests tier 3   = 0.03 
#   - timedstorage-gda  = 0.03 
#   operation 
#   - s3-gdatransition  = 0.03 
#   - da-storage        = 0.03
# 3/12 = 0.03 
#   usage 
#   - timedstorage-gda  = 0.03
#   operation 
#   - da-storage        = 0.03 
# 3/13 
#   usage 
#   - 
# 3/14
#   usage 
#   - 
