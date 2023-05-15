##

## deploy

| make rule | result | use case |
|---|---|---|
| `make install` | `/usr/local/bin/bckt` | static install, normal use |
| `make link-install` | `/usr/local/bin/bckt -> src/backup.py` | editable, use-in-development | 

### Deployment Caveats

* `frank-common` and `cowpy` are both required dependencies, although these projects aren't formally distributed. See the table below for installation instructions.

### database 

MariaDB [(none)]> create user if not exists bckt identified by 'bckt';
MariaDB [(none)]> create database if not exists bckt;
MariaDB [(none)]> grant all privileges on bckt.* to bckt;

## running development notes

5/5/23

target list, need to see:
* default sort is 'last archive timestamp'
* don't show paused targets, but show count of paused targets, maybe shortname list in status block
* see 'last result at' - timestamp of the last attempt
* see 'last result' - whatever resulted from the last run: archive created, nothing new, not scheduled
* see 'last reason' - the reason for whatever resulted from the last run
* some visibility on include/exclude, we want to know if we're missing important stuff and if we're capturing too much 
* what would happen per target if run now: would backup, would check, nothing <-- a check can be triggered manually and results cached, to allow "would backup"
    * maybe this is "not scheduled", "scheduled", and "ready" (basicaly implying scheduled.. we wouldn't know to be in ready state if not scheduled)

make clear the various flows: 
    - not scheduled: last archive is within the target's defined backup period
    - scheduled: due to check for new files 
    - ready: as of the last check, there are new files
    
    - a target can be attempted at any time, but its frequency will limit whether it will even try to determine if there are new files
    - if a target's frequency determines it should be checked, new files will be looked for

tasks:
    - fix sorting interface
    - filter out paused targets, include names in status block
    - add 'last_result_at' and 'last_result' columns, populate appropriately -- "result of last scheduled attempt" NOT "not scheduled"
        - last result (enum): created archive, nothing new, failed
        - last reason (text): generally the specific error if result is failed (should be "last error" ??)
        - last result at: careful, different than 'last archive at', though will be the same if 'last result' is 'created archive'.. this is basically 'last time we attempted and the target was actually scheduled'
    - codify ephemeral field 'status', shown in target list either color coded or displayed explicitly: not scheduled, scheduled, new files
    - add # files/size of exclude, to complement 'last archive size'
    - tweak target list to show necessary columns: 'last attempt' columns, 'exclude details' columns, 'status' column

track time elapsed to backup an archive
BUG: marker timestamps aren't getting set in the database, causing many unnecessary, enormous backups
programmatic status output, oldest or newest error reason
migrate to frank.database 
better help.. show actual parameters for each command .. normal help stuff 
cleaning up orphaned archives 
install dependencies cowpy + frank-common

4/20/23

lock file to block concurrent runs from crontab 
audit table, or "last reason"
at least one remote archive regardless of budget, under a max threshold
splitting option.. analyze a folder to determine how to break into multiple targets 
choose between multiple storage classes, per target 
estimate actual compressed size, not on-disk, for projection
use excludes when calculating projected size 


2/3/23

last archive GB doesn't show actual folder size, so we can't tell if space is a problem 
why have 2022 pics not been archived in 153 days? 
.MOV still being excluded from pics folders.. 
need to move to not deep/cold glacier storage - more accessible.. maybe selectable per target.. or the bucket itself is selectable
list archive shows a ton of 'remote only' archives.. is this accurate? are the #'s of local/remote on target list accurate?
need a one-liner 'status' for the window manager action bar to show 1) most stale backup 2) reason for most stale backup failure 3) highest costing backup and maybe 4) most stale remote AND everywhere
better --help, more standardized flags, manpage maybe 
see last timestamp observed for each target 
excludes to show - instead of None 
maybe take another look at this columnizer thing.. use third party?

6/28/22

- internalize /var/log logging (not wrapped in shell script)
- get away from using marker files as timeline placeholders

5/11/22

- track archive size, on disk size, average compression rate, and use this instead of size on disk for space + cost calculations 
- fix s3 pull, do we need to code in object restore with boto, deep archive storage requires restore init 
- include start/stop timings in header/post-header?

5/3/22

`version-py.js` and `.versionrc.js` also probably related to `standard-version` supporting `setup.cfg`
as a bump target.

Should have a `.frankbackrc` sample file, and document `FRANKBACK_RC_FILE` usage.

3/5/22

needs work for situational awareness 
- more clear view of what would happen on a run (new files and would push are good, but expensive to calculate and not very visible)
- missing info on expired remote archives
- total estimated cost per target 
- average archive size per target 
- time since last archive, remote push per target

seeing if new files are present should factor in excludes
is it possible to use glob to filter out new files found
also use glob to get a better estimate of archive size 
maybe.. even use compression history for the target path to estimate actual archive space needs 

12/30/21

DC targets list -literally anything- will parse to full=True 
DC the function has a 'full' parameter, and we should be able to access this by name 
DC or at least parse something like -f or -a into a boolean for this parameter - how to genericize?

11/17/21:
* DONE direct command line disk cleanup / remote prune

11/16/21: 

Many of the items below (9/9/21) may be done.. 

* FLOATED report of filtered/non-backed-up files 
* FLOATED handling of orphaned files 
    - these aren't addressed by runtime cleanup 
    - generally, how do local orphans happen? remote? is this a bug or old code?

done:
* rearrange cleanup/run workflow to do local cleanup across all targets to make room for the one archive, and do remote cleanup prior to the main run

9/9/21:

* halt/interrupt handling - must Ctrl-C for each target 
* main try/except block needs to manage output streams better, only prints traceback to tee -a target 
* tee -a seems to not flush?
* fix remote cleanup for sensible defaults (always leave at least one archive per target)
* DONE? what? fix target listing local/remote counts to show how many local and remote and total
* DONE? target listing to show relevant stats about archives, status, cost
* EHH fix command handling to be more maintainable, documented, standard 
* GOOD Q what other reports, listings are useful?
* DONE target enabled flag
* DONE command-line support for tweaking and editing targets 

# Appendix A: old stuff 

 # def init_commands(self):
    #     '''
    #     bckt db init
    #     bckt info 
    #     bckt target add <path> [-n NAME] [-f FREQUENCY] [-b BUDGET] [-e EXCLUDES]
    #     bckt target edit <TARGET NAME> [-f FREQUENCY] [-b BUDGET] [-e EXCLUDES]
    #     bckt target pause|unpause <TARGET NAME>
    #     bckt target list [TARGET NAME]
    #     bckt run [TARGET NAME]
    #     bckt target run <TARGET NAME>
    #     bckt target push <TARGET NAME>
    #     bckt archive list [TARGET NAME]
    #     bckt archive prune 
    #     bckt archive aggressive-prune 
    #     bckt archive restore <ID>
    #     globals:
    #         set log level:  -l <LOG LEVEL>
    #         set quiet:      -q
    #         set verbose:    -v
    #         set dry run:    -d
    #         set no headers: --no-headers
    #     '''
        
    #     return {
    #         'db init': self.initialize_database,
    #         'info': self.print_header,
    #         'target add': self.create_target,
    #         'target edit': self.edit_target,
    #         'target info': self.target_info,
    #         'target pause': self.pause_target,
    #         'target unpause': self.unpause_target,
    #         'target list': self.print_targets,
    #         'run': self.run,
    #         'target run': self.add_archive,
    #         'target push': self.push_target_latest,
    #         'archive list': self.print_archives,
    #         'archive last': self.print_last_archive,
    #         'archive prune': self.prune_archives,
    #         'archive aggressive-prune': self.prune_archives_aggressively,
    #         'archive restore': self.restore_archive,
    #         'fixarchives': self.db.fix_archive_filenames,
    #         'help': self.print_help
    #     }