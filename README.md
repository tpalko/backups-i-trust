##

## development

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

targets list -literally anything- will parse to full=True 
the function has a 'full' parameter, and we should be able to access this by name 
or at least parse something like -f or -a into a boolean for this parameter - how to genericize?

11/17/21:
* direct command line disk cleanup / remote prune

11/16/21: 

Many of the items below (9/9/21) may be done.. 

* report of filtered/non-backed-up files 
* handling of orphaned files 
    - these aren't addressed by runtime cleanup 
    - generally, how do local orphans happen? remote? is this a bug or old code?

done:
* rearrange cleanup/run workflow to do local cleanup across all targets to make room for the one archive, and do remote cleanup prior to the main run

9/9/21:

* halt/interrupt handling
* main try/except block needs to manage output streams better, only prints traceback to tee -a target 
* tee -a seems to not flush?
* fix remote cleanup for sensible defaults (always leave at least one archive per target)
* what? fix target listing local/remote counts to show how many local and remote and total
* target listing to show relevant stats about archives, status, cost
* fix command handling to be more maintainable, documented, standard 
* what other reports, listings are useful?
* DONE target enabled flag
* DONE command-line support for tweaking and editing targets 
