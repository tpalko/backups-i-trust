##

## development

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
