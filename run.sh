#!/bin/bash 

LOG_FOLDER=${LOG_FOLDER:=/var/log/frankback}

[[ ! -d ${LOG_FOLDER} ]] && mkdir -p ${LOG_FOLDER} && echo "Created log folder ${LOG_FOLDER}"

DATE=$(date +%Y%m%d_%H%M%S)
OUTFILE=${LOG_FOLDER}/backup_${DATE}.out

frankback $@ 2>&1 | tee -a ${OUTFILE}
