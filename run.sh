#!/bin/bash 

if [ -z "${S3_BUCKET}" ]; then 
  echo "Provide S3_BUCKET"
  exit 1
fi 

OUTFILE=backup_$(date +%Y%m%d_%H%M%S).out
CMD="./backup.py $@"

echo ${CMD} > ${OUTFILE}

export S3_BUCKET
${CMD} | tee -a ${OUTFILE} 2>&1
