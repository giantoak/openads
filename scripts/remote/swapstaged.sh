#!/bin/bash
echo BEGINNING SWAP STAGED SCRIPT
date
OLDSWAP=$(cat lastswapped.output)
NEWSWAP=$(mysql --host=roxy-db.istresearch.com --port=3306 --user=oculus --password=RrzGuS6s3GaUZ3yB < getstaged.sql | awk 'NR==2')
echo Old staged $OLDSWAP
echo New staged $NEWSWAP
if [ "$OLDSWAP" == "$NEWSWAP" ]; then
  echo SAME STAGED FILE. DO NOTHING.
  exit
fi
echo DIFFERENT STAGED FILE. UPDATE.
echo $NEWSWAP > lastswapped.output

date
