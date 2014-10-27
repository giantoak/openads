#!/bin/bash
echo BEGINNING OPENADS PREPROCESSING SCRIPT
date
OLDCOUNT=$(cat lastcount.output)
NEWCOUNT=$(mysql --host=roxy-db.istresearch.com --port=3306 --user=oculus --password=RrzGuS6s3GaUZ3yB < count.sql | awk 'NR==2')
echo Old ad count $OLDCOUNT
echo New ad count $NEWCOUNT
if [ "$OLDCOUNT" == "$NEWCOUNT" ]; then
  echo SAME AD COUNT. DO NOTHING.
  exit
fi
echo DIFFERENT AD COUNT. UPDATE.
echo $NEWCOUNT > lastcount.output
echo BEGIN PRECLUSTERING
date
./doprecluster.sh > log/precluster.output
echo PRECLUSTERING COMPLETE
date
echo BEGIN CLEAN
./clean.sh > log/clean.output
echo CLEAN COMPLETE
date
echo BEGIN AGGREGATION
./aggregate.sh > log/aggregate.output
echo AGGREGATION COMPLETE
date
echo BEGIN GRAPH LINKS
./graphlinks.sh > log/graphlinks.output
echo GRAPH LINKS COMPLETE
date
