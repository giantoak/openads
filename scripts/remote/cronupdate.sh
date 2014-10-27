#!/bin/sh
cd ~ehall
LOGFILE=log/log`date +%Y%m%d%H`.log
./update.sh > $LOGFILE
