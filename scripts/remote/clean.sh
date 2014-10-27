#!/bin/bash
cd /home/ehall/precluster
echo BEGIN CLEAN COLUMN
date
java -Xmx1024m -cp "/home/ehall/precluster/*" oculus.xdataht.clustering.CleanColumn /home/ehall/precluster/clean.properties
echo END CLEAN COLUMN
date
echo BEGIN FIX WASHINGTON
java -Xmx1024m -cp "/home/ehall/precluster/*" oculus.xdataht.preprocessing.FixWashington /home/ehall/precluster/clean.properties
echo END FIX WASHINGTON
date
cd ~

