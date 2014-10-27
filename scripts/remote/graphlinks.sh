#!/bin/bash
cd /home/ehall/precluster
echo BEGIN PRECALCULATING GRAPH LINKS
date
java -Xmx4096m -cp "/home/ehall/precluster/*" oculus.xdataht.preprocessing.ClusterLinks /home/ehall/precluster/clean.properties
echo END PRECALCULATING GRAPH LINKS
date
cd ~

