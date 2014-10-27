#!/bin/bash
cd /home/ehall/precluster
echo BEGIN LOCATION TIME AGGREGATION
date
java -Xmx4096m -cp "/home/ehall/precluster/*" oculus.xdataht.preprocessing.LocationTimeAggregation /home/ehall/precluster/clean.properties
echo END LOCATION TIME AGGREGATION
date
echo BEGIN TIME AGGREGATION
java -Xmx4096m -cp "/home/ehall/precluster/*" oculus.xdataht.preprocessing.TimeAggregation /home/ehall/precluster/clean.properties
echo END TIME AGGREGATION
date
echo BEGIN LOCATION AGGREGATION
java -Xmx4096m -cp "/home/ehall/precluster/*" oculus.xdataht.preprocessing.LocationAggregation /home/ehall/precluster/clean.properties
echo END LOCATION AGGREGATION
date
echo BEGIN LOCATION CLUSTER TABLE CALCULATION
java -Xmx4096m -cp "/home/ehall/precluster/*" oculus.xdataht.preprocessing.LocationCluster /home/ehall/precluster/clean.properties
echo END LOCATION CLUSTER TABLE CALCULATION
date
echo BEGIN AD KEYWORD EXTRACTION
java -Xmx4096m -cp "/home/ehall/precluster/*" oculus.xdataht.preprocessing.AdKeywords /home/ehall/precluster/clean.properties
echo END AD KEYWORD EXTRACTION
date
echo BEGIN CLUSTER DETAILS COMPUTATION
java -Xmx4096m -cp "/home/ehall/precluster/*" oculus.xdataht.preprocessing.ClusterDetails /home/ehall/precluster/clean.properties
echo END CLUSTER DETAILS COMPUTATION
date
cd ~ehall
