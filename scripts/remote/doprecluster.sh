#!/bin/bash
cd /home/ehall/precluster
java -Xmx2048m -cp "/home/ehall/precluster/*" oculus.xdataht.clustering.PrecomputeClusters /home/ehall/precluster/precluster.properties
cd ~
