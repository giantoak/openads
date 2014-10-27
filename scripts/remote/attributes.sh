#!/bin/bash
cd ~memex/oculus
echo BEGIN ATTRIBUTES DETAILS CALCULATION
date
java -Xmx6144m -cp "/home/memex/oculus/*" oculus.memex.attributes.AttributeDetails /home/memex/oculus/db.properties
echo END ATTRIBUTES DETAILS CALCULATION
date
cd ~memex
