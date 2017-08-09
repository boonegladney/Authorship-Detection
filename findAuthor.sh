#!/bin/bash

$HADOOP_HOME/bin/hadoop fs -rm -r /DEMO;
$HADOOP_HOME/bin/hadoop jar HW2-P3.jar MainClass /dataLoc/$1 /TestOut/MAXIDFOUT /DEMO/ARBAAVOUT /TestOut/TFIDFOUT /DEMO/COSDIST1OUT /DEMO/COSTDIST /DEMO/TOP10;
echo "DONE!"
