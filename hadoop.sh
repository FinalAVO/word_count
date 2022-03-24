#!/bin/sh

hadoop fs -rm -R /input

hadoop fs -rm -R /output

hadoop fs -put /analy1/result/${2}_wc.csv /input

timestamp=`date "+%Y%m%d%H%M"`

hadoop jar /usr/hadoop/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.10.1.jar wordcount /input /output

hadoop fs -get /output/part-r-00000 /analy1/result/${2}_${timestamp}

echo "$timestamp"
