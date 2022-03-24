#!/bin/sh

hadoop fs -rm -R /input

hadoop fs -rm -R /output

hadoop fs -put /word_count/result/${1}_wc.csv /input

timestamp=`date "+%Y%m%d%H%M"`

hadoop jar /usr/hadoop/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.10.1.jar wordcount /input /output

hadoop fs -get /output/part-r-00000 /word_count/result/${1}_${timestamp}

echo "$timestamp"
