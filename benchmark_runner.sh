#!/bin/bash
# declare an array called array and define 3 vales
runVals=( 1 2 4 16 64 256 1024 )
for j in "${runVals[@]}"
do
	for i in "${runVals[@]}"
	do
		$HADOOP_HOME/sbin/stop-all.sh
		$HADOOP_HOME/sbin/mr-jobhistory-daemon.sh stop historyserver
		$HADOOP_HOME/sbin/start-dfs.sh
		$HADOOP_HOME/sbin/start-yarn.sh
		$HADOOP_HOME/sbin/mr-jobhistory-daemon.sh start historyserver
		sleep 15
		hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-2.7.1-tests.jar TestDFSIO -write -nrFiles $i -fileSize $j
		hadoop fs -rm -r -f /tmp/*
	done
done