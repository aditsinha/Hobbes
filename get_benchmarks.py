from subprocess import Popen, PIPE
import re
import pdb
import time


def initRunner():
	print "Setting up for run"
	p = Popen(['/usr/local/hadoop/sbin/stop-all.sh'], stdin=PIPE, stdout=PIPE, stderr=PIPE)
	output, err = p.communicate(b"input data that is passed to subprocess' stdin")
	p = Popen(['/usr/local/hadoop/sbin/mr-jobhistory-daemon.sh', 'stop', 'historyserver'], stdin=PIPE, stdout=PIPE, stderr=PIPE)
	output, err = p.communicate(b"input data that is passed to subprocess' stdin")
	p = Popen(['/usr/local/hadoop/sbin/start-dfs.sh'], stdin=PIPE, stdout=PIPE, stderr=PIPE)
	output, err = p.communicate(b"input data that is passed to subprocess' stdin")
	p = Popen(['/usr/local/hadoop/sbin/start-yarn.sh'], stdin=PIPE, stdout=PIPE, stderr=PIPE)
	output, err = p.communicate(b"input data that is passed to subprocess' stdin")
	p = Popen(['/usr/local/hadoop/sbin/mr-jobhistory-daemon.sh', 'start', 'historyserver'], stdin=PIPE, stdout=PIPE, stderr=PIPE)
	output, err = p.communicate(b"input data that is passed to subprocess' stdin")
	print "Finihsed setup for run"
	time.sleep(20)

benchmark_results = open('benchmark_results.txt', 'a')
# benchmark_results.write("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" %("File Size", "DateTime", "Num Files", "Total MBytes Processed", "Throughput mb/sec", "Average IO Rate mb/sec", "IO Rate Std Dev", "Test Exec Time"))
# for numFiles in [1, 4, 16, 64, 256, 512, 1024]:
	# for fileSize in [1, 4, 16, 64, 256, 512, 1024]:
	#[16, 64], 
for combination in [[16, 256], [16, 512], [16, 1024], [64, 1], [64, 4], [64, 16], [64, 64], [64, 256], [64, 512], [64, 1024], [256, 1], [256, 4], [256, 16], [256, 64], [256, 256], [256, 512], [256, 1024], [512, 1], [512, 4], [512, 16], [512, 64], [512, 256], [512, 512], [512, 1024], [1024, 1], [1024, 4], [1024, 16], [1024, 64], [1024, 256], [1024, 512], [1024, 1024]]:
	if int(combination[0])*int(combination[1]) > 2100:
		print "Skipping since total memory needed exceeds 5 gigabytes. This test needs %sMB" %(str(int(combination[0])*int(combination[1])))
		continue
	initRunner()
	p = Popen(['hadoop', 'jar', '/home/ubuntu/hadoop-2.7.1-src/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-jobclient/target/hadoop-mapreduce-client-jobclient-2.7.1-tests.jar', 'TestDFSIO', '-write', '-nrFiles', str(combination[0]), '-fileSize', str(combination[1])], stdin=PIPE, stdout=PIPE, stderr=PIPE)
	output, err = p.communicate(b"input data that is passed to subprocess' stdin")
	try:
		dateTime = re.search('Date & time: (.*)\n', err).group(1)
		numFiles = re.search('Number of files: (.*)\n', err).group(1)
		totalMbytesProcessed = re.search('Total MBytes processed: (.*)\n', err).group(1)
		throughput = re.search('Throughput mb\/sec: (.*)\n', err).group(1)
		averageIO = re.search('Average IO rate mb/sec: (.*)\n', err).group(1)
		ioStdDev = re.search('IO rate std deviation: (.*)\n', err).group(1)
		testExecTime = re.search('Test exec time sec: (.*)\n', err).group(1)
		print "Processed test case with %s files of size %sMb" %(str(combination[0]), str(combination[1]))
		# print [dateTime, numFiles, totalMbytesProcessed, throughput, averageIO, ioStdDev, testExecTime]
		benchmark_results.write("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" %(str(combination[0]), dateTime, numFiles, totalMbytesProcessed, throughput, averageIO, ioStdDev, testExecTime))
		benchmark_results.flush()
	except:
		print err
		print "ERRORED ON %s %s" %(str(combination[0]), str(combination[1]))
	p = Popen(['hadoop', 'jar', '/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-2.7.1-tests.jar', 'TestDFSIO', '-clean'], stdin=PIPE, stdout=PIPE, stderr=PIPE)
	output, err = p.communicate(b"input data that is passed to subprocess' stdin")
	p = Popen(['hadoop', 'fs', '-rm', '-r', '-f', '/tmp/*'], stdin=PIPE, stdout=PIPE, stderr=PIPE)
	output, err = p.communicate(b"input data that is passed to subprocess' stdin")
	p = Popen(['sudo', 'sync'], stdin=PIPE, stdout=PIPE, stderr=PIPE)
	output, err = p.communicate(b"input data that is passed to subprocess' stdin")
	p = Popen(['sudo', 'echo', '3', '|', 'sudo', 'tee', '/proc/sys/vm/drop_caches'], stdin=PIPE, stdout=PIPE, stderr=PIPE)
	output, err = p.communicate(b"input data that is passed to subprocess' stdin")
benchmark_results.close()




