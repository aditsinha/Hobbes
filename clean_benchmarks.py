import re
import pdb
import csv

with open("/home/ubuntu/TestDFSIO_results.log", 'r') as myfile:
	data = myfile.read()
	data = data.split('\n\n')
	parsedBenchmarks = []
	for benchmarkIter in data:
		try:
			# pdb.set_trace()
			testType = re.search('-- Hobbes TestDFSIO - : (.*)\n', benchmarkIter).group(1)
			dateTime = re.search('Date & time: (.*)\n', benchmarkIter).group(1)
			numFiles = re.search('Number of files: (.*)\n', benchmarkIter).group(1)
			totalMbytesProcessed = re.search('Total MBytes processed: (.*)\n', benchmarkIter).group(1)
			throughput = re.search('Throughput mb\/sec: (.*)\n', benchmarkIter).group(1)
			averageIO = re.search('Average IO rate mb/sec: (.*)\n', benchmarkIter).group(1)
			ioStdDev = re.search('IO rate std deviation: (.*)\n', benchmarkIter).group(1)
			testExecTime = re.search('Test exec time sec: (.*)', benchmarkIter).group(1)
			parsedBenchmarks.append([testType, dateTime, numFiles, totalMbytesProcessed, float(throughput), averageIO, ioStdDev, testExecTime])
		except:
			continue

	averagedBenchmarks = {}
	print parsedBenchmarks
	for benchmarkIter in parsedBenchmarks:
		# print benchmarkIter[3]
		searchString = "%s-%s" %(benchmarkIter[0].replace(' ', '-'), str(int(float(benchmarkIter[3]))))
		if searchString in averagedBenchmarks:
			averagedBenchmarks[searchString] = averagedBenchmarks[searchString] + benchmarkIter[4]
		else:
			averagedBenchmarks[searchString] = benchmarkIter[4]
	

	with open('hobbes-results-4.csv', 'wb') as csvfile:
		resultsWriter = csv.writer(csvfile, delimiter=';')

		for key, value in averagedBenchmarks.iteritems():
			averagedBenchmarks[key] = averagedBenchmarks[key] / 1.0
			resultsWriter.writerow([key, averagedBenchmarks[key]])

	# print averagedBenchmarks