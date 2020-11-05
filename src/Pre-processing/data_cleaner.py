import csv
import sys
import json
import math
import time


def extractor(file_name):
	try:
		with open(file_name) as f:
			data = json.load(f)

			res = []
			
			for element in data:
				temp =[]
				epoch = element['_source']['layers']['frame']['frame.time_epoch']
				trunc_time = math.trunc(float(epoch))
				temp.append(trunc_time)
				temp.append(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(trunc_time)))
				temp.append(element['_source']['layers']['frame']['frame.len'])
				temp.append(element['_source']['layers']['frame']['frame.protocols'][14:])
				temp.append(element['_source']['layers']['eth']['eth.src'])
				temp.append(element['_source']['layers']['eth']['eth.dst'])
				res.append(temp)

		return res

	except IOError:
		print('file not found')

def list2csv(data):
	with open('output.csv', 'w') as f:
		write = csv.writer(f)

		write.writerows(data)

if __name__ == '__main__':
	file = sys.argv[1]
	data = extractor(file)
	list2csv(data)