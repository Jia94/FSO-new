#!/usr/bin/env python3

import argparse
from glob import glob
import os
import re
import pendulum

def parse_time(string):
	match = re.match(r'(\d{4}\d{2}\d{2})(\d{2})?(\d{2})?', string)
	if not match:
		print('[Error]: Invalid time format (YYYYMMDD[HH][MM])!')
		exit(1)
	if match.group(1) and match.group(2) and match.group(3):
		return pendulum.from_format(string, '%Y%m%d%H%M')
	elif match.group(1) and match.group(2):
		return pendulum.from_format(string, '%Y%m%d%H')
	elif match.group(1):
		return pendulum.from_format(string, '%Y%m%d')

parser = argparse.ArgumentParser(description='Convert Little-R data.')
parser.add_argument('-t', '--time', help='Download data at this time (YYYYMMDDHH[MM]).', type=parse_time)
args = parser.parse_args()

raw_root = '/home/data/raw'

file_list = ''

if args.time:
   time = args.time
else:
#	time = pendulum.now(0).set(minute=0, second=0)
	time = pendulum.now(0)

hour1 = time.subtract(hours=6)
hour2 = time.subtract(hours=5) ##time-4hr
hour3 = time.subtract(hours=4)

# SURF_CHN_MAIN_MIN
for file in glob('{}/cimiss/SURF_CHN_MAIN_MIN/{}*'.format(raw_root, hour1.format('%Y%m%d%H'))):
	file_list += ':' + file

for file in glob('{}/cimiss/SURF_CHN_MAIN_MIN/{}*'.format(raw_root, hour2.format('%Y%m%d%H'))):
	file_list += ':' + file

# UPAR_CHN_MUL_FTM
for file in glob('{}/cimiss/UPAR_CHN_MUL_FTM/{}*'.format(raw_root, hour2.format('%Y%m%d%H'))):
	file_list += ':' + file

# Wind profilers
# Z_RADA_G7190_WPRD_MOC_NWQC_ROBS_LC_QI_20180603114800.TXT
for file in glob('{}/profiler/{}/*'.format(raw_root, hour1.format('%Y%m%d'))):
	file_time = pendulum.from_format(file.split('_')[9].split('.')[0], '%Y%m%d%H%M%S')
	file = file.replace(' ', '\ ')
	file_type =file.split('_')[6]
	if file_type=='HOBS':
		if file_time >= hour1 and file_time < hour2:
			os.system('dos2unix {}'.format(file))
			file_list += ':' + file

for file in glob('{}/profiler/{}/*'.format(raw_root, hour2.format('%Y%m%d'))):
	file_time = pendulum.from_format(file.split('_')[9].split('.')[0], '%Y%m%d%H%M%S')
	file = file.replace(' ', '\ ')
	file_type =file.split('_')[6]
	if file_type=='HOBS':
		if file_time >= hour2 and file_time < hour3:
			os.system('dos2unix {}'.format(file))
			file_list += ':' + file

output_root = '/home/data/raw/little_r/{}'.format(hour2.format('%Y%m%d%H'))
if not os.path.isdir(output_root):
	os.makedirs(output_root)

os.system('/home/data/raw/little_r/convert_cimiss_2_littler.exe -i {} -o {}/ob.ascii'.format(file_list, output_root))
