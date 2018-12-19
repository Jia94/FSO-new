#!/usr/bin/env python3.6

import subprocess
import argparse
import pendulum
import math
import re
import os
import psutil
import pygrib
import shutil

if not os.getenv('GFS_SFTP_HOST'):
	print('[Error]: GFS_SFTP_HOST is not set!')
	exit(1)
if not os.getenv('GFS_SFTP_USER'):
	print('[Error]: GFS_SFTP_USER is not set!')
	exit(1)

time_interval = pendulum.Interval(hours=6)

def parse_date(string):
	match = re.match(r'(\d{4}\d{2}\d{2}\d{2})(\d{2})?', string)
	if match.group(2):
		return pendulum.from_format(string, '%Y%m%d%H%M')
	else:
		return pendulum.from_format(string, '%Y%m%d%H')

def parse_date_range(string):
	match = re.match(r'(\d{4}\d{2}\d{2}\d{2})-(\d{4}\d{2}\d{2}\d{2})', string)
	if not match:
		raise argparse.ArgumentError('"' + string + '" is not a date range (YYYYMMDDHH)!')
	return (pendulum.from_format(match.group(1), '%Y%m%d%H'), pendulum.from_format(match.group(2), '%Y%m%d%H'))

def parse_forecast(string):
	match = re.match(r'(\d+)-(\d+)\+(\d+)', string)
	if match:
		start = int(match.group(1))
		end = int(match.group(2))
		step = int(match.group(3))
		return [x for x in range(start, end + step, step)]
	match = re.findall(r'(\d+):?', string)
	if match:
		return [int(match[i]) for i in range(len(match))]

parser = argparse.ArgumentParser(description='Download GFS data.')
parser.add_argument('-o', '--root-dir', dest='root_dir', default='.', help='Root directory to store GFS data.')
parser.add_argument('-d', '--date', help='Download data at this date (YYYYMMDDHH).', type=parse_date)
parser.add_argument('-r', '--date-range', dest='date_range', help='Download data in this date range (YYYYMMDDHH-YYYYMMDDHH).', type=parse_date_range)
parser.add_argument('-f', '--forecast', help='Download forecast hours (HH-HH+XX).', type=parse_forecast)
parser.add_argument('--resolution', help='Set data resolution (1p00, 0p50, 0p25).', default='0p25')
parser.add_argument('-c', '--cycle-period', dest='cycle_period', help='Set LAPS analysis cycle period in seconds.', type=int, default=3600)
parser.add_argument('--cycle-number', dest='cycle_number', help='Set how many cycles that are needed.', type=int, default=6)
parser.add_argument('--remote-root', dest='remote_root', help='Set remote root of GFS data.', default='/home/rtrr/com/now/prod')
args = parser.parse_args()

args.root_dir = os.path.abspath(args.root_dir)
if not os.path.isdir(args.root_dir): os.makedirs(args.root_dir)

host = '{}@{}'.format(os.getenv('GFS_SFTP_USER'), os.getenv('GFS_SFTP_HOST'))

def is_downloading(file_path):
	for pid in psutil.pids():
		p = psutil.Process(pid)
		if file_path in p.cmdline():
			return True
		else:
			return False

def check_file_size(remote_path, local_path):
	res = subprocess.run(['ssh', host, 'ls -l {}'.format(remote_path)], stdout=subprocess.PIPE)
	return ' {} '.format(os.path.getsize(local_path)) in res.stdout.decode('utf-8')

def download_gfs(date, forecast):
	dirname = 'gfs.{}'.format(date.format('%Y%m%d%H'))
	file_name = 'gfs.t{:02d}z.pgrb2.{}.f{:03d}'.format(date.hour, args.resolution, forecast)
	if not os.path.isdir('{}/{}'.format(args.root_dir, dirname)):
		print('[Notice]: Make directory {}/{}.'.format(args.root_dir, dirname))
		os.makedirs('{}/{}'.format(args.root_dir, dirname))
	if not os.path.isdir('{}/{}/.backup'.format(args.root_dir, dirname)):
		os.makedirs('{}/{}/.backup'.format(args.root_dir, dirname))
	print('[Notice]: Downloading {}.'.format(file_name))
	file_path = '{}/{}/.backup/{}'.format(args.root_dir, dirname, file_name)
	if is_downloading(file_path):
		print('[Error]: {} is being downloaded! Try to rerun later.'.format(file_path))
		exit(1)
	if os.path.isfile(file_path):
		if check_file_size('{}/{}/{}'.format(args.remote_root, dirname, file_name), file_path):
			print('[Warning]: File {} exists.'.format(file_path))
			return
		else:
			# File is not downloaded completely.
			print('[Error]: {} is downloaded, but not completed.'.format(file_name))
			exit(1)
			# os.remove(file_path)
	subprocess.call(['scp', '{}:{}/{}/{}'.format(host, args.remote_root, dirname, file_name), file_path])
	if not check_file_size('{}/{}/{}'.format(args.remote_root, dirname, file_name), file_path):
		print('[Error]: Failed to download {}'.format(file_name))
		os.remove(file_path)
		exit(1)
	elif forecast <= 1:
		shutil.copy(file_path, '{}/{}/{}'.format(args.root_dir, dirname, file_name))
	elif forecast > 1:
		# Subtract previous accumulated total precipitation to get 1-hour values.
		previous_file_name = 'gfs.t{:02d}z.pgrb2.{}.f{:03d}'.format(date.hour, args.resolution, forecast - 1)
		previous_file_path = '{}/{}/.backup/{}'.format(args.root_dir, dirname, previous_file_name)
		if os.path.isfile(previous_file_path):
			gribs = pygrib.open(file_path)
			previous_gribs = pygrib.open(previous_file_path)
			new_gribs = open('{}/{}/{}'.format(args.root_dir, dirname, file_name), 'wb')
			for grib in gribs:
				if grib.shortName == 'tp':
					grib.values -= previous_gribs.select(shortName='tp')[0].values
				new_gribs.write(grib.tostring())
			new_gribs.close()
		else:
			shutil.copy(file_path, '{}/{}/{}'.format(args.root_dir, dirname, file_name))

def remote_gfs_exist(date, hour):
	file_name = 'gfs.t{:02d}z.pgrb2.{}.f000'.format(hour, args.resolution)
	res = subprocess.run(['ssh', host, 'test -f {}/gfs.{}{:02d}/{}'.format(args.remote_root, date.format('%Y%m%d'), hour, file_name)], stdout=subprocess.PIPE)
	return res.returncode == 0

# If date is not given, use current system date.
# If hour is not a initial hour, and set a suitable hour.
# If remote data is not available, use previous one.
if not args.date: args.date = pendulum.now('UTC')
analysis_date = args.date
found = False
for date in (args.date, args.date.subtract(days=1)):
	if found: break
	for hour in (18, 12, 6, 0):
		if ((args.date - date).days == 1 or args.date.hour >= hour) and remote_gfs_exist(date, hour):
			args.date = pendulum.create(date.year, date.month, date.day, hour)
			found = True
			break

# Set a suitable forecast hours that include analysis hour.
# stmas_mg.exe needs 6 background files.
if not args.forecast:
	dt = analysis_date - args.date
	if dt.seconds / args.cycle_period <= 4:
		args.date -= time_interval
		dt = analysis_date - args.date
	args.forecast = []
	for i in range(1 - args.cycle_number, 1):
		if len(args.forecast) == 0:
			hour = int((dt.seconds + i * args.cycle_period) / 3600)
		else:
			hour = math.ceil((dt.seconds + i * args.cycle_period) / 3600)
		if not hour in args.forecast:
			args.forecast.append(hour)

if args.date:
	for forecast in args.forecast:
		download_gfs(args.date, forecast)
elif args.date_range:
	date = args.date_range[0]
	while date <= args.date_range[1]:
		for forecast in args.forecast:
			download_gfs(date, forecast)
		date += time_interval

if 'dt' in locals(): print('{}00{:02d}{:02d}'.format(args.date.format('%y%j%H'), dt.hours, analysis_date.minute))
