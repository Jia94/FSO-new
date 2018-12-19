#!/usr/bin/env python3.6

import argparse
import ftplib
import pendulum
import re
import os

if not os.getenv('CIMISS_FTP_HOST'):
	print('[Error]: CIMISS_FTP_HOST is not set!')
	exit(1)
if not os.getenv('CIMISS_FTP_USER'):
	print('[Error]: CIMISS_FTP_USER is not set!')
	exit(1)
if not os.getenv('CIMISS_FTP_PASSWD'):
	print('[Error]: CIMISS_FTP_PASSWD is not set!')
	exit(1)

def parse_date(string):
	match = re.match(r'(\d{4}\d{2}\d{2})(\d{2})?(\d{2})?', string)
	if not match:
		print('[Error]: Invalid date format (YYYYMMDD[HH][MM])!')
		exit(1)
	if match.group(1) and match.group(2) and match.group(3):
		return pendulum.from_format(string, '%Y%m%d%H%M')
	elif match.group(1) and match.group(2):
		return pendulum.from_format(string, '%Y%m%d%H')
	elif match.group(1):
		return pendulum.from_format(string, '%Y%m%d')

parser = argparse.ArgumentParser(description='Download wind profiler data.')
parser.add_argument('-o', '--root-dir', dest='root_dir', default='/home/data/raw', help='Root directory to store profiler data.')
parser.add_argument('-d', '--date', help='Download data at this date (YYYYMMDDHH[MM]).', type=parse_date)
args = parser.parse_args()

args.root_dir = os.path.abspath(args.root_dir)
if not os.path.isdir(args.root_dir): os.makedirs(args.root_dir)
os.chdir(args.root_dir)

if not args.date: args.date = pendulum.now('UTC')

def ftp_get(remote_file_path, fatal=True):
	local_file_path = '{}/{}/{}'.format(args.root_dir, os.path.basename(os.path.dirname(remote_file_path)), os.path.basename(remote_file_path))
	try:
		if os.path.isfile(local_file_path) and ftp.size(remote_file_path) == os.path.getsize(local_file_path):
			print('[Warning]: File {} exists!'.format(local_file_path))
			return
	except ftplib.error_perm as e:
		if e.args[0][:3] == '550':
			print('[Error]: {} is missing!'.format(remote_file_path))
		else:
			print('[Error]: Failed to check file size of {}! {}'.format(remote_file_path, e))
		if fatal: exit(1)
	print('[Notice]: Get {} ...'.format(remote_file_path))
	if not os.path.isdir(os.path.dirname(local_file_path)): os.makedirs(os.path.dirname(local_file_path))
	try:
		ftp.retrbinary('RETR {}'.format(remote_file_path), open(local_file_path, 'wb').write)
	except ftplib.error_perm as e:
		if e.args[0][:3] == '550':
			print('[Error]: {} does not exist!'.format(remote_file_path))
		else:
			print('[Error]: Failed to get {}! {}'.format(remote_file_path, e))
		if fatal: exit(1)

def download_profiler(date):
	remote_dir = '/WPRD/QI/{}'.format(date.subtract(hours=1).format('%Y%m%d'))
	try:
		remote_file_list = ftp.nlst(remote_dir)
		print(remote_file_list)
	except:
		print('[Warning]: Remote directory {} does not exist!'.format(remote_dir))
		remote_file_list = []
	for data_file in remote_file_list:
		data_time = pendulum.from_format(re.findall(r'\d{14}', os.path.basename(data_file))[0], '%Y%m%d%H%M%S')
		dt = (date - data_time).in_minutes()
		if dt >= -10 and dt <= 10:
			ftp_get('{}/{}'.format(remote_dir, data_file), fatal=False)

ftp = ftplib.FTP(os.getenv('CIMISS_FTP_HOST'))
ftp.login(user=os.getenv('CIMISS_FTP_USER'), passwd=os.getenv('CIMISS_FTP_PASSWD'))

download_profiler(args.date)

ftp.quit()
