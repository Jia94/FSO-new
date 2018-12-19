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

parser = argparse.ArgumentParser(description='Download CIMISS sounding data.')
parser.add_argument('-o', '--root-dir', dest='root_dir', default='/data/raw/cimiss', help='Root directory to store sounding data.')
parser.add_argument('-t', '--time', help='Download data at this time (YYYYMMDDHH[MM]).', type=parse_time)
args = parser.parse_args()

args.root_dir = os.path.abspath(args.root_dir)
if not os.path.isdir(args.root_dir): os.makedirs(args.root_dir)
os.chdir(args.root_dir)

if not args.time: args.time = pendulum.now('UTC')

def ftp_get(file_path, fatal=True):
	try:
		if os.path.isfile(file_path) and ftp.size(file_path) == os.path.getsize(file_path):
			print('[Warning]: File {} exists!'.format(file_path))
			return
	except ftplib.error_perm as e:
		if e.args[0][:3] == '550':
			print('[Error]: {} is missing!'.format(file_path))
		else:
			print('[Error]: Failed to check file size of {}! {}'.format(file_path, e))
		if fatal: exit(1)
	print('[Notice]: Get {} ...'.format(file_path))
	local_dir = os.path.dirname(file_path)
	if not os.path.isdir(local_dir): os.makedirs(local_dir)
	try:
		ftp.retrbinary('RETR {}'.format(file_path), open(file_path, 'wb').write)
	except ftplib.error_perm as e:
		if e.args[0][:3] == '550':
			print('[Error]: {} does not exist!'.format(file_path))
		else:
			print('[Error]: Failed to get {}! {}'.format(file_path, e))
		try:
			os.remove(file_path)
		except:
			pass
		if fatal: exit(1)

ftp = ftplib.FTP(os.getenv('CIMISS_FTP_HOST'))
ftp.login(user=os.getenv('CIMISS_FTP_USER'), passwd=os.getenv('CIMISS_FTP_PASSWD'))

if args.time.hour == 0 or args.time.hour == 12:
	ftp_get(f'UPAR_CHN_MUL_FTM/{args.time.format("%Y%m%d%H%M")}.xml', fatal=True)

try:
	ftp.quit()
except:
	pass
