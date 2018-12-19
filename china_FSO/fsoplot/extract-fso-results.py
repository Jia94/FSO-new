#!/usr/bin/env python3

import psycopg2

start_time = 2018072812
end_time = 2018072812
obs_types = ( 'surface', 'profiler')

conn = psycopg2.connect(database="fso",user="fso",password="fSO@2018") 

cur = conn.cursor()

for obs_type in obs_types:
	print('[Notice]: Extracting {} ...'.format(obs_type))
	cur.execute('''
		SELECT a.id, time, var, lev, pres, inv FROM tb_{0} AS a, tb_{0}data AS b
		WHERE a.time >= {1} AND a.time <= {2} AND a.id = b.id;
	'''.format(obs_type, start_time, end_time))

	f = open('fso.{}.{}-{}.txt'.format(obs_type, start_time, end_time), 'w')
	for row in cur.fetchall():
		f.write(' '.join(str(field) for field in row) + '\n')
	f.close()
