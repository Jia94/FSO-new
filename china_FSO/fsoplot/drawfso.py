#!/usr/bin/python
#import psycopg2
import os
import sys
#conn = psycopg2.connect(database="fso",user="mac",password="",host="127.0.0.1",port="5432")
from sql import *
from timepath import *
os.environ["NCARG_ROOT"]=os.path.join('/', 'opt', 'ncl', '6.5.0')
cur = conn.cursor()
type_name = ( 'synop', 'ships', 'buoy', 'surface', 'sonde_sfc', 'tamdar_sfc',
              'pilot', 'profiler', 'geoamv', 'qscat', 'polaramv',
              'gpspw',
              'sound', 'tamdar', 'airep',
              'ssmir',
              'ssmit',
              'satem',
              'ssmt1' , 'ssmt2',
              'bogus',
              'airsr',
              'gpsref',
              'rain' )
typenum=[]
typeok=[]
i=0
cur.execute('DROP TABLE IF EXISTS tb_invstat')

cur.execute('''CREATE TABLE IF NOT EXISTS tb_invstat (id bigserial primary key,
                                      time bigint not null,
                                      var varchar, typen varchar,
                                      pres real,inv real,num int)''')
conn.commit()

for tn in type_name:
    cur.execute('SELECT inv FROM tb_'+tn+'data,tb_'+tn+'  WHERE tb_'+tn+'.id=tb_'+tn+"data.id and \
                 obs>-8888.0 and qc>=0 and time="+datatime)
    rs=cur.fetchall()
    rcds=cur.rowcount
    if rcds>0:
        tsum=0
        for r in rs: 
            tsum=tsum+r[0]
        typenum.append(round(tsum,2))
        typeok.append(tn)
        #print tn , tsum 
        cur.execute("DELETE FROM tb_invstat WHERE time="+datatime+" and typen='"+tn+"' and var='all' and pres<0")
        #if cur.rowcount<1:
        cur.execute("INSERT INTO tb_invstat(time,var,typen,pres,inv,num) VALUES ("+datatime+",\
                           'all','"+tn+"',-1,"+str(tsum)+','+str(rcds)+')')
    i=i+1
cur.execute("SELECT * FROM tb_invstat WHERE time="+datatime+" and var='all' order by typen desc") 
rs=cur.fetchall()
i=0
for r in rs:
#    print r[3],r[6]
    typeok[i]=r[3]
    typenum[i]=r[5]
    i=i+1
	
alln="tn=(/"
alls="ts=(/"
j=0
for a in typeok:
    if j<i-1:
        alls=alls+"\""+str(a)+"\","  #ts=(/"surface","sound"/)
        alln=alln+str(typenum[j])+","#tn=(/474.415,-2283.1/)
    else:
        alls=alls+"\""+str(a)+"\"/)"
        alln=alln+str(typenum[j])+"/)"
    j=j+1
with open('var.ncl', 'w') as f:
    f.write(alln+"\n")
    f.write(alls+"\n")
    f.write("title=\"ALL ( "+datatime+" )\"\n")
    f.write("outtype=\"png\"\n")
    f.write("outname=\"fso_all\"\n")
os.system("/opt/ncl/6.5.0/bin/ncl ./fso.ncl")


for var in ('u','v','t','p','q','tpw','spd','rain','ref'):
    typenum=[]
    typeok=[]
    for tn in type_name: #surface
        cur.execute('SELECT inv FROM tb_'+tn+'data,tb_'+tn+'  WHERE tb_'+tn+'.id=tb_'+tn+"data.id and \
                     obs>-8888.0 and qc>=0 and var='"+var+"' and time="+datatime) 
        rs=cur.fetchall()
        rcds=cur.rowcount
        if rcds>0:
            tsum=0
            for r in rs:
                tsum=tsum+r[0]
            typenum.append(round(tsum,2))
            typeok.append(tn)
            #print tn , tsum 
            cur.execute("DELETE FROM tb_invstat WHERE time="+datatime+" and var='"+var+"' and typen='"+tn+"' and pres<0")
            #if cur.rowcount<1:
            cur.execute("INSERT INTO tb_invstat(time,var,typen,pres,inv,num) VALUES ("+datatime+",\
                               '"+var+"','"+tn+"',-1,"+str(tsum)+','+str(rcds)+')')
        i=i+1


    cur.execute("SELECT * FROM tb_invstat WHERE time="+datatime+" and var='"+var+"' and pres<0 order by typen desc")
    rs=cur.fetchall()
    if cur.rowcount>1:
        i=0
        for r in rs:
            #print i,r[3],r[5],var
            typeok[i]=r[3]
            typenum[i]=r[5]
            i=i+1
        print (typeok)
        print (typenum)
        alln="tn=(/"
        alls="ts=(/"
        j=0
        for a in typeok:
            if j<i-1:
                alls=alls+"\""+str(a)+"\","
                alln=alln+str(typenum[j])+","
            else:
                alls=alls+"\""+str(a)+"\"/)"
                alln=alln+str(typenum[j])+"/)"
            j=j+1
        with open('var.ncl', 'w') as f:
            f.write(alln+"\n")
            f.write(alls+"\n")
            f.write("title=\""+var.upper()+" ( "+datatime+" )\"\n")
            f.write("outtype=\"png\"\n")
            f.write("outname=\"fso_"+var+"\"\n")
        os.system("/opt/ncl/6.5.0/bin/ncl ./fso.ncl")

conn.commit()
cur.close
os.system("python3 ./drawvar.py")
os.system("python3 ./drawlev.py")
os.system("python3 ./drawvarlev.py")
os.system("python3 ./drawmap.py")
