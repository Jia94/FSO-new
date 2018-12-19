#!/usr/bin/python
#import psycopg2
import os
import sys
#conn = psycopg2.connect(database="fso",user="mac",password="",host="127.0.0.1",port="5432")
#print "success!"
from timepath import * 
from sql import *
cur = conn.cursor()
# type_name = ( 'synop', 'ships', 'buoy', 'surface', 'sonde_sfc', 'tamdar_sfc',
              # 'pilot', 'geoamv', 'qscat', 'polaramv',
              # 'gpspw',
              # 'sound', 'tamdar', 'airep',
              # 'ssmir',
              # 'ssmit',
              # 'satem',
              # 'ssmt1' , 'ssmt2',
              # 'bogus',
              # 'airsr',
              # 'gpsref',
              # 'rain' )
# type_name_all = ( 'synop', 'ships', 'buoy', 'surface', 'sonde_sfc', 'tamdar_sfc',
              # 'pilot', 'profiler', 'geoamv', 'qscat', 'polaramv',
              # 'gpspw',
              # 'sound', 'tamdar', 'airep',
              # 'ssmir',
              # 'ssmit',
              # 'satem',
              # 'ssmt1' , 'ssmt2',
              # 'bogus',
              # 'airsr',
              # 'gpsref',
              # 'rain' )
type_name = ( 'sound','surface')
type_name_all = ( 'sound','profiler','surface')
varlist =['u','v','t','p','q','tpw','spd','ref','rain']
levlist1=[110000.0,85000.0,70000.0,50000.0,40000.0,30000.0,20000.0,10000.0,7000.0,5000.0,2000.0]
levlist2=[85000.0,70000.0,50000.0,40000.0,30000.0,20000.0,10000.0,7000.0,5000.0,2000.0,0.0]
levlist =[850,700,500,400,300,200,100,70,50,20,0]
levlist3=[10000.0,8000.0,6000.0,4000.0,3000.0,2000.0,1000.0,800.0,500.0,200.0]
levlist4=[8000.0,6000.0,4000.0,3000.0,2000.0,1000.0,800.0,500.0,200.0,0.0]
levlist5=[8.0,6.0,4.0,3.0,2.0,1.0,0.8,0.5,0.2,0.0]
# cur.execute('DROP TABLE IF EXISTS tb_invstat')
# cur.execute('''CREATE TABLE IF NOT EXISTS tb_invstat (id bigserial primary key,
                                      # time bigint not null,
                                      # var varchar, typen varchar,
                                      # pres int,inv real,num int)''')
# conn.commit()
k=0
for lev in levlist4:
    for v in varlist:
        tn='profiler'
        cur.execute('SELECT inv FROM tb_'+tn+'data,tb_'+tn+'  WHERE tb_'+tn+'.id=tb_'+tn+"data.id and \
            obs>-8888.0 and time="+datatime+" and pres>"+str(lev)+" and pres<="+str(levlist3[k])+" and var='"+v+"'")
        rs=cur.fetchall()
        rcds=cur.rowcount
        if rcds>0:
            tsum=0
            for r in rs: 
                tsum=tsum+r[0]
            cur.execute("DELETE FROM tb_invstat WHERE time="+datatime+" and typen='"+tn+"' and \
                        var='"+v+"' and pres="+str(levlist5[k]))
            cur.execute("INSERT INTO tb_invstat(time,var,typen,pres,inv,num) VALUES ("+datatime+",\
                                '"+v+"','"+tn+"',"+str(levlist5[k])+","+str(tsum)+','+str(rcds)+')')
    k=k+1 
k=0
for lev in levlist2:
    for v in varlist:
        for tn in type_name:
            cur.execute('SELECT inv FROM tb_'+tn+'data,tb_'+tn+'  WHERE tb_'+tn+'.id=tb_'+tn+"data.id and \
                        obs>-8888.0 and time="+datatime+" and pres>"+str(lev)+" and \
                        pres<="+str(levlist1[k])+" and var='"+v+"'")
            rs=cur.fetchall()
            rcds=cur.rowcount
            if rcds>0:
                tsum=0
                for r in rs: 
                    tsum=tsum+r[0]
                cur.execute("DELETE FROM tb_invstat WHERE time="+datatime+" and typen='"+tn+"' and \
                          var='"+v+"' and pres="+str(levlist[k]))
                #if cur.rowcount<1:
                cur.execute("INSERT INTO tb_invstat(time,var,typen,pres,inv,num) VALUES ("+datatime+",\
                                   '"+v+"','"+tn+"',"+str(levlist[k])+","+str(tsum)+','+str(rcds)+')')
    k=k+1        
conn.commit()

typenum=[]
typeok=[]
cur.execute("SELECT sum(inv),var FROM tb_invstat WHERE time="+datatime+" and pres>=0 group by var")
rs=cur.fetchall()
recs=cur.rowcount
if recs>0:
    i=0
    for r in rs:
    #    print r[3],r[6]
        typeok.append(r[1].upper())
        typenum.append(r[0])
        i=i+1
    typeok.reverse()
    typenum.reverse()
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
        f.write("title=\"All Types  ( "+datatime+" )\"\n")
        f.write("outtype=\"png\"\n")
        f.write("outname=\"var_all\"\n")
    os.system("/opt/ncl/6.5.0/bin/ncl fso.ncl")

for tn in type_name_all:
    typenum=[]
    typeok=[]
    cur.execute("SELECT sum(inv),var FROM tb_invstat WHERE time="+datatime+" and pres>=0 and typen='"+tn+"' group by var")
    rs=cur.fetchall()
    recs=cur.rowcount
    if recs>0:
        i=0
        for r in rs:
        #    print r[3],r[6]
            typeok.append(r[1].upper())
            typenum.append(r[0])
            i=i+1
        typeok.reverse()
        typenum.reverse()
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
            f.write("title=\""+tn.capitalize()+" ( "+datatime+" )\"\n")
            f.write("outtype=\"png\"\n")
            f.write("outname=\"var_"+tn+"\"\n")
        os.system("/opt/ncl/6.5.0/bin/ncl fso.ncl")


cur.close
