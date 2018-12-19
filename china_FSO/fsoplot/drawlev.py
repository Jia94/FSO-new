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
type_name = ( 'sound','profiler','surface')
varlist =['u','v','t','p','q','tpw','spd','ref','rain']
levlist1=[110000.0,85000.0,70000.0,50000.0,40000.0,30000.0,20000.0,10000.0,7000.0,5000.0,2000.0]
levlist2=[85000.0,70000.0,50000.0,40000.0,30000.0,20000.0,10000.0,7000.0,5000.0,2000.0,0.0]
levlist =[850,700,500,400,300,200,100,70,50,20,0]

typenum=[]
typeok=[]
cur.execute("SELECT sum(inv),pres FROM tb_invstat WHERE time="+datatime+" and pres>=0 group by pres order by pres")
rs=cur.fetchall()
recs=cur.rowcount
if recs>0:
    i=0
    for r in rs:
    #    print r[3],r[6]
        typeok.append(int(r[1]))
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
        if j==0:
            alls=alls+"\">"+str(a)+"\","
            alln=alln+str(typenum[j])+","
        elif j>0 and j<i-1:
            alls=alls+"\""+str(typeok[j-1])+"-"+str(a)+"\","
            alln=alln+str(typenum[j])+","
        else:
            alls=alls+"\"<"+str(typeok[j-1])+"\"/)"
            alln=alln+str(typenum[j])+"/)"
        j=j+1
    with open('var.ncl', 'w') as f:
        f.write(alln+"\n")
        f.write(alls+"\n")
        f.write("title=\"All variables ( "+datatime+" )\"\n")
        f.write("outtype=\"png\"\n")
        f.write("outname=\"lev_all\"\n")
    os.system("/opt/ncl/6.5.0/bin/ncl ./fso.ncl")
   
    for tn in type_name:
        for var in ('u','v','t','p','q','tpw','spd','ref','rain'):
            cur.execute("SELECT sum(inv),pres from tb_invstat WHERE time="+datatime+" and pres>=0 and var='"\
                        +var+"' and typen='"+tn+"' group by pres" )
            if cur.rowcount>1:
                typenum=[]
                typeok=[]
                cur.execute("SELECT sum(inv),pres FROM tb_invstat WHERE time="+datatime+" and pres>=0 and var='"\
                         +var+"' and typen='"+tn+"' group by pres order by pres")
                rs=cur.fetchall()
                recs=cur.rowcount
                if recs>0:
                    i=0
                    for r in rs:
                        typeok.append(int(r[1]))
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
                        if j==0:
                            alls=alls+"\">"+str(a)+"\","
                            alln=alln+str(typenum[j])+","
                        elif j>0 and j<i-1:
                            alls=alls+"\""+str(typeok[j-1])+"-"+str(a)+"\","
                            alln=alln+str(typenum[j])+","
                        else:
                            alls=alls+"\"<"+str(typeok[j-1])+"\"/)"
                            alln=alln+str(typenum[j])+"/)"
                        j=j+1
                    with open('var.ncl', 'w') as f:
                        f.write(alln+"\n")
                        f.write(alls+"\n")
                        f.write("title=\""+tn.capitalize()+" "+var.upper()+" ( "+datatime+" )\"\n")
                        f.write("outtype=\"png\"\n")
                        f.write("outname=\"lev_"+tn+"_"+var+"\"\n")
                    os.system("/opt/ncl/6.5.0/bin/ncl ./fso.ncl")

cur.close
