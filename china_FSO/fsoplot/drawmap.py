#!/usr/bin/python
import os
import sys
from sql import *
from timepath import *
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
              # 'rain' )
varlist =['u','v','t','p','q','tpw','spd','ref','rain','all']
levlist1=[110000.0,100000.0,85000.0,70000.0,50000.0,40000.0,30000.0,20000.0,10000.0,7000.0,5000.0,2000.0]
levlist2=[100000.0,85000.0,70000.0,50000.0,40000.0,30000.0,20000.0,10000.0,7000.0,5000.0,2000.0,0.0]
levlist =[1000,850,700,500,400,300,200,100,70,50,20,0]
strlat=[]
strlon=[]
strinv=[]
for tn in type_name: 
    for v in varlist:
        strlon=[]
        strlat=[]
        strinv=[]
        if v=='all':
            cur.execute('SELECT sum(inv),lon,lat,count(*) FROM tb_'+tn+'data,tb_'+tn+'  WHERE \
                      tb_'+tn+'.id=tb_'+tn+"data.id and qc>=0 and time="+datatime+" group by lon,lat")
        else:
            cur.execute('SELECT sum(inv),lon,lat,count(*) FROM tb_'+tn+'data,tb_'+tn+'  WHERE \
                      tb_'+tn+'.id=tb_'+tn+"data.id and qc>=0 and time="+datatime+" and var='"+v+"' group by lon,lat")
        rs=cur.fetchall()
        recs=cur.rowcount
        if recs>0:
            i=0
            for r in rs:
                strinv.append(round(r[0],2))
                strlon.append(r[1])
                strlat.append(r[2])
                i=i+1
            #typeok.reverse()
            #typenum.reverse()
            alllon="lon=(/"
            alllat="lat=(/"
            allinv="R=(/"
            j=0
            for a in strlon:
                if j<i-1:
                    if j%50==49:
                        alllon=alllon+"\\ \n"
                        alllat=alllat+"\\ \n"
                        allinv=allinv+"\\ \n"
                    alllon=alllon+str(a)+","
                    alllat=alllat+str(strlat[j])+","
                    allinv=allinv+str(strinv[j])+","
                else:
                    alllon=alllon+str(a)+"/)"
                    alllat=alllat+str(strlat[j])+"/)"
                    allinv=allinv+str(strinv[j])+"/)"
                j=j+1
            with open('varmap.ncl', 'w') as f:
                f.write(alllon+"\n")
                f.write(alllat+"\n")
                f.write(allinv+"\n")
                f.write("title=\""+tn.upper()+"_"+v.upper()+"            "+datatime+"\"\n")
                f.write("outtype=\"png\"\n")
                f.write("outname=\"map_"+tn+"_"+v+"\"\n")
            os.system("/opt/ncl/6.5.0/bin/ncl map.ncl")

# tn='sound'
for tn in type_name:
    k=0
    for lev in levlist2:
        for v in varlist:
            strlon=[]
            strlat=[]
            strinv=[]
            if v=='all':
                cur.execute('SELECT sum(inv),lon,lat,count(*) FROM tb_'+tn+'data,tb_'+tn+'  WHERE \
                          tb_'+tn+'.id=tb_'+tn+"data.id and qc>=0 and time="+datatime+\
                          " and pres>"+str(lev)+" and pres<="+str(levlist1[k])+" group by lon,lat")
            else:
                cur.execute('SELECT sum(inv),lon,lat,count(*) FROM tb_'+tn+'data,tb_'+tn+'  WHERE \
                          tb_'+tn+'.id=tb_'+tn+"data.id and qc>=0 and time="+datatime+" and var='"+v+\
                         "' and pres>"+str(lev)+" and pres<="+str(levlist1[k])+" group by lon,lat")
            rs=cur.fetchall()
            recs=cur.rowcount
            if recs>0:
                i=0
                for r in rs:
                    strinv.append(round(r[0],2))
                    strlon.append(r[1])
                    strlat.append(r[2])
                    i=i+1
                typeok.reverse()
                typenum.reverse()
                alllon="lon=(/"
                alllat="lat=(/"
                allinv="R=(/"
                j=0
                for a in strlon:
                    if j<i-1:
                        if j%50==49:
                            alllon=alllon+"\\ \n"
                            alllat=alllat+"\\ \n"
                            allinv=allinv+"\\ \n"
                        alllon=alllon+str(a)+","
                        alllat=alllat+str(strlat[j])+","
                        allinv=allinv+str(strinv[j])+","
                    else:
                        alllon=alllon+str(a)+"/)"
                        alllat=alllat+str(strlat[j])+"/)"
                        allinv=allinv+str(strinv[j])+"/)"
                    j=j+1
                with open('varmap.ncl', 'w') as f:
                    f.write(alllon+"\n")
                    f.write(alllat+"\n")
                    f.write(allinv+"\n")
                    if str(lev)=='100000.0':
                        f.write("title=\""+tn.upper()+"_"+v.upper()+" >"+str(int(lev/100))+"hPa              "+datatime+"\"\n")
                    elif str(lev)=='0.0':
                        f.write("title=\""+tn.upper()+"_"+v.upper()+" <="+str(int(levlist1[k]/100))+"hPa           "\
                        +datatime+"\"\n")
                    else:
                        f.write("title=\""+tn.upper()+"_"+v.upper()+" "+str(int(levlist1[k]/100))+" - "+str(int(lev/100))+
                         "hPa           "+datatime+" \"\n")
                    f.write("outtype=\"png\"\n")
                    f.write("outname=\"map_"+tn+"_"+v+str(levlist[k])+"\"\n")
                os.system("/opt/ncl/6.5.0/bin/ncl map.ncl")
        k=k+1
cur.close
conn.close
#if cur.rowcount<1:
#cur.execute('INSERT INTO tb_'+tn+"data(id,var,lev,pres,obs,inv,qc,error,inc) VALUES ("+str(curn)+\
