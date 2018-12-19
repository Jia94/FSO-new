#!/usr/bin/python
import os
import sys
import threading
import datetime
import psycopg2 
def writesql(tpname,freqint,thds,filename,datatime):
    conn = psycopg2.connect(database="fso",user="fso",password="fSO@2018") 
#    from sql import *
    if tpname=='metar':
        tpname='surface'
    starttime=datetime.datetime.now()
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
 
    for tn in type_name:
       cur.execute('DROP TABLE tb_'+tn)
       cur.execute('DROP TABLE tb_'+tn+'data')
        cur.execute('CREATE TABLE IF NOT EXISTS tb_'+tn+''' (id bigserial primary key,
                                          time bigint not null,
                                          stnid char(8) not null,
                                          lon real not null,
                                          lat real not null)''')
        cur.execute('CREATE TABLE IF NOT EXISTS tb_'+tn+'''data (id bigint not null, var varchar,
                   lev int, pres real, obs real,inv real, qc int, error real, inc real,primary key(id,var,lev))''')
    conn.commit()  
    if not os.path.isfile(filename):
        print(filename+" does not exist!")
        sys.exit() 
    fo = open("log."+tpname+str(thds), "w")
    fo.write("start time: "+str(starttime)+"\n")
    fo.flush()
    rl=0
    ll=0
    linevar=[]
    lv=[]
    lv0=[]
    try:
    #    for line in fp:
         with open (filename) as fp:
            line=fp.readline()
            while line:
                line = line.strip()
                linevar=line.split()
                for tn in type_name:
                    linevar[0].strip()
                    if tn=='surface' and linevar[0]=='metar':
                       rl=0
                       ll=int(linevar[1])
                       print (ll)
                       break
                    if linevar[0]==tn:
                        rl=0
                        ll=int(linevar[1])
                        print (ll)
                        #print linevar,'threads:',thds,'\n'
                        break
                while rl <  ll:
                    rl = rl+1
                    #linevar=line.split()
                    line=fp.readline()
                    levels=int(line)
                    nn=0
                    if tn != tpname:
                        while nn<levels:
                            line=fp.readline()
                            nn=nn+1
                        continue
                    if rl<=freqint*thds or rl>freqint*(thds+1):
                        while nn<levels:
                            line=fp.readline()
                            nn=nn+1
                        continue
                    #print tn,rl,levels
                    while nn < levels:
                        nn=nn+1
                        line=fp.readline()
                        line=line.strip()
                        lv0=[]
                        lv=[]
                        lv0=line.split()
                        if len(lv0[1].strip())>len(str(levels)):
                            lv=line.split()
                        else:
                            nnn=0
                            for ttt in lv0:
                                if nnn==2:
                                    lv[1]=str(lv[1])+str(ttt)
                                else:
                                    lv.append(ttt)
                                nnn=nnn+1
                        kk=lv[0]
                        #print kk,thds
                        l=str(nn)
                        stn_id=lv[1][len(l):]
                        lat=lv[2]
                        lon=lv[3]
                        pres=lv[4]
                        fo.write(str(kk)+'\n')
                        fo.flush()
                        #cur.execute("SELECT * FROM tb_"+tn+" where time="+datatime+" and lon="+str(lon)+" and lat="+str(lat))
                        #----- get the ID of station whose dataset are going to be wrote into pg------
                        cur.execute("SELECT * FROM tb_"+tn+" WHERE time="+datatime+" and stnid='"+stn_id+"' \
                                       and ABS(lon-"+str(lon)+")< 0.001 and ABS(lat-"+str(lat)+")< 0.001")
                        if cur.rowcount<1:
                            cur.execute('INSERT INTO tb_'+tn+"(time,stnid,lon,lat) VALUES ("+datatime+",\
                                       '"+stn_id+"',"+lon+','+lat+')')
                            cur.execute("SELECT * FROM tb_"+tn+" WHERE time="+datatime+" and stnid='"+stn_id+"' \
                                       and ABS(lon-"+str(lon)+")<0.001 and ABS(lat-"+str(lat)+")< 0.001")
                            rs=cur.fetchall()
                        else:
                            rs=cur.fetchall()
                        curn=rs[0][0]
                        #---- write synop/ships/buoy/metar/sonde_sfc/tamdar_sfc type data into sql ----
                        if tn=='synop' or tn=='ships' or tn=='buoy' or tn=='surface' or tn=='sonde_sfc' or tn=='tamdar_sfc':
                            u_obs=lv[5];u_inv=lv[6]; u_qc=lv[7]; u_error=lv[8]; u_inc=lv[9]  
                            v_obs=lv[10];v_inv=lv[11]; v_qc=lv[12]; v_error=lv[13]; v_inc=lv[14]
                            t_obs=lv[15];t_inv=lv[16]; t_qc=lv[17]; t_error=lv[18]; t_inc=lv[19]
                            p_obs=lv[20];p_inv=lv[21]; p_qc=lv[22]; p_error=lv[23]; p_inc=lv[24]
                            q_obs=lv[25];q_inv=lv[26]; q_qc=lv[27]; q_error=lv[28]; q_inc=lv[29]
                            #---------------- u select write and update -------------------
                            cur.execute('SELECT * FROM tb_'+tn+'data WHERE id='+str(curn)+" and var='u' and lev="+l)
                            if cur.rowcount<1:
                                cur.execute('INSERT INTO tb_'+tn+"data(id,var,lev,pres,obs,inv,qc,error,inc) VALUES ("+\
                                str(curn)+",'u',"+l+","+str(pres)+","+str(u_obs)+","+str(u_inv)+","\
                                +str(u_qc)+","+str(u_error)+","+str(u_inc)+")")
                            if cur.rowcount>0:
                                cur.execute('UPDATE tb_'+tn+'data SET inv='+str(u_inv)+' WHERE id='+str(curn)\
                                                +" and var='u' and lev="+l+" and inv>"+str(u_inv))
                            #---------------- v select write and update -------------------
                            cur.execute('SELECT * FROM tb_'+tn+'data WHERE id='+str(curn)+" and var='v' and lev="+l)
                            if cur.rowcount<1:
                                cur.execute('INSERT INTO tb_'+tn+"data(id,var,lev,pres,obs,inv,qc,error,inc) VALUES ("\
                                +str(curn)+ ",'v',"+l+","+str(pres)+","+str(v_obs)+","+str(v_inv)+","\
                                +str(v_qc)+","+str(v_error)+","+str(v_inc)+")")
                            if cur.rowcount>0:
                                cur.execute('UPDATE tb_'+tn+'data SET inv='+str(v_inv)+' WHERE id='+str(curn)\
                                                +" and var='v' and lev="+l+" and inv>"+str(v_inv))
                            #---------------- t select write and update -------------------
                            cur.execute('SELECT * FROM tb_'+tn+'data WHERE id='+str(curn)+" and var='t' and lev="+l)
                            if cur.rowcount<1:
                                cur.execute('INSERT INTO tb_'+tn+"data(id,var,lev,pres,obs,inv,qc,error,inc) VALUES ("\
                                +str(curn)+",'t',"+l+","+str(pres)+","+str(t_obs)+","+str(t_inv)+","\
                                +str(t_qc)+","+str(t_error)+","+str(t_inc)+")")
                            if cur.rowcount>0:
                                cur.execute('UPDATE tb_'+tn+'data SET inv='+str(t_inv)+' WHERE id='+str(curn)\
                                                +" and var='t' and lev="+l+" and inv>"+str(t_inv))
                            #---------------- p select write and update -------------------
                            cur.execute('SELECT * FROM tb_'+tn+'data WHERE id='+str(curn)+" and var='p' and lev="+l)
                            if cur.rowcount<1:
                                cur.execute('INSERT INTO tb_'+tn+"data(id,var,lev,pres,obs,inv,qc,error,inc) VALUES ("\
                                +str(curn)+",'p',"+l+","+str(pres)+","+str(p_obs)+","+str(p_inv)+","\
                                +str(p_qc)+","+str(p_error)+","+str(p_inc)+")")
                            if cur.rowcount>0:
                                cur.execute('UPDATE tb_'+tn+'data SET inv='+str(p_inv)+' WHERE id='+str(curn)\
                                                +" and var='p' and lev="+l+" and inv>"+str(p_inv))
                            #---------------- q select write and update -------------------
                            cur.execute('SELECT * FROM tb_'+tn+'data WHERE id='+str(curn)+" and var='q' and lev="+l)
                            if cur.rowcount<1:
                                cur.execute('INSERT INTO tb_'+tn+"data(id,var,lev,pres,obs,inv,qc,error,inc) VALUES ("\
                                +str(curn)+",'q',"+l+","+str(pres)+","+str(q_obs)+","+str(q_inv)+","\
                                +str(q_qc)+","+str(q_error)+","+str(q_inc)+")")
                            if cur.rowcount>0:
                                cur.execute('UPDATE tb_'+tn+'data SET inv='+str(q_inv)+' WHERE id='+str(curn)\
                                                +" and var='q' and lev="+l+" and inv>"+str(q_inv))
                        #----------------------pilot profiler geoamv qscat polaramv -------------
                        if tn=='pilot' or tn=='profiler' or tn=='geoamv' or tn=='qscat' or tn=='polaramv':
                            u_obs=lv[5];u_inv=lv[6]; u_qc=lv[7]; u_error=lv[8]; u_inc=lv[9]
                            v_obs=lv[10];v_inv=lv[11]; v_qc=lv[12]; v_error=lv[13]; v_inc=lv[14]
                            #---------------- u select write and update -------------------
                            cur.execute('SELECT * FROM tb_'+tn+'data WHERE id='+str(curn)+" and var='u' and lev="+l)
                            if cur.rowcount<1:
                                cur.execute('INSERT INTO tb_'+tn+"data(id,var,lev,pres,obs,inv,qc,error,inc) VALUES ("\
                                +str(curn)+",'u',"+l+","+str(pres)+","+str(u_obs)+","+str(u_inv)+","\
                                +str(u_qc)+","+str(u_error)+","+str(u_inc)+")")
                            if cur.rowcount>0:
                                cur.execute('UPDATE tb_'+tn+'data SET inv='+str(u_inv)+' WHERE id='+str(curn)\
                                                +" and var='u' and lev="+l+" and inv>"+str(u_inv))
                            #---------------- v select write and update -------------------
                            cur.execute('SELECT * FROM tb_'+tn+'data WHERE id='+str(curn)+" and var='v' and lev="+l)
                            if cur.rowcount<1:
                                cur.execute('INSERT INTO tb_'+tn+"data(id,var,lev,pres,obs,inv,qc,error,inc) VALUES ("\
                                +str(curn)+",'v',"+l+","+str(pres)+","+str(v_obs)+","+str(v_inv)+","\
                                +str(v_qc)+","+str(v_error)+","+str(v_inc)+")")
                            if cur.rowcount>0:
                                cur.execute('UPDATE tb_'+tn+'data SET inv='+str(v_inv)+' WHERE id='+str(curn)\
                                                +" and var='v' and lev="+l+" and inv>"+str(v_inv))
                        #------- gpspw -------
                        if tn=='gpspw':
                            u_obs=lv[5];u_inv=lv[6]; u_qc=lv[7]; u_error=lv[8]; u_inc=lv[9]
                            cur.execute('SELECT * FROM tb_'+tn+'data WHERE id='+str(curn)+" and var='tpw' and lev="+l)
                            if cur.rowcount<1:
                                cur.execute('INSERT INTO tb_'+tn+"data(id,var,lev,pres,obs,inv,qc,error,inc) VALUES ("\
                                +str(curn)+",'tpw',"+l+","+str(pres)+","+str(u_obs)+","+str(u_inv)+","\
                                +str(u_qc)+","+str(u_error)+","+str(u_inc)+")")
                            if cur.rowcount>0:
                                cur.execute('UPDATE tb_'+tn+'data SET inv='+str(u_inv)+' WHERE id='+str(curn)\
                                                +" and var='tpw' and lev="+l+" and inv>"+str(u_inv))
                        #------------- sound tamdar airep ------------
                        if tn=='sound' or tn=='tamdar' or tn=='airep':
                            u_obs=lv[5];u_inv=lv[6]; u_qc=lv[7]; u_error=lv[8]; u_inc=lv[9]
                            v_obs=lv[10];v_inv=lv[11]; v_qc=lv[12]; v_error=lv[13]; v_inc=lv[14]
                            t_obs=lv[15];t_inv=lv[16]; t_qc=lv[17]; t_error=lv[18]; t_inc=lv[19]
                            q_obs=lv[20];q_inv=lv[21]; q_qc=lv[22]; q_error=lv[23]; q_inc=lv[24]
                            #---------------- u select write and update -------------------
                            cur.execute('SELECT * FROM tb_'+tn+'data WHERE id='+str(curn)+" and var='u' and lev="+l)
                            if cur.rowcount<1:
                                cur.execute('INSERT INTO tb_'+tn+"data(id,var,lev,pres,obs,inv,qc,error,inc) VALUES ("\
                                      +str(curn)+ ",'u',"+l+","+str(pres)+","+str(u_obs)+","+str(u_inv)+","\
                                +str(u_qc)+","+str(u_error)+","+str(u_inc)+")")
                            if cur.rowcount>0:
                                cur.execute('UPDATE tb_'+tn+'data SET inv='+str(u_inv)+' WHERE id='+str(curn)\
                                                +" and var='u' and lev="+l+" and inv>"+str(u_inv))
                            #---------------- v select write and update -------------------
                            cur.execute('SELECT * FROM tb_'+tn+'data WHERE id='+str(curn)+" and var='v' and lev="+l)
                            if cur.rowcount<1:
                                cur.execute('INSERT INTO tb_'+tn+"data(id,var,lev,pres,obs,inv,qc,error,inc) VALUES ("\
                                +str(curn)+ ",'v',"+l+","+str(pres)+","+str(v_obs)+","+str(v_inv)+","\
                                +str(v_qc)+","+str(v_error)+","+str(v_inc)+")")
                            if cur.rowcount>0:
                                cur.execute('UPDATE tb_'+tn+'data SET inv='+str(v_inv)+' WHERE id='+str(curn)\
                                                +" and var='v' and lev="+l+" and inv>"+str(v_inv))
                           #---------------- t select write and update -------------------
                            cur.execute('SELECT * FROM tb_'+tn+'data WHERE id='+str(curn)+" and var='t' and lev="+l)
                            if cur.rowcount<1:
                                cur.execute('INSERT INTO tb_'+tn+"data(id,var,lev,pres,obs,inv,qc,error,inc) VALUES ("\
                                +str(curn)+",'t',"+l+","+str(pres)+","+str(t_obs)+","+str(t_inv)+","\
                                +str(t_qc)+","+str(t_error)+","+str(t_inc)+")")
                            if cur.rowcount>0:
                                cur.execute('UPDATE tb_'+tn+'data SET inv='+str(t_inv)+' WHERE id='+str(curn)\
                                                +" and var='t' and lev="+l+" and inv>"+str(t_inv))
                           #---------------- q select write and update -------------------
                            cur.execute('SELECT * FROM tb_'+tn+'data WHERE id='+str(curn)+" and var='q' and lev="+l)
                            if cur.rowcount<1:
                                cur.execute('INSERT INTO tb_'+tn+"data(id,var,lev,pres,obs,inv,qc,error,inc) VALUES ("\
                                +str(curn)+",'q',"+l+","+str(pres)+","+str(q_obs)+","+str(q_inv)+","\
                                +str(q_qc)+","+str(q_error)+","+str(q_inc)+")")
                            if cur.rowcount>0:
                                cur.execute('UPDATE tb_'+tn+'data SET inv='+str(q_inv)+' WHERE id='+str(curn)\
                                                +" and var='q' and lev="+l+" and inv>"+str(q_inv))
                      #--- ssmir ---  
                        if tn=='ssmir':
                            #spd_obs=lv[5];spd_inv=lv[6]; spd_qc=lv[7]; spd_error=lv[8]; spd_inc=lv[9]
                            #tpw_obs=lv[10];tpw_inv=lv[11]; tpw_qc=lv[12]; tpw_error=lv[13]; tpw_inc=lv[14]
                            u_obs=lv[5];u_inv=lv[6]; u_qc=lv[7]; u_error=lv[8]; u_inc=lv[9]
                            v_obs=lv[10];v_inv=lv[11]; v_qc=lv[12]; v_error=lv[13]; v_inc=lv[14]
                            #---------------- spd select write and update -------------------
                            cur.execute('SELECT * FROM tb_'+tn+'data WHERE id='+str(curn)+" and var='spd' and lev="+l)
                            if cur.rowcount<1:
                                cur.execute('INSERT INTO tb_'+tn+"data(id,var,lev,pres,obs,inv,qc,error,inc) VALUES ("\
                                +str(curn)+",'spd',"+l+","+str(pres)+","+str(u_obs)+","+str(u_inv)+","\
                                +str(u_qc)+","+str(u_error)+","+str(u_inc)+")")
                            if cur.rowcount>0:
                                cur.execute('UPDATE tb_'+tn+'data SET inv='+str(u_inv)+' WHERE id='+str(curn)\
                                                +" and var='spd' and lev="+l+" and inv>"+str(u_inv))
                            #---------------- tpw select write and update -------------------
                            cur.execute('SELECT * FROM tb_'+tn+'data WHERE id='+str(curn)+" and var='tpw' and lev="+l)
                            if cur.rowcount<1:
                                cur.execute('INSERT INTO tb_'+tn+"data(id,var,lev,pres,obs,inv,qc,error,inc) VALUES ("\
                                +str(curn)+ ",'tpw',"+l+","+str(pres)+","+str(v_obs)+","+str(v_inv)+","\
                                +str(v_qc)+","+str(v_error)+","+str(v_inc)+")")
                            if cur.rowcount>0:
                                cur.execute('UPDATE tb_'+tn+'data SET inv='+str(v_inv)+' WHERE id='+str(curn)\
                                                +" and var='tpw' and lev="+l+" and inv>"+str(v_inv))
    #                    #if tn=='ssmit':
                        if tn=='satem':    
                            #tpw_obs=lv[5];tpw_inv=lv[6]; tpw_qc=lv[7]; tpw_error=lv[8]; tpw_inc=lv[9]
                            u_obs=lv[5];u_inv=lv[6]; u_qc=lv[7]; u_error=lv[8]; u_inc=lv[9]
                            #---------------- tpw select write and update -------------------
                            cur.execute('SELECT * FROM tb_'+tn+'data WHERE id='+str(curn)+" and var='tpw' and lev="+l)
                            if cur.rowcount<1:
                                cur.execute('INSERT INTO tb_'+tn+"data(id,var,lev,pres,obs,inv,qc,error,inc) VALUES ("\
                                +str(curn)+ ",'tpw',"+l+","+str(pres)+","+str(u_obs)+","+str(u_inv)+","\
                                +str(u_qc)+","+str(u_error)+","+str(u_inc)+")")
                            if cur.rowcount>0:
                                cur.execute('UPDATE tb_'+tn+'data SET inv='+str(u_inv)+' WHERE id='+str(curn)\
                                                +" and var='tpw' and lev="+l+" and inv>"+str(u_inv))
    #                    #if tn=='ssmt1' or tn=='ssmt2' 
                        if tn=='bogus':
                            u_obs=lv[5];u_inv=lv[6]; u_qc=lv[7]; u_error=lv[8]; u_inc=lv[9]
                            v_obs=lv[10];v_inv=lv[11]; v_qc=lv[12]; v_error=lv[13]; v_inc=lv[14]
                            #---------------- u select write and update -------------------
                            cur.execute('SELECT * FROM tb_'+tn+'data WHERE id='+str(curn)+" and var='u' and lev="+l)
                            if cur.rowcount<1:
                                cur.execute('INSERT INTO tb_'+tn+"data(id,var,lev,pres,obs,inv,qc,error,inc) VALUES ("\
                                +str(curn)+ ",'u',"+l+","+str(pres)+","+str(u_obs)+","+str(u_inv)+","\
                                +str(u_qc)+","+str(u_error)+","+str(u_inc)+")")
                            if cur.rowcount>0:
                                cur.execute('UPDATE tb_'+tn+'data SET inv='+str(u_inv)+' WHERE id='+str(curn)\
                                                +" and var='u' and lev="+l+" and inv>"+str(u_inv))
                            #---------------- v select write and update -------------------
                            cur.execute('SELECT * FROM tb_'+tn+'data WHERE id='+str(curn)+" and var='v' and lev="+l)
                            if cur.rowcount<1:
                                cur.execute('INSERT INTO tb_'+tn+"data(id,var,lev,pres,obs,inv,qc,error,inc) VALUES ("\
                                +str(curn)+",'v',"+l+","+str(pres)+","+str(v_obs)+","+str(v_inv)+","\
                                +str(v_qc)+","+str(v_error)+","+str(v_inc)+")")
                            if cur.rowcount>0:
                                cur.execute('UPDATE tb_'+tn+'data SET inv='+str(v_inv)+' WHERE id='+str(curn)\
                                                +" and var='v' and lev="+l+" and inv>"+str(v_inv))
                        if tn=='airsr':
                            #t_obs=lv[5];t_inv=lv[6]; t_qc=lv[7]; t_error=lv[8]; t_inc=lv[9]
                            #q_obs=lv[10];q_inv=lv[11]; q_qc=lv[12]; q_error=lv[13]; q_inc=lv[14]      
                            u_obs=lv[5];u_inv=lv[6]; u_qc=lv[7]; u_error=lv[8]; u_inc=lv[9]
                            v_obs=lv[10];v_inv=lv[11]; v_qc=lv[12]; v_error=lv[13]; v_inc=lv[14]
                            #---------------- u select write and update -------------------
                            cur.execute('SELECT * FROM tb_'+tn+'data WHERE id='+str(curn)+" and var='t' and lev="+l)
                            if cur.rowcount<1:
                                cur.execute('INSERT INTO tb_'+tn+"data(id,var,lev,pres,obs,inv,qc,error,inc) VALUES ("\
                                +str(curn)+",'t',"+l+","+str(pres)+","+str(u_obs)+","+str(u_inv)+","\
                                +str(u_qc)+","+str(u_error)+","+str(u_inc)+")")
                            if cur.rowcount>0:
                                cur.execute('UPDATE tb_'+tn+'data SET inv='+str(u_inv)+' WHERE id='+str(curn)\
                                                +" and var='t' and lev="+l+" and inv>"+str(u_inv))
                            #---------------- v select write and update -------------------
                            cur.execute('SELECT * FROM tb_'+tn+'data WHERE id='+str(curn)+" and var='q' and lev="+l)
                            if cur.rowcount<1:
                                cur.execute('INSERT INTO tb_'+tn+"data(id,var,lev,pres,obs,inv,qc,error,inc) VALUES ("\
                                +str(curn)+",'q',"+l+","+str(pres)+","+str(v_obs)+","+str(v_inv)+","\
                                +str(v_qc)+","+str(v_error)+","+str(v_inc)+")")
                            if cur.rowcount>0:
                                cur.execute('UPDATE tb_'+tn+'data SET inv='+str(v_inv)+' WHERE id='+str(curn)\
                                                +" and var='q' and lev="+l+" and inv>"+str(v_inv))
                        #--- gpsref ---
                        if tn=='gpsref':
                            #ref_obs=lv[5];ref_inv=lv[6]; ref_qc=lv[7]; ref_error=lv[8]; ref_inc=lv[9]
                            u_obs=lv[5];u_inv=lv[6]; u_qc=lv[7]; u_error=lv[8]; u_inc=lv[9]
                            #---------------- ref select write and update -------------------
                            cur.execute('SELECT * FROM tb_'+tn+'data WHERE id='+str(curn)+" and var='ref' and lev="+l)
                            if cur.rowcount<1:
                                cur.execute('INSERT INTO tb_'+tn+"data(id,var,lev,pres,obs,inv,qc,error,inc) VALUES ("\
                                +str(curn)+ ",'ref',"+l+","+str(pres)+","+str(u_obs)+","+str(u_inv)+","\
                                +str(u_qc)+","+str(u_error)+","+str(u_inc)+")")
                            if cur.rowcount>0:
                                cur.execute('UPDATE tb_'+tn+'data SET inv='+str(u_inv)+' WHERE id='+str(curn)\
                                                +" and var='ref' and lev="+l+" and inv>"+str(u_inv))
                        #---- rain ----
                        if tn=='rain':
                            #rain_obs=lv[5];rain_inv=lv[6]; rain_qc=lv[7]; rain_error=lv[8]; rain_inc=lv[9]
                            #---------------- rain select write and update -------------------
                            u_obs=lv[5];u_inv=lv[6]; u_qc=lv[7]; u_error=lv[8]; u_inc=lv[9]
                            cur.execute('SELECT * FROM tb_'+tn+'data WHERE id='+str(curn)+" and var='rain' and lev="+l)
                            if cur.rowcount<1:
                                cur.execute('INSERT INTO tb_'+tn+"data(id,var,lev,pres,obs,inv,qc,error,inc) VALUES ("\
                                +str(curn)+",'rain',"+l+","+str(pres)+","+str(u_obs)+","+str(u_inv)+","\
                                +str(u_qc)+","+str(u_error)+","+str(u_inc)+")")
                            if cur.rowcount>0:
                                cur.execute('UPDATE tb_'+tn+'data SET inv='+str(u_inv)+' WHERE id='+str(curn)\
                                                +" and var='rain' and lev="+l+" and inv>"+str(u_inv))
                        conn.commit()
                line=fp.readline()    
    finally:
        conn.commit()
        cur.close
        conn.close
        fp.close()
        endtime=datetime.datetime.now()
        fo.write("end time: "+str(endtime)+"\n")
        fo.write(str(endtime-starttime))
        fo.flush()
        fo.close()
        print (endtime.strftime('%Y-%m-%d %H:%M:%S'))
        print (endtime-starttime)
        print ('---------threads:',tpname,thds,'completed!--------------')
from timepath import *
starttime=datetime.datetime.now()
print (starttime.strftime('%Y-%m-%d %H:%M:%S'))
#filename='west/gts_omb_oma_01'
filetime=datatime#'2018012900'
if not os.path.isfile(filename):
    print(filename+" does not exist!")
    sys.exit()
try:
    rl=0
    ll=0
    linevar=[]
    lv=[]
    lv0=[]
    setint=2000
    type_name = ( 'synop', 'ships', 'buoy', 'metar', 'sonde_sfc', 'tamdar_sfc',
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
    try:
    #    for line in fp:
         with open (filename) as fp:
            line=fp.readline()
            while line:
                line = line.strip()
                linevar=line.split()
                for tn in type_name:
                    linevar[0].strip()
                    if linevar[0]==tn:
                        rl=0
                        ll=int(linevar[1])
                        tnum=0
                        tnum=ll/setint
                        if ll%setint>0:
                           tnum=tnum+1
                        cyclen=0
                        #print cyclen,tnum
                        while cyclen<tnum:
                            threading.Thread(target=writesql,args =(tn,setint,cyclen,filename,filetime)).start()
                            cyclen=cyclen+1
                        #print linevar
                        break
                while rl <  ll:
                    rl = rl+1
                    #linevar=line.split()
                    line=fp.readline()
                    levels=int(line)
                    nn=0
                    #print tn,rl,levels
                    while nn < levels:
                        nn=nn+1
                        line=fp.readline()  
                line=fp.readline()    
    except Exception as e:
        print (e)
    finally: 
        endtime=datetime.datetime.now()
        print (endtime.strftime('%Y-%m-%d %H:%M:%S'))
        print (endtime-starttime)
        print (endtime-starttime)
finally:
    print ('end!')
