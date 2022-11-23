#!/usr/local/python-virtualenv/bin/python

import time
import sys
import os
import datetime
import os
import sys
from datetime import datetime
from dateutil import tz

#todays_date
todays_date=datetime.now().strftime("%Y-%m-%d")

#CurrentWorkingDirectory
cwd = os.getcwd()

#PopingScriptName
sys.argv.pop(0)

#Extracting_Current_Load_File
cfile=sys.argv[-1]

#Final_File
ffile="/opt/monitoring/yarn_queues/log/tmp/"+todays_date+"/"+"final.log"

def convert(cfile,ffile):
    with open(cfile,"r+") as f:
        a=f.read().strip().split("\n")

    initial_list=[]
    for i in range(len(a)):
        app_id,user,st_temp,end_temp,mem_sec,v_sec,queue=a[i].split("|")
        if app_id != "App_ID":
            if end_temp != "0":
                start_time_gmt=time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(st_temp)/1000.0))
                end_time_gmt=time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(end_temp)/1000.0))
                time_in_seconds=int((int(end_temp)-int(st_temp))/1000)
                if time_in_seconds != 0:
                     memory=str(int(int(mem_sec)/time_in_seconds))
                else:
                     memory=str(int(0))
                if v_sec != "0":
                        vCores=str(int(int(v_sec)/time_in_seconds))
                else:
                        vCores=str(int(0))
                initial_list.append((app_id+"|"+user+"|"+start_time_gmt+"|"+end_time_gmt+"|"+memory+"|"+vCores+"|"+queue))
            else:
                end_temp = "running"
                start_time_gmt=time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(st_temp)/1000.0))
                #end_time_gmt=time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(end_temp)/1000.0))
                current_time_ms=int(round(time.time() * 1000))
                time_in_seconds=int((int(current_time_ms)-int(st_temp))/1000)
                if time_in_seconds != 0:
                        memory=str(int(int(mem_sec)/time_in_seconds))
                else:
                        memory=str(int(0))
                if v_sec != "0":
                        vCores=str(int(int(v_sec)/time_in_seconds))
                else:
                        vCores=str(int(0))
                initial_list.append((app_id+"|"+user+"|"+start_time_gmt+"|"+end_temp+"|"+memory+"|"+vCores+"|"+queue))

    final_list=[]
    for item in initial_list:
        from_zone = tz.gettz('GMT')
        to_zone = tz.gettz('America/Chicago')
        app_id,user,st_temp,end_temp,mem,vCores,queue=item.split("|")
        if end_temp == "running":
            st_temp_gmt=datetime.strptime(st_temp, '%Y-%m-%d %H:%M:%S').replace(tzinfo=from_zone)
            st_cdt=str(st_temp_gmt.astimezone(to_zone)).split("-05:00")[0]
            mem_in_gb=(round(int(mem)/1024))
            final_list.append((app_id+"|"+user+"|"+str(st_cdt)+"|"+str(end_temp)+"|"+str(mem_in_gb)+"|"+vCores+"|"+queue))
        else:
            st_temp_gmt=datetime.strptime(st_temp, '%Y-%m-%d %H:%M:%S').replace(tzinfo=from_zone)
            end_temp_gmt=datetime.strptime(end_temp, '%Y-%m-%d %H:%M:%S').replace(tzinfo=from_zone)
            st_cdt=str(st_temp_gmt.astimezone(to_zone)).split("-05:00")[0]
            end_cdt=str(end_temp_gmt.astimezone(to_zone)).split("-05:00")[0]
            mem_in_gb=(round(int(mem)/1024))
            final_list.append((app_id+"|"+user+"|"+str(st_cdt)+"|"+str(end_cdt)+"|"+str(mem_in_gb)+"|"+vCores+"|"+queue))

    with open(ffile, 'w+') as final:
        for item in final_list:
            final.write("%s\n" % item)

if __name__ == "__main__":
        convert(cfile,ffile)
