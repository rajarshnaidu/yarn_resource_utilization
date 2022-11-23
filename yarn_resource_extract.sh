#!/bin/bash

#Enable Debug
set -x

##Initializing_Kerberos
/usr/bin/klist -s || kinit -kt /etc/security/keytabs/a6736148-Prod.keytab a6736148@ADHCSCINT.NET

##Todays_Date##
pass=$1
ambari_host=""
todays_date=$(date +%Y-%m-%d)
enddate=$(date +%s%3N)
startdate=$(date +%s%3N -d"1 day ago")

#Directories_Location
base_directory="/opt/monitoring/yarn_queues"
bin_directory="/opt/monitoring/yarn_queues/bin"
conf_directory="/opt/monitoring/yarn_queues/conf"
log_directory="/opt/monitoring/yarn_queues/log"
todays_dir="$log_directory/tmp/"$todays_date""

#Cleanup
rm -r "$todays_dir";mkdir -p "$todays_dir"

######Reading_from_param_file###
db=$(cat $conf_directory/create_table_params_dont_modify.log | grep 'db=' | awk -F'=' '{print $2}' | awk -F';' '{print $1}')
table=$(cat $conf_directory/create_table_params_dont_modify.log | grep 'table' | awk -F'=' '{print $2}' | awk -F';' '{print $1}')
db_location=$(cat $conf_directory/create_table_params_dont_modify.log | grep 'db_location' | awk -F'=' '{print $2}' | awk -F';' '{print $1}')

yarn_extraction()
{
        cluster_name()
        {
                name=$(curl -u $user:$1 -H "X-Requested-By: ambari" -i -X GET -k https://$ambari_host:8443/api/v1/clusters | grep "cluster_name" | awk -F ':' '{print $2}' | sed 's+"++g' | sed 's+,++g' | awk '{$1=$1;print}')
                echo $name
        }

        cname=$(cluster_name "$pass" "$ambari_host")

        rms=$(curl -k -H "X-Requested-By: ambari" -X GET -u $user:$pass https://"$ambari_host":8443/api/v1/clusters/"$cname"/services/YARN/components/RESOURCEMANAGER | grep 'host_name' | awk -F':' '{print $2}' | sed 's+"++g' | awk '{$1=$1;print}')

        resource_manager=$(for rm in $rms; do a=$(curl -k --negotiate -u : https://$rm:8090); if [[ -z $a ]]; then echo $rm; fi; done)

        echo -e "App_ID|User|Start_Time|End_Time|Mem|vCores|Queue" > $todays_dir/"$todays_date".log

        appids()
        {
                appid_yes=$(curl -k --negotiate -u : "https://$resource_manager:8090/ws/v1/cluster/apps?startedTimeBegin=$startdate&startedTimeEnd=$enddate" | jq '.apps.app[].id' | sed 's+"++g' | sort | uniq)
                #appid_daybefore_completed_post=$(curl -k --negotiate -u : "https://$resource_manager:8090/ws/v1/cluster/apps?finishedTimeBegin=$finishedstartdate&finishedTimeEnd=$startdate" | jq '.apps.app[].id' | sed 's+"++g' | sort | uniq)
                #app_ids=$(echo $appid_yes $appid_daybefore_completed_post | sed 's+ +\n+g' | sort | uniq)
                app_ids=$(echo $appid_yes | sed 's+ +\n+g' | sort | uniq)
                echo $app_ids
        }

        app_ids=$(appids)

        convertEpoch(){
            echo `date '+%Y-%m-%d %H:%M:%S' -d@$(expr $1 / 1000)`
        }

        for app_id in `echo $app_ids`
        do
                user_who_ran=$(curl -k --negotiate -u : https://$resource_manager:8090/ws/v1/cluster/apps/$app_id | jq .app.user | sed 's+"++g')
                started_time=$(curl -k --negotiate -u : https://$resource_manager:8090/ws/v1/cluster/apps/$app_id | jq .app.startedTime)
                end_time=$(curl -k --negotiate -u : https://$resource_manager:8090/ws/v1/cluster/apps/$app_id | jq .app.finishedTime)
                queue_name=$(curl -k --negotiate -u : https://$resource_manager:8090/ws/v1/cluster/apps/$app_id | jq .app.queue | sed 's+"++g')
                mem_api=$(curl -k --negotiate -u : https://$resource_manager:8090/ws/v1/cluster/apps/$app_id | jq .app.memorySeconds)
                vCores_api=$(curl -k --negotiate -u : https://$resource_manager:8090/ws/v1/cluster/apps/$app_id | jq .app.vcoreSeconds)
                echo -e "$app_id|$user_who_ran|$started_time|$end_time|$mem_api|$vCores_api|$queue_name"
        done 1>>$todays_dir/"$todays_date".log 2>/dev/null
}

##Extracting_Results
yarn_extraction

##Converting_Results_As_Required
$PWD/convert.py $todays_dir/"$todays_date".log

if [[ $? -eq 0 ]] && [[ -s "$todays_dir"/final.log ]]; then
        hadoop fs -rm -r "$db_location"/"$todays_date";hadoop fs -mkdir -p "$db_location"/"$todays_date"
        hadoop fs -put "$todays_dir"/final.log "$db_location"/"$todays_date"/
        beeline -u $beeline_url -f "$conf_directory"/create_table.hql --hivevar db="$db" --hivevar table="$table" --hivevar db_location="$db_location"
        echo "alter table "$db"."$table" add IF NOT EXISTS partition (load_date='"$todays_date"') location '"hdfs://sdlpnn"$db_location"/"$todays_date""';" | hive
fi