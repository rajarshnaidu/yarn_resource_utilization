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

#beeline_url
beeline_url="jdbc:hive2://pwauslmnisd09.app.hcscint.net:2181,pwauslmnisd10.app.hcscint.net:2181,pwauslmnisd11.app.hcscint.net:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2"

#Cleanup
rm -r "$todays_dir";mkdir -p "$todays_dir"

######Reading_from_param_file###
read -r db table db_location <<< $(cat $conf_directory/create_table_params_dont_modify.log | awk -F'=|;' '{print $2}')

yarn_extraction()
{
        cluster_name()
        {
                name=$(curl -u admin:$1 -H "X-Requested-By: ambari" -i -X GET -k https://$ambari_host:8443/api/v1/clusters | grep "cluster_name" | awk -F ':' '{print $2}' | sed 's+"++g' | sed 's+,++g' | awk '{$1=$1;print}')
                echo $name
        }

        cname=$(cluster_name "$pass" "$ambari_host")

        rms=$(curl -k -H "X-Requested-By: ambari" -X GET -u admin:$pass https://"$ambari_host":8443/api/v1/clusters/"$cname"/services/YARN/components/RESOURCEMANAGER | grep 'host_name' | awk -F':' '{print $2}' | sed 's+"++g' | awk '{$1=$1;print}')

        resource_manager=$(for rm in $rms; do a=$(curl -k --negotiate -u : https://$rm:8090); if [[ -z $a ]]; then echo $rm; fi; done)

        echo -e "App_ID|App_name|User|Job_State|Start_Time|End_Time|Run_Time|Final_Status|Mem|vCores|Queue" > $todays_dir/"$todays_date".log

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
                read -r app_name user_who_ran started_time end_time ellapsed_time job_state final_status queue_name mem_api vCores_api \
                <<< $(curl -k --negotiate -u : https://$resource_manager:8090/ws/v1/cluster/apps/$app_id | \
                    jq '.app | .name,.user,.startedTime,.finishedTime,.elapsedTime,.state,.finalStatus,.queue,.memorySeconds,.vcoreSeconds' | \
                    sed 's+"++g' | sed 's+ +_+g')

                echo -e "$app_id|$app_name|$user_who_ran|$job_state|$started_time|$end_time|$ellapsed_time|$final_status|$mem_api|$vCores_api|$queue_name"
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
        echo "alter table "$db"."$table" add IF NOT EXISTS partition (reporting_date='"$todays_date"') location '"hdfs://sdlpnn"$db_location"/"$todays_date""';" | beeline -u "$beeline_url"
fi
