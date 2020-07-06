#!/bin/bash

START_DATE='2019-07-18'
END_DATE='2019-09-01'
echo 'Create directory at /tmp/cch/'$START_DATE

. ~/somenergia/conf/empowering_vars.sh


startdate=$(date -d "$START_DATE" +%s) || exit -1
enddate=$(date -d "$END_DATE" +%s)     || exit -1

d="$startdate"
d_in_days=$(date -I -d "$START_DATE")
start_date_days=$(date -I -d "$START_DATE")

while [ "$d" -le "$enddate" ]; do
    mkdir /tmp/cch/$d_in_days
    echo $d_in_days
    echo $start_date_days
    end_date=$(date -I -d "$d_in_days + 1 day")
    echo $end_date

    PYTHONPATH=~/somenergia/erp/server/sitecustomize/ /home/joana/.virtualenvs/erp/bin/python ~/somenergia/cchuploader/cchuploader/uploader.py --curve cchfact --start_date $start_date_days --end_date $end_date --contracts_file ~/somenergia/empowering-scripts/ctrl/id_contracts_empresas_2020_07_02.csv post /tmp/cch/$d_in_days
    echo 'we are done with cchfact'

    d_in_days=$(date -I -d "$d_in_days + 1 day")
    start_date_days=$(date -I -d "$start_date_days + 1 day")
    echo $start_date_days
    d=$(date -d "$d_in_days" +%s)

done
echo 'Done'
