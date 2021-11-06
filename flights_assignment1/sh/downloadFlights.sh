#!/bin/bash

STARTYEAR=2018
ENDYEAR=2021

echo "Downloading data: "
for YEAR in `seq -w ${STARTYEAR} ${ENDYEAR}`
do
    for MONTH in `seq 1 12`
    do
      URL="https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_${YEAR}_${MONTH}.zip"
      echo -n "...downloading ${MONTH}/${YEAR}"
      DOWNLOADED_FILE=${YEAR}-${MONTH}.zip
      curl -k --ciphers 'HIGH:!DH:!aNULL' -o ${DOWNLOADED_FILE} ${URL}
      if [ -f ${DOWNLOADED_FILE} ]; then
        echo " --> SUCCESS"
      else
        echo " --> ERROR"
      fi
    done
done
ls -htl *.zip

echo "Unzipping data: "
for zipfile in `ls *-*.zip`
do
    echo "...Unzipping ${zipfile}"
    unzip ${zipfile}
    rm -f readme.html
    rm -f ${zipfile}
done
ls -htl *.csv

echo "Cleaning data: "
for YEAR in `seq -w ${STARTYEAR} ${ENDYEAR}`
do
    for MONTH in `seq 1 12`
    do
      DOWNLOADED_FILE=${YEAR}-${MONTH}.zip
      if [ -f ${DOWNLOADED_FILE} ]; then
            # If the download file exists...
            echo -n "...Cleaning ${MONTH}/${YEAR}"
            cat On_Time*${YEAR}_${MONTH}.csv | sed -e 's/,$//g' -e 's/"//g' > ${YEAR}-${MONTH}.csv
            rm -f ${DOWNLOAD_FILE}
            rm -f On_Time*${YEAR}_${MONTH}.csv
            if [ -f ${YEAR}-${MONTH}.csv ]; then
              echo " --> SUCCESS"
            else
              echo " --> ERROR"
            fi
        fi
    done
done
ls -htl *.csv