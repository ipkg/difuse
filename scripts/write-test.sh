#!/bin/bash

PORT=$1
COUNT=$2
PREFIX=$3
#VALUE_PREFIX=$4


if [ "${PORT}" == "" ]; then
    PORT=9090
fi

if [ "${COUNT}" == "" ]; then
    COUNT=100
fi

if [ "${PREFIX}" == "" ]; then
    PREFIX="key"
fi

#if [ "${VALUE_PREFIX}" == "" ]; then
#    VALUE_PREFIX="value"
#fi
stime=`date`
for i in `seq ${COUNT}`; do
    echo "* Key: ${PREFIX}${i}"
    d=`date '+%d.%m.%Y-%H:%M:%S'`
    curl -i localhost:${PORT}/${PREFIX}${i} -d "${i}-${d}"
    echo -e "\n-"
done
etime=`date`

echo "Start: ${stime}"
echo "End  : ${etime}"
