#!/bin/bash
#
# This script spins up a three node chordstore cluster.  It will kill all 3
# nodes on an interrupt or term.  Trying to interrupt before all 3 nodes are spun
# up will leave behind straggling processes.
#
set -e

NAME="difused"

[ -x "./${NAME}" ] || { echo "${NAME} not found!"; exit 1; }

# Additional nodes aside from the first one
COUNT=$1
if [ "${COUNT}" == "" ]; then
    COUNT=2
fi

bstart=4624
astart=9090

sltime=1
#PIDS=()

./${NAME} -debug &

for i in `seq 1 ${COUNT}`; do
    slt=`expr ${sltime} \+ ${i}`
    sleep ${slt};

    b=`expr ${bstart} \+ ${i}`
    a=`expr ${astart} \+ ${i}`

    ./${NAME} -b 127.0.0.1:$b -a 127.0.0.1:$a -j 127.0.0.1:$bstart -debug &
done

#echo "PIDS: ${PIDS[@]}"
trap "{ pkill ${NAME}; }" SIGINT SIGTERM
wait
