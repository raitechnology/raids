#!/bin/bash

. $(dirname $0)/ds_test_functions.sh

find_build_binary2 wrk
check_onload
results=wrk_results.${test_type}

function usage()
{
  echo usage: $0 address baseport NCPU
  echo where NCPU is the number of wrk instances to run
  exit 1
}

if [ "x$3" = "x" ] ; then
  usage
fi

address=$1
baseport=$2
count=$3

function start_wrk()
{
  $accelerate taskset -c $cpu wrk -t1 -c50 -d60s http://${address}:${port}/index.html > wrk.${port} 2>&1 &
}

start_clients start_wrk

echo svrpids = ${svrpid[*]}

do_shutdown="kill -INT"

trap sigint_handler SIGINT

echo waiting for $count instances, run time 60s

wait_for_all

# sum the rq/sec on each CPU/port
i=0
sum=0
while [ $i -lt $count ] ; do
  port=$(($baseport + $i))
  i=$(($i + 1))
  rqs=$(grep Requests wrk.${port} | cut -d ':' -f 2)
  if [ ! -z "${rqs}" ] ; then
    sum=$(echo ${sum}+${rqs} | bc)
  else
    echo no result from wrk.${port}
  fi
done

# save results
echo Requests/sec: ${sum}
touch ${results}
echo ${count} ${sum} >> ${results}

echo complete

