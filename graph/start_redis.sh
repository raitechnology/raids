#!/bin/bash

. $(dirname $0)/ds_test_functions.sh

find_build_binary ds_server
find_sys_binary taskset
check_onload
check_kv_server

function usage()
{
  echo usage: $0 baseport NCPU
  echo where NCPU is the number of ds_server instances to run
  echo set USE_ONLOAD=yes to use the onload accelerator
  exit 1
}

if [ "x$2" = "x" ] ; then
  usage
fi

# start each instance with taskset CPU from 1 -> $count
baseport=$1
count=$2
ds_type=-p

start_ds_servers

# catch ctrl-c and jump to handler() to stop instances
trap sigint_handler SIGINT

echo waiting for ctr-c to stop $count instances

# wait for ctrl-c or instances to exit
i=0
while [ $i -lt $count ] ; do
  wait ${svrpid[$i]}
  status=$?
  echo wrk pid ${svrpid[$i]} exit status $status
  i=$(($i + 1))
done
