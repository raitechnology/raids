#!/bin/bash

. $(dirname $0)/ds_test_functions.sh

find_build_binary ds_server
find_sys_binary taskset
check_onload
check_kv_server

function usage()
{
  echo usage: $0 baseport NCPU type
  echo where NCPU is the number of ds_server instances to run
  echo type restricts to a single protocol, one of
  echo   redis, memcd, memcd_udp, www, nats, capr, rv
  echo set USE_ONLOAD=yes to use the onload accelerator
  exit 1
}

if [ "x$3" = "x" ] ; then
  usage
fi

# start each instance with taskset CPU from 1 -> $count
baseport=$1
count=$2
ds_type=

case $3 in
  redis)
  ds_type=-p
  ;;
  memc*d)
  ds_type=-d
  ;;
  memc*d_udp)
  ds_type=-u
  ;;
  www|http)
  ds_type=-w
  ;;
  nats)
  ds_type=-n
  ;;
  capr)
  ds_type=-c
  ;;
  rv)
  ds_type=-r
  ;;
esac

start_ds_servers

# catch ctrl-c and jump to handler() to stop instances
trap sigint_handler SIGINT

echo waiting for ctr-c to stop $count instances
wait_for_all
