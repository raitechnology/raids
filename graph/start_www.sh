#!/bin/bash

. $(dirname $0)/ds_test_functions.sh

find_build_binary kv_server
find_build_binary ds_server
find_sys_binary taskset
find_sys_binary curl
check_onload

function usage()
{
  echo usage: $0 baseport NCPU
  echo NCPU is the number of ds_server instances to run
  echo baseport is the first port to use
  echo set USE_ONLOAD=yes to use the onload accelerator
  stop_kv_server
  exit 1
}

if [ "x$2" = "x" ] ; then
  usage
fi

# start each instance with taskset CPU from 1 -> $count
baseport=$1
count=$2
ds_type=-w

start_kv_server
start_ds_servers

# catch ctrl-c and jump to handler() to stop instances
trap sigint_handler SIGINT

# create a web page to testing
cat > /tmp/mypage <<DATA
<html>
<body>
hello world
</body>
</html>
DATA

echo loading data to index.html

curl -X POST -H 'Content-Type: text/plain' --data-binary '@/tmp/mypage' http://127.0.0.1:${baseport}/index.html
/bin/rm /tmp/mypage

echo waiting for exit or ctrl-c to stop $count instances
wait_for_all
stop_kv_server
