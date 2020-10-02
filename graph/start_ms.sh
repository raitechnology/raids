#!/bin/bash

. $(dirname $0)/ds_test_functions.sh

find_sys_binary redis-benchmark
check_onload

function usage()
{
  echo usage: $0 address baseport NCPU multiget datasize
  echo where NCPU is the number of memaslap instances to run
  exit 1
}

if [ "x$5" = "x" ] ; then
  usage
fi

address=$1
baseport=$2
count=$3
multiget=$4
datasize=$5
# will be redis_get.kernel or redis_get.onload */
results=memaslap_p${multiget}_d${datasize}.${test_type}

cat > ms.cfg <<XX
# start_len end_len proportion (all 16 bytes)
key
16 16 1
# start_len end_len proportion (all 64 bytes)
value
${datasize} ${datasize} 1
# cmd_type(0:set,1:get) cmd_proportion (90% read, 10% write)
cmd
0 0
1 1
XX

function start_memaslap()
{
  args="-s ${address}:${port} -d ${multiget} -B -c 50 -o 1 -t 30s -F ms.cfg"
  $accelerate taskset -c $cpu memaslap $args > ms.${port} &
}

start_clients start_memaslap

echo svrpids = ${svrpid[*]}

do_shutdown="kill -INT"

trap sigint_handler SIGINT

echo running $count instances for 30 seconds
wait_for_all

# sum the rq/sec on each CPU/port
i=0
sum=0
while [ $i -lt $count ] ; do
  port=$(($baseport + $i))
  i=$(($i + 1))
  s=$(grep TPS ms.${port})
  read -a ar <<< $s
  rqs=${ar[6]}
  if [ ! -z "${rqs}" ] ; then
    sum=$(echo ${sum}+${rqs} | bc)
  else
    echo no result from ms.${port}
  fi
done

# save results
echo Requests/sec: ${sum}
touch ${results}
echo ${count} ${sum} >> ${results}

echo complete

