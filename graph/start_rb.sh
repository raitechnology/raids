#!/bin/bash

. $(dirname $0)/ds_test_functions.sh

find_sys_binary redis-benchmark
check_onload

function usage()
{
  echo usage: $0 address baseport NCPU pipesize datasize
  echo where NCPU is the number of redis-benchmark instances to run
  exit 1
}

if [ "x$5" = "x" ] ; then
  usage
fi

address=$1
baseport=$2
count=$3
pipesize=$4
datasize=$5
quantity=1000000000
# will be redis_get.kernel or redis_get.onload */
results=redis_get_p${pipesize}_d${datasize}.${test_type}

function start_redis_benchmark()
{
  args="-h ${address} -p ${port} -n ${quantity} -P ${pipesize} -c 50 -t get --csv"
  $accelerate taskset -c $cpu redis-benchmark $args > rb_get.${port} &
}

# ppulate the record
redis-benchmark -h ${address} -p ${baseport} -d ${datasize} -n 1 -c 1 -t set --csv > /dev/null

start_clients start_redis_benchmark

echo svrpids = ${svrpid[*]}

do_shutdown="kill -INT"

trap sigint_handler SIGINT

echo running $count instances for 30 seconds
sleep 30
do_shutdown="kill -HUP"
signal_all
wait_for_all

# sum the rq/sec on each CPU/port
i=0
sum=0
while [ $i -lt $count ] ; do
  port=$(($baseport + $i))
  i=$(($i + 1))
  IFS=',' read -a ar < rb_get.${port}
  rqs=$(sed -e 's/^"//' -e 's/"$//' <<<"${ar[1]}")
  if [ ! -z "${rqs}" ] ; then
    sum=$(echo ${sum}+${rqs} | bc)
  else
    echo no result from rb_get.${port}
  fi
done

# save results
echo Requests/sec: ${sum}
touch ${results}
echo ${count} ${sum} >> ${results}

echo complete

