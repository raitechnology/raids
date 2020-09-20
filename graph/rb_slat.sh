. $(dirname $0)/ds_test_functions.sh

find_sys_binary redis-benchmark
check_onload
results=rb_slat.${test_type}

function usage()
{
  echo usage: $0 address npipe
  echo where npipe is the maximum pipeline to run
  exit 1
}

address=$1
npipe=$2
pipe=1

function run_redis_benchmark()
{
  args="-h ${address} -p 6379 -n $(( ${pipe} * 100000 )) -P ${pipe} -c 1 -t get --csv"
  $accelerate taskset -c 2 redis-benchmark $args
}

if [ "$address" = "127.0.0.1" ] ; then
  results=rb_slat.localhost
fi

while [ $pipe -le $npipe ] ; do
  echo -n "$pipe "
  run_redis_benchmark | sed 's/[\"A-Z,]*\([0-9.]*\).*/\1/'
  pipe=$(( $pipe + 1 ))
done | tee ${results}
