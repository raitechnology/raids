#!/bin/bash

export osver=$(lsb_release -rs | sed 's/[.].*//')
export osdist=$(lsb_release -i | sed 's/.*:\t//')

if [ "${osdist}x" == "Fedorax" ] ; then
  export ospref=FC
elif [ "${osdist}x" == "Ubuntux" ] ; then
  export ospref=UB
elif [ "${osdist}x" == "Debianx" ] ; then
  export ospref=DEB
else
  export ospref=RH
fi

# look for binaries in build directory
function find_build_binary()
{
  if ! command -v $1 &> /dev/null
  then
    b=../${ospref}${osver}_x86_64/bin/$1
    if command -v $b &> /dev/null
    then
      echo found $b
      export PATH=$(dirname $b):$PATH
    else
      b=./${ospref}${osver}_x86_64/bin/$1
      if command -v $b &> /dev/null
      then
        echo found $b
        export PATH=$(dirname $b):$PATH
      else
        echo "$1 could not be found"
        exit 1
      fi
    fi
  fi
}

function find_sys_binary()
{
  if ! command -v $1 &> /dev/null
  then
    echo "$1 could not be found"
    exit 1
  fi
}

function find_build_binary2()
{
  if ! command -v $1 &> /dev/null
  then
    b=(./*/$1)
    if command -v ${b[0]} &> /dev/null
    then
      echo found ${b[0]}
      export PATH=$(dirname ${b[0]}):$PATH
    else
      b=(../*/$1)
      if command -v ${b[0]} &> /dev/null
      then
        echo found ${b[0]}
        export PATH=$(dirname ${b[0]}):$PATH
      else
        echo "$1 could not be found"
        exit 1
      fi
    fi
  fi
}

accelerate=""
test_type=kernel

function check_onload()
{
  # if using onload, may need to `sudo onload_tool reload` to load onload.ko
  if [ "${USE_ONLOAD}x" = "yesx" ] ; then
    find_sys_binary onload
    accelerate="onload --profile=latency"
    echo using onload with profile=latency
    test_type=onload
  else
    accelerate=""
    test_type=kernel
  fi
}

function check_kv_server()
{
  # kv_server creates shared memory segment for use
  if ! killall -0 kv_server
  then
    echo run kv_server to create shared memory segment
    echo this script does not create the shared memory, it reuses the existing
    exit 1
  fi
}

# the first port, increments from there
baseport=0
# the count of processes to create
count=0
# the processes tracked
svrpid=()
# what type of server to run
ds_type=-w
# which cpu to run
cpu=0

# logical cpu numbers are not linear with the siblings at the tail,
# make an array index where that is the case, (primary thr + hyper thr)
# this makes it easy to assign threads to cores without using
# hyperthreads unless there are no unused cores left
nprocessors=$(grep processor /proc/cpuinfo | wc -l)
primary_core_index=()
sibling_core_index=()
i=0
k=0
while [ $i -lt $nprocessors ] ; do
  f=/sys/devices/system/cpu/cpu${i}/topology/thread_siblings_list
  if [ ! -f $f ] ; then
    echo $f not found
    break
  fi
  # rh7 rh8 have 0,1
  IFS=',' read -a sib < $f
  if [ ${#sib[@]} -lt 2 ] ; then
    # fedora 31 has 0-1
    IFS='-' read -a sib < $f
    if [ ${#sib[@]} -lt 2 ] ; then
      break
    fi
  fi
  # use the lowest core number as the primary core
  if [ ${sib[0]} -lt ${sib[1]} ] ; then
    x=${sib[0]}
    y=${sib[1]}
  else
    x=${sib[1]}
    y=${sib[0]}
  fi
  # if this is the primary index, not the hyperthread
  if [ $x -eq $i ] ; then
    primary_core_index[$k]=$x
    sibling_core_index[$k]=$y
    k=$(($k + 1))
  fi
  i=$(($i + 1))
done

# no hyperthreads found if $i < $nprocessors
while [ $i -lt $nprocessors ] ; do
  primary_core_index+=($i)
  i=$(($i + 1))
done

core_index=(${primary_core_index[*]} ${sibling_core_index[*]})

primary_core_index=()
sibling_core_index=()
i=0
k=0
while [ $i -lt $nprocessors ] ; do
  f=/sys/devices/system/cpu/cpu${i}/topology/thread_siblings_list
  if [ -f $f ] ; then
    IFS=',' read -a sib < $f
    # use the lowest core number as the primary core
    if [ ${sib[0]} -lt ${sib[1]} ] ; then
      x=${sib[0]} 
      y=${sib[1]} 
    else
      x=${sib[1]} 
      y=${sib[0]} 
    fi
    # if this is the primary index, not the hyperthread
    if [ $x -eq $i ] ; then
      primary_core_index[$k]=$x
      sibling_core_index[$k]=$y
      k=$(($k + 1))
    fi
  fi
  i=$(($i + 1))
done

core_index=(${primary_core_index[*]} ${sibling_core_index[*]})
#echo ${#core_index[@]}
#echo ${core_index[*]}

# start at cpu 1 instead of cpu 0
if [ ! -z "${BASECPU}" ] ; then
  basecpu=${BASECPU}
else
  basecpu=1
fi

# convert an index into a core number
function get_cpu()
{
  cpu=${core_index[$((($basecpu + $1) % $nprocessors))]}
}

function start_ds_servers()
{
  i=0
  while [ $i -lt $count ] ; do
    port=$(($baseport + $i))
    get_cpu $i
    i=$(($i + 1))
    echo Starting instance on CPU $cpu listen on port $port
    $accelerate taskset -c $cpu ds_server -X $ds_type $port &
    svrpid+=( $! )
    usleep 1000000
  done
  if [ ${#svrpid[@]} -eq 0 ] ; then
    echo no instances started
    usage
  fi
}

function start_clients()
{
  i=0
  while [ $i -lt $count ] ; do
    port=$(($baseport + $i))
    get_cpu $i
    i=$(($i + 1))
    echo Starting instance on CPU $cpu connect to port $port
    $1
    svrpid+=( $! )
    usleep 100000
  done
  if [ ${#svrpid[@]} -eq 0 ] ; then
    echo no instances started
    usage
  fi
}

do_shutdown="kill -HUP"
pid=0

function signal_all()
{
  i=0
  while [ $i -lt ${#svrpid[@]} ] ; do
    pid=${svrpid[$i]}
    port=$(($baseport + $i))
    get_cpu $i
    i=$(($i + 1))
    echo Stopping instance on CPU $cpu port $port pid $pid
    echo $do_shutdown $pid
    $do_shutdown $pid
    if [ ! -z "$1" ] ; then
      usleep $1
    fi
  done
}

# send a shutdown to ds_server port
function sigint_handler()
{
  echo caught ctrl-c
  signal_all 100000
  echo done
  exit 0
}

function wait_for_all()
{
# wait for ctrl-c or instances to exit
  i=0
  while [ $i -lt ${#svrpid[@]} ] ; do
    wait ${svrpid[$i]}
    status=$?
    echo pid ${svrpid[$i]} exit status $status
    i=$(($i + 1))
  done
}

