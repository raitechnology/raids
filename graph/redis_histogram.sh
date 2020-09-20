#!/bin/bash

function usage()
{
  echo "usage: $0 file1" 1>&2
  echo "Extract Redis histograph for gnuplot" 1>&2
  exit 1
}

if [ $# -ne 1 ] ; then
  usage
fi

file1=$1

if [ ! -f "${file1}" ] ; then
  echo missing: "${file1}" 1>&2
  usage
fi

function strip_quotes()
{
  sed -e 's/^"//' -e 's/"$//' <<<"$1"
}

while true ; do
  IFS=',' read -r -u 3 -a array
  if [ ${#array[@]} -eq 0 ] ; then
    break;
  fi
  cmd=$(strip_quotes ${array[0]})
  rate=$(strip_quotes ${array[1]})
  case $cmd in
    PING*)
    ;;
    LRANGE_100*)
    echo LR100 $rate
    ;;
    LRANGE_300*)
    echo LR300 $rate
    ;;
    LRANGE_500*)
    echo LR500 $rate
    ;;
    LRANGE_600*)
    echo LR600 $rate
    ;;
    MSET)
    echo MSET10 $rate
    ;;
    *)
    echo $cmd $rate
    ;;
  esac
done 3<"${file1}"

