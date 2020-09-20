#!/bin/bash

function usage()
{
  echo "usage: $0 file1 file2" 1>&2
  echo "merges 2 files with an equal number of items of the form: ncpu rate" 1>&2
  exit 1
}

if [ $# -ne 2 ] ; then
  usage
fi

file1=$1
file2=$2

if [ ! -f "${file1}" ] || [ ! -f "${file2}" ] ; then
  echo missing: "${file1}" or "${file2}" 1>&2
  usage
fi

while true ; do
  IFS=' ' read -r -u 3 -a array1
  if [ ${#array1[@]} -ne 2 ] ; then
    break;
  fi
  IFS=' ' read -r -u 4 -a array2
  if [ ${#array2[@]} -ne 2 ] ; then
    break;
  fi
  n=$(echo ${array1[0]}+${array2[0]} | bc)
  rqs=$(echo ${array1[1]}+${array2[1]} | bc)
  echo $n $rqs
done 3<"${file1}" 4<"${file2}"

