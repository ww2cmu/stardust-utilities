#!/bin/sh
if [ -d "$1" ] 
then
  l=`find $1 -type f -printf "%p\n"`
  x=1
  for f in ${l}
  do
    cat ${f} | java SpecRecon ${x} 
    x=$(($x+1))
  done
fi
