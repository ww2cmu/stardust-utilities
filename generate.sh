#!/bin/sh
if [ -d "$1" ] 
then
  l=`find $1 -type f -printf "%p\n"`
  x=1
  y=0
  z=0
  for f in ${l}
  do
    cat ${f} | java Reconstruct ${x} 
    if [ $? -eq 1 ]
    then
      y=$(($y+1))
    else
      x=$(($x+1))
    fi
    z=$(($z+1))
  done
  printf "Total number: $z\n"
  printf "Malformed: $y\n"
fi
