#!/bin/sh

if [ $# -eq 2 ] 
then
  if [ -d $1 ] 
  then
    java -cp $1/lib/*:$1/*:$1/conf/*: HBaseToFile $2
  fi
fi
