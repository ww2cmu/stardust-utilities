#!/bin/sh

if [ -d "$1" ]
then
  for file in $1/*
  do
    java -cp lib/xtrace-2.0.jar edu.berkeley.xtrace.server.XTraceCollector $file data
  done
fi
