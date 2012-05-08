#!/bin/bash
WORKERS=$1
for (( i=0; i <= ${WORKERS} - 1; i++ )) 
do
  echo "starting $i node"
  node dump.js ${WORKERS} $i &
done
