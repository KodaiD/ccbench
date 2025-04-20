#!/bin/bash

echo "Start benchmark"

mkdir -p results && rm -rf results/*
dest=$(pwd)/results

## Server
#protocol=("silo" "silo2")
#threads=(1 8 16 24 32 40 48 56 64)
#read_rates=(0 50 100)
#num_rec=1000000
#num_ops=10
#num_sec=5

# Local
protocol=("silo" "silo2")
threads=(1 2 4 8)
read_rates=(0 50 100)
num_rec=1000000
num_ops=10
num_sec=5

for protocol in "${protocol[@]}"; do
for rratio in "${read_rates[@]}"; do
  mkdir -p ${dest}/ycsb-r${rratio}
  for thread in "${threads[@]}"; do
    print_latencies=0
    if [ $thread -eq 64 ]; then
      print_latencies=1
    fi
#    CMD="numactl --interleave=all ./${protocol}/${protocol}.exe"
    CMD="./build/${protocol}/ycsb_${protocol}.exe"
    CMD+=" --ycsb-rratio ${rratio}"
    CMD+=" --extime ${num_sec}"
    CMD+=" --ycsb-max-ope ${num_ops}"
    CMD+=" --ycsb-tuple-num ${num_rec}"
    CMD+=" --thread-num ${thread}"
    CMD+=" --print-latencies ${print_latencies}"
    echo ${CMD}
    echo Executing: ${CMD} >> ${dest}/ycsb-r${rratio}/${protocol}.log
    ${CMD} >> ${dest}/ycsb-r${rratio}/${protocol}.log || { echo "Error: Command failed - ${CMD}"; exit 1; }
  done
done
done