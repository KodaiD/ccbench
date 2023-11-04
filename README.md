# CCBench

This repository provides an extended version of [CCBench](https://github.com/thawk105/ccbench), which supports additional workloads and protocols.

## Build

First, clone the repository and install the required packages. Note that the following steps are for Ubuntu. For other Linux distributions, install the equivalent packages.

```sh
$ git clone --recurse-submodules this_repository
$ cd ccbench
$ sudo apt update -y && sudo apt-get install -y $(cat build_tools/ubuntu.deps)
```

Then, build a dependent library.

```sh
$ cd ccbench
$ ./build_tools/bootstrap.sh
$ ./build_tools/bootstrap_mimalloc.sh
```

Finally, build benchmark binaries with all supported protocols.

```sh
$ mkdir build
$ cd build
$ cmake -DCMAKE_BUILD_TYPE=Release ..
$ make
```

## Run

You can run three benchmark workloads with various protocols: YCSB, TPC-C, and BoMB. BoMB (Bill of Materials Benchmark) is a new OLTP benchmark reproducing a workload for manufacturing companies, including long-running update transactions and various short transactions.

For example, if you want to run BoMB with Silo, run the following command.

```sh
$ cd build
$ ./silo/bomb_silo.exe --thread-num 8 --bomb-mixed-mode --bomb-mixed-short-rate 1000
```

Workloads and protocols can be switched like the following.

```sh
$ ./tictoc/bomb_tictoc.exe --thread-num 8 --bomb-mixed-mode --bomb-mixed-short-rate 1000
$ ./cicada/tpcc_cicada.exe --thread-num 8 --tpcc-num-wh 8
$ ./cicada/ycsb_cicada.exe --thread-num 8 --ycsb-rratio 50
```

See usage with the `--help` option for details of the workload-specific options.