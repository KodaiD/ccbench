#pragma once

#include <pthread.h>
#include <atomic>
#include <iostream>
#include <queue>

#include "../../include/cache_line_size.hh"
#include "../../include/int64byte.hh"
#include "../../include/masstree_wrapper.hh"
#include "../../include/lock.hh"
#include "tuple.hh"
#include "version.hh"

#include "gflags/gflags.h"
#include "glog/logging.h"

#ifdef GLOBAL_VALUE_DEFINE
#define GLOBAL
alignas(CACHE_LINE_SIZE) GLOBAL std::atomic<uint64_t> MinRts(0);
alignas(CACHE_LINE_SIZE) GLOBAL std::atomic<uint64_t> MinWts(0);
alignas(
    CACHE_LINE_SIZE) GLOBAL std::atomic<unsigned int> FirstAllocateTimestamp(0);
#if MASSTREE_USE
alignas(CACHE_LINE_SIZE) GLOBAL MasstreeWrapper<Tuple> MT;
#endif
#else
#define GLOBAL extern
alignas(CACHE_LINE_SIZE) GLOBAL std::atomic<uint64_t> MinRts;
alignas(CACHE_LINE_SIZE) GLOBAL std::atomic<uint64_t> MinWts;
alignas(
CACHE_LINE_SIZE) GLOBAL std::atomic<unsigned int> FirstAllocateTimestamp;
#if MASSTREE_USE
alignas(CACHE_LINE_SIZE) GLOBAL MasstreeWrapper<Tuple> MT;
#endif
#endif

#ifdef GLOBAL_VALUE_DEFINE
DEFINE_uint64(clocks_per_us, 2100, "CPU_MHz. Use this info for measuring time.");
DEFINE_uint64(extime, 3, "Execution time[sec].");
DEFINE_uint64(gc_inter_us, 10, "GC interval[us].");
DEFINE_uint64(group_commit, 0, "Group commit number of transactions.");
DEFINE_uint64(group_commit_timeout_us, 2, "Timeout used for deadlock resolution when performing group commit[us].");
DEFINE_uint64(io_time_ns, 5, "Delay inserted instead of IO.");
DEFINE_uint64(max_ope, 10,
              "Total number of operations per single transaction.");
DEFINE_uint64(pre_reserve_version, 10000, "Pre-allocating memory for the version.");
DEFINE_bool(p_wal, false, "Parallel write-ahead logging.");
DEFINE_bool(rmw, false,
            "True means read modify write, false means blind write.");
DEFINE_uint64(rratio, 50, "read ratio of single transaction.");
DEFINE_bool(s_wal, false, "Normal write-ahead logging.");
DEFINE_uint64(thread_num, 10, "Total number of worker threads.");
DEFINE_uint64(tuple_num, 1000000, "Total number of records.");
DEFINE_bool(ycsb, true,
            "True uses zipf_skew, false uses faster random generator.");
DEFINE_uint64(worker1_insert_delay_rphase_us, 0, "Worker 1 insert delay in the end of read phase[us].");
DEFINE_double(zipf_skew, 0, "zipf skew. 0 ~ 0.999...");
DEFINE_uint64(ronly_ratio, 0, "ratio of read-only online transaction.");
DEFINE_uint64(batch_th_num, 0, "Number of batch worker threads.");
DEFINE_uint64(batch_ratio, 0, "ratio of batch transaction.");
DEFINE_uint64(batch_max_ope, 1000,
              "Total number of operations per single batch transaction.");
DEFINE_uint64(batch_rratio, 100, "read ratio of single batch transaction.");
DEFINE_uint64(batch_tuples, 0, "Number of update-only records for batch transaction.");
DEFINE_bool(batch_simple_rr, false, "No one touches update-only records of batch transaction.");
#else
DECLARE_uint64(clocks_per_us);
DECLARE_uint64(extime);
DECLARE_uint64(gc_inter_us);
DECLARE_uint64(group_commit);
DECLARE_uint64(group_commit_timeout_us);
DECLARE_uint64(io_time_ns);
DECLARE_uint64(max_ope);
DECLARE_uint64(pre_reserve_version);
DECLARE_bool(p_wal);
DECLARE_bool(rmw);
DECLARE_uint64(rratio);
DECLARE_bool(s_wal);
DECLARE_uint64(thread_num);
DECLARE_uint64(tuple_num);
DECLARE_bool(ycsb);
DECLARE_uint64(worker1_insert_delay_rphase_us);
DECLARE_double(zipf_skew);
DECLARE_uint64(ronly_ratio);
DECLARE_uint64(batch_th_num);
DECLARE_uint64(batch_ratio);
DECLARE_uint64(batch_max_ope);
DECLARE_uint64(batch_rratio);
DECLARE_uint64(batch_tuples);
DECLARE_bool(batch_simple_rr);
#endif

GLOBAL uint64_t TotalThreadNum;

alignas(CACHE_LINE_SIZE) GLOBAL uint64_t_64byte *ThreadWtsArray;
alignas(CACHE_LINE_SIZE) GLOBAL uint64_t_64byte *ThreadRtsArray;
alignas(CACHE_LINE_SIZE) GLOBAL uint64_t_64byte
        *ThreadRtsArrayForGroup;  // グループコミットをする時，これが必要である．

alignas(CACHE_LINE_SIZE) GLOBAL uint64_t_64byte *GROUP_COMMIT_INDEX;
alignas(CACHE_LINE_SIZE) GLOBAL uint64_t_64byte
        *GROUP_COMMIT_COUNTER;  // s-walの時は[0]のみ使用。全スレッドで共有。

alignas(
CACHE_LINE_SIZE) GLOBAL Version ***PLogSet;  // [thID][index] pointer array
alignas(CACHE_LINE_SIZE) GLOBAL Version **SLogSet;  // [index] pointer array
GLOBAL RWLock SwalLock;
GLOBAL RWLock CtrLock;

alignas(CACHE_LINE_SIZE) GLOBAL uint64_t_64byte *GCFlag;
alignas(CACHE_LINE_SIZE) GLOBAL uint64_t_64byte *GCExecuteFlag;

alignas(CACHE_LINE_SIZE) GLOBAL Tuple *Table;
alignas(CACHE_LINE_SIZE) GLOBAL uint64_t InitialWts;

#define SPIN_WAIT_TIMEOUT_US 2
