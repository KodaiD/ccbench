#include <ctype.h>
#include <pthread.h>
#include <sched.h>
#include <stdlib.h>
#include <string.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <unistd.h>
#include <algorithm>
#include <cctype>
#include <cstdint>
#include <functional>
#include <random>
#include <thread>

#define GLOBAL_VALUE_DEFINE

#include "../include/atomic_wrapper.hh"
#include "../include/backoff.hh"
#include "../include/compiler.hh"
#include "../include/cpu.hh"
#include "../include/debug.hh"
#include "../include/delay.hh"
#include "../include/int64byte.hh"
#include "../include/procedure.hh"
#include "../include/random.hh"
#include "../include/result.hh"
#include "../include/util.hh"
#include "../include/zipf.hh"
#include "include/common.hh"
#include "include/result.hh"
#include "include/transaction.hh"
#include "include/util.hh"

using namespace std;

void worker(size_t thid, char &ready, const bool &start, const bool &quit) {
  Xoroshiro128Plus rnd;
  rnd.init();
  TxExecutor trans(thid, (Result *) &OzeResult[thid], quit);
  Result &myres = std::ref(OzeResult[thid]);
  uint64_t epoch_timer_start, epoch_timer_stop;
  FastZipf zipf(&rnd, FLAGS_zipf_skew, FLAGS_tuple_num);
  Backoff backoff(FLAGS_clocks_per_us);

#ifdef Linux
  setThreadAffinity(thid);
  // printf("Thread #%d: on CPU %d\n", *myid, sched_getcpu());
  // printf("sysconf(_SC_NPROCESSORS_CONF) %d\n",
  // sysconf(_SC_NPROCESSORS_CONF));
#endif  // Linux

#ifdef Darwin
  int nowcpu;
  GETCPU(nowcpu);
  // printf("Thread %d on CPU %d\n", *myid, nowcpu);
#endif  // Darwin

   uint64_t tuples = FLAGS_tuple_num;
  if (FLAGS_batch_simple_rr) {
    tuples = FLAGS_tuple_num - FLAGS_batch_tuples;
  }

  storeRelease(ready, 1);
  while (!loadAcquire(start)) _mm_pause();
  if (thid == 0) epoch_timer_start = rdtscp();
  while (!loadAcquire(quit)) {
#if PARTITION_TABLE
    makeProcedure(trans.pro_set_, rnd, zipf, FLAGS_tuple_num, FLAGS_max_ope, FLAGS_thread_num,
                  FLAGS_rratio, FLAGS_rmw, FLAGS_ycsb, true, thid, myres);
#else
    auto r = rnd.next() % 100;
    if ((FLAGS_thread_num && thid >= FLAGS_thread_num)
      || (r < FLAGS_batch_ratio)) {
      trans.is_batch_ = true;
      makeBatchProcedure(trans.pro_set_, rnd, FLAGS_tuple_num, FLAGS_batch_tuples,
                         FLAGS_batch_max_ope, FLAGS_batch_rratio, FLAGS_rmw,
                         myres);
    } else if (r >= FLAGS_batch_ratio
                && r < FLAGS_batch_ratio + FLAGS_ronly_ratio) {
      trans.is_batch_ = false;
      makeProcedure(trans.pro_set_, rnd, FLAGS_tuple_num, FLAGS_batch_tuples,
                    FLAGS_max_ope, myres);
    } else {
      trans.is_batch_ = false;
      makeProcedure(trans.pro_set_, rnd, zipf, tuples, FLAGS_max_ope,
                    FLAGS_thread_num, FLAGS_rratio, FLAGS_rmw, FLAGS_ycsb,
                    false, thid, myres);
    }
#endif

RETRY:
    if (thid == 0) {
      leaderWork(epoch_timer_start, epoch_timer_stop);
#if BACK_OFF
      leaderBackoffWork(backoff, CicadaResult);
#endif
    }
    if (loadAcquire(quit)) break;

    trans.tbegin();
    for (auto itr = trans.pro_set_.begin(); itr != trans.pro_set_.end();
         ++itr) {
      if ((*itr).ope_ == Ope::READ) {
        trans.tread((*itr).key_);
#ifdef INSERT_READ_DELAY_MS
        sleepMs(INSERT_READ_DELAY_MS);
#endif
      } else if ((*itr).ope_ == Ope::WRITE) {
        trans.twrite((*itr).key_);
      } else if ((*itr).ope_ == Ope::READ_MODIFY_WRITE) {
        trans.tread((*itr).key_);
#ifdef INSERT_READ_DELAY_MS
        sleepMs(INSERT_READ_DELAY_MS);
#endif
        trans.twrite((*itr).key_);
      } else {
        ERR;
      }

      if (trans.status_ == TransactionStatus::abort) {
        trans.abort();
        trans.mainte();
        goto RETRY;
      }
    }

#ifdef INSERT_BATCH_DELAY_MS
    if (trans.is_batch_) {
      sleepMs(INSERT_BATCH_DELAY_MS);
    }
#endif

    /**
     * Validation phase
     */
    if (!trans.validation()) {
      trans.abort();
      if (trans.is_batch_) {
        ++myres.local_batch_abort_counts_;
      } else {
        ++myres.local_abort_counts_;
      }
#if SINGLE_EXEC
#else
      /**
       * Maintenance phase
       */
      trans.mainte();
#endif
      goto RETRY;
    }

    // Abondon ongoing long transaction when time up
    if (trans.status_ == TransactionStatus::invalid)
      break;

    /**
     * Write phase
     */
    trans.writePhase();
    /**
     * local_commit_counts is used at ../include/backoff.hh to calcurate about
     * backoff.
     */
    if (trans.is_batch_) {
      storeRelease(myres.local_batch_commit_counts_,
                  loadAcquire(myres.local_batch_commit_counts_) + 1);
    } else {
      storeRelease(myres.local_commit_counts_,
                  loadAcquire(myres.local_commit_counts_) + 1);
    }

    /**
     * Maintenance phase
     */
    trans.mainte();
  }

  return;
}

int main(int argc, char *argv[]) try {
  google::InitGoogleLogging(argv[0]);
  google::InstallFailureSignalHandler();

  gflags::SetUsageMessage("Oze benchmark.");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  chkArg();
  uint64_t initial_wts;
  makeDB(&initial_wts);

  alignas(CACHE_LINE_SIZE) bool start = false;
  alignas(CACHE_LINE_SIZE) bool quit = false;
  initResult();
  std::vector<char> readys(TotalThreadNum);
  std::vector<std::thread> thv;
  for (size_t i = 0; i < TotalThreadNum; ++i)
    thv.emplace_back(worker, i, std::ref(readys[i]), std::ref(start),
                     std::ref(quit));
  waitForReady(readys);
  uint64_t start_tsc = rdtscp();
  storeRelease(start, true);
  for (size_t i = 0; i < FLAGS_extime; ++i) {
    sleepMs(1000);
  }
  storeRelease(quit, true);
  for (auto &th : thv) th.join();
  uint64_t end_tsc = rdtscp();
  long double actual_extime = round(
    (end_tsc-start_tsc) /
    ((long double)FLAGS_clocks_per_us * powl(10.0, 6.0)));

  for (unsigned int i = 0; i < TotalThreadNum; ++i) {
    OzeResult[0].addLocalAllResult(OzeResult[i]);
  }
  ShowOptParameters();
  std::cout << "actual_extime:\t" << actual_extime << std::endl;
  OzeResult[0].displayAllResult(FLAGS_clocks_per_us, FLAGS_extime,
                                 TotalThreadNum,
                                 FLAGS_max_ope, FLAGS_batch_max_ope);
  deleteDB();

  return 0;
} catch (bad_alloc) {
  ERR;
}
