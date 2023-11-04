
#include <ctype.h>  //isdigit,
#include <pthread.h>
#include <string.h>       //strlen,
#include <sys/syscall.h>  //syscall(SYS_gettid),
#include <sys/types.h>    //syscall(SYS_gettid),
#include <time.h>
#include <unistd.h>  //syscall(SYS_gettid),
#include <iostream>
#include <string>  //string

#define GLOBAL_VALUE_DEFINE

#include "include/common.hh"
#include "include/garbage_collection.hh"
#include "include/result.hh"
#include "include/transaction.hh"
#include "include/util.hh"

#include "../include/atomic_wrapper.hh"
#include "../include/backoff.hh"
#include "../include/bomb.hh"
#include "../include/cpu.hh"
#include "../include/debug.hh"
#include "../include/int64byte.hh"
#include "../include/masstree_wrapper.hh"
#include "../include/procedure.hh"
#include "../include/random.hh"
#include "../include/result.hh"
#include "../include/tsc.hh"
#include "../include/util.hh"
#include "../include/zipf.hh"

using namespace std;

void worker(size_t thid, char &ready, const bool &start, const bool &quit) {
#if MASSTREE_USE
  MasstreeWrapper<Tuple>::thread_init(int(thid));
#endif

  Backoff backoff(FLAGS_clocks_per_us); // Cicada's backoff opt.
  TxExecutor trans(thid, backoff, (Result *) &ErmiaResult[thid], quit);
  Result &myres = std::ref(ErmiaResult[thid]);
  BombWorkload<Tuple,void> workload;

#ifdef Linux
  setThreadAffinity(thid);
  // printf("Thread #%zu: on CPU %d\n", thid, sched_getcpu());
  // printf("sysconf(_SC_NPROCESSORS_CONF) %ld\n",
  // sysconf(_SC_NPROCESSORS_CONF));
#endif  // Linux
  // printf("Thread #%d: on CPU %d\n", *myid, sched_getcpu());

  if (trans.isLeader()) trans.gcob.decideFirstRange();
  trans.gcstart_ = rdtscp();
  workload.prepare(trans, nullptr);

  storeRelease(ready, 1);
  while (!loadAcquire(start)) _mm_pause();
  while (!loadAcquire(quit)) {
    workload.run<TxExecutor,TransactionStatus>(trans);
  }
  return;
}

int main(int argc, char *argv[]) try {
  gflags::SetUsageMessage("BOMB ERMIA benchmark.");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  chkArg();
  BombWorkload<Tuple,void>::displayWorkloadParameter();
  BombWorkload<Tuple,void>::makeDB(nullptr);

  alignas(CACHE_LINE_SIZE) bool start = false;
  alignas(CACHE_LINE_SIZE) bool quit = false;
  initResult();
  std::vector<char> readys(TotalThreadNum + (FLAGS_bomb_mixed_mode ? 1 : 0));
  std::vector<std::thread> thv;
  for (size_t i = 0; i < TotalThreadNum; ++i)
    thv.emplace_back(worker, i, std::ref(readys[i]),
                     std::ref(start), std::ref(quit));
  if (FLAGS_bomb_mixed_mode) {
    thv.emplace_back(BombWorkload<Tuple,void>::request_dispatcher,
                     TotalThreadNum, std::ref(readys[TotalThreadNum]),
                     std::ref(start), std::ref(quit));
  }
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

  std::cout << "done" << std::endl;
  for (unsigned int i = 0; i < TotalThreadNum; ++i) {
    ErmiaResult[0].addLocalAllResult(ErmiaResult[i]);
    ErmiaResult[0].addLocalPerTxResult(ErmiaResult[i], TxTypes);
  }
  ShowOptParameters();
  std::cout << "actual_extime:\t" << actual_extime << std::endl;
  ErmiaResult[0].displayAllResult(FLAGS_clocks_per_us, FLAGS_extime, TotalThreadNum);
  std::cout << "Details per transaction type:" << std::endl;
  ErmiaResult[0].displayPerTxResult(TxTypes);

  return 0;
} catch (bad_alloc) {
  ERR;
}
