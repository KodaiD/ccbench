#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <cstring>
#include <pthread.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include "boost/filesystem.hpp"

#define GLOBAL_VALUE_DEFINE

#include "include/atomic_tool.hh"
#include "include/common.hh"
#include "include/result.hh"
#include "include/transaction.hh"
#include "include/util.hh"

#include "../include/atomic_wrapper.hh"
#include "../include/backoff.hh"
#include "../include/debug.hh"
#include "../include/fileio.hh"
#include "../include/masstree_wrapper.hh"
#include "../include/result.hh"
#include "../include/tsc.hh"
#include "../include/util.hh"
#include "../include/ycsb.hh"

using namespace std;

void worker(size_t thid, char& ready, const bool& start, const bool& quit) {
    Result& my_res = std::ref(SiloResult[thid]);
    TxExecutor trans(static_cast<int>(thid), &my_res, quit);
    YcsbWorkload workload;
#if BACK_OFF
    Backoff backoff(FLAGS_clocks_per_us);
#endif
#ifdef Linux
    setThreadAffinity(thid);
    // printf("Thread #%d: on CPU %d\n", res.thid_, sched_getcpu());
    // printf("sysconf(_SC_NPROCESSORS_CONF) %d\n",
    // sysconf(_SC_NPROCESSORS_CONF));
#endif
#if MASSTREE_USE
    MasstreeWrapper<Tuple>::thread_init(static_cast<int>(thid));
#endif
    storeRelease(ready, 1);
    while (!loadAcquire(start)) _mm_pause();
    if (thid == 0) trans.epoch_timer_start = rdtscp();
    while (!loadAcquire(quit)) {
        workload.run<TxExecutor, TransactionStatus>(trans);
    }
}

int main(int argc, char* argv[]) try {
    gflags::SetUsageMessage("YCSB Silo benchmark.");
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    chkArg();
    YcsbWorkload::displayWorkloadParameter();
    YcsbWorkload::makeDB<Tuple, void>(nullptr);
    alignas(CACHE_LINE_SIZE) bool start = false;
    alignas(CACHE_LINE_SIZE) bool quit = false;
    initResult();
    std::vector<char> ready(TotalThreadNum);
    std::vector<std::thread> thv;
    for (size_t i = 0; i < TotalThreadNum; ++i)
        thv.emplace_back(worker, i, std::ref(ready[i]), std::ref(start),
                         std::ref(quit));
    waitForReady(ready);
    const uint64_t start_tsc = rdtscp();
    storeRelease(start, true);
    for (size_t i = 0; i < FLAGS_extime; ++i) { sleepMs(1000); }
    storeRelease(quit, true);
    for (auto& th : thv) th.join();
    const uint64_t end_tsc = rdtscp();
    const long double actual_extime = round(
            (end_tsc - start_tsc) /
            (static_cast<long double>(FLAGS_clocks_per_us) * powl(10.0, 6.0)));
    for (unsigned int i = 0; i < TotalThreadNum; ++i) {
        SiloResult[0].addLocalAllResult(SiloResult[i]);
    }
    ShowOptParameters();
    std::cout << "actual_extime:\t" << actual_extime << std::endl;
    SiloResult[0].displayAllResult(FLAGS_clocks_per_us, FLAGS_extime,
                                   TotalThreadNum);
    SiloResult[0].displayLatencies("silo", FLAGS_print_latencies);
    return 0;
} catch (bad_alloc) { ERR; }
