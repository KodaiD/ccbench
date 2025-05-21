#include <cstdio>
#include <sys/time.h>
#include <sys/types.h> // syscall(SYS_gettid),
#include <unistd.h>    // syscall(SYS_gettid),

#include <bitset>
#include <cstdint>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <thread>
#include <vector>

#include "include/atomic_tool.hh"
#include "include/common.hh"
#include "include/transaction.hh"
#include "include/tuple.hh"
#include "include/util.hh"

#include "../include/debug.hh"
#include "../include/masstree_wrapper.hh"
#include "../include/tsc.hh"
#include "../include/util.hh"

void chkArg() {
    displayParameter();
    TotalThreadNum = FLAGS_thread_num + FLAGS_batch_th_num;
    if (posix_memalign(reinterpret_cast<void**>(&ThLocalEpoch), CACHE_LINE_SIZE,
                       TotalThreadNum * sizeof(uint64_t_64byte)) != 0)
        ERR;
    if (posix_memalign(reinterpret_cast<void**>(&ThLocalTS), CACHE_LINE_SIZE,
                       TotalThreadNum * sizeof(uint64_t_64byte)) != 0)
        ERR;
    if (posix_memalign(reinterpret_cast<void**>(&ThLocalStatus),
                       CACHE_LINE_SIZE,
                       TotalThreadNum * sizeof(uint64_t_64byte)) != 0)
        ERR;
    if (posix_memalign(reinterpret_cast<void**>(&CTIDW), CACHE_LINE_SIZE,
                       TotalThreadNum * sizeof(uint64_t_64byte)) != 0)
        ERR;
    for (unsigned int i = 0; i < TotalThreadNum; ++i) {
        ThLocalEpoch[i].obj_ = 0;
        ThLocalTS[i].obj_ = i;
        ThLocalStatus[i].obj_ = STATUS_ACTIVE;
        CTIDW[i].obj_ = 0;
    }
}

bool chkEpochLoaded() {
    const uint64_t now_epo = atomicLoadGE();
    for (unsigned int i = 1; i < TotalThreadNum; ++i) {
        if (__atomic_load_n(&(ThLocalEpoch[i].obj_), __ATOMIC_ACQUIRE) !=
            now_epo)
            return false;
    }
    return true;
}

void displayParameter() {
    cout << "#FLAGS_clocks_per_us:\t" << FLAGS_clocks_per_us << endl;
    cout << "#FLAGS_epoch_time:\t" << FLAGS_epoch_time << endl;
    cout << "#FLAGS_extime:\t\t" << FLAGS_extime << endl;
    cout << "#FLAGS_thread_num:\t" << FLAGS_thread_num << endl;
    if (FLAGS_batch_th_num > 0 || FLAGS_batch_ratio > 0) {
        cout << "#FLAGS_batch_th_num:\t" << FLAGS_batch_th_num << endl;
        cout << "#FLAGS_batch_ratio:\t" << FLAGS_batch_ratio << endl;
        cout << "#FLAGS_batch_max_ope:\t" << FLAGS_batch_max_ope << endl;
        cout << "#FLAGS_batch_rratio:\t" << FLAGS_batch_rratio << endl;
        cout << "#FLAGS_batch_tuples:\t" << FLAGS_batch_tuples << endl;
        cout << "#FLAGS_batch_simple_rr:\t" << FLAGS_batch_simple_rr << endl;
    }
}

void siloLeaderWork(uint64_t& epoch_timer_start, uint64_t& epoch_timer_stop) {
    epoch_timer_stop = rdtscp();
    if (chkClkSpan(epoch_timer_start, epoch_timer_stop,
                   FLAGS_epoch_time * FLAGS_clocks_per_us * 1000) &&
        chkEpochLoaded()) {
        atomicAddGE();
        const uint32_t cur_epoch = atomicLoadGE();
        ReclamationEpoch = cur_epoch > 2 ? cur_epoch - 2 : 0;
        epoch_timer_start = epoch_timer_stop;
    }
}

void ShowOptParameters() {
    cout << "#ShowOptParameters()"
         << ": ADD_ANALYSIS " << ADD_ANALYSIS << ": BACK_OFF " << BACK_OFF
         << ": KEY_SIZE " << KEY_SIZE << ": MASSTREE_USE " << MASSTREE_USE
         << ": NO_WAIT_LOCKING_IN_VALIDATION " << NO_WAIT_LOCKING_IN_VALIDATION
         << ": PARTITION_TABLE " << PARTITION_TABLE << ": PROCEDURE_SORT "
         << PROCEDURE_SORT << ": SLEEP_READ_PHASE " << SLEEP_READ_PHASE
         << ": VAL_SIZE " << VAL_SIZE << ": WAL " << WAL
#ifdef INSERT_READ_DELAY_MS
         << ": INSERT_READ_DELAY_MS " << INSERT_READ_DELAY_MS
#endif
#ifdef INSERT_BATCH_DELAY_MS
         << ": INSERT_BATCH_DELAY_MS " << INSERT_BATCH_DELAY_MS
#endif
         << endl;
}
