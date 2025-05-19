#pragma once

#include <string_view>
#include <vector>

#include "../../include/backoff.hh"
#include "../../include/fileio.hh"
#include "../../include/procedure.hh"
#include "../../include/result.hh"
#include "../../include/status.hh"
#include "../../include/workload.hh"
#include "common.hh"
#include "scan_callback.hh"
#include "silo_op_element.hh"
#include "tuple.hh"

enum class TransactionStatus : uint8_t {
    invalid,
    inflight,
    committed,
    aborted,
};

enum class RC : uint8_t {
    WOUND,
    DIE,
    RETRY,
};

class TxScanCallback;

class TxExecutor {
public:
    std::vector<ReadElement<Tuple>> read_set_;
    std::vector<WriteElement<Tuple>> write_set_;
    std::unordered_map<void*, uint64_t> node_map_;
    std::vector<Procedure> pro_set_;
    std::deque<Tuple*> gc_records_;
    TransactionStatus status_;
    size_t thid_;
    Result* result_;
    uint64_t epoch_timer_start, epoch_timer_stop;
    Backoff backoff_;
    const bool& quit_;
    TxScanCallback callback_;
    bool reconnoitering_ = false;
    bool is_ronly_ = false;
    bool is_batch_ = false;

    Tidword mrc_tid_;
    Tidword max_rset_, max_wset_;
    bool super_ = false;
    uint64_t abort_cnt_ = 0;
    uint64_t t_ = 0;

    TxExecutor(const int thid, Result* res, const bool& quit)
        : status_(), thid_(thid), result_(res), backoff_(FLAGS_clocks_per_us),
          quit_(quit), callback_(TxScanCallback(this)) {
        max_rset_.obj_ = 0;
        max_wset_.obj_ = 0;
        epoch_timer_start = rdtsc();
        epoch_timer_stop = 0;
        ThLocalTS[thid].obj_ = thid;
    }

    void abort();

    void begin();

    Status insert(Storage s, std::string_view key, TupleBody&& body);

    Status delete_record(Storage s, std::string_view key);

    void lockWriteSet();

    Status read(Storage s, std::string_view key, TupleBody** body);

    Status read_internal(Storage s, std::string_view key, Tuple* tuple);

    Status scan(Storage s, std::string_view left_key, bool l_exclusive,
                std::string_view right_key, bool r_exclusive,
                std::vector<TupleBody*>& result);

    Status scan(Storage s, std::string_view left_key, bool l_exclusive,
                std::string_view right_key, bool r_exclusive,
                std::vector<TupleBody*>& result, int64_t limit);

    ReadElement<Tuple>* searchReadSet(Storage s, std::string_view key);

    WriteElement<Tuple>* searchWriteSet(Storage s, std::string_view key);

    void unlockWriteSet() const;

    void unlockWriteSet(std::vector<WriteElement<Tuple>>::iterator end);

    void unlockReadSet() const;

    bool validationPhase();

    Status write(Storage s, std::string_view key, TupleBody&& body);

    void writePhase();

    bool commit();

    void reconnoiter_begin();

    void reconnoiter_end();

    bool isLeader() const;

    void leaderWork();

    void gc_records();

    static void notify(uint16_t thid);

    bool less_than(const Tidword& tid_word) const;

    void prepare_abort();

    RC wound_or_die(Tuple* tuple, Tidword expected, uint8_t mode);

    bool try_lock(Tuple* tuple, Tidword& expected, uint8_t mode) const;

    static bool validate(Tuple* tuple, Tidword& expected);

    void unlock(Tuple* tuple) const;
};
