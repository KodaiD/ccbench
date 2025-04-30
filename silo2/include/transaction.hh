#pragma once

#include <iostream>
#include <set>
#include <string_view>
#include <vector>

#include "../../include/backoff.hh"
#include "../../include/fileio.hh"
#include "../../include/procedure.hh"
#include "../../include/result.hh"
#include "../../include/status.hh"
#include "../../include/string.hh"
#include "../../include/workload.hh"
#include "common.hh"
#include "log.hh"
#include "scan_callback.hh"
#include "silo_op_element.hh"
#include "tuple.hh"

#define LOGSET_SIZE 1000
#define NODE_POOL_SIZE 1000

enum class TransactionStatus : uint8_t {
    invalid,
    inflight,
    committed,
    aborted,
};

class TxScanCallback;

class TxExecutor {
public:
    // Local sets
    std::vector<ReadElement<Tuple>> read_set_;
    std::vector<WriteElement<Tuple>> write_set_;
    std::vector<Procedure> pro_set_;
    std::deque<Tuple*> gc_records_;
    std::unordered_map<void*, uint64_t> node_map_;
    std::vector<LogRecord> log_set_;
    std::array<MCSMutex::MCSNode, NODE_POOL_SIZE> lock_nodes_{};
    std::unordered_map<MCSMutex::MCSNode*, bool> lock_nodes_map_;
    size_t next_free_node = 0;

    // Utility
    LogHeader latest_log_header_;
    Result* result_;
    uint64_t epoch_timer_start, epoch_timer_stop;
    Backoff backoff_;
    const bool& quit_;
    TxScanCallback callback_;
    File logfile_;
    bool reconnoitering_ = false;
    bool is_ronly_ = false;
    bool is_batch_ = false;
    TransactionStatus status_;
    size_t thid_;

    // Protocol-related
    Tidword mrctid_;
    Tidword max_rset_, max_wset_;
    uint64_t abort_cnt = 0;
    bool is_pcc_ = false;
    std::vector<uint64_t>& privileges_;

    TxExecutor(int thid, Result* res, const bool& quit,
               std::vector<uint64_t>& privileges);

    void abort();

    void begin();

    void tx_delete(std::uint64_t key);

    void displayWriteSet();

    Tuple* get_tuple(Tuple* table, std::uint64_t key) { return &table[key]; }

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

    void unlockReadSet();

    vector<ReadElement<Tuple>>::iterator
    searchReadSetIterator(Storage s, std::string_view key);

    ReadElement<Tuple>* searchReadSet(Storage s, std::string_view key);

    WriteElement<Tuple>* searchWriteSet(Storage s, std::string_view key);

    void unlockWriteSet();

    void unlockWriteSet(std::vector<WriteElement<Tuple>>::iterator end);

    bool validationPhase();

    void wal(std::uint64_t ctid);

    Status write(Storage s, std::string_view key, TupleBody&& body);

    void writePhase();

    bool commit();

    void reconnoiter_begin();

    void reconnoiter_end();

    bool isLeader() const;

    void leaderWork();

    void gc_records();

    bool has_privilege() const;

    void hand_over_privilege() const;

    MCSMutex::MCSNode* allocate_node();
};
