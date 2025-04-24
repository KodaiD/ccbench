#include <algorithm>
#include <stdio.h>
#include <string>

#include "include/atomic_tool.hh"
#include "include/log.hh"
#include "include/scan_callback.hh"
#include "include/transaction.hh"

extern std::vector<Result> SiloResult;
extern void displayDB();
extern void siloLeaderWork(uint64_t& epoch_timer_start,
                           uint64_t& epoch_timer_stop);

void TxExecutor::gc_records() {
    const auto r_epoch = ReclamationEpoch;

    // for records
    while (!gc_records_.empty()) {
        Tuple* rec = gc_records_.front();
        if (rec->tidword_.epoch > r_epoch) break;
        delete rec;
        gc_records_.pop_front();
    }
}

void TxExecutor::abort() {
    removeReserveAll();
    unlockWriteSet();

    // remove inserted records
    for (auto& we : write_set_) {
        if (we.op_ == OpType::INSERT) {
            Masstrees[get_storage(we.storage_)].remove_value(we.key_);
            delete we.rcdptr_;
        }
    }

    abort_cnt++;

    gc_records();

    read_set_.clear();
    write_set_.clear();
    node_map_.clear();

#if BACK_OFF
#if ADD_ANALYSIS
    std::uint64_t start(rdtscp());
#endif

    Backoff::backoff(FLAGS_clocks_per_us);

#if ADD_ANALYSIS
    result_->local_backoff_latency_ += rdtscp() - start;
#endif
#endif
}

void TxExecutor::begin() {
    status_ = TransactionStatus::inflight;
    max_wset_.obj_ = 0;
    max_rset_.obj_ = 0;

    t = FLAGS_polaris_t;
    s = FLAGS_polaris_s;

    if (abort_cnt < t) {
        prio = p0;
    } else {
        prio = p0 + (abort_cnt - t) / s;
        if (prio >= 16) prio = 15;
    }
}

// TODO: enable this if we want to use
// void TxExecutor::displayWriteSet() {
//   printf("display_write_set()\n");
//   printf("--------------------\n");
//   for (auto &ws : write_set_) {
//     printf("key\t:\t%lu\n", ws.key_);
//   }
// }

Status TxExecutor::insert(Storage s, std::string_view key, TupleBody&& body) {
#if ADD_ANALYSIS
    std::uint64_t start = rdtscp();
#endif

    if (searchWriteSet(s, key)) return Status::WARN_ALREADY_EXISTS;

    Tuple* tuple = Masstrees[get_storage(s)].get_value(key);
#if ADD_ANALYSIS
    ++result_->local_tree_traversal_;
#endif
    if (tuple != nullptr) { return Status::WARN_ALREADY_EXISTS; }

    tuple = new Tuple();
    tuple->init(std::move(body));

    MasstreeWrapper<Tuple>::insert_info_t insert_info{};
    Status stat =
            Masstrees[get_storage(s)].insert_value(key, tuple, &insert_info);
    if (stat == Status::WARN_ALREADY_EXISTS) {
        delete tuple;
        return stat;
    }
    if (insert_info.node) {
        if (!node_map_.empty()) {
            auto it = node_map_.find((void*) insert_info.node);
            if (it != node_map_.end()) {
                if (unlikely(it->second != insert_info.old_version)) {
                    status_ = TransactionStatus::aborted;
                    return Status::ERROR_CONCURRENT_WRITE_OR_DELETE;
                }
                // otherwise, bump the version
                it->second = insert_info.new_version;
            }
        }
    } else {
        ERR;
    }

    write_set_.emplace_back(s, key, tuple, OpType::INSERT);

#if ADD_ANALYSIS
    result_->local_write_latency_ += rdtscp() - start;
#endif
    return Status::OK;
}

Status TxExecutor::delete_record(Storage s, std::string_view key) {
#if ADD_ANALYSIS
    std::uint64_t start = rdtscp();
#endif
    Tidword tidw;

    // cancel previous write
    for (auto itr = write_set_.begin(); itr != write_set_.end(); ++itr) {
        if ((*itr).storage_ != s) continue;
        if ((*itr).key_ == key) { write_set_.erase(itr); }
    }

    Tuple* tuple = Masstrees[get_storage(s)].get_value(key);
#if ADD_ANALYSIS
    ++result_->local_tree_traversal_;
#endif
    if (tuple == nullptr) { return Status::WARN_NOT_FOUND; }

    tidw.obj_ = loadAcquire(tuple->tidword_.obj_);
    if (tidw.absent) { return Status::WARN_NOT_FOUND; }
    write_set_.emplace_back(s, key, tuple, OpType::DELETE);

#if ADD_ANALYSIS
    result_->local_write_latency_ += rdtscp() - start;
#endif
    return Status::OK;
}

void TxExecutor::lockWriteSet() {
    Tidword expected;
    for (auto& itr : write_set_) {
        if (itr.op_ == OpType::INSERT) continue;
        expected.obj_ = loadAcquire(itr.rcdptr_->tidword_.obj_);
        for (;;) {
            if (expected.prio > prio) {
                this->status_ = TransactionStatus::aborted;
                return;
            }
            if (expected.lock) {
                if (FLAGS_no_wait) {
                    this->status_ = TransactionStatus::aborted;
                    return;
                }
                expected.obj_ = loadAcquire(itr.rcdptr_->tidword_.obj_);
                continue;
            }
            Tidword desired = expected;
            desired.lock = true;
            if (compareExchange(itr.rcdptr_->tidword_.obj_, expected.obj_,
                                desired.obj_)) {
                itr.set_locked(true);
                break;
            }
        }
        if (itr.op_ == OpType::UPDATE && itr.rcdptr_->tidword_.absent) {
            unlockWriteSet();
            this->status_ = TransactionStatus::aborted;
            return;
        }
        max_wset_ = std::max(max_wset_, expected);
    }
}

Status TxExecutor::read(Storage s, std::string_view key, TupleBody** body) {
#if ADD_ANALYSIS
    std::uint64_t start = rdtscp();
#endif

    // these variable cause error (-fpermissive)
    // "crosses initialization of ..."
    // So it locate before first goto instruction.
    Status stat;
    ReadElement<Tuple>* re;
    WriteElement<Tuple>* we;

    /**
   * read-own-writes or re-read from local read set.
   */
    re = searchReadSet(s, key);
    if (re) {
        *body = &(re->body_);
        goto FINISH_READ;
    }
    we = searchWriteSet(s, key);
    if (we) {
        *body = &(we->body_);
        goto FINISH_READ;
    }

    /**
   * Search tuple from data structure.
   */
    Tuple* tuple;
    tuple = Masstrees[get_storage(s)].get_value(key);
#if ADD_ANALYSIS
    ++result_->local_tree_traversal_;
#endif
    if (tuple == nullptr) return Status::WARN_NOT_FOUND;

    stat = read_internal(s, key, tuple);
    if (stat != Status::OK) { return stat; }
    *body = &(read_set_.back().body_);

FINISH_READ:
#if ADD_ANALYSIS
    result_->local_read_latency_ += rdtscp() - start;
#endif
    return Status::OK;
}

Status TxExecutor::read_internal(Storage s, std::string_view key,
                                 Tuple* tuple) {
    bool is_reserved = false;
    Tidword expected, desired;
    expected.obj_ = loadAcquire(tuple->tidword_.obj_);
    for (;;) {
        while (expected.lock) expected.obj_ = loadAcquire(tuple->tidword_.obj_);
        if (expected.absent) return Status::WARN_NOT_FOUND;
        is_reserved = try_reserve(expected, desired, prio);
        TupleBody body = TupleBody(key, tuple->body_.get_val(),
                                   tuple->body_.get_val_align());
        if (is_reserved) {
            if (compareExchange(tuple->tidword_.obj_, expected.obj_,
                                desired.obj_)) {
                read_set_.emplace_back(s, key, tuple, std::move(body), expected,
                                       true);
                break;
            }
        } else {
            Tidword check;
            check.obj_ = loadAcquire(tuple->tidword_.obj_);
            if (check.obj_ == expected.obj_) {
                read_set_.emplace_back(s, key, tuple, std::move(body), expected,
                                       false);
                break;
            }
            expected.obj_ = check.obj_;
        }
    }
    return Status::OK;
}

Status TxExecutor::scan(const Storage s, std::string_view left_key,
                        bool l_exclusive, std::string_view right_key,
                        bool r_exclusive, std::vector<TupleBody*>& result) {
    return scan(s, left_key, l_exclusive, right_key, r_exclusive, result, -1);
}

Status TxExecutor::scan(const Storage s, std::string_view left_key,
                        bool l_exclusive, std::string_view right_key,
                        bool r_exclusive, std::vector<TupleBody*>& result,
                        int64_t limit) {
    result.clear();
    auto rset_init_size = read_set_.size();

    std::vector<Tuple*> scan_res;
    Masstrees[get_storage(s)].scan(
            left_key.empty() ? nullptr : left_key.data(), left_key.size(),
            l_exclusive, right_key.empty() ? nullptr : right_key.data(),
            right_key.size(), r_exclusive, &scan_res, limit, callback_);

    for (auto&& itr : scan_res) {
        ReadElement<Tuple>* re = searchReadSet(s, itr->body_.get_key());
        if (re) {
            result.emplace_back(&(re->body_));
            continue;
        }

        WriteElement<Tuple>* we = searchWriteSet(s, itr->body_.get_key());
        if (we) {
            result.emplace_back(&(we->body_));
            continue;
        }

        if (Status stat = read_internal(s, itr->body_.get_key(), itr);
            stat != Status::OK) {
            return stat;
        }
    }

    if (rset_init_size != read_set_.size()) {
        for (auto itr = read_set_.begin() + rset_init_size;
             itr != read_set_.end(); ++itr) {
            result.emplace_back(&((*itr).body_));
        }
    }

    return Status::OK;
}

void tx_delete([[maybe_unused]] std::uint64_t key) {}

ReadElement<Tuple>* TxExecutor::searchReadSet(Storage s, std::string_view key) {
    for (auto& re : read_set_) {
        if (re.storage_ != s) continue;
        if (re.key_ == key) return &re;
    }

    return nullptr;
}

WriteElement<Tuple>* TxExecutor::searchWriteSet(Storage s,
                                                std::string_view key) {
    for (auto& we : write_set_) {
        if (we.storage_ != s) continue;
        if (we.key_ == key) return &we;
    }

    return nullptr;
}

void TxExecutor::unlockWriteSet() {
    for (auto& itr : write_set_) {
        Tidword expected;
        if (!itr.is_locked()) continue;
        if (itr.op_ == OpType::INSERT) continue;
        expected.obj_ = loadAcquire(itr.rcdptr_->tidword_.obj_);
        Tidword desired = expected;
        desired.lock = false;
        storeRelease(itr.rcdptr_->tidword_.obj_, desired.obj_);
    }
}

bool TxExecutor::validationPhase() { // Validation Phase
#if ADD_ANALYSIS
    std::uint64_t start = rdtscp();
#endif

    /* Phase 1
   * lock write_set_ sorted.*/
    sort(write_set_.begin(), write_set_.end());
    lockWriteSet();
    if (this->status_ == TransactionStatus::aborted) return false;

    asm volatile("" ::: "memory");
    atomicStoreThLocalEpoch(thid_, atomicLoadGE());
    asm volatile("" ::: "memory");

    /* Phase 2 abort if any condition of below is satisfied.
   * 1. tid of read_set_ changed from it that was got in Read Phase.
   * 2. not latest version
   * 3. the tuple is locked and it isn't included by its write set.*/

    for (auto& itr : read_set_) {
        Tidword check;
        // 1
        check.obj_ = loadAcquire(itr.rcdptr_->tidword_.obj_);
        if (itr.get_tidword().epoch != check.epoch ||
            itr.get_tidword().data_ver != check.data_ver) {
#if ADD_ANALYSIS
            result_->local_vali_latency_ += rdtscp() - start;
#endif
            this->status_ = TransactionStatus::aborted;
            unlockWriteSet();
            return false;
        }
        // 2
        // if (!check.latest) return false;

        // 3
        if (check.lock && !searchWriteSet(itr.storage_, itr.key_)) {
#if ADD_ANALYSIS
            result_->local_vali_latency_ += rdtscp() - start;
#endif
            this->status_ = TransactionStatus::aborted;
            unlockWriteSet();
            return false;
        }
        max_rset_ = std::max(max_rset_, check);
    }

    // node validation
    for (auto it : node_map_) {
        auto node = (MasstreeWrapper<Tuple>::node_type*) it.first;
        if (node->full_version_value() != it.second) {
            this->status_ = TransactionStatus::aborted;
            unlockWriteSet();
            return false;
        }
    }

    // goto Phase 3
#if ADD_ANALYSIS
    result_->local_vali_latency_ += rdtscp() - start;
#endif
    this->status_ = TransactionStatus::committed;
    return true;
}

void TxExecutor::wal(std::uint64_t ctid) {
    for (auto& itr : write_set_) {
        LogRecord log(ctid, itr.key_, (char*) "FIXME"); // TODO: logging
        log_set_.emplace_back(log);
        latest_log_header_.chkSum_ += log.computeChkSum();
        ++latest_log_header_.logRecNum_;
    }

    if (log_set_.size() > LOGSET_SIZE / 2) {
        // prepare write header
        latest_log_header_.convertChkSumIntoComplementOnTwo();

        // write header
        logfile_.write((void*) &latest_log_header_, sizeof(LogHeader));

        // write log record
        // for (auto itr = log_set_.begin(); itr != log_set_.end(); ++itr)
        //  logfile_.write((void *)&(*itr), sizeof(LogRecord));
        logfile_.write((void*) &(log_set_[0]),
                       sizeof(LogRecord) * latest_log_header_.logRecNum_);

        // logfile_.fdatasync();

        // clear for next transactions.
        latest_log_header_.init();
        log_set_.clear();
    }
}

Status TxExecutor::write(Storage s, std::string_view key, TupleBody&& body) {
#if ADD_ANALYSIS
    std::uint64_t start = rdtscp();
#endif

    if (searchWriteSet(s, key)) goto FINISH_WRITE;

    /**
   * Search tuple from data structure.
   */
    Tuple* tuple;
    ReadElement<Tuple>* re;
    re = searchReadSet(s, key);
    if (re) {
        tuple = re->rcdptr_;
    } else {
        tuple = Masstrees[get_storage(s)].get_value(key);
#if ADD_ANALYSIS
        ++result_->local_tree_traversal_;
#endif
        if (tuple == nullptr) return Status::WARN_NOT_FOUND;
    }

    write_set_.emplace_back(s, key, tuple, std::move(body), OpType::UPDATE);

FINISH_WRITE:

#if ADD_ANALYSIS
    result_->local_write_latency_ += rdtscp() - start;
#endif
    return Status::OK;
}

void TxExecutor::writePhase() {
    // It calculates the smallest number that is
    //(a) larger than the TID of any record read or written by the transaction,
    //(b) larger than the worker's most recently chosen TID,
    // and (C) in the current global epoch.

    Tidword tid_a, tid_b, tid_c;

    // calculates (a)
    // about read_set_
    tid_a = std::max(max_wset_, max_rset_);
    tid_a.data_ver++;

    // calculates (b)
    // larger than the worker's most recently chosen TID,
    tid_b = mrctid_;
    tid_b.data_ver++;

    // calculates (c)
    tid_c.epoch = ThLocalEpoch[thid_].obj_;

    // compare a, b, c
    Tidword maxtid = std::max({tid_a, tid_b, tid_c});
    maxtid.lock = false;
    maxtid.latest = true;
    mrctid_ = maxtid;

    removeReserveAll();

    // write(record, commit-tid)
    for (auto& itr : write_set_) {
        // update and unlock
        switch (itr.op_) {
            case OpType::UPDATE: {
                memcpy(itr.rcdptr_->body_.get_val_ptr(),
                       itr.body_.get_val_ptr(), itr.body_.get_val_size());
                Tidword cur_tidw, new_tidw;
                cur_tidw.obj_ = loadAcquire(itr.rcdptr_->tidword_.obj_);
                new_tidw = cleanup_write_for_commit(cur_tidw, maxtid.data_ver,
                                                    maxtid.epoch);
                storeRelease(itr.rcdptr_->tidword_.obj_, new_tidw.obj_);
                break;
            }
            case OpType::INSERT: {
                maxtid.absent = false;
                Tidword cur_tidw, new_tidw;
                cur_tidw.obj_ = loadAcquire(itr.rcdptr_->tidword_.obj_);
                new_tidw = cleanup_write_for_commit(cur_tidw, maxtid.data_ver,
                                                    maxtid.epoch);
                storeRelease(itr.rcdptr_->tidword_.obj_, new_tidw.obj_);
                break;
            }
            case OpType::DELETE: {
                maxtid.absent = true;
                Tidword cur_tidw, new_tidw;
                cur_tidw.obj_ = loadAcquire(itr.rcdptr_->tidword_.obj_);
                new_tidw = cleanup_write_for_commit(cur_tidw, maxtid.data_ver,
                                                    maxtid.epoch);
                Masstrees[get_storage(itr.storage_)].remove_value(itr.key_);
                storeRelease(itr.rcdptr_->tidword_.obj_, new_tidw.obj_);
                // create information for garbage collection
                gc_records_.push_back(itr.rcdptr_);
                break;
            }
            default:
                ERR;
        }
    }

    gc_records();
    read_set_.clear();
    write_set_.clear();
    node_map_.clear();

    abort_cnt = 0;
}

bool TxExecutor::commit() {
    if (validationPhase()) {
        writePhase();
        return true;
    } else {
        return false;
    }
}

bool TxExecutor::isLeader() const { return this->thid_ == 0; }

void TxExecutor::leaderWork() {
    siloLeaderWork(this->epoch_timer_start, this->epoch_timer_stop);
#if BACK_OFF
    leaderBackoffWork(backoff_, SiloResult);
#endif
}

void TxExecutor::reconnoiter_begin() { reconnoitering_ = true; }

void TxExecutor::reconnoiter_end() {
    read_set_.clear();
    node_map_.clear();
    reconnoitering_ = false;
    begin();
}

void TxScanCallback::on_resp_node(const MasstreeWrapper<Tuple>::node_type* n,
                                  uint64_t version) {
    auto it = tx_->node_map_.find((void*) n);
    if (it == tx_->node_map_.end()) {
        tx_->node_map_.emplace_hint(it, (void*) n, version);
    } else if ((*it).second != version) {
        tx_->status_ = TransactionStatus::aborted;
    }
}

bool TxExecutor::try_reserve(Tidword& tid, Tidword& new_tid, uint8_t prio) {
    bool is_reserved;
    new_tid = tid;
    if (tid.prio == prio) {
        if (prio == 0) return false;
        new_tid.ref_cnt++;
        is_reserved = true;
    } else if (tid.prio < prio) {
        new_tid.prio = prio;
        new_tid.ref_cnt = 1;
        is_reserved = true;
    } else {
        is_reserved = false;
    }
    return is_reserved;
}

Tidword TxExecutor::cleanup_read(Tidword tidw, uint8_t prio,
                                 uint32_t prio_ver) {
    if (tidw.lock) return tidw;
    if (tidw.prio != prio || tidw.prio_ver != prio_ver) return tidw;
    Tidword new_tidw = tidw;
    if (new_tidw.ref_cnt == 0) { return tidw; }
    if (--new_tidw.ref_cnt == 0) {
        new_tidw.prio = 0;
        new_tidw.prio_ver++;
    }
    return new_tidw;
}

Tidword TxExecutor::cleanup_write_for_abort(Tidword tidw) {
    Tidword new_tidw;
    new_tidw.obj_ = tidw.obj_;
    new_tidw.lock = false;
    new_tidw.prio = 0;
    new_tidw.prio_ver++;
    new_tidw.ref_cnt = 0;
    return new_tidw;
}

Tidword TxExecutor::cleanup_write_for_commit(Tidword tidw,
                                             uint32_t new_data_ver,
                                             uint32_t epoch) {
    Tidword new_tidw;
    new_tidw.epoch = epoch;
    new_tidw.data_ver = new_data_ver;
    new_tidw.lock = false;
    new_tidw.prio = 0;
    new_tidw.prio_ver = tidw.prio_ver + 1;
    new_tidw.ref_cnt = 0;
    return new_tidw;
}

void TxExecutor::removeReserveAll() {
    for (auto& iter : read_set_) {
        if (!iter.is_reserved()) continue;
        const auto tuple = iter.rcdptr_;
        Tidword tidw;
        tidw.obj_ = loadAcquire(tuple->tidword_.obj_);
        while (true) {
            const Tidword new_tidw =
                    cleanup_read(tidw, prio, iter.get_tidword().prio_ver);
            if (new_tidw.obj_ == tidw.obj_) { break; }
            if (compareExchange(tuple->tidword_.obj_, tidw.obj_,
                                new_tidw.obj_)) {
                break;
            }
        }
    }
}