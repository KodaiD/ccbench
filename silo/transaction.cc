#include <algorithm>
#include <cstdio>
#include <string>

#include "include/atomic_tool.hh"
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
        const Tuple* rec = gc_records_.front();
        if (rec->tidword_.epoch > r_epoch) break;
        delete rec;
        gc_records_.pop_front();
    }
}

void TxExecutor::abort() {
    for (auto& we : write_set_) {
        if (we.op_ == OpType::INSERT) {
            Masstrees[get_storage(we.storage_)].remove_value(we.key_);
            delete we.rcdptr_;
        }
    }
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
}

Status TxExecutor::insert(const Storage s, const std::string_view key,
                          TupleBody&& body) {
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
    const Status stat =
            Masstrees[get_storage(s)].insert_value(key, tuple, &insert_info);
    if (stat == Status::WARN_ALREADY_EXISTS) {
        delete tuple;
        return stat;
    }
    if (insert_info.node) {
        if (!node_map_.empty()) {
            if (const auto it = node_map_.find((void*) insert_info.node);
                it != node_map_.end()) {
                if (unlikely(it->second != insert_info.old_version)) {
                    status_ = TransactionStatus::aborted;
                    return Status::ERROR_CONCURRENT_WRITE_OR_DELETE;
                }
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

Status TxExecutor::delete_record(const Storage s, const std::string_view key) {
#if ADD_ANALYSIS
    std::uint64_t start = rdtscp();
#endif
    Tidword tidw;
    for (auto itr = write_set_.begin(); itr != write_set_.end(); ++itr) {
        if (itr->storage_ != s) continue;
        if (itr->key_ == key) { write_set_.erase(itr); }
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
    for (auto itr = write_set_.begin(); itr != write_set_.end(); ++itr) {
        if (itr->op_ == OpType::INSERT) continue;
        expected.obj_ = loadAcquire(itr->rcdptr_->tidword_.obj_);
        for (;;) {
            if (expected.lock) {
                if (FLAGS_no_wait) {
                    this->status_ = TransactionStatus::aborted;
                    if (itr != write_set_.begin()) unlockWriteSet(itr);
                    return;
                }
                expected.obj_ = loadAcquire(itr->rcdptr_->tidword_.obj_);
            } else {
                Tidword desired = expected;
                desired.lock = true;
                if (compareExchange(itr->rcdptr_->tidword_.obj_, expected.obj_,
                                    desired.obj_)) {
                    break;
                }
            }
        }
        if (itr->op_ == OpType::UPDATE && itr->rcdptr_->tidword_.absent) {
            unlockWriteSet(itr);
            this->status_ = TransactionStatus::aborted;
            return;
        }
        max_wset_ = std::max(max_wset_, expected);
    }
}

Status TxExecutor::read(const Storage s, const std::string_view key,
                        TupleBody** body) {
#if ADD_ANALYSIS
    std::uint64_t start = rdtscp();
#endif
    Status stat;
    if (ReadElement<Tuple>* re = searchReadSet(s, key)) {
        *body = &(re->body_);
#if ADD_ANALYSIS
        result_->local_read_latency_ += rdtscp() - start;
#endif
        return Status::OK;
    }
    if (WriteElement<Tuple>* we = searchWriteSet(s, key)) {
        *body = &(we->body_);
#if ADD_ANALYSIS
        result_->local_read_latency_ += rdtscp() - start;
#endif
        return Status::OK;
    }
    Tuple* tuple = Masstrees[get_storage(s)].get_value(key);
#if ADD_ANALYSIS
    ++result_->local_tree_traversal_;
#endif
    if (tuple == nullptr) return Status::WARN_NOT_FOUND;
    stat = read_internal(s, key, tuple);
    if (stat != Status::OK) { return stat; }
    *body = &(read_set_.back().body_);
#if ADD_ANALYSIS
    result_->local_read_latency_ += rdtscp() - start;
#endif
    return Status::OK;
}

Status TxExecutor::read_internal(const Storage s, const std::string_view key,
                                 Tuple* tuple) {
    TupleBody body;
    Tidword expected;
    expected.obj_ = loadAcquire(tuple->tidword_.obj_);
    for (;;) {
        Tidword check;
        while (expected.lock) {
            expected.obj_ = loadAcquire(tuple->tidword_.obj_);
        }
        if (expected.absent) { return Status::WARN_NOT_FOUND; }
        body = TupleBody(key, tuple->body_.get_val(),
                         tuple->body_.get_val_align());
        check.obj_ = loadAcquire(tuple->tidword_.obj_);
        if (expected == check) break;
        expected = check;
#if ADD_ANALYSIS
        ++result_->local_extra_reads_;
#endif
    }
    read_set_.emplace_back(s, key, tuple, std::move(body), expected);
#if SLEEP_READ_PHASE
    sleepTics(SLEEP_READ_PHASE);
#endif
    return Status::OK;
}

Status TxExecutor::scan(const Storage s, const std::string_view left_key,
                        const bool l_exclusive,
                        const std::string_view right_key,
                        const bool r_exclusive,
                        std::vector<TupleBody*>& result) {
    return scan(s, left_key, l_exclusive, right_key, r_exclusive, result, -1);
}

Status TxExecutor::scan(const Storage s, const std::string_view left_key,
                        const bool l_exclusive,
                        const std::string_view right_key,
                        const bool r_exclusive, std::vector<TupleBody*>& result,
                        const int64_t limit) {
    result.clear();
    const auto r_set_init_size = read_set_.size();
    std::vector<Tuple*> scan_res;
    Masstrees[get_storage(s)].scan(
            left_key.empty() ? nullptr : left_key.data(), left_key.size(),
            l_exclusive, right_key.empty() ? nullptr : right_key.data(),
            right_key.size(), r_exclusive, &scan_res, limit, callback_);
    for (auto&& itr : scan_res) {
        if (ReadElement<Tuple>* re = searchReadSet(s, itr->body_.get_key())) {
            result.emplace_back(&(re->body_));
            continue;
        }
        if (WriteElement<Tuple>* we = searchWriteSet(s, itr->body_.get_key())) {
            result.emplace_back(&(we->body_));
            continue;
        }
        read_internal(s, itr->body_.get_key(), itr);
    }
    if (r_set_init_size != read_set_.size()) {
        for (auto itr = read_set_.begin() + r_set_init_size;
             itr != read_set_.end(); ++itr) {
            result.emplace_back(&(itr->body_));
        }
    }
    return Status::OK;
}

ReadElement<Tuple>* TxExecutor::searchReadSet(const Storage s,
                                              const std::string_view key) {
    for (auto& re : read_set_) {
        if (re.storage_ != s) continue;
        if (re.key_ == key) return &re;
    }
    return nullptr;
}

WriteElement<Tuple>* TxExecutor::searchWriteSet(const Storage s,
                                                const std::string_view key) {
    for (auto& we : write_set_) {
        if (we.storage_ != s) continue;
        if (we.key_ == key) return &we;
    }
    return nullptr;
}

void TxExecutor::unlockWriteSet() const {
    for (const auto& itr : write_set_) {
        Tidword expected;
        if (itr.op_ == OpType::INSERT) continue;
        expected.obj_ = loadAcquire(itr.rcdptr_->tidword_.obj_);
        Tidword desired = expected;
        desired.lock = false;
        storeRelease(itr.rcdptr_->tidword_.obj_, desired.obj_);
    }
}

void TxExecutor::unlockWriteSet(
        const std::vector<WriteElement<Tuple>>::iterator end) {
    for (auto itr = write_set_.begin(); itr != end; ++itr) {
        Tidword expected;
        if (itr->op_ == OpType::INSERT) continue;
        expected.obj_ = loadAcquire(itr->rcdptr_->tidword_.obj_);
        Tidword desired = expected;
        desired.lock = false;
        storeRelease(itr->rcdptr_->tidword_.obj_, desired.obj_);
    }
}

bool TxExecutor::validationPhase() {
#if ADD_ANALYSIS
    std::uint64_t start = rdtscp();
#endif
    sort(write_set_.begin(), write_set_.end());
    lockWriteSet();
    if (this->status_ == TransactionStatus::aborted) return false;
    asm volatile("" ::: "memory");
    atomicStoreThLocalEpoch(thid_, atomicLoadGE());
    asm volatile("" ::: "memory");
    for (auto& itr : read_set_) {
        Tidword check;
        check.obj_ = loadAcquire(itr.rcdptr_->tidword_.obj_);
        if (itr.get_tidword().epoch != check.epoch ||
            itr.get_tidword().tid != check.tid) {
#if ADD_ANALYSIS
            result_->local_vali_latency_ += rdtscp() - start;
#endif
            this->status_ = TransactionStatus::aborted;
            unlockWriteSet();
            return false;
        }
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
    for (auto [fst, snd] : node_map_) {
        if (auto node = static_cast<MasstreeWrapper<Tuple>::node_type*>(fst);
            node->full_version_value() != snd) {
            this->status_ = TransactionStatus::aborted;
            unlockWriteSet();
            return false;
        }
    }
#if ADD_ANALYSIS
    result_->local_vali_latency_ += rdtscp() - start;
#endif
    this->status_ = TransactionStatus::committed;
    return true;
}

Status TxExecutor::write(const Storage s, const std::string_view key,
                         TupleBody&& body) {
#if ADD_ANALYSIS
    std::uint64_t start = rdtscp();
#endif
    if (searchWriteSet(s, key)) goto FINISH_WRITE;
    Tuple* tuple;
    if (const ReadElement<Tuple>* re = searchReadSet(s, key)) {
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
    Tidword tid_a = std::max(max_wset_, max_rset_);
    tid_a.tid++;
    Tidword tid_b = mrctid_;
    tid_b.tid++;
    Tidword tid_c;
    tid_c.epoch = ThLocalEpoch[thid_].obj_;
    Tidword max_tid = std::max({tid_a, tid_b, tid_c});
    max_tid.lock = false;
    max_tid.latest = true;
    mrctid_ = max_tid;
    for (auto& itr : write_set_) {
        switch (itr.op_) {
            case OpType::UPDATE: {
                memcpy(itr.rcdptr_->body_.get_val_ptr(),
                       itr.body_.get_val_ptr(), itr.body_.get_val_size());
                storeRelease(itr.rcdptr_->tidword_.obj_, max_tid.obj_);
                break;
            }
            case OpType::INSERT: {
                max_tid.absent = false;
                storeRelease(itr.rcdptr_->tidword_.obj_, max_tid.obj_);
                break;
            }
            case OpType::DELETE: {
                max_tid.absent = true;
                Masstrees[get_storage(itr.storage_)].remove_value(itr.key_);
                storeRelease(itr.rcdptr_->tidword_.obj_, max_tid.obj_);
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
}

bool TxExecutor::commit() {
    if (validationPhase()) {
        writePhase();
        return true;
    }
    return false;
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
    if (const auto it = tx_->node_map_.find((void*) n);
        it == tx_->node_map_.end()) {
        tx_->node_map_.emplace_hint(it, (void*) n, version);
    } else if (it->second != version) {
        tx_->status_ = TransactionStatus::aborted;
    }
}
