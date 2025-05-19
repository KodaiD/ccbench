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
    Backoff::backoff(FLAGS_clocks_per_us);
#endif
    abort_cnt_++;
}

void TxExecutor::begin() {
    status_ = TransactionStatus::inflight;
    max_wset_.obj_ = 0;
    max_rset_.obj_ = 0;
    if (abort_cnt_ >= t_) {
        super_ = true;
    } else {
        super_ = false;
    }
    ThLocalStatus[thid_].obj_ = STATUS_ACTIVE;
}

Status TxExecutor::insert(Storage s, std::string_view key, TupleBody&& body) {
    if (loadAcquire(ThLocalStatus[thid_].obj_) == STATUS_ABORTED) {
        prepare_abort();
        return Status::ERROR_LOCK_FAILED;
    }
    if (searchWriteSet(s, key)) return Status::WARN_ALREADY_EXISTS;
    Tuple* tuple = Masstrees[get_storage(s)].get_value(key);
    if (tuple != nullptr) { return Status::WARN_ALREADY_EXISTS; }
    tuple = new Tuple();
    if (super_) {
        tuple->init(std::move(body), SUPER_EXLOCKED);
    } else {
        tuple->init(std::move(body), EXLOCKED);
    }
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
    return Status::OK;
}

Status TxExecutor::delete_record(Storage s, std::string_view key) {
    if (loadAcquire(ThLocalStatus[thid_].obj_) == STATUS_ABORTED) {
        prepare_abort();
        return Status::ERROR_LOCK_FAILED;
    }
    for (auto itr = write_set_.begin(); itr != write_set_.end(); ++itr) {
        if (itr->storage_ != s) continue;
        if (itr->key_ == key) { write_set_.erase(itr); }
    }
    Tuple* tuple = Masstrees[get_storage(s)].get_value(key);
    if (tuple == nullptr) { return Status::WARN_NOT_FOUND; }
    if (super_) {
        Tidword expected;
        expected.obj_ = loadAcquire(tuple->tidword_.obj_);
        while (true) {
            if (expected.lock == UNLOCKED) {
                if (try_lock(tuple, expected, SUPER_EXLOCKED)) { break; }
                if (expected.absent) {
                    prepare_abort();
                    return Status::WARN_NOT_FOUND;
                }
            } else if (expected.lock == SUPER_SHLOCKED ||
                       expected.lock == SUPER_EXLOCKED) {
                if (const auto rc =
                            wound_or_die(tuple, expected, SUPER_EXLOCKED);
                    rc == RC::WOUND) {
                    break;
                } else {
                    if (rc == RC::RETRY) { continue; }
                    return Status::ERROR_LOCK_FAILED;
                }
            } else { // Wait
                expected.obj_ = loadAcquire(tuple->tidword_.obj_);
            }
        }
        write_set_.emplace_back(s, key, tuple, OpType::DELETE);
        return Status::OK;
    }
    Tidword expected;
    expected.obj_ = loadAcquire(tuple->tidword_.obj_);
    if (expected.absent) { return Status::WARN_NOT_FOUND; }
    write_set_.emplace_back(s, key, tuple, OpType::DELETE);
    return Status::OK;
}

// Used by normal
void TxExecutor::lockWriteSet() {
    Tidword expected;
    for (auto itr = write_set_.begin(); itr != write_set_.end(); ++itr) {
        if (itr->op_ == OpType::INSERT) continue;
        expected.obj_ = loadAcquire(itr->rcdptr_->tidword_.obj_);
        while (true) {
            if (expected.lock == EXLOCKED) {
                expected.obj_ = loadAcquire(itr->rcdptr_->tidword_.obj_);
            } else if (expected.lock == SUPER_SHLOCKED ||
                       expected.lock == SUPER_EXLOCKED) {
                this->status_ = TransactionStatus::aborted;
                if (itr != write_set_.begin()) unlockWriteSet(itr);
                return;
            } else {
                Tidword desired = expected;
                desired.lock = EXLOCKED;
                if (compareExchange(itr->rcdptr_->tidword_.obj_, expected.obj_,
                                    desired.obj_)) {
                    break;
                }
            }
        }
        if (itr->op_ == OpType::UPDATE && itr->rcdptr_->tidword_.absent) {
            Tidword desired = expected;
            desired.lock = UNLOCKED;
            storeRelease(itr->rcdptr_->tidword_.obj_, desired.obj_);
            unlockWriteSet(itr);
            this->status_ = TransactionStatus::aborted;
            return;
        }
        max_wset_ = std::max(max_wset_, expected);
    }
}

Status TxExecutor::read(const Storage s, const std::string_view key,
                        TupleBody** body) {
    if (loadAcquire(ThLocalStatus[thid_].obj_) == STATUS_ABORTED) {
        prepare_abort();
        return Status::ERROR_LOCK_FAILED;
    }
    if (ReadElement<Tuple>* re = searchReadSet(s, key)) {
        *body = &(re->body_);
        return Status::OK;
    }
    if (WriteElement<Tuple>* we = searchWriteSet(s, key)) {
        *body = &(we->body_);
        return Status::OK;
    }
    Tuple* tuple = Masstrees[get_storage(s)].get_value(key);
    if (tuple == nullptr) return Status::WARN_NOT_FOUND;
    if (const Status stat = read_internal(s, key, tuple); stat != Status::OK) {
        return stat;
    }
    *body = &(read_set_.back().body_);
    return Status::OK;
}

Status TxExecutor::read_internal(Storage s, std::string_view key,
                                 Tuple* tuple) {
    if (super_) {
        TupleBody body;
        Tidword expected;
        expected.obj_ = loadAcquire(tuple->tidword_.obj_);
        while (true) {
            if (expected.lock == UNLOCKED) { // Get read lock
                if (try_lock(tuple, expected, SUPER_SHLOCKED)) { break; }
                if (expected.absent) {
                    prepare_abort();
                    return Status::WARN_NOT_FOUND;
                }
            } else if (expected.lock == SUPER_SHLOCKED) {
                if (less_than(expected)) { // Overwrite thid
                    Tidword desired = expected;
                    desired.thid = thid_;
                    if (compareExchange(tuple->tidword_.obj_, expected.obj_,
                                        desired.obj_)) {
                        break;
                    }
                } else { // Validation
                    body = TupleBody(key, tuple->body_.get_val(),
                                     tuple->body_.get_val_align());
                    if (validate(tuple, expected)) {
                        read_set_.emplace_back(s, key, tuple, std::move(body),
                                               expected, false);
                        return Status::OK;
                    }
                }
            } else if (expected.lock == SUPER_EXLOCKED) {
                if (const auto rc =
                            wound_or_die(tuple, expected, SUPER_SHLOCKED);
                    rc == RC::WOUND) {
                    break;
                } else {
                    if (rc == RC::RETRY) { continue; }
                    return Status::ERROR_LOCK_FAILED;
                }
            } else { // Wait
                expected.obj_ = loadAcquire(tuple->tidword_.obj_);
            }
        }
        body = TupleBody(key, tuple->body_.get_val(),
                         tuple->body_.get_val_align());
        read_set_.emplace_back(s, key, tuple, std::move(body), expected, true);
        return Status::OK;
    }
    TupleBody body;
    Tidword expected;
    expected.obj_ = loadAcquire(tuple->tidword_.obj_);
    while (true) {
        while (expected.lock == EXLOCKED || expected.lock == SUPER_EXLOCKED) {
            expected.obj_ = loadAcquire(tuple->tidword_.obj_);
        }
        if (expected.absent) { return Status::WARN_NOT_FOUND; }
        body = TupleBody(key, tuple->body_.get_val(),
                         tuple->body_.get_val_align());
        if (validate(tuple, expected)) {
            read_set_.emplace_back(s, key, tuple, std::move(body), expected,
                                   false);
            return Status::OK;
        }
    }
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
    if (loadAcquire(ThLocalStatus[thid_].obj_) == STATUS_ABORTED) {
        prepare_abort();
        return Status::ERROR_LOCK_FAILED;
    }
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
        if (const Status stat = read_internal(s, itr->body_.get_key(), itr);
            stat != Status::OK && stat != Status::WARN_NOT_FOUND) {
            return stat;
        }
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
    for (auto& itr : write_set_) {
        Tidword expected;
        if (itr.op_ == OpType::INSERT) continue;
        expected.obj_ = loadAcquire(itr.rcdptr_->tidword_.obj_);
        if (super_) {
            unlock(itr.rcdptr_);
        } else {
            Tidword desired = expected;
            desired.lock = UNLOCKED;
            storeRelease(itr.rcdptr_->tidword_.obj_, desired.obj_);
        }
    }
}

// Used by normal
void TxExecutor::unlockWriteSet(
        const std::vector<WriteElement<Tuple>>::iterator end) {
    for (auto itr = write_set_.begin(); itr != end; ++itr) {
        Tidword expected;
        if (itr->op_ == OpType::INSERT) continue;
        expected.obj_ = loadAcquire(itr->rcdptr_->tidword_.obj_);
        Tidword desired = expected;
        desired.lock = UNLOCKED;
        storeRelease(itr->rcdptr_->tidword_.obj_, desired.obj_);
    }
}

void TxExecutor::unlockReadSet() const {
    for (auto& itr : read_set_) {
        if (!itr.is_locked()) continue;
        unlock(itr.rcdptr_);
    }
}

bool TxExecutor::validationPhase() {
    if (super_) {
        auto status = loadAcquire(ThLocalStatus[thid_].obj_);
        if (status == STATUS_ABORTED) {
            this->status_ = TransactionStatus::aborted;
            unlockReadSet();
            unlockWriteSet();
            return false;
        }
        if (!compareExchange(ThLocalStatus[thid_].obj_, status,
                             STATUS_COMMITTING)) {
            status_ = TransactionStatus::aborted;
            unlockReadSet();
            unlockWriteSet();
            return false;
        }
    } else {
        sort(write_set_.begin(), write_set_.end());
        lockWriteSet();
        if (this->status_ == TransactionStatus::aborted) return false;
    }
    asm volatile("" ::: "memory");
    atomicStoreThLocalEpoch(thid_, atomicLoadGE());
    asm volatile("" ::: "memory");
    for (auto& itr : read_set_) {
        Tidword check;
        check.obj_ = loadAcquire(itr.rcdptr_->tidword_.obj_);
        if (itr.get_tidword().epoch != check.epoch ||
            itr.get_tidword().tid != check.tid) {
            this->status_ = TransactionStatus::aborted;
            if (super_) {
                storeRelease(ThLocalStatus[thid_].obj_, STATUS_ABORTED);
                unlockReadSet();
            }
            unlockWriteSet();
            return false;
        }
        if ((check.lock == EXLOCKED || check.lock == SUPER_EXLOCKED) &&
            !searchWriteSet(itr.storage_, itr.key_)) {
            this->status_ = TransactionStatus::aborted;
            if (super_) {
                storeRelease(ThLocalStatus[thid_].obj_, STATUS_ABORTED);
                unlockReadSet();
            }
            unlockWriteSet();
            return false;
        }
        max_rset_ = std::max(max_rset_, check);
    }
    for (auto [fst, snd] : node_map_) {
        if (auto node = static_cast<MasstreeWrapper<Tuple>::node_type*>(fst);
            node->full_version_value() != snd) {
            this->status_ = TransactionStatus::aborted;
            if (super_) {
                storeRelease(ThLocalStatus[thid_].obj_, STATUS_ABORTED);
                unlockReadSet();
            }
            unlockWriteSet();
            return false;
        }
    }
    this->status_ = TransactionStatus::committed;
    return true;
}

Status TxExecutor::write(Storage s, std::string_view key, TupleBody&& body) {
    if (loadAcquire(ThLocalStatus[thid_].obj_) == STATUS_ABORTED) {
        prepare_abort();
        return Status::ERROR_LOCK_FAILED;
    }
    if (searchWriteSet(s, key)) { return Status::OK; }
    if (super_) {
        std::vector<ReadElement<Tuple>>::iterator re;
        for (re = read_set_.begin(); re != read_set_.end(); ++re) {
            if (re->storage_ == s && re->key_ == key) break;
        }
        if (re != read_set_.end()) {
            Tidword expected;
            expected.obj_ = loadAcquire(re->rcdptr_->tidword_.obj_);
            while (true) {
                if (expected.lock == UNLOCKED) {
                    if (expected.absent ||
                        expected.tid != re->get_tidword().tid ||
                        expected.epoch != re->get_tidword().epoch) {
                        prepare_abort();
                        return Status::WARN_NOT_FOUND;
                    }
                    if (try_lock(re->rcdptr_, expected, SUPER_SHLOCKED)) {
                        break;
                    }
                } else if (expected.lock == SUPER_SHLOCKED &&
                           expected.thid == thid_) { // Upgrade
                    Tidword desired = expected;
                    desired.lock = SUPER_EXLOCKED;
                    if (compareExchange(re->rcdptr_->tidword_.obj_,
                                        expected.obj_, desired.obj_)) {
                        break;
                    }
                } else if (expected.lock == SUPER_SHLOCKED ||
                           expected.lock == SUPER_EXLOCKED) {
                    if (expected.tid != re->get_tidword().tid ||
                        expected.epoch != re->get_tidword().epoch) { // Validate
                        prepare_abort();
                        return Status::ERROR_CONCURRENT_WRITE_OR_DELETE;
                    }
                    if (const auto rc = wound_or_die(re->rcdptr_, expected,
                                                     SUPER_EXLOCKED);
                        rc == RC::WOUND) {
                        break;
                    } else {
                        if (rc == RC::RETRY) { continue; }
                        return Status::ERROR_LOCK_FAILED;
                    }
                } else { // Wait
                    expected.obj_ = loadAcquire(re->rcdptr_->tidword_.obj_);
                }
            }
            write_set_.emplace_back(s, key, re->rcdptr_, std::move(body),
                                    OpType::UPDATE);
            read_set_.erase(re);
            return Status::OK;
        }
        Tuple* tuple = Masstrees[get_storage(s)].get_value(key);
        if (tuple == nullptr) { return Status::WARN_NOT_FOUND; }
        Tidword expected;
        expected.obj_ = loadAcquire(tuple->tidword_.obj_);
        while (true) {
            if (expected.lock == UNLOCKED) { // Get write lock
                if (try_lock(tuple, expected, SUPER_EXLOCKED)) { break; }
                if (expected.absent) {
                    prepare_abort();
                    return Status::WARN_NOT_FOUND;
                }
            } else if (expected.lock == SUPER_SHLOCKED ||
                       expected.lock == SUPER_EXLOCKED) {
                if (const auto rc =
                            wound_or_die(tuple, expected, SUPER_EXLOCKED);
                    rc == RC::WOUND) {
                    break;
                } else {
                    if (rc == RC::RETRY) { continue; }
                    return Status::ERROR_LOCK_FAILED;
                }
                // Retry
            } else { // Wait
                expected.obj_ = loadAcquire(tuple->tidword_.obj_);
            }
        }
        write_set_.emplace_back(s, key, tuple, std::move(body), OpType::UPDATE);
        return Status::OK;
    }
    Tuple* tuple;
    if (const ReadElement<Tuple>* re = searchReadSet(s, key)) {
        tuple = re->rcdptr_;
    } else {
        tuple = Masstrees[get_storage(s)].get_value(key);
        if (tuple == nullptr) return Status::WARN_NOT_FOUND;
    }
    write_set_.emplace_back(s, key, tuple, std::move(body), OpType::UPDATE);
    return Status::OK;
}

void TxExecutor::writePhase() {
    if (super_) { unlockReadSet(); }
    Tidword tid_a = std::max(max_wset_, max_rset_);
    tid_a.tid++;
    Tidword tid_b = mrc_tid_;
    tid_b.tid++;
    Tidword tid_c;
    tid_c.epoch = ThLocalEpoch[thid_].obj_;
    Tidword max_tid = std::max({tid_a, tid_b, tid_c});
    max_tid.lock = UNLOCKED;
    max_tid.thid = UINT16_MAX;
    max_tid.latest = true;
    mrc_tid_ = max_tid;
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
        abort_cnt_ = 0;
        atomicStoreThLocalTS(thid_, loadAcquire(ThLocalTS[thid_].obj_) +
                                            FLAGS_thread_num);
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

INLINE bool TxExecutor::wound(const uint16_t thid) {
    auto status = loadAcquire(ThLocalStatus[thid].obj_);
    while (true) {
        if (status == STATUS_COMMITTING) { return false; }
        if (status == STATUS_ABORTED) { return true; }
        if (compareExchange(ThLocalStatus[thid].obj_, status, STATUS_ABORTED)) {
            return true;
        }
    }
}

INLINE bool TxExecutor::less_than(const Tidword& tid_word) const {
    const auto owner_thid = tid_word.thid;
    const auto owner_ts = loadAcquire(ThLocalTS[owner_thid].obj_);
    if (const auto my_ts = loadAcquire(ThLocalTS[thid_].obj_);
        my_ts < owner_ts) {
        return true;
    }
    return false;
}

INLINE void TxExecutor::prepare_abort() {
    this->status_ = TransactionStatus::aborted;
    storeRelease(ThLocalStatus[thid_].obj_, STATUS_ABORTED);
    unlockReadSet();
    unlockWriteSet();
}

INLINE RC TxExecutor::wound_or_die(Tuple* tuple, Tidword expected,
                                   const uint8_t mode) {
    if (less_than(expected)) { // Wound & Lock
        Tidword desired = expected;
        desired.thid = thid_;
        desired.reserved = true;
        if (compareExchange(tuple->tidword_.obj_, expected.obj_,
                            desired.obj_)) { // Reserve
            wound(expected.thid);
            expected.obj_ = loadAcquire(tuple->tidword_.obj_);
            while (true) {
                if (expected.thid != thid_) { // My reservation is overwritten
                    prepare_abort();
                    return RC::DIE;
                }
                if (expected.lock != UNLOCKED) { // Not unlocked yet
                    expected.obj_ = loadAcquire(tuple->tidword_.obj_);
                    continue;
                }
                desired = expected;
                desired.lock = mode;
                desired.reserved = false;
                if (compareExchange(tuple->tidword_.obj_, expected.obj_,
                                    desired.obj_)) { // Lock
                    return RC::WOUND;
                }
            }
        }
        return RC::RETRY;
    }
    // Die
    prepare_abort();
    return RC::DIE;
}

INLINE bool TxExecutor::try_lock(Tuple* tuple, Tidword& expected,
                                 const uint8_t mode) const {
    if (expected.reserved && expected.thid != UINT16_MAX) {
        if (!less_than(expected)) {
            expected.obj_ = loadAcquire(tuple->tidword_.obj_);
            return false;
        }
    }
    if (expected.absent) { return false; }
    Tidword desired = expected;
    desired.lock = mode;
    desired.thid = thid_;
    desired.reserved = false;
    if (compareExchange(tuple->tidword_.obj_, expected.obj_, desired.obj_)) {
        return true;
    }
    return false;
}

INLINE bool TxExecutor::validate(Tuple* tuple, Tidword& expected) {
    Tidword check;
    check.obj_ = loadAcquire(tuple->tidword_.obj_);
    if (check.epoch == expected.epoch && check.tid == expected.tid &&
        check.latest == expected.latest && check.absent == expected.absent &&
        (check.lock == UNLOCKED || check.lock == SUPER_SHLOCKED)) {
        return true;
    }
    expected = check;
    return false;
}

INLINE void TxExecutor::unlock(Tuple* tuple) const {
    Tidword expected;
    expected.obj_ = loadAcquire(tuple->tidword_.obj_);
    while (true) {
        Tidword desired = expected;
        desired.lock = UNLOCKED;
        if (expected.thid == thid_) { desired.thid = UINT16_MAX; }
        if (!expected.reserved) { // Read owner is changed
            return;
        }
        if (compareExchange(tuple->tidword_.obj_, expected.obj_,
                            desired.obj_)) {
            break;
        }
    }
}
