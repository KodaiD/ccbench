#include <algorithm>
#include <cstdio>
#include <string>

#include "include/atomic_tool.hh"
#include "include/scan_callback.hh"
#include "include/transaction.hh"

thread_local MCSMutex::MCSNode MCSMutex::my_node_;

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
    if (is_pcc_) {
        unlockRead();
        unlockWrite();
    }
    for (auto& we : write_set_) {
        if (we.op_ == OpType::INSERT) {
            Masstrees[get_storage(we.storage_)].remove_value(we.key_);
            delete we.rcdptr_;
        }
    }
    gc_records();
    read_set_.clear();
    write_set_.clear();
    read_lock_set_.clear();
    write_lock_set_.clear();
    node_map_.clear();
    abort_cnt++;
#if BACK_OFF
    Backoff::backoff(FLAGS_clocks_per_us);
#endif
}

void TxExecutor::begin() {
    status_ = TransactionStatus::inflight;
    max_wset_.obj_ = 0;
    max_rset_.obj_ = 0;
    if (has_privilege()) {
        if (abort_cnt >= FLAGS_threshold) {
            is_pcc_ = true;
        } else {
            is_pcc_ = false;
            hand_over_privilege();
        }
    } else {
        is_pcc_ = false;
    }
}

Status TxExecutor::read(Storage s, std::string_view key, TupleBody** body) {
    if (has_privilege() && abort_cnt < FLAGS_threshold) {
        hand_over_privilege();
    }
    if (WriteElement<Tuple>* we = searchWriteSet(s, key)) {
        *body = &(we->body_);
        return Status::OK;
    }
    if (ReadElement<Tuple>* re = searchReadSet(s, key)) {
        *body = &(re->body_);
        return Status::OK;
    }
    Tuple* tuple = Masstrees[get_storage(s)].get_value(key);
    if (tuple == nullptr) return Status::WARN_NOT_FOUND;
    if (const Status stat = read_internal(s, key, tuple); stat != Status::OK)
        return stat;
    *body = &(read_set_.back().body_);
    return Status::OK;
}

Status TxExecutor::read_internal(Storage s, std::string_view key,
                                 Tuple* tuple) {
    if (is_pcc_) {
        Tidword tid_word = get_lock(tuple, LockType::SH_LOCKED);
        if (tid_word.absent) return Status::WARN_NOT_FOUND;
        auto body = TupleBody(key, tuple->body_.get_val(),
                              tuple->body_.get_val_align());
        read_set_.emplace_back(s, key, tuple, std::move(body), tid_word);
        read_lock_set_.emplace_back(s, key, tuple, tid_word,
                                    LockType::SH_LOCKED);
        return Status::OK;
    }
    TupleBody body;
    Tidword expected;
    while (true) {
        expected = get_stable_tid_word(tuple);
        if (expected.lock == LockType::EX_LOCKED) {
            this->status_ = TransactionStatus::aborted;
            return Status::ERROR_CONCURRENT_WRITE_OR_DELETE;
        }
        if (expected.absent) return Status::WARN_NOT_FOUND;
        body = TupleBody(key, tuple->body_.get_val(),
                         tuple->body_.get_val_align());
        Tidword check;
        check.obj_ = loadAcquire(tuple->tidword_.obj_);
        if (expected.tid == check.tid && expected.epoch == check.epoch &&
            expected.latest == check.latest &&
            expected.absent == check.absent &&
            (check.lock == LockType::UNLOCKED ||
             check.lock == LockType::SH_LOCKED)) {
            break;
        }
        expected.obj_ = check.obj_;
    }
    read_set_.emplace_back(s, key, tuple, std::move(body), expected);
    return Status::OK;
}

Status TxExecutor::write(Storage s, std::string_view key, TupleBody&& body) {
    if (has_privilege() && abort_cnt < FLAGS_threshold) {
        hand_over_privilege();
    }
    if (searchWriteSet(s, key)) return Status::OK;
    Tuple* tuple;
    if (ReadElement<Tuple>* re = searchReadSet(s, key)) {
        tuple = re->rcdptr_;
        write_set_.emplace_back(s, key, tuple, std::move(body), OpType::UPDATE);
        return Status::OK;
    }
    tuple = Masstrees[get_storage(s)].get_value(key);
    if (tuple == nullptr) return Status::WARN_NOT_FOUND;
    write_set_.emplace_back(s, key, tuple, std::move(body), OpType::UPDATE);
    return Status::OK;
}

Status TxExecutor::insert(Storage s, std::string_view key, TupleBody&& body) {
    if (has_privilege() && abort_cnt < FLAGS_threshold) {
        hand_over_privilege();
    }
    if (searchWriteSet(s, key)) return Status::WARN_ALREADY_EXISTS;
    Tuple* tuple = Masstrees[get_storage(s)].get_value(key);
    if (tuple != nullptr) return Status::WARN_ALREADY_EXISTS;
    tuple = new Tuple();
    tuple->init(std::move(body));
    if (is_pcc_) tuple->tidword_.lock = LockType::EX_LOCKED;
    Tidword tid_word = tuple->tidword_;
    MasstreeWrapper<Tuple>::insert_info_t insert_info{};
    const Status stat =
            Masstrees[get_storage(s)].insert_value(key, tuple, &insert_info);
    if (stat == Status::WARN_ALREADY_EXISTS) {
        delete tuple;
        return stat;
    }
    write_set_.emplace_back(s, key, tuple, OpType::INSERT);
    if (is_pcc_)
        write_lock_set_.emplace_back(s, key, tuple, tid_word,
                                     LockType::EX_LOCKED);
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
    return Status::OK;
}

Status TxExecutor::delete_record(Storage s, std::string_view key) {
    if (has_privilege() && abort_cnt < FLAGS_threshold) {
        hand_over_privilege();
    }
    for (auto itr = write_set_.begin(); itr != write_set_.end(); ++itr) {
        if (itr->storage_ != s) continue;
        if (itr->key_ == key) write_set_.erase(itr);
    }
    Tuple* tuple = Masstrees[get_storage(s)].get_value(key);
    if (tuple == nullptr) return Status::WARN_NOT_FOUND;
    write_set_.emplace_back(s, key, tuple, OpType::DELETE);
    return Status::OK;
}

void TxExecutor::lockWriteSet() {
    for (auto itr = write_set_.begin(); itr != write_set_.end(); ++itr) {
        Tidword expected;
        if (itr->op_ == OpType::INSERT) continue;
        if (is_pcc_) {
            if (const auto rle = searchReadLockSet(itr->storage_, itr->key_);
                rle != read_lock_set_.end()) {
                const Tidword tid_word =
                        upgrade_lock(rle->rcdptr_, rle->tidword_);
                read_lock_set_.erase(rle);
                write_lock_set_.emplace_back(itr->storage_, itr->key_,
                                             itr->rcdptr_, tid_word,
                                             LockType::EX_LOCKED);
            } else {
                Tidword tid_word = get_lock(itr->rcdptr_, LockType::EX_LOCKED);
                if (tid_word.absent) {
                    this->status_ = TransactionStatus::aborted;
                    return;
                }
                write_lock_set_.emplace_back(itr->storage_, itr->key_,
                                             itr->rcdptr_, tid_word,
                                             LockType::EX_LOCKED);
            }
        } else {
            expected.obj_ = loadAcquire(itr->rcdptr_->tidword_.obj_);
            if (expected.lock == LockType::SH_LOCKED ||
                expected.lock == LockType::EX_LOCKED ||
                expected.lock == LockType::LATCHED) {
                this->status_ = TransactionStatus::aborted;
                if (itr != write_set_.begin()) unlockWriteSet(itr);
                return;
            }
            if (expected.lock != LockType::UNLOCKED) ERR;
            if (expected.absent) {
                this->status_ = TransactionStatus::aborted;
                if (itr != write_set_.begin()) unlockWriteSet(itr);
                return;
            }
            if (!itr->rcdptr_->mutex_.try_lock()) {
                this->status_ = TransactionStatus::aborted;
                if (itr != write_set_.begin()) unlockWriteSet(itr);
                return;
            }
            expected.obj_ = loadAcquire(itr->rcdptr_->tidword_.obj_);
            if (expected.lock != LockType::UNLOCKED) {
                itr->rcdptr_->mutex_.unlock();
                this->status_ = TransactionStatus::aborted;
                if (itr != write_set_.begin()) unlockWriteSet(itr);
                return;
            }
            Tidword desired = expected;
            desired.lock = LockType::LATCHED;
            storeRelease(itr->rcdptr_->tidword_.obj_, desired.obj_);
            itr->rcdptr_->mutex_.unlock();
        }
        max_wset_ = std::max(max_wset_, expected);
    }
}

Status TxExecutor::scan(const Storage s, std::string_view left_key,
                        bool l_exclusive, std::string_view right_key,
                        bool r_exclusive, std::vector<TupleBody*>& result) {
    return scan(s, left_key, l_exclusive, right_key, r_exclusive, result, -1);
}

Status TxExecutor::scan(const Storage s, std::string_view left_key,
                        const bool l_exclusive, std::string_view right_key,
                        const bool r_exclusive, std::vector<TupleBody*>& result,
                        const int64_t limit) {
    if (has_privilege() && abort_cnt < FLAGS_threshold) {
        hand_over_privilege();
    }
    result.clear();
    const auto read_set_init_size = read_set_.size();
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
            stat != Status::OK && stat != Status::WARN_NOT_FOUND)
            return stat;
    }
    if (read_set_init_size != read_set_.size()) {
        for (auto itr =
                     read_set_.begin() +
                     static_cast<
                             std::vector<ReadElement<Tuple>>::difference_type>(
                             read_set_init_size);
             itr != read_set_.end(); ++itr)
            result.emplace_back(&(itr->body_));
    }
    return Status::OK;
}

bool TxExecutor::validationPhase() {
    sort(write_set_.begin(), write_set_.end());
    if (is_pcc_) sort(read_lock_set_.begin(), read_lock_set_.end());
    lockWriteSet();
    if (this->status_ == TransactionStatus::aborted) return false;
    asm volatile("" ::: "memory");
    atomicStoreThLocalEpoch(thid_, atomicLoadGE());
    asm volatile("" ::: "memory");
    for (auto& itr : read_set_) {
        Tidword check;
        check.obj_ = loadAcquire(itr.rcdptr_->tidword_.obj_);
        if (!is_pcc_) {
            if (itr.get_tidword().epoch != check.epoch ||
                itr.get_tidword().tid != check.tid) {
                this->status_ = TransactionStatus::aborted;
                unlockWriteSet();
                return false;
            }
            if (check.lock == LockType::EX_LOCKED) {
                this->status_ = TransactionStatus::aborted;
                unlockWriteSet();
                return false;
            }
            if (check.lock == LockType::LATCHED &&
                !searchWriteSet(itr.storage_, itr.key_)) {
                this->status_ = TransactionStatus::aborted;
                unlockWriteSet();
                return false;
            }
        }
        max_rset_ = std::max(max_rset_, check);
    }
    if (is_pcc_) unlockRead();
    for (auto [fst, snd] : node_map_) {
        if (auto node = static_cast<MasstreeWrapper<Tuple>::node_type*>(fst);
            node->full_version_value() != snd) {
            this->status_ = TransactionStatus::aborted;
            if (is_pcc_) {
                unlockWrite();
            } else {
                unlockWriteSet();
            }
            return false;
        }
    }
    this->status_ = TransactionStatus::committed;
    return true;
}

void TxExecutor::writePhase() {
    Tidword tid_a = std::max(max_wset_, max_rset_);
    tid_a.tid++;
    Tidword tid_b = mrctid_;
    tid_b.tid++;
    Tidword tid_c;
    tid_c.epoch = ThLocalEpoch[thid_].obj_;
    Tidword max_tid_word = std::max({tid_a, tid_b, tid_c});
    max_tid_word.lock = LockType::UNLOCKED;
    max_tid_word.latest = true;
    mrctid_ = max_tid_word;
    for (auto& itr : write_set_) {
        switch (itr.op_) {
            case OpType::UPDATE: {
                memcpy(itr.rcdptr_->body_.get_val_ptr(),
                       itr.body_.get_val_ptr(), itr.body_.get_val_size());
                storeRelease(itr.rcdptr_->tidword_.obj_, max_tid_word.obj_);
                break;
            }
            case OpType::INSERT: {
                max_tid_word.absent = false;
                storeRelease(itr.rcdptr_->tidword_.obj_, max_tid_word.obj_);
                break;
            }
            case OpType::DELETE: {
                max_tid_word.absent = true;
                Masstrees[get_storage(itr.storage_)].remove_value(itr.key_);
                storeRelease(itr.rcdptr_->tidword_.obj_, max_tid_word.obj_);
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
    read_lock_set_.clear();
    write_lock_set_.clear();
    node_map_.clear();

    abort_cnt = 0;
    if (is_pcc_) hand_over_privilege();
}

bool TxExecutor::commit() {
    if (validationPhase()) {
        writePhase();
        return true;
    }
    return false;
}

std::vector<LockElement<Tuple>>::iterator
TxExecutor::searchReadLockSet(const Storage s, const std::string_view key) {
    const auto it = std::lower_bound(
            read_lock_set_.begin(), read_lock_set_.end(),
            LockElement<Tuple>(s, key, nullptr, Tidword{}, LockType::UNLOCKED));
    if (it != read_lock_set_.end() && it->key_ == key && it->storage_ == s)
        return it;
    return read_lock_set_.end();
}

void TxExecutor::unlockRead() const {
    if (!is_pcc_) ERR;
    for (const auto& le : read_lock_set_) {
        const Tidword expected = le.tidword_;
        Tidword desired = expected;
        if (expected.lock != LockType::SH_LOCKED) ERR;
        desired.lock = LockType::UNLOCKED;
        storeRelease(le.rcdptr_->tidword_.obj_, desired.obj_);
    }
}

void TxExecutor::unlockWrite() const {
    if (!is_pcc_) ERR;
    for (const auto& le : write_lock_set_) {
        const Tidword expected = le.tidword_;
        Tidword desired = expected;
        if (expected.lock != LockType::EX_LOCKED) ERR;
        desired.lock = LockType::UNLOCKED;
        storeRelease(le.rcdptr_->tidword_.obj_, desired.obj_);
    }
}

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
    if (is_pcc_) ERR;
    for (const auto& itr : write_set_) {
        Tidword expected;
        if (itr.op_ == OpType::INSERT) continue;
        expected.obj_ = loadAcquire(itr.rcdptr_->tidword_.obj_);
        if (expected.lock == LockType::UNLOCKED) ERR;
        Tidword desired = expected;
        desired.lock = LockType::UNLOCKED;
        storeRelease(itr.rcdptr_->tidword_.obj_, desired.obj_);
    }
}

void TxExecutor::unlockWriteSet(
        const std::vector<WriteElement<Tuple>>::iterator end) {
    if (is_pcc_) ERR;
    for (auto itr = write_set_.begin(); itr != end; ++itr) {
        Tidword expected;
        if (itr->op_ == OpType::INSERT) continue;
        expected.obj_ = loadAcquire(itr->rcdptr_->tidword_.obj_);
        if (expected.lock == LockType::UNLOCKED) ERR;
        Tidword desired = expected;
        desired.lock = LockType::UNLOCKED;
        storeRelease(itr->rcdptr_->tidword_.obj_, desired.obj_);
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
    if (const auto it = tx_->node_map_.find((void*) n);
        it == tx_->node_map_.end()) {
        tx_->node_map_.emplace_hint(it, (void*) n, version);
    } else if (it->second != version) {
        tx_->status_ = TransactionStatus::aborted;
    }
}

bool TxExecutor::has_privilege() const {
    return loadAcquire(privileges_[thid_]) == 1;
}

void TxExecutor::hand_over_privilege() const {
    storeRelease(privileges_[thid_], 0);
    if (thid_ == privileges_.size() - 1) storeRelease(privileges_[0], 1);
    else
        storeRelease(privileges_[thid_ + 1], 1);
}

Tidword TxExecutor::get_lock(Tuple* tuple, const LockType lock_type) {
    if (lock_type != LockType::SH_LOCKED && lock_type != LockType::EX_LOCKED)
        ERR;
    tuple->mutex_.lock();
    Tidword expected;
    expected.obj_ = loadAcquire(tuple->tidword_.obj_);
    if (expected.lock == LockType::SH_LOCKED ||
        expected.lock == LockType::EX_LOCKED)
        ERR;
    while (expected.lock == LockType::LATCHED)
        expected.obj_ = loadAcquire(tuple->tidword_.obj_);
    if (expected.lock != LockType::UNLOCKED) ERR;
    if (expected.absent) {
        tuple->mutex_.unlock();
        return expected;
    }
    Tidword desired = expected;
    desired.lock = lock_type;
    storeRelease(tuple->tidword_.obj_, desired.obj_);
    tuple->mutex_.unlock();
    return desired;
}

Tidword TxExecutor::upgrade_lock(Tuple* tuple, const Tidword current) {
    if (current.lock != LockType::SH_LOCKED) ERR;
    Tidword desired = current;
    desired.lock = LockType::EX_LOCKED;
    storeRelease(tuple->tidword_.obj_, desired.obj_);
    return desired;
}

Tidword TxExecutor::get_stable_tid_word(Tuple* tuple) {
    Tidword tid_word;
    tid_word.obj_ = loadAcquire(tuple->tidword_.obj_);
    while (true) {
        if (tid_word.lock != LockType::LATCHED) break;
        tid_word.obj_ = loadAcquire(tuple->tidword_.obj_);
    }
    return tid_word;
}
