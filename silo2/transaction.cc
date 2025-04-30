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

TxExecutor::TxExecutor(int thid, Result* res, const bool& quit,
                       std::vector<uint64_t>& privileges)
    : result_(res), backoff_(FLAGS_clocks_per_us), quit_(quit),
      callback_(TxScanCallback(this)), status_(), thid_(thid),
      privileges_(privileges) {
    max_rset_.obj_ = 0;
    max_wset_.obj_ = 0;
    epoch_timer_start = rdtsc();
    epoch_timer_stop = 0;
    for (auto& lock_node : lock_nodes_) {
        lock_node.reset();
        lock_nodes_map_[&lock_node] = false;
    }
}

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
    if (is_pcc_) unlockReadSet();
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
    abort_cnt++;
#if BACK_OFF
    Backoff::backoff(FLAGS_clocks_per_us);
#endif
}

void TxExecutor::begin() {
    status_ = TransactionStatus::inflight;
    atomicStoreThLocalEpoch(thid_, atomicLoadGE());
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
    next_free_node = 0;
}

Status TxExecutor::read(Storage s, std::string_view key, TupleBody** body) {
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
        auto node = allocate_node();
        tuple->mutex_.read_lock(node);
        Tidword tid_word;
        tid_word.obj_ = loadRelaxed(tuple->tidword_.obj_);
        if (tid_word.absent) {
            tuple->mutex_.unlock(node);
            return Status::WARN_NOT_FOUND;
        }
        auto body = TupleBody(key, tuple->body_.get_val(),
                              tuple->body_.get_val_align());
        read_set_.emplace_back(s, key, tuple, std::move(body), tid_word, node);
        return Status::OK;
    }
    TupleBody body;
    Tidword expected;
    while (true) {
        expected.obj_ = loadAcquire(tuple->tidword_.obj_);
        if (tuple->mutex_.is_locked()) {
            expected.obj_ = loadAcquire(tuple->tidword_.obj_);
            continue;
        }
        if (expected.absent) return Status::WARN_NOT_FOUND;
        body = TupleBody(key, tuple->body_.get_val(),
                         tuple->body_.get_val_align());
        Tidword check;
        check.obj_ = loadAcquire(tuple->tidword_.obj_);
        if (expected.tid == check.tid && expected.epoch == check.epoch &&
            expected.latest == check.latest &&
            expected.absent == check.absent && !tuple->mutex_.is_locked()) {
            break;
        }
        expected.obj_ = check.obj_;
    }
    read_set_.emplace_back(s, key, tuple, std::move(body), expected, nullptr);
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
        write_set_.emplace_back(s, key, tuple, std::move(body), OpType::UPDATE,
                                re->node_);
        return Status::OK;
    }
    tuple = Masstrees[get_storage(s)].get_value(key);
    if (tuple == nullptr) return Status::WARN_NOT_FOUND;
    write_set_.emplace_back(s, key, tuple, std::move(body), OpType::UPDATE,
                            nullptr);
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
    auto node = allocate_node();
    tuple->init(thid_, std::move(body), node);
    MasstreeWrapper<Tuple>::insert_info_t insert_info{};
    const Status stat =
            Masstrees[get_storage(s)].insert_value(key, tuple, &insert_info);
    if (stat == Status::WARN_ALREADY_EXISTS) {
        tuple->mutex_.unlock(node);
        delete tuple;
        return stat;
    }
    write_set_.emplace_back(s, key, tuple, OpType::INSERT, node);
    if (insert_info.node) {
        if (!node_map_.empty()) {
            if (const auto it = node_map_.find((void*) insert_info.node);
                it != node_map_.end()) {
                if (unlikely(it->second != insert_info.old_version)) {
                    tuple->mutex_.unlock(node);
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
    write_set_.emplace_back(s, key, tuple, OpType::DELETE, nullptr);
    return Status::OK;
}

void TxExecutor::lockWriteSet() {
    for (auto itr = write_set_.begin(); itr != write_set_.end(); ++itr) {
        Tidword expected;
        if (itr->op_ == OpType::INSERT) continue;
        if (is_pcc_) {
            if (auto re = searchReadSetIterator(itr->storage_, itr->key_);
                re != read_set_.end()) {
                itr->rcdptr_->mutex_.upgrade(re->node_);
                read_set_.erase(re);
            } else {
                const auto node = allocate_node();
                itr->rcdptr_->mutex_.lock(node);
                itr->node_ = node;
                expected.obj_ = loadRelaxed(itr->rcdptr_->tidword_.obj_);
                if (expected.absent) {
                    itr->rcdptr_->mutex_.unlock(node);
                    this->status_ = TransactionStatus::aborted;
                    if (itr != write_set_.begin()) unlockWriteSet(itr);
                    return;
                }
            }
        } else {
            const auto node = allocate_node();
            if (!itr->rcdptr_->mutex_.try_lock(node)) {
                this->status_ = TransactionStatus::aborted;
                if (itr != write_set_.begin()) unlockWriteSet(itr);
                return;
            }
            expected.obj_ = loadRelaxed(itr->rcdptr_->tidword_.obj_);
            if (expected.absent) {
                itr->rcdptr_->mutex_.unlock(node);
                this->status_ = TransactionStatus::aborted;
                if (itr != write_set_.begin()) unlockWriteSet(itr);
                return;
            }
            itr->node_ = node;
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
            if (itr.rcdptr_->mutex_.is_locked() &&
                !searchWriteSet(itr.storage_, itr.key_)) {
                this->status_ = TransactionStatus::aborted;
                unlockWriteSet();
                return false;
            }
        }
        max_rset_ = std::max(max_rset_, check);
    }
    if (is_pcc_) unlockReadSet();
    for (auto [fst, snd] : node_map_) {
        if (auto node = static_cast<MasstreeWrapper<Tuple>::node_type*>(fst);
            node->full_version_value() != snd) {
            this->status_ = TransactionStatus::aborted;
            unlockWriteSet();
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
    max_tid_word.latest = true;
    mrctid_ = max_tid_word;
    for (auto& itr : write_set_) {
        switch (itr.op_) {
            case OpType::UPDATE: {
                memcpy(itr.rcdptr_->body_.get_val_ptr(),
                       itr.body_.get_val_ptr(), itr.body_.get_val_size());
                storeRelease(itr.rcdptr_->tidword_.obj_, max_tid_word.obj_);
                itr.rcdptr_->mutex_.unlock(itr.node_);
                break;
            }
            case OpType::INSERT: {
                max_tid_word.absent = false;
                storeRelease(itr.rcdptr_->tidword_.obj_, max_tid_word.obj_);
                itr.rcdptr_->mutex_.unlock(itr.node_);
                break;
            }
            case OpType::DELETE: {
                max_tid_word.absent = true;
                Masstrees[get_storage(itr.storage_)].remove_value(itr.key_);
                storeRelease(itr.rcdptr_->tidword_.obj_, max_tid_word.obj_);
                gc_records_.push_back(itr.rcdptr_);
                itr.rcdptr_->mutex_.unlock(itr.node_);
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
    if (is_pcc_) hand_over_privilege();
}

bool TxExecutor::commit() {
    if (validationPhase()) {
        writePhase();
        return true;
    }
    return false;
}

void TxExecutor::unlockReadSet() {
    if (!is_pcc_) ERR;
    for (const auto& re : read_set_) { re.rcdptr_->mutex_.unlock(re.node_); }
}

vector<ReadElement<Tuple>>::iterator
TxExecutor::searchReadSetIterator(Storage s, std::string_view key) {
    for (auto re = read_set_.begin(); re != read_set_.end(); ++re) {
        if (re->storage_ != s) continue;
        if (re->key_ == key) return re;
    }
    return read_set_.end();
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
    for (const auto& itr : write_set_) {
        itr.rcdptr_->mutex_.unlock(itr.node_);
    }
}

void TxExecutor::unlockWriteSet(
        const std::vector<WriteElement<Tuple>>::iterator end) {
    for (auto itr = write_set_.begin(); itr != end; ++itr) {
        itr->rcdptr_->mutex_.unlock(itr->node_);
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

MCSMutex::MCSNode* TxExecutor::allocate_node() {
    return &lock_nodes_[next_free_node++];
}
