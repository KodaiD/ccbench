#pragma once

#include <stdio.h>
#include <atomic>
#include <iostream>
#include <map>
#include <queue>

#include "../../include/backoff.hh"
#include "../../include/config.hh"
#include "../../include/debug.hh"
#include "../../include/inline.hh"
#include "../../include/procedure.hh"
#include "../../include/result.hh"
#include "../../include/status.hh"
#include "../../include/string.hh"
#include "../../include/util.hh"
#include "../../include/workload.hh"
#include "cicada_op_element.hh"
#include "common.hh"
#include "scan_callback.hh"
#include "time_stamp.hh"
#include "tuple.hh"
#include "version.hh"

#define CONTINUING_COMMIT_THRESHOLD 5

enum class TransactionStatus : uint8_t {
  invalid,
  inflight,
  committed,
  aborted,
};

class TxScanCallback;

class TxExecutor {
public:
  TransactionStatus status_ = TransactionStatus::invalid;
  TimeStamp wts_;
  std::vector<ReadElement<Tuple>> read_set_;
  std::vector<WriteElement<Tuple>> write_set_;
  std::unordered_map<void*, uint64_t> node_map_;
  std::deque<Tuple*> gc_records_;    // for records
  std::deque<GCElement<Tuple>> gcq_; // for versions
  std::deque<Version *> reuse_version_from_gc_;
  std::vector<Procedure> pro_set_;
  Result *result_ = nullptr;
  const bool& quit_; // for thread termination control
  TxScanCallback callback_;
  Backoff& backoff_;

  bool reconnoitering_ = false;
  bool is_ronly_ = false;
  bool is_batch_ = false;

  uint8_t thid_ = 0;
  uint64_t rts_;
  uint64_t start_, stop_;                // for one-sided synchronization
  uint64_t grpcmt_start_, grpcmt_stop_;  // for group commit
  uint64_t gcstart_, gcstop_;            // for garbage collection

  TxExecutor(uint8_t thid, Backoff& backoff, Result *res, const bool &quit)
    : result_(res), thid_(thid), backoff_(backoff), quit_(quit), callback_(TxScanCallback(this)) {

    // wait to initialize MinWts
    while (MinWts.load(memory_order_acquire) == 0);
    rts_ = MinWts.load(memory_order_acquire) - 1;
    wts_.generateTimeStampFirst(thid_);

    __atomic_store_n(&(ThreadWtsArray[thid_].obj_), wts_.ts_, __ATOMIC_RELEASE);
    unsigned int expected, desired;
    expected = FirstAllocateTimestamp.load(memory_order_acquire);
    for (;;) {
      desired = expected + 1;
      if (FirstAllocateTimestamp.compare_exchange_weak(expected, desired,
                                                       memory_order_acq_rel))
        break;
    }

    read_set_.reserve(FLAGS_max_ope);
    write_set_.reserve(FLAGS_max_ope);
    pro_set_.reserve(FLAGS_max_ope);

    if (FLAGS_pre_reserve_version) {
      for (size_t i = 0; i < FLAGS_pre_reserve_version; ++i)
        reuse_version_from_gc_.emplace_back(new Version());
    }

    start_ = rdtscp();
    gcstart_ = start_;
  }

  ~TxExecutor() {
    for (auto itr = reuse_version_from_gc_.begin();
         itr != reuse_version_from_gc_.end(); ++itr) {
      if ((*itr)->status_ == VersionStatus::unused) delete (*itr);
    }
    reuse_version_from_gc_.clear();
    read_set_.clear();
    write_set_.clear();
    gcq_.clear();
    pro_set_.clear();
  }

  void abort();

  bool chkGcpvTimeout();

  void cpv();  // commit pending versions
  void displayWriteSet();

  void earlyAbort();

  void mainte();  // maintenance
  void gcpv();    // group commit pending versions
  void precpv();  // pre-commit pending versions
  void pwal();    // parallel write ahead log.
  void swal();

  void begin();

  Status read(Storage s, std::string_view key, TupleBody** body);
  Version* read_internal(Storage s, std::string_view key, Tuple* tuple);

  Status write(Storage s, std::string_view key, TupleBody&& body);

  Status insert(Storage s, std::string_view key, TupleBody&& body);

  Status delete_record(Storage s, std::string_view key);

  Status scan(Storage s,
              std::string_view left_key, bool l_exclusive,
              std::string_view right_key, bool r_exclusive,
              std::vector<TupleBody *>&result);

  Status scan(Storage s,
              std::string_view left_key, bool l_exclusive,
              std::string_view right_key, bool r_exclusive,
              std::vector<TupleBody *>&result, int64_t limit);

  bool validation();

  void writePhase();

  bool commit();

  bool isLeader();

  void leaderWork();

  void reconnoiter_begin(); 

  void reconnoiter_end(); 

  void gc_records();

  void gc_versions();

  void backoff() {
#if ADD_ANALYSIS
    uint64_t start = rdtscp();
#endif

    Backoff::backoff(FLAGS_clocks_per_us);

#if ADD_ANALYSIS
    result_->local_backoff_latency_ += rdtscp() - start;
#endif
  }

  void gcAfterThisVersion([[maybe_unused]] Tuple *tuple, Version *delTarget) {
    while (delTarget != nullptr) {
      // escape next pointer
      Version *tmp = delTarget->next_.load(std::memory_order_acquire);

#if INLINE_VERSION_OPT
      if (delTarget == &(tuple->inline_ver_)) {
        tuple->returnInlineVersionRight();
        goto gcAfterThisVersion_NEXT_LOOP;
      }
#endif  // if INLINE_VERSION_OPT

#if REUSE_VERSION
      reuse_version_from_gc_.emplace_back(delTarget);
#else   // if REUSE_VERSION
      delete delTarget;
#endif  // if REUSE_VERSION

[[maybe_unused]] gcAfterThisVersion_NEXT_LOOP :
#if ADD_ANALYSIS
      ++result_->local_gc_version_counts_;
#endif
      delTarget = tmp;
    }
  }

#if INLINE_VERSION_OPT
#if INLINE_VERSION_PROMOTION
  void inlineVersionPromotion(Storage s, std::string_view key, Tuple* tuple,
                              Version* later_ver, Version* ver) {
    if (ver != &(tuple->inline_ver_) &&
        MinRts.load(std::memory_order_acquire) > ver->ldAcqWts() &&
        tuple->inline_ver_.status_.load(std::memory_order_acquire) ==
            VersionStatus::unused) {
      write(s, key, TupleBody(ver->body_));
      if (this->is_ronly_) {
        this->is_ronly_ = false;
        read_set_.emplace_back(s, key, tuple, later_ver, ver);
      }
    }
  }
#endif
#endif

  Version *newVersionGeneration([[maybe_unused]] Tuple *tuple, TupleBody&& body) {
#if INLINE_VERSION_OPT
    if (tuple->getInlineVersionRight()) {
      tuple->inline_ver_.set(0, this->wts_.ts_, std::move(body));
#if ADD_ANALYSIS
      ++result_->local_version_reuse_;
#endif
      return &(tuple->inline_ver_);
    }
#endif  // if INLINE_VERSION_OPT

#if REUSE_VERSION
    if (!reuse_version_from_gc_.empty()) {
#if ADD_ANALYSIS
      ++result_->local_version_reuse_;
#endif
      Version* newVersion = reuse_version_from_gc_.back();
      reuse_version_from_gc_.pop_back();
      newVersion->set(0, this->wts_.ts_, std::move(body));
      return newVersion;
    }
#endif

#if ADD_ANALYSIS
    ++result_->local_version_malloc_;
#endif
    return new Version(0, this->wts_.ts_, std::move(body));
  }

  bool precheckInValidation() {
    // Two optimizations can add unnecessary overhead under low contention
    // because they do not improve the performance of uncontended workloads.
    // Each thread adaptively omits both steps if the recent transactions have
    // been committed (5 in a row in our implementation).
    //
    // Sort write set by contention
    partial_sort(write_set_.begin(),
                 write_set_.begin() + (write_set_.size() / 2),
                 write_set_.end());

    /* Pre-check version consistency
     * every currently visible version v of the records in the write set
     * satisfies (v.rts) <= (tx.ts)
     * satisfies (v.wts) < tx.ts (wts order)
     */
    for (auto itr = write_set_.begin();
         itr != write_set_.begin() + (write_set_.size() / 2); ++itr) {
      if ((*itr).op_ == OpType::INSERT) continue;
      if ((*itr).rcdptr_->continuing_commit_.load(memory_order_acquire) <
          CONTINUING_COMMIT_THRESHOLD) {
        Version *ver;
        if ((*itr).op_ == OpType::RMW || (*itr).op_ == OpType::DELETE) {
          ver = (*itr).rcdptr_->ldAcqLatest();
          if (ver->ldAcqWts() > this->wts_.ts_ ||
              ver->ldAcqRts() > this->wts_.ts_)
            return false;
        } else {
          if ((*itr).later_ver_)
            ver = (*itr).later_ver_;
          else
            ver = (*itr).rcdptr_->ldAcqLatest();
          while (ver->ldAcqWts() > this->wts_.ts_) {
            (*itr).later_ver_ = ver;
            ver = ver->ldAcqNext();
          }
          while (ver->ldAcqStatus() != VersionStatus::committed
                  && ver->ldAcqStatus() != VersionStatus::deleted) {
            ver = ver->ldAcqNext();
          }
          if (ver->ldAcqRts() > this->wts_.ts_) return false;
        }
      }
    }

    return true;
  }

  void readTimestampUpdateInValidation() {
    for (auto itr = read_set_.begin(); itr != read_set_.end(); ++itr) {
      uint64_t expected;
      expected = (*itr).ver_->ldAcqRts();
      for (;;) {
        if (expected > this->wts_.ts_) break;
        if ((*itr).ver_->rts_.compare_exchange_strong(expected, this->wts_.ts_,
                                                      memory_order_acq_rel,
                                                      memory_order_acquire))
          break;
      }
    }
  }

  /**
   * @brief Search xxx set
   * @detail Search element of local set corresponding to given key.
   * In this prototype system, the value to be updated for each worker thread 
   * is fixed for high performance, so it is only necessary to check the key match.
   * @param Key [in] the key of key-value
   * @return Corresponding element of local set
   */
  inline ReadElement<Tuple> *searchReadSet(Storage s, std::string_view key) {
    for (auto &re : read_set_) {
      if (re.storage_ != s) continue;
      if (re.key_ == key) return &re;
    }

    return nullptr;
  }

  /**
   * @brief Search xxx set
   * @detail Search element of local set corresponding to given key.
   * In this prototype system, the value to be updated for each worker thread 
   * is fixed for high performance, so it is only necessary to check the key match.
   * @param Key [in] the key of key-value
   * @return Corresponding element of local set
   */
  inline WriteElement<Tuple> *searchWriteSet(Storage s, std::string_view key) {
    for (auto &we : write_set_) {
      if (we.storage_ != s) continue;
      if (we.key_ == key) return &we;
    }

    return nullptr;
  }

  void writeSetClean() {
    for (auto itr = write_set_.begin(); itr != write_set_.end(); ++itr) {
      (*itr).rcdptr_->continuing_commit_.store(0, memory_order_release);
      if ((*itr).finish_version_install_) {
        (*itr).new_ver_->status_.store(VersionStatus::aborted,
                                       std::memory_order_release);
        continue;
      } else {
#if INLINE_VERSION_OPT
        if ((*itr).new_ver_ == &(*itr).rcdptr_->inline_ver_) {
          (*itr).rcdptr_->returnInlineVersionRight();
          continue;
        }
#endif

#if REUSE_VERSION
        (*itr).new_ver_->status_.store(VersionStatus::unused,
                                       std::memory_order_release);
        reuse_version_from_gc_.emplace_back((*itr).new_ver_);
#else
        delete (*itr).new_ver_;
#endif
      }
    }
    write_set_.clear();
  }

  static INLINE Tuple *get_tuple(Tuple *table, uint64_t key) {
    return &table[key];
  }
};
