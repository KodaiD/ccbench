#pragma once

#include <atomic>

#include "../../include/rwlock.hh"
#include "../../include/tuple_body.hh"

using namespace std;

class Tuple {
public:
    alignas(CACHE_LINE_SIZE) ReaderWriteLock lock_;
    TupleBody body_;

    Tuple() = default;

    void init([[maybe_unused]] size_t thid, TupleBody&& body,
              [[maybe_unused]] void* p) {
        body_ = std::move(body);
    }

    void init(TupleBody&& body) {
        body_ = std::move(body);
        lock_.w_lock();
    }
};
