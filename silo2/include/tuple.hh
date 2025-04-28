#pragma once

#include <cstdint>
#include <cstring>

#include "../../include/cache_line_size.hh"
#include "../../include/tuple_body.hh"

#include "mutex.h"

struct Tidword {
    union {
        uint64_t obj_;
        struct {
            bool latest : 1;
            bool absent : 1;
            uint64_t tid : 20;
            uint64_t epoch : 32;
        };
    };

    Tidword() : obj_(0) {};

    bool operator==(const Tidword& right) const { return obj_ == right.obj_; }

    bool operator!=(const Tidword& right) const { return !operator==(right); }

    bool operator<(const Tidword& right) const {
        return this->obj_ < right.obj_;
    }
};

class Tuple {
public:
    alignas(CACHE_LINE_SIZE) Tidword tidword_;
    alignas(CACHE_LINE_SIZE) MCSMutex mutex_;
    TupleBody body_;

    Tuple() = default;

    void init([[maybe_unused]] size_t thid, TupleBody&& body,
              [[maybe_unused]] void* p) {
        // for initializer
        tidword_.epoch = 1;
        tidword_.latest = true;
        tidword_.absent = false;
        body_ = std::move(body);
    }

    void init(TupleBody&& body) {
        tidword_.absent = true;
        mutex_.lock();
        body_ = std::move(body);
    }
};
