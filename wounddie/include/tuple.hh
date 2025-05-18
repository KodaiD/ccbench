#pragma once

#include "../../include/cache_line_size.hh"
#include "../../include/tuple_body.hh"

constexpr uint8_t UNLOCKED = 0;
constexpr uint8_t EXLOCKED = 1;
constexpr uint8_t SUPER_SHLOCKED = 2;
constexpr uint8_t SUPER_EXLOCKED = 3;

struct Tidword {
    union {
        uint64_t obj_;
        struct {
            uint8_t lock : 2;
            uint32_t thid : 16;
            bool reserved : 1;
            bool latest : 1;
            bool absent : 1;
            uint64_t tid : 11;
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
    TupleBody body_;

    Tuple() {}

    void init([[maybe_unused]] size_t thid, TupleBody&& body,
              [[maybe_unused]] void* p) {
        tidword_.epoch = 1;
        tidword_.tid = 0;
        tidword_.latest = true;
        tidword_.absent = false;
        tidword_.reserved = false;
        tidword_.thid = UINT16_MAX;
        tidword_.lock = UNLOCKED;
        body_ = std::move(body);
    }

    void init(TupleBody&& body, const uint8_t lock_mode) {
        tidword_.absent = true;
        tidword_.reserved = false;
        tidword_.thid = UINT16_MAX;
        tidword_.lock = lock_mode;
        body_ = std::move(body);
    }
};
