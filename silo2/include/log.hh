#pragma once

#include <cstdint>
#include <cstring>
#include <memory>

class LogHeader {
public:
    int chkSum_ = 0;
    unsigned int logRecNum_ = 0;
    const std::size_t len_val_ = VAL_SIZE;

    void init() {
        chkSum_ = 0;
        logRecNum_ = 0;
    }

    void convertChkSumIntoComplementOnTwo() {
        chkSum_ ^= 0xffffffff;
        ++chkSum_;
    }
};

class LogRecord {
public:
    uint64_t tid_;
    std::string_view key_;
    char val_[VAL_SIZE];

    LogRecord() : tid_(0), val_() {}

    LogRecord(uint64_t tid, std::string_view key, char* val)
        : tid_(tid), key_(key), val_() {
        memcpy(this->val_, val, VAL_SIZE);
    }

    int computeChkSum() {
        int chkSum = 0;
        auto itr = reinterpret_cast<int*>(this);
        for (unsigned int i = 0; i < sizeof(LogRecord) / sizeof(int); ++i) {
            chkSum += (*itr);
            ++itr;
        }
        return chkSum;
    }
};

class LogPackage {
public:
    LogHeader header_;
    std::unique_ptr<LogRecord[]> log_records_;
};
