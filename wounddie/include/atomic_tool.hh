#pragma once

#include "common.hh"

#include "../../include/inline.hh"

INLINE uint64_t atomicLoadGE();

INLINE void atomicAddGE() {
    uint64_t expected;

    expected = atomicLoadGE();
    for (;;) {
        if (const uint64_t desired = expected + 1; __atomic_compare_exchange_n(
                    &(GlobalEpoch.obj_), &expected, desired, false,
                    __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE))
            break;
    }
}

INLINE uint64_t atomicLoadGE() {
    const uint64_t_64byte result =
            __atomic_load_n(&(GlobalEpoch.obj_), __ATOMIC_ACQUIRE);
    return result.obj_;
}

INLINE void atomicStoreThLocalEpoch(const unsigned int thid,
                                    const uint64_t new_val) {
    __atomic_store_n(&(ThLocalEpoch[thid].obj_), new_val, __ATOMIC_RELEASE);
}

INLINE void atomicStoreThLocalTS(const unsigned int thid,
                                 const uint64_t new_val) {
    __atomic_store_n(&(ThLocalTS[thid].obj_), new_val, __ATOMIC_RELEASE);
}

INLINE void atomicStoreThLocalStatus(const unsigned int thid,
                                     const uint64_t new_val) {
    __atomic_store_n(&(ThLocalStatus[thid].obj_), new_val, __ATOMIC_RELEASE);
}