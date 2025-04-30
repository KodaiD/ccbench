#pragma once

#include <atomic>

class MCSMutex {
public:
    MCSMutex() : tail_(nullptr) {}

    struct MCSNode;

    bool try_lock(MCSNode* my_node) {
        auto tail = tail_.load(std::memory_order_acquire);
        while (true) {
            if (tail != nullptr) break;
            if (tail_.compare_exchange_weak(tail, my_node,
                                            std::memory_order_acquire,
                                            std::memory_order_relaxed))
                return true;
        }
        return false;
    }

    void lock(MCSNode* my_node) {
        if (MCSNode* prev = tail_.exchange(my_node, std::memory_order_acquire);
            prev != nullptr) {
            my_node->locked = true;
            prev->next.store(my_node, std::memory_order_release);
            while (my_node->locked.load(std::memory_order_acquire)) {}
        }
    }

    void read_lock(MCSNode* my_node) {
        my_node->is_read.store(true);
        if (MCSNode* prev = tail_.exchange(my_node, std::memory_order_acquire);
            prev != nullptr) {
            my_node->locked = true;
            prev->next.store(my_node, std::memory_order_release);
            while (my_node->locked.load(std::memory_order_acquire)) {}
        }
    }

    // Because normal tx does not wait for read lock, tail_ is always me
    void upgrade(MCSNode* my_node) {
        my_node->is_read.store(false, std::memory_order_release);
    }

    void unlock(MCSNode* my_node) {
        MCSNode* next = my_node->next;
        if (next == nullptr) {
            if (MCSNode* expected = my_node; tail_.compare_exchange_strong(
                        expected, nullptr, std::memory_order_release,
                        std::memory_order_relaxed)) {
                my_node->reset();
                return;
            }
            while ((next = my_node->next) == nullptr) {}
        }
        next->locked.store(false, std::memory_order_release);
        my_node->reset();
    }

    [[nodiscard]] bool is_locked() const {
        const auto tail = tail_.load(std::memory_order_acquire);
        if (tail == nullptr) return false;
        return !tail->is_read.load(std::memory_order_acquire);
    }

    struct MCSNode {
        std::atomic<bool> locked = false;
        std::atomic<MCSNode*> next = nullptr;
        std::atomic<bool> is_read = false;

        MCSNode() = default;

        void reset() {
            locked.store(false);
            next.store(nullptr);
            is_read.store(false);
        }
    };

private:
    std::atomic<MCSNode*> tail_;
};