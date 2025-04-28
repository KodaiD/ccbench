#pragma once

#include <atomic>

class MCSMutex {
public:
    MCSMutex() : tail_(nullptr) {}

    bool try_lock(const size_t id) {
        MCSNode* my_node_ = &nodes_[id];
        auto tail = tail_.load(std::memory_order_acquire);
        while (true) {
            if (tail != nullptr) break;
            if (tail_.compare_exchange_weak(tail, my_node_,
                                            std::memory_order_acquire,
                                            std::memory_order_relaxed))
                return true;
        }
        return false;
    }

    void lock(const size_t id) {
        MCSNode* my_node_ = &nodes_[id];
        if (MCSNode* prev = tail_.exchange(my_node_, std::memory_order_acquire);
            prev != nullptr) {
            my_node_->locked = true;
            prev->next.store(my_node_, std::memory_order_release);
            while (my_node_->locked.load(std::memory_order_acquire)) {}
        }
    }

    void read_lock(const size_t id) {
        MCSNode* my_node_ = &nodes_[id];
        my_node_->is_read.store(true);
        if (MCSNode* prev = tail_.exchange(my_node_, std::memory_order_acquire);
            prev != nullptr) {
            my_node_->locked = true;
            prev->next.store(my_node_, std::memory_order_release);
            while (my_node_->locked.load(std::memory_order_acquire)) {}
        }
    }

    // Because normal tx does not wait for read lock, tail_ is always me
    void upgrade(const size_t id) {
        MCSNode* my_node_ = &nodes_[id];
        my_node_->is_read.store(false, std::memory_order_release);
    }

    void unlock(const size_t id) {
        MCSNode* my_node_ = &nodes_[id];
        MCSNode* next = my_node_->next;
        if (next == nullptr) {
            if (MCSNode* expected = my_node_; tail_.compare_exchange_strong(
                        expected, nullptr, std::memory_order_release,
                        std::memory_order_relaxed)) {
                my_node_->reset();
                return;
            }
            while ((next = my_node_->next) == nullptr) {}
        }
        next->locked.store(false, std::memory_order_release);
        my_node_->reset();
    }

    [[nodiscard]] bool is_locked() const {
        const auto tail = tail_.load(std::memory_order_acquire);
        if (tail == nullptr) return false;
        return !tail->is_read.load(std::memory_order_acquire);
    }

private:
    struct MCSNode {
        std::atomic<bool> locked;
        std::atomic<MCSNode*> next;
        std::atomic<bool> is_read;

        MCSNode() : locked(false), next(nullptr), is_read(false) {}

        void reset() {
            locked.store(false);
            next.store(nullptr);
            is_read.store(false);
        }
    };

    std::atomic<MCSNode*> tail_;
    std::array<MCSNode, 128> nodes_;
};