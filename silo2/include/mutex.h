#pragma once

#include <atomic>

class MCSMutex
{
public:
    MCSMutex() : tail_(nullptr)
    {
    }

    void lock()
    {
        if (MCSNode* prev = tail_.exchange(&my_node_, std::memory_order_acquire); prev != nullptr)
        {
            my_node_.locked = true;
            prev->next.store(&my_node_, std::memory_order_release);
            while (my_node_.locked.load(std::memory_order_acquire))
            {
            }
        }
    }

    void unlock()
    {
        MCSNode* next = my_node_.next;
        if (next == nullptr)
        {
            if (MCSNode* expected = &my_node_;
                tail_.compare_exchange_strong(expected, nullptr, std::memory_order_release))
            {
                my_node_.reset();
                return;
            }
            while ((next = my_node_.next) == nullptr)
            {
            }
        }
        next->locked.store(false, std::memory_order_release);
        my_node_.reset();
    }

private:
    struct MCSNode
    {
        std::atomic<bool> locked;
        std::atomic<MCSNode*> next;

        MCSNode() : locked(false), next(nullptr)
        {
        }

        void reset()
        {
            locked.store(false);
            next.store(nullptr);
        }
    };

    std::atomic<MCSNode*> tail_;
    thread_local static MCSNode my_node_;
};