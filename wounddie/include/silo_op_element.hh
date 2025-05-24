#pragma once

#include <memory>

#include "../../include/op_element.hh"

template<typename T>
class ReadElement : public OpElement<T> {
public:
    using OpElement<T>::OpElement;
    TupleBody body_;

    ReadElement(Storage s, std::string_view key, T* rcd_ptr, const char* val,
                const Tidword tidword, const bool is_locked)
        : OpElement<T>::OpElement(s, key, rcd_ptr), is_locked_(is_locked) {
        tidword_.obj_ = tidword.obj_;
        memcpy(this->val_, val, VAL_SIZE);
    }

    ReadElement(Storage s, std::string_view key, T* rcd_ptr, TupleBody&& body,
                const Tidword tidword, const bool is_locked)
        : OpElement<T>::OpElement(s, key, rcd_ptr), body_(std::move(body)),
          is_locked_(is_locked) {
        tidword_.obj_ = tidword.obj_;
    }

    bool operator<(const ReadElement& right) const {
        if (this->storage_ != right.storage_)
            return this->storage_ < right.storage_;
        return this->key_ < right.key_;
    }

    [[nodiscard]] Tidword get_tidword() const { return tidword_; }

    void set_locked(const bool locked) { is_locked_ = locked; }

    [[nodiscard]] bool is_locked() const { return is_locked_; }

private:
    Tidword tidword_;
    char val_[VAL_SIZE]{};
    bool is_locked_;
};

template<typename T>
class WriteElement : public OpElement<T> {
public:
    using OpElement<T>::OpElement;
    TupleBody body_;

    WriteElement(Storage s, std::string_view key, T* rcd_ptr,
                 const std::string_view val, OpType op)
        : OpElement<T>::OpElement(s, key, rcd_ptr, op) {
        static_assert(std::string_view("").empty(),
                      "Expected behavior was broken.");
        if (!val.empty()) {
            val_ptr_ = std::make_unique<char[]>(val.size());
            memcpy(val_ptr_.get(), val.data(), val.size());
            val_length_ = val.size();
        } else {
            val_length_ = 0;
        }
        is_locked_ = false;
    }

    WriteElement(Storage s, std::string_view key, T* rcd_ptr, OpType op)
        : OpElement<T>::OpElement(s, key, rcd_ptr, op) {
        is_locked_ = false;
    }

    WriteElement(Storage s, std::string_view key, T* rcd_ptr, TupleBody&& body,
                 OpType op)
        : OpElement<T>::OpElement(s, key, rcd_ptr, op), body_(std::move(body)) {
        is_locked_ = false;
    }

    bool operator<(const WriteElement& right) const {
        if (this->storage_ != right.storage_)
            return this->storage_ < right.storage_;
        return this->key_ < right.key_;
    }

    [[nodiscard]] char* get_val_ptr() const { return val_ptr_.get(); }

    [[nodiscard]] std::size_t get_val_length() const { return val_length_; }

private:
    std::unique_ptr<char[]> val_ptr_;
    std::size_t val_length_{};
    bool is_locked_;
};
