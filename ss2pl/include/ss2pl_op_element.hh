#pragma once

#include "../../include/op_element.hh"

template<typename T>
class SetElement : public OpElement<T> {
public:
    using OpElement<T>::OpElement;
    TupleBody body_;

    SetElement(Storage s, std::string_view key, T* rcd_ptr, TupleBody&& body)
        : OpElement<T>::OpElement(s, key, rcd_ptr), body_(std::move(body)) {}

    SetElement(Storage s, std::string_view key, T* rcd_ptr)
        : OpElement<T>::OpElement(s, key, rcd_ptr) {}

    SetElement(Storage s, std::string_view key, T* rcd_ptr, OpType op)
        : OpElement<T>::OpElement(s, key, rcd_ptr, op) {}

    SetElement(Storage s, std::string_view key, T* rcd_ptr, TupleBody&& body,
               OpType op)
        : OpElement<T>::OpElement(s, key, rcd_ptr, op), body_(std::move(body)) {
    }

    bool operator<(const SetElement& right) const {
        if (this->storage_ != right.storage_)
            return this->storage_ < right.storage_;
        return this->key_ < right.key_;
    }
};
