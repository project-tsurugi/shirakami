/**
 * @file tuple.concurrency_control
 * @brief about tuple
 */

#include "shirakami/tuple.h"

#include "include/tuple_local.h"

namespace shirakami {

Tuple::Impl::Impl(std::string_view key, std::string_view val) {
    value_.assign(val.data(), val.size());
    key_.assign(key.data(), key.size());
}

Tuple::Impl::Impl(const Impl& right) : key_(right.key_), value_(right.value_) {}

Tuple::Impl::Impl(Impl&& right) { // NOLINT
    this->key_ = std::move(right.key_);
    this->value_ = std::move(right.value_);
}

Tuple::Impl& Tuple::Impl::operator=(const Impl& right) { // NOLINT
    // process about copy assign
    this->key_ = right.key_;
    this->value_ = right.value_;

    return *this;
}

Tuple::Impl& Tuple::Impl::operator=(Impl&& right) { // NOLINT
    // process about move assign
    this->key_ = std::move(right.key_);
    this->value_ = std::move(right.value_);

    return *this;
}

void Tuple::Impl::get_key(std::string& out) const { // NOLINT
    out = key_;
}

void Tuple::Impl::get_value(std::string& out) { // NOLINT
    out = value_;
}

void Tuple::Impl::reset() {
    key_.clear();
    value_.clear();
}

Tuple::Tuple() : pimpl_(std::make_unique<Impl>()) {}

Tuple::Tuple(std::string_view key, std::string_view val)
    : pimpl_(std::make_unique<Impl>(key, val)) {}

Tuple::Tuple(const Tuple& right) {
    pimpl_ = std::make_unique<Impl>(*right.pimpl_);
}

Tuple::Tuple(Tuple&& right) { pimpl_ = std::move(right.pimpl_); }

Tuple& Tuple::operator=(const Tuple& right) { // NOLINT
    this->pimpl_ = std::make_unique<Impl>(*right.pimpl_);

    return *this;
}

Tuple& Tuple::operator=(Tuple&& right) { // NOLINT
    this->pimpl_ = std::move(right.pimpl_);

    return *this;
}

void Tuple::get_key(std::string& out) const { // NOLINT
    return pimpl_->get_key(out);
}

void Tuple::get_value(std::string& out) const { // NOLINT
    return pimpl_->get_value(out);
}

Tuple::Impl* Tuple::get_pimpl() { return pimpl_.get(); } // NOLINT

const Tuple::Impl* Tuple::get_pimpl_cst() const {
    return pimpl_.get();
} // NOLINT

} // namespace shirakami
