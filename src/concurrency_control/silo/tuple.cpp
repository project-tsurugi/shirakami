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

[[nodiscard]] std::string_view Tuple::Impl::get_key() const { // NOLINT
    return std::string_view{key_.data(), key_.size()};
}

[[nodiscard]] std::string Tuple::Impl::get_value() { // NOLINT
    std::shared_lock<std::shared_mutex> lk{mtx_value_};
    return value_;
}

void Tuple::Impl::reset() {
    key_.clear();
    value_.clear();
    return;
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

std::string_view Tuple::get_key() const { // NOLINT
    return pimpl_->get_key();
}

std::string Tuple::get_value() const { // NOLINT
    return pimpl_->get_value();
}

Tuple::Impl* Tuple::get_pimpl() { return pimpl_.get(); } // NOLINT

const Tuple::Impl* Tuple::get_pimpl_cst() const {
    return pimpl_.get();
} // NOLINT

} // namespace shirakami
