/**
 * @file tuple.concurrency_control
 * @brief about tuple
 */

#include "shirakami/tuple.h"

#include "concurrency_control/include/tuple_local.h"

namespace shirakami {

Tuple::Impl::Impl(std::string_view key, std::string_view val) {
    key_ = key;   // NOLINT
    value_ = val; // NOLINT
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

Tuple::Tuple(Tuple&& right) { pimpl_ = std::move(right.pimpl_); } // NOLINT

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
