
#include <string_view>

#include "include/tuple_local.h"

#include "shirakami/tuple.h"

namespace shirakami {

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

Tuple::Impl* Tuple::get_pimpl() { return pimpl_.get(); } // NOLINT

const Tuple::Impl* Tuple::get_pimpl_cst() const { // NOLINT
    return pimpl_.get();
}

std::string_view Tuple::get_key() const { return pimpl_->get_key(); }

std::string_view Tuple::get_value() const { return pimpl_->get_val(); }

} // namespace shirakami