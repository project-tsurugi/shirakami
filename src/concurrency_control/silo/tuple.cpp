/**
 * @file tuple.concurrency_control
 * @brief about tuple
 */

#include "shirakami/tuple.h"

#include "include/tuple_local.h"

namespace shirakami {

#if PARAM_VAL_PRO == 0
Tuple::Impl::Impl(std::string_view key, std::string_view val)
    : pvalue_(new std::string(val.data(), val.size())), // NOLINT
      need_delete_pvalue_(true) {
#elif PARAM_VAL_PRO == 1
Tuple::Impl::Impl(std::string_view key, std::string_view val) {
    value_.assign(val.data(), val.size());
#endif
    key_.assign(key.data(), key.size());
}

Tuple::Impl::Impl(const Impl& right) : key_(right.key_) {
#if PARAM_VAL_PRO == 0
    if (right.need_delete_pvalue_) {
        this->need_delete_pvalue_ = true;
        this->pvalue_.store(new std::string(*right.pvalue_.load(
                                    std::memory_order_acquire)), // NOLINT
                            std::memory_order_release);
    } else {
        this->need_delete_pvalue_ = false;
        this->pvalue_.store(nullptr, std::memory_order_release);
    }
#elif PARAM_VAL_PRO == 1
    value_ = right.key_;
#endif
}

Tuple::Impl::Impl(Impl&& right) { // NOLINT
    this->key_ = std::move(right.key_);
#if PARAM_VAL_PRO == 0
    if (right.need_delete_pvalue_) {
        this->need_delete_pvalue_ = true;
        this->pvalue_.store(right.pvalue_.load(std::memory_order_acquire),
                            std::memory_order_release);
        right.need_delete_pvalue_ = false;
    } else {
        this->need_delete_pvalue_ = false;
        this->pvalue_.store(nullptr, std::memory_order_release);
    }
#elif PARAM_VAL_PRO == 1
    this->value_ = right.value_;
#endif
}

Tuple::Impl& Tuple::Impl::operator=(const Impl& right) { // NOLINT
    // process about copy assign
    this->key_ = right.key_;

#if PARAM_VAL_PRO == 0
    if (this->need_delete_pvalue_) {
        delete this->pvalue_.load(std::memory_order_acquire); // NOLINT
    }
    if (right.need_delete_pvalue_) {
        this->need_delete_pvalue_ = true;
        this->pvalue_.store(new std::string(*right.pvalue_.load( // NOLINT
                                    std::memory_order_acquire)),
                            std::memory_order_release);
    } else {
        this->need_delete_pvalue_ = false;
        this->pvalue_.store(nullptr, std::memory_order_release);
    }
#elif PARAM_VAL_PRO == 1
    this->value_ = right.value_;
#endif

    return *this;
}

Tuple::Impl& Tuple::Impl::operator=(Impl&& right) { // NOLINT
    // process about move assign
    this->key_ = std::move(right.key_);

#if PARAM_VAL_PRO == 0
    // process about this
    if (this->need_delete_pvalue_) {
        delete this->pvalue_.load(std::memory_order_acquire); // NOLINT
    }
    if (right.need_delete_pvalue_) {
        this->need_delete_pvalue_ = true;
        this->pvalue_.store(right.pvalue_.load(std::memory_order_acquire),
                            std::memory_order_release);
        right.need_delete_pvalue_ = false;
    } else {
        this->need_delete_pvalue_ = false;
        this->pvalue_.store(nullptr, std::memory_order_release);
    }
#elif PARAM_VAL_PRO == 1
    this->value_ = std::move(right.value_);
#endif

    return *this;
}

[[nodiscard]] std::string_view Tuple::Impl::get_key() const { // NOLINT
    return std::string_view{key_.data(), key_.size()};
}

[[nodiscard]] std::string_view Tuple::Impl::get_value() const { // NOLINT
#if PARAM_VAL_PRO == 0
    // common subexpression elimination
    std::string* value = pvalue_.load(std::memory_order_acquire);
    if (value == nullptr) { return {}; }
    return std::string_view{value->data(), value->size()};
#elif PARAM_VAL_PRO == 1
    return value_;
#endif
}

void Tuple::Impl::reset() {
#if PARAM_VAL_PRO == 0
    if (need_delete_pvalue_) {
        delete pvalue_.load(std::memory_order_acquire); // NOLINT
    }
#elif PARAM_VAL_PRO == 1
    return;
#endif
}

void Tuple::Impl::set(const char* key_ptr, std::size_t key_length,
                      const char* value_ptr, std::size_t value_length) {
    key_.assign(key_ptr, key_length);

#if PARAM_VAL_PRO == 0
    if (need_delete_pvalue_) {
        delete pvalue_.load(std::memory_order_acquire); // NOLINT
    }

    pvalue_.store(new std::string(value_ptr, value_length), // NOLINT
                  std::memory_order_release);
    this->need_delete_pvalue_ = true;
#elif PARAM_VAL_PRO == 1
    value_.assign(value_ptr, value_length);
#endif
}


void Tuple::Impl::set(std::string_view key, const std::string* value) {
    key_ = key;
#if PARAM_VAL_PRO == 0
    pvalue_.store(const_cast<std::string*>(value), std::memory_order_relaxed);
    this->need_delete_pvalue_ = false;
#elif PARAM_VAL_PRO == 1
    value_ = *value;
#endif
}

#if PARAM_VAL_PRO == 1
void Tuple::Impl::set(std::string_view key, std::string_view val) {
    key_ = key;
    value_ = val;
}
#endif

[[maybe_unused]] void Tuple::Impl::set_key(const char* key_ptr,
                                           std::size_t key_length) {
    key_.assign(key_ptr, key_length);
}

void Tuple::Impl::set_value(const char* value_ptr, std::size_t value_length) {
#if PARAM_VAL_PRO == 0
    if (this->need_delete_pvalue_) {
        pvalue_.load(std::memory_order_acquire)
                ->assign(value_ptr, value_length);
    } else {
        pvalue_.store(new std::string(value_ptr, value_length), // NOLINT
                      std::memory_order_release);
        need_delete_pvalue_ = true;
    }
#elif PARAM_VAL_PRO == 1
    value_.assign(value_ptr, value_length);
#endif
}

#if PARAM_VAL_PRO == 0
void Tuple::Impl::set_value(const char* value_ptr, std::size_t value_length,
                            std::string** old_value) {
    if (this->need_delete_pvalue_) {
        *old_value = pvalue_.load(std::memory_order_acquire);
    } else {
        *old_value = nullptr;
    }

    pvalue_.store(new std::string(value_ptr, value_length), // NOLINT
                  std::memory_order_release);
    this->need_delete_pvalue_ = true;
}
#endif

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

std::string_view Tuple::get_value() const { // NOLINT
    return pimpl_->get_value();
}

Tuple::Impl* Tuple::get_pimpl() { return pimpl_.get(); } // NOLINT

const Tuple::Impl* Tuple::get_pimpl_cst() const {
    return pimpl_.get();
} // NOLINT

} // namespace shirakami
