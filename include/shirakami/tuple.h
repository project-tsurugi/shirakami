/**
 * @file tuple.h
 * @brief about tuple
 */
#pragma once

#include <memory>
#include <string>
#include <string_view>
#include <utility>

namespace shirakami {

class Tuple { // NOLINT
public:
    class Impl;

    Tuple();

    Tuple(std::string_view key, std::string_view val);

    Tuple(const Tuple& right);

    Tuple(Tuple&& right);

    Tuple& operator=(const Tuple& right); // NOLINT
    Tuple& operator=(Tuple&& right);      // NOLINT

    void get_key(std::string& out) const;   // NOLINT
    void get_value(std::string& out) const; // NOLINT
    Impl* get_pimpl();                      // NOLINT
    const Impl* get_pimpl_cst() const;      // NOLINT

private:
    std::unique_ptr<Impl> pimpl_;
};

} // namespace shirakami
