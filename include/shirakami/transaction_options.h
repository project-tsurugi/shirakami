#pragma once

#include <vector>

#include "scheme.h"

#include "glog/logging.h"

namespace shirakami {

class transaction_options final {
public:
    using write_preserve_type = std::vector<Storage>;

    enum class transaction_type : std::int32_t {
        LONG,
        SHORT,
        READ_ONLY,
    };

    transaction_options() = default; // NOLINT

    transaction_options(Token token) : token_(token) {} // NOLINT

    transaction_options(Token token, transaction_type tt)
        : token_(token), transaction_type_(tt) {}

    transaction_options(Token token, transaction_type tt,
                        write_preserve_type wp)
        : token_(token), transaction_type_(tt), write_preserve_(std::move(wp)) {
    }

    Token get_token() const { return token_; }

    transaction_type get_transaction_type() const { return transaction_type_; }

    write_preserve_type get_write_preserve() const { return write_preserve_; }

    void set_token(Token token) { token_ = token; }

    void set_transaction_type(transaction_type tt) { transaction_type_ = tt; }

    void set_write_preserve(write_preserve_type wp) { write_preserve_ = wp; }

private:
    /**
     * @brief This is a transaction executor information got from enter command.
     */
    Token token_{};

    /**
     * @brief Transaction type. There are three types: SHORT, LONG, READ_ONLY.
     * @details
     * In SHORT, the transaction is executed as an occ transaction, and an occ 
     * transaction is suitable for short transaction.
     * In LONG, the transaction is executed as yatsumine protocol being treated
     * better than SHORT. Occ transactions is weak for contentions. If long 
     * transaction is executed as occ protocol, the transaction is hard to win 
     * conflict resolutions. In the case, You can use LONG(yatsumine) for the 
     * long transaction.
     * In READ_ONLY, the transaction is executed as some old transaction(*1), 
     * and it can success definitely due to (*1).
     */
    transaction_type transaction_type_{transaction_type::SHORT};

    /**
     * @brief write preserve
     * @details If you use transaction_type::LONG, you must use this member.
     * This shows the tables which the long transaction will write.
     */
    std::vector<Storage> write_preserve_{};
};

inline constexpr std::string_view
to_string_view(const transaction_options::transaction_type tp) noexcept {
    using namespace std::string_view_literals;
    switch (tp) {
        case transaction_options::transaction_type::SHORT:
            return "SHORT"sv; // NOLINT
        case transaction_options::transaction_type::LONG:
            return "LONG"sv; // NOLINT
        case transaction_options::transaction_type::READ_ONLY:
            return "READ_ONLY"sv; // NOLINT
    }
    LOG(ERROR) << "unknown transaction type";
    return ""sv;
}

inline std::ostream&
operator<<(std::ostream& out,
           const transaction_options::transaction_type tp) { // NOLINT
    return out << to_string_view(tp);
}

inline std::string
to_string(const std::vector<Storage> write_preserve) noexcept {
    std::string buf{""};
    bool at_least_once{false};
    for (auto&& elem : write_preserve) {
        buf += std::to_string(elem);
        buf += " ";
        at_least_once = true;
    }
    if (at_least_once) { buf.pop_back(); }
    return buf;
}

inline std::ostream& operator<<(std::ostream& out,
                                const transaction_options to) { // NOLINT
    return out << "Token: " << to.get_token()
               << ", transaction_type: " << to.get_transaction_type()
               << ", write_preserve: "
               << to_string(to.get_write_preserve());
}

} // namespace shirakami