#pragma once

#include <set>
#include <vector>

#include "logging.h"
#include "scheme.h"
#include "storage_options.h"

#include "glog/logging.h"

namespace shirakami {

class transaction_options final {
public:
    using write_preserve_type = std::vector<Storage>;

    enum class transaction_type : std::int32_t {
        /**
         * @brief It is optimized for long transaction which its abort rate is
         * over 99.9 % when it executes by short mode.
         */
        LONG,
        /**
         * @brief It is optimized for few contention workload.
         */
        SHORT,
        /**
         * @brief It is optmized for read only transaction. It reads slightly
         * old safe snapshot without verify. It is also LTX and it must wait 1
         * epoch to start at least.
         */
        READ_ONLY,
    };

    class read_area {
    public:
        using list_type = std::set<Storage>;

        read_area() = default;

        read_area(list_type plist, list_type nlist)
            : positive_list_(std::move(plist)),
              negative_list_(std::move(nlist)) {}

        [[nodiscard]] bool empty() const {
            return positive_list_.empty() && negative_list_.empty();
        }

        void erase_from_positive_list(Storage st) { positive_list_.erase(st); }

        [[nodiscard]] list_type get_positive_list() { return positive_list_; }

        [[nodiscard]] list_type const& get_positive_list() const {
            return positive_list_;
        }

        [[nodiscard]] list_type get_negative_list() { return negative_list_; }

        [[nodiscard]] list_type const& get_negative_list() const {
            return negative_list_;
        }

        void insert_to_positive_list(Storage st) { positive_list_.insert(st); }

        void insert_to_negative_list(Storage st) { negative_list_.insert(st); }

    private:
        list_type positive_list_{};
        list_type negative_list_{};
    };

    transaction_options() = default; // LINT

    transaction_options(Token token) : token_(token) {} // LINT

    transaction_options(Token token, transaction_type tt)
        : token_(token), transaction_type_(tt) {}

    transaction_options(Token token, transaction_type tt,
                        write_preserve_type wp)
        : token_(token), transaction_type_(tt), write_preserve_(std::move(wp)) {
    }

    transaction_options(Token token, transaction_type tt,
                        write_preserve_type wp, read_area ra)
        : token_(token), transaction_type_(tt), write_preserve_(std::move(wp)),
          read_area_(std::move(ra)) {}

    [[nodiscard]] Token get_token() const { return token_; }

    [[nodiscard]] transaction_type get_transaction_type() const {
        return transaction_type_;
    }

    [[nodiscard]] write_preserve_type get_write_preserve() const {
        return write_preserve_;
    }

    [[nodiscard]] read_area get_read_area() const { return read_area_; }

    void set_read_area(read_area const& ra) { read_area_ = ra; }

    void set_token(Token token) { token_ = token; }

    void set_transaction_type(transaction_type tt) { transaction_type_ = tt; }

    void set_write_preserve(write_preserve_type const& wp) {
        write_preserve_ = wp;
    }

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
    write_preserve_type write_preserve_{};

    /**
     * @brief read area
     * @details The storage list information that the area which may be read by
     * this tx and the list that the area which must not be read by this tx.
     * This information is used for optimizations.
     * If you begins transaction with this information, empty positive /
     * negative list is invalid(i.e. not used).
     */
    read_area read_area_{};
};

inline constexpr std::string_view
to_string_view(const transaction_options::transaction_type tp) noexcept {
    using namespace std::string_view_literals;
    switch (tp) {
        case transaction_options::transaction_type::SHORT:
            return "SHORT"sv; // LINT
        case transaction_options::transaction_type::LONG:
            return "LONG"sv; // LINT
        case transaction_options::transaction_type::READ_ONLY:
            return "READ_ONLY"sv; // LINT
    }
    /**
     * LOG_FIRST_N マクロは下記エラーによって利用できない。
     * error: ‘occurrences_162’ declared ‘static’ in ‘constexpr’ function
     */
    LOG(ERROR) << log_location_prefix << log_location_prefix
               << "unknown transaction type";
    return ""sv;
}

inline std::ostream&
operator<<(std::ostream& out,
           const transaction_options::transaction_type tp) { // LINT
    return out << to_string_view(tp);
}

inline std::string
to_string(std::vector<Storage> const& write_preserve) noexcept {
    std::string buf{};
    bool at_least_once{false};
    for (auto&& elem : write_preserve) {
        buf += std::to_string(elem);
        buf += " ";
        at_least_once = true;
    }
    if (at_least_once) { buf.pop_back(); }
    return buf;
}

inline std::string
to_string(const transaction_options::read_area& ra) noexcept {
    std::string buf{};
    if (ra.empty()) { return buf; }
    if (!ra.get_positive_list().empty()) {
        buf += "positive list{";
        for (auto&& elem : ra.get_positive_list()) {
            buf += std::to_string(elem);
            if (elem != *ra.get_positive_list().rbegin()) { buf += ", "; }
        }
        buf += "}";
        if (!ra.get_negative_list().empty()) { buf += ", "; }
    }
    if (!ra.get_negative_list().empty()) {
        buf += "negative list{";
        for (auto&& elem : ra.get_negative_list()) {
            buf += std::to_string(elem);
            if (elem != *ra.get_negative_list().rbegin()) { buf += ", "; }
        }
        buf += "}";
    }
    return buf;
}

inline std::ostream& operator<<(std::ostream& out,
                                const transaction_options to) { // LINT
    return out << "Token: " << to.get_token()
               << ", transaction_type: " << to.get_transaction_type()
               << ", write_preserve: " << to_string(to.get_write_preserve())
               << ", read_area: " << to_string(to.get_read_area());
}

} // namespace shirakami
