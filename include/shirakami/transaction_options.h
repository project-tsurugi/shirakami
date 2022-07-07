#pragma once

#include <vector>

#include "scheme.h"

namespace shirakami {

class transaction_options final {
public:
    using write_preserve_type = std::vector<Storage>;

    enum class transaction_type : std::int32_t {
        LONG,
        SHORT,
        READ_ONLY,
    };

    transaction_options() = default;

    transaction_options(Token token, transaction_type tt,
                        write_preserve_type wp)
        : token_(token), transaction_type_(tt), write_preserve_(wp) {}

    Token get_token() { return token_; }

    transaction_type get_transaction_type() { return transaction_type_; }

    write_preserve_type& get_write_preserve() { return write_preserve_; }

    void set_token(Token token) { token_ = token; }

    void set_transaction_type(transaction_type tt) { transaction_type_ = tt; }

    void set_write_preserve(write_preserve_type& wp) { write_preserve_ = wp; }

private:
    Token token_{};

    transaction_type transaction_type_{transaction_type::SHORT};

    std::vector<Storage> write_preserve_{};
};

} // namespace shirakami