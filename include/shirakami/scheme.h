/**
 * @file include/shirakami/scheme.h
 */

#pragma once

#include <pthread.h>
#include <unistd.h>

#include <algorithm>
#include <cassert>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <functional>
#include <iostream>
#include <memory>
#include <vector>

namespace shirakami {

/**
 * @brief Session token
 */
using Token = void*;

/**
 * @brief Scan Handle
 */
using ScanHandle = std::size_t;

enum class scan_endpoint : char {
    EXCLUSIVE,
    INCLUSIVE,
    INF,
};

inline constexpr std::string_view to_string_view(scan_endpoint ep) noexcept {
    using namespace std::string_view_literals;
    switch (ep) {
        case scan_endpoint::EXCLUSIVE:
            return "EXCLUSIVE"sv; //NOLINT
        case scan_endpoint::INCLUSIVE:
            return "INCLUSIVE"sv; //NOLINT
        case scan_endpoint::INF:
            return "INF"sv; //NOLINT
    }
    std::abort();
}

inline std::ostream& operator<<(std::ostream& out, scan_endpoint op) { // NOLINT
    return out << to_string_view(op);
}

/**
 * @brief the status which is after some function.
 * OK is success return code.
 * WARN_... is no problem for extra progressing due to work but last command
 * was canceled.
 * ERR_... is problem for extra progressing so it was executed abort command 
 * internally.
 * INTERNAL_BEGIN is a boundary between public status and internal status.
 * INTERNAL_WARN .. is return code for internal implements.
 * INTERNAL_ERR... is also return code for internal implements.
 */
enum class Status : std::int32_t {
    /**
     * @brief Warning.
     * @details When it uses multiple tx_begin without termination command, 
     * this is returned.
     */
    WARN_ALREADY_BEGIN = -1000,
    /**
     * @brief Warning.
     * @details The transaction executed some operation for the page which it 
     * already executed delete operation for the page.
     */
    WARN_ALREADY_DELETE,
    /**
     * @brief Warning.
     * @details The transaction tried to insert, but failed due to concurrent 
     * insert.
     */
    WARN_ALREADY_EXISTS,
    /**
     * @brief Warning.
     * @details When init function is called more than twice without fin, this 
     * status code is returned. This is for blocking invalid multiple 
     * initialization.
     */
    WARN_ALREADY_INIT,
    /**
     * @brief Warning.
     * @details The transaction executed delete operation for the page which
     * it executed insert operation for the page.
     */
    WARN_CANCEL_PREVIOUS_INSERT,
    /**
     * @brief Warning.
     * @details The transaction executed delete operation for the page which
     * it executed upsert operation for the page.
     */
    WARN_CANCEL_PREVIOUS_UPSERT,
    /**
     * @brief Warning.
     * @details The transaction failed operation due to concurrent insert 
     * operation.
     */
    WARN_CONCURRENT_INSERT,
    /**
     * @brief Warning.
     * @details The transaction failed operation due to concurrent update 
     * operation.
     */
    WARN_CONCURRENT_UPDATE,
    /**
     * @brief Warning.
     * @details This means conflict between short tx and long tx's wp.
     */
    WARN_CONFLICT_ON_WRITE_PRESERVE,
    /**
     * @brief Warning.
     * @details This means that you executed an illegal operation. For example, 
     * invalid combinations about arguments.
     */
    WARN_ILLEGAL_OPERATION,
    /**
     * @brief Warning.
     * @details The arguments of api is invalid.
     */
    WARN_INVALID_ARGS,
    /**
     * @brief Warning.
     * @details The handle of arguments for api is invalid.
     */
    WARN_INVALID_HANDLE,
    /**
     * @brief Warning
     * @details The status that the user calls api which needs tx_begin and 
     * some operations.
     */
    WARN_NOT_BEGIN,
    /**
     * @brief Warning
     * @details The target is not found.
     */
    WARN_NOT_FOUND,
    /**
     * @brief Warning
     * @details 
     * @a leave : If the session is already ended. @n
     */
    WARN_NOT_IN_A_SESSION,
    /**
     * @brief Warning
     * @details If it calls fin function without init, 
     * this status is returned.
     */
    WARN_NOT_INIT,
    /**
     * @brief Warning
     * @details When a long tx mode's transaction tries to start 
     * an operation, the status is returned if it is not yet 
     * time to start.
     */
    WARN_PREMATURE,
    /**
     * @brief Warning
     * @details
     * @a open_scan : The scan could find some records but could not preserve 
     * result due to capacity limitation. @n
     * @a read_from_scan : It have read all records in range of open_scan. @n
     */
    WARN_SCAN_LIMIT,
    /**
     * @brief Warning
     * @details Storage id is depletion.
     */
    WARN_STORAGE_ID_DEPLETION,
    /**
     * @brief Warning
     * @details The target storage of operation is not found.
     */
    WARN_STORAGE_NOT_FOUND,
    /**
     * @brief Warning
     * @details Wait for some tx to commit.
     */
    WARN_WAITING_FOR_OTHER_TX,
    /**
     * @brief Warning
     * @details If the long mode transaction tries to write to some area 
     * without wp, this code will be returned.
     */
    WARN_WRITE_WITHOUT_WP,
    /**
     * @brief success status.
     */
    OK = 0,
    /**
     * @brief Error about concurrency control.
     */
    ERR_CC,
    /**
     * @brief Error
     * @details This means that insert operation fails due to existence.
     */
    ERR_FAIL_INSERT,
    /**
     * @brief Error
     * @details This means that wp failed.
     */
    ERR_FAIL_WP,
    /**
     * @brief Error
     * @details Some fatal error. For example, programming error.
     */
    ERR_FATAL,
    /**
     * @brief Error
     * @details The unknown fatal error about index.
     */
    ERR_FATAL_INDEX,
    /**
     * @brief Error
     * @details The configuration at init command is invalid.
     */
    ERR_INVALID_CONFIGURATION,
    /**
     * @brief Error about key value store.
     */
    ERR_KVS,
    /**
     * @brief Error
     * @details The error shows the long transaction execute read for the 
     * storage included read negative list.
     */
    ERR_READ_AREA_VIOLATION,
    /**
     * @brief Error
     * @details This means that it is not implemented.
     */
    ERR_NOT_IMPLEMENTED,
    /**
     * @brief Error
     * @details
     * @a read_from_scan : It is the error due to phantom problems. @n
     * @a scan_key : It is the error due to phantom problems. @n
     */
    ERR_PHANTOM,
    /**
     * @brief Error
     * @details
     * @a enter : There are no capacity of session. @n
     */
    ERR_SESSION_LIMIT,
    /**
     * @brief Error
     * @details
     * @a commit : This means read validation failure and it already executed 
     * abort(). After this, do tx_begin to start next transaction or leave to 
     * leave the session. @n
     */
    ERR_VALIDATION,
    /**
     * @brief Error
     * @details
     * @a commit : This transaction including update operations was interrupted 
     * by some delete transaction between read phase and validation phase. 
     * So it called abort. @n
     */
    ERR_WRITE_TO_DELETED_RECORD,
    INTERNAL_BEGIN = 1000,
    INTERNAL_WARN_CONCURRENT_INSERT,
    INTERNAL_WARN_NOT_DELETED,
    INTERNAL_WARN_NOT_FOUND,
    INTERNAL_WARN_PREMATURE,
};

inline constexpr std::string_view to_string_view( // NOLINT
        const Status value) noexcept {
    using namespace std::string_view_literals;
    switch (value) {
        case Status::WARN_ALREADY_BEGIN:
            return "WARN_ALREADY_BEGIN"sv; // NOLINT
        case Status::WARN_ALREADY_DELETE:
            return "WARN_ALREADY_DELETE"sv; // NOLINT
        case Status::WARN_ALREADY_EXISTS:
            return "WARN_ALREADY_EXISTS"sv; // NOLINT
        case Status::WARN_ALREADY_INIT:
            return "WARN_ALREADY_INIT"sv; // NOLINT
        case Status::WARN_CANCEL_PREVIOUS_INSERT:
            return "WARN_CANCEL_PREVIOUS_INSERT"sv; // NOLINT
        case Status::WARN_CANCEL_PREVIOUS_UPSERT:
            return "WARN_CANCEL_PREVIOUS_UPSERT"sv; // NOLINT
        case Status::WARN_CONCURRENT_INSERT:
            return "WARN_CONCURRENT_INSERT"sv; // NOLINT
        case Status::WARN_CONCURRENT_UPDATE:
            return "WARN_CONCURRENT_UPDATE"sv; // NOLINT
        case Status::WARN_CONFLICT_ON_WRITE_PRESERVE:
            return "WARN_CONFLICT_ON_WRITE_PRESERVE"sv; // NOLINT
        case Status::WARN_ILLEGAL_OPERATION:
            return "WARN_ILLEGAL_OPERATION"sv; // NOLINT
        case Status::WARN_INVALID_ARGS:
            return "WARN_INVALID_ARGS"sv; // NOLINT
        case Status::WARN_INVALID_HANDLE:
            return "WARN_INVALID_HANDLE"sv; // NOLINT
        case Status::WARN_NOT_BEGIN:
            return "WARN_NOT_BEGIN"sv; // NOLINT
        case Status::WARN_NOT_FOUND:
            return "WARN_NOT_FOUND"sv; // NOLINT
        case Status::WARN_NOT_IN_A_SESSION:
            return "WARN_NOT_IN_A_SESSION"sv; // NOLINT
        case Status::WARN_NOT_INIT:
            return "WARN_NOT_INIT"sv; // NOLINT
        case Status::WARN_PREMATURE:
            return "WARN_PREMATURE"sv; // NOLINT
        case Status::WARN_SCAN_LIMIT:
            return "WARN_SCAN_LIMIT"sv; // NOLINT
        case Status::WARN_STORAGE_ID_DEPLETION:
            return "WARN_STORAGE_ID_DEPLETION"sv; // NOLINT
        case Status::WARN_STORAGE_NOT_FOUND:
            return "WARN_STORAGE_NOT_FOUND"sv; // NOLINT
        case Status::WARN_WAITING_FOR_OTHER_TX:
            return "WARN_WAITING_FOR_OTHER_TX"sv; // NOLINT
        case Status::WARN_WRITE_WITHOUT_WP:
            return "WARN_WRITE_WITHOUT_WP"sv; // NOLINT
        case Status::OK:
            return "OK"sv; // NOLINT
        case Status::ERR_CC:
            return "ERR_CC"sv; // NOLINT
        case Status::ERR_FAIL_INSERT:
            return "ERR_FAIL_INSERT"sv; // NOLINT
        case Status::ERR_FAIL_WP:
            return "ERR_FAIL_WP"sv; // NOLINT
        case Status::ERR_READ_AREA_VIOLATION:
            return "ERR_READ_AREA_VIOLATION"sv; // NOLINT
        case Status::ERR_FATAL:
            return "ERR_FATAL"sv; // NOLINT
        case Status::ERR_FATAL_INDEX:
            return "ERR_FATAL_INDEX"sv; // NOLINT
        case Status::ERR_INVALID_CONFIGURATION:
            return "ERR_INVALID_CONFIGURATION"sv; // NOLINT
        case Status::ERR_KVS:
            return "ERR_KVS"sv; // NOLINT
        case Status::ERR_NOT_IMPLEMENTED:
            return "ERR_NOT_IMPLEMENTED"sv; // NOLINT
        case Status::ERR_SESSION_LIMIT:
            return "ERR_SESSION_LIMIT"sv; // NOLINT
        case Status::ERR_PHANTOM:
            return "ERR_PHANTOM"sv; // NOLINT
        case Status::ERR_VALIDATION:
            return "ERR_VALIDATION"sv; // NOLINT
        case Status::ERR_WRITE_TO_DELETED_RECORD:
            return "ERR_WRITE_TO_DELETED_RECORD"sv; // NOLINT
        case Status::INTERNAL_BEGIN:
            return "INTERNAL_BEGIN"sv; // NOLINT
        case Status::INTERNAL_WARN_NOT_DELETED:
            return "INTERNAL_WARN_NOT_DELETED"sv; // NOLINT
        case Status::INTERNAL_WARN_NOT_FOUND:
            return "INTERNAL_WARN_NOT_FOUND"sv; // NOLINT
        case Status::INTERNAL_WARN_CONCURRENT_INSERT:
            return "INTERNAL_WARN_CONCURRENT_INSERT"sv; // NOLINT
        case Status::INTERNAL_WARN_PREMATURE:
            return "INTERNAL_WARN_PREMATURE"sv; // NOLINT
    }
    std::abort();
}

inline std::ostream& operator<<(std::ostream& out,
                                const Status value) { // NOLINT
    return out << to_string_view(value);
}

enum class OP_TYPE : std::int32_t {
    ABORT,
    BEGIN,
    COMMIT,
    DELETE,
    NONE,
    INSERT,
    SCAN,
    SEARCH,
    UPDATE,
    UPSERT,
};

inline constexpr std::string_view to_string_view( // NOLINT
        const OP_TYPE op) noexcept {
    using namespace std::string_view_literals;
    switch (op) {
        case OP_TYPE::ABORT:
            return "ABORT"sv; // NOLINT
        case OP_TYPE::BEGIN:
            return "BEGIN"sv; // NOLINT
        case OP_TYPE::COMMIT:
            return "COMMIT"sv; // NOLINT
        case OP_TYPE::DELETE:
            return "DELETE"sv; // NOLINT
        case OP_TYPE::INSERT:
            return "INSERT"sv; // NOLINT
        case OP_TYPE::NONE:
            return "NONE"sv; // NOLINT
        case OP_TYPE::SCAN:
            return "SCAN"sv; // NOLINT
        case OP_TYPE::SEARCH:
            return "SEARCH"sv; // NOLINT
        case OP_TYPE::UPDATE:
            return "UPDATE"sv; // NOLINT
        case OP_TYPE::UPSERT:
            return "UPSERT"sv; // NOLINT
    }
    std::abort();
}

inline std::ostream& operator<<(std::ostream& out, const OP_TYPE op) { // NOLINT
    return out << to_string_view(op);
}

} // namespace shirakami