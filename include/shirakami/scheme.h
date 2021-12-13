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
#include <iostream>
#include <memory>
#include <vector>

namespace shirakami {

/**

 * @details This is used for args of commit command.
 */
enum class commit_property : char {
    /**
     * @brief It waits commit (durable) at commit command.
     */
    WAIT_FOR_COMMIT,
    /**
     * @brief It does not wait commit (durable) at commit command.
     * If you need to notify commit to client, use check_commit function.
     */
    NOWAIT_FOR_COMMIT,
};

class commit_param {
public:
    commit_property get_cp() { return cp_; } // NOLINT

    [[maybe_unused]] [[nodiscard]] std::uint64_t get_ctid() const {
        return ctid_;
    } // NOLINT

    void set_cp(commit_property cp) { cp_ = cp; }

    void set_ctid(std::uint64_t ctid) { ctid_ = ctid; }

private:
    commit_property cp_{commit_property::NOWAIT_FOR_COMMIT};
    std::uint64_t ctid_{0};
};

/**
 * @brief Session token
 */
using Token = void*;

/**
 * @brief Scan Handle
 */
using ScanHandle = std::size_t;

/**
 * @brief Storage Handle
 */
using Storage = std::uint64_t;

enum class scan_endpoint : char {
    EXCLUSIVE,
    INCLUSIVE,
    INF,
};

/**
 * @brief the status which is after some function.
 *
 * Warn is no problem for progressing.
 * ERR is problem for progressing.
 */
enum class Status : std::int32_t {
    /**
     * @brief warning.
     * @details When it uses multiple tx_begin without termination command, this is returned.
     */
    WARN_ALREADY_BEGIN,
    /**
     * @brief warning.
     * @details
     * @a delete_all_records : There are no records. @n
     * @a read_from_scan : The read targets was deleted by delete operation of own transaction. @n
     * @a scan_key : The read targets was deleted by delete operation of own transaction. @n
     * @a search_key : The read targets was deleted by delete operation of own transaction. @n
     */
    WARN_ALREADY_DELETE,
    /**
     * @brief warning.
     * @details
     * @a insert : The records whose key is the same as @a key exists in MTDB, so this function returned immediately. @n
     */
    WARN_ALREADY_EXISTS,
    /**
     * @brief warning.
     * @details When init function is called twice without fin, this status code is returned.
     * This is for blocking invalid multiple initialization.
     */
    WARN_ALREADY_INIT,
    WARN_CANCEL_PREVIOUS_INSERT,
    /**
     * @brief warning.
     * @details
     * @a delete_record : it canceled an update/insert operation before this function and did delete operation. @n
     */
    WARN_CANCEL_PREVIOUS_OPERATION,
    /**
     * @brief warning.
     * @details
     * @a read_from_scan : The read targets was deleted by delete operation of concurrent transaction. @n
     * @a read_record : The read targets was deleted by delete operation of concurrent transaction. @n
     * @a scan_key : The read targets was deleted by delete operation of concurrent transaction. @n
     * @a search_key : The read targets was deleted by delete operation of concurrent transaction. @n
     */
    WARN_CONCURRENT_DELETE,
    /**
     * @brief warning.
     * @details
     * @a read_record : The expected operation could not be performed because a record being inserted in
     * parallel was detected. @n
     * @a read_from_scan : The expected operation could not be performed because a record being inserted in
     * parallel was detected. @n
     */
    WARN_CONCURRENT_INSERT,
    /**
     * @brief warning
     */
    WARN_CONCURRENT_UPDATE,
    /**
     * @brief warning.
     * @details The process could not be executed because the invariant was violated if the process was being executed. 
     * But it's not a fatal problem. It's just a warning.
     */
    WARN_INVARIANT,
    /**
     * @brief warning
     * @details
     * @a init : The args as a log directory path is invalid. @n
     */
    WARN_INVALID_ARGS,
    /**
     * @brief warning.
     * @details
     * @a close_scan : The handle is invalid. @n
     * @a read_from_scan : The handle is invalid. @n
     */
    WARN_INVALID_HANDLE,
    /**
     * @brief warning
     * @details
     * @a delete_record : No corresponding record in masstree. If you have problem by WARN_NOT_FOUND, you should do abort. @n
     * @a open_scan : The scan couldn't find any records. @n
     * @a search_key : No corresponding record in masstree. If you have problem by WARN_NOT_FOUND, you should do abort. @n
     * @a update : No corresponding record in masstree. If you have problem by WARN_NOT_FOUND, you should do abort. @n
     */
    WARN_NOT_FOUND,
    /**
     * @brief warning
     * @details
     * @a leave : If the session is already ended. @n
     */
    WARN_NOT_IN_A_SESSION,
    /**
     * @brief warning
     * @details If it calls fin function without init, 
     * this status is returned.
     */
    WARN_NOT_INIT,
    /**
     * @brief warning
     * @details When a batch mode transaction tries to start 
     * an operation, the status is returned if it is not yet 
     * time to start.
     */
    WARN_PREMATURE,
    /**
     * @brief waring
     * @details
     * @a read_from_scan : It read the records from own 
     * preceding write. @n
     * @a insert : operation in the same tx. @n
     * @a update : operation in the same tx. @n
     * @a upsert : operation in the same tx. @n
     */
    WARN_READ_FROM_OWN_OPERATION,
    /**
     * @brief warning
     * @details
     * @a open_scan : The scan could find some records but could not preserve result due to capacity limitation. @n
     * @a read_from_scan : It have read all records in range of open_scan. @n
     */
    WARN_SCAN_LIMIT,
    /**
     * @brief warning
     * @details warning about trying to insert a page and there already is a page 
     * which has same primary key.
     */
    WARN_UNIQUE_CONSTRAINT,
    /**
     * @brief warning
     * @details
     * @a update : It already executed update/insert, so it up date the value which is going to be updated. @n
     * @a upsert : It already did insert/update/upsert, so it overwrite its local write set. @n
     */
    WARN_WRITE_TO_LOCAL_WRITE,
    /**
     * @brief success status.
     */
    OK,
    ERR_CPR_ORDER_VIOLATION,
    /**
     * @brief error
     * @details This means that wp failed.
     */
    ERR_FAIL_WP,
    ERR_FATAL,
    /**
     * @brief error
     * @details
     * @a delete_storage : If the storage is not registered with the given name. @n
     * @a get_storage : If the storage is not registered with the given name. @n
     */
    ERR_NOT_FOUND,
    /**
     * @brief error
     * @details
     * @a read_from_scan : It is the error due to phantom problems. @n
     * @a scan_key : It is the error due to phantom problems. @n
     */
    ERR_PHANTOM,
    /**
     * @brief error
     * @details
     * @a enter : There are no capacity of session. @n
     */
    ERR_SESSION_LIMIT,
    /**
     * @brief error
     * @details Error about storage.
     */
    ERR_STORAGE,
    /**
     * @brief error
     * @details
     * @a commit : This means read validation failure and it already executed abort(). After this, do tx_begin to start
     * next transaction or leave to leave the session. @n
     */
    ERR_VALIDATION,
    /**
     * @brief error
     * @details
     * @a commit : This transaction including update operations was interrupted by some delete transaction between
     * read phase and validation phase. So it called abort. @n
     */
    ERR_WRITE_TO_DELETED_RECORD,
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
        case Status::WARN_CANCEL_PREVIOUS_OPERATION:
            return "WARN_CANCEL_PREVIOUS_OPERATION"sv; // NOLINT
        case Status::WARN_CONCURRENT_DELETE:
            return "WARN_CONCURRENT_DELETE"sv; // NOLINT
        case Status::WARN_CONCURRENT_INSERT:
            return "WARN_CONCURRENT_INSERT"sv; // NOLINT
        case Status::WARN_CONCURRENT_UPDATE:
            return "WARN_CONCURRENT_UPDATE"sv; // NOLINT
        case Status::WARN_INVARIANT:
            return "WARN_INVARIANT"sv; // NOLINT
        case Status::WARN_INVALID_ARGS:
            return "WARN_INVALID_ARGS"sv; // NOLINT
        case Status::WARN_INVALID_HANDLE:
            return "WARN_INVALID_HANDLE"sv; // NOLINT
        case Status::WARN_NOT_FOUND:
            return "WARN_NOT_FOUND"sv; // NOLINT
        case Status::WARN_NOT_IN_A_SESSION:
            return "WARN_NOT_IN_A_SESSION"sv; // NOLINT
        case Status::WARN_NOT_INIT:
            return "WARN_NOT_INIT"sv; // NOLINT
        case Status::WARN_PREMATURE:
            return "WARN_PREMATURE"sv; // NOLINT
        case Status::WARN_READ_FROM_OWN_OPERATION:
            return "WARN_READ_FROM_OWN_OPERATION"sv; // NOLINT
        case Status::WARN_SCAN_LIMIT:
            return "WARN_SCAN_LIMIT"sv; // NOLINT
        case Status::WARN_UNIQUE_CONSTRAINT:
            return "WARN_UNIQUE_CONSTRAINT"sv; // NOLINT
        case Status::WARN_WRITE_TO_LOCAL_WRITE:
            return "WARN_WRITE_TO_LOCAL_WRITE"sv; // NOLINT
        case Status::OK:
            return "OK"sv; // NOLINT
        case Status::ERR_CPR_ORDER_VIOLATION:
            return "ERR_CPR_ORDER_VIOLATION"sv; // NOLINT
        case Status::ERR_FAIL_WP:
            return "ERR_FAIL_WP"sv; // NOLINT
        case Status::ERR_FATAL:
            return "ERR_FATAL"sv; // NOLINT
        case Status::ERR_NOT_FOUND:
            return "ERR_NOT_FOUND"sv; // NOLINT
        case Status::ERR_SESSION_LIMIT:
            return "ERR_SESSION_LIMIT"sv; // NOLINT
        case Status::ERR_STORAGE:
            return "ERR_STORAGE"sv; // NOLINT
        case Status::ERR_PHANTOM:
            return "ERR_PHANTOM"sv; // NOLINT
        case Status::ERR_VALIDATION:
            return "ERR_VALIDATION"sv; // NOLINT
        case Status::ERR_WRITE_TO_DELETED_RECORD:
            return "ERR_WRITE_TO_DELETED_RECORD"sv; // NOLINT
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
    }
    std::abort();
}

inline std::ostream& operator<<(std::ostream& out, const OP_TYPE op) { // NOLINT
    return out << to_string_view(op);
}

} // namespace shirakami
