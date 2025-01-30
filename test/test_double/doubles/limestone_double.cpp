#define _GNU_SOURCE 1
#include <dlfcn.h>

#include "glog/logging.h"

#include "limestone_double.h"

#define EXTERN_C 1

namespace test_double::log_channel_add_entry1 { hook_type hook_func = nullptr; }
namespace test_double::log_channel_add_entry2 { hook_type hook_func = nullptr; }
namespace test_double::datastore_switch_available_boundary_version { hook_type hook_func = nullptr; }

#if EXTERN_C == 0

namespace limestone::api {

void log_channel::add_entry(storage_id_type storage_id, std::string_view key, std::string_view value, write_version_type write_version) {
    using namespace test_double::log_channel_add_entry1;
    static std::atomic<orig_type> orig_ptr{nullptr};
    if (!orig_ptr) {
        orig_ptr = reinterpret_cast<orig_type>(dlsym(RTLD_NEXT, sym_name));
    }
    LOG_IF(FATAL, !orig_ptr) << "dlsym " << sym_name << " failed";
    if (hook_func) {
        return hook_func(orig_ptr.load(), this, storage_id, key, value, write_version);
    } else {
        return orig_ptr.load()(this, storage_id, key, value, write_version);
    }
}

void log_channel::add_entry(storage_id_type storage_id, std::string_view key, std::string_view value, write_version_type write_version, const std::vector<blob_id_type>& large_objects) {
    using namespace test_double::log_channel_add_entry2;
    static std::atomic<orig_type> orig_ptr{nullptr};
    if (!orig_ptr) {
        orig_ptr = reinterpret_cast<orig_type>(dlsym(RTLD_NEXT, sym_name));
    }
    LOG_IF(FATAL, !orig_ptr) << "dlsym " << sym_name << " failed";
    if (hook_func) {
        return hook_func(orig_ptr.load(), this, storage_id, key, value, write_version, large_objects);
    } else {
        return orig_ptr.load()(this, storage_id, key, value, write_version, large_objects);
    }
}

void datastore::switch_available_boundary_version(write_version_type version) {
    using namespace test_double::datastore_switch_available_boundary_version;
    static std::atomic<orig_type> orig_ptr{nullptr};
    if (!orig_ptr) {
        orig_ptr = reinterpret_cast<orig_type>(dlsym(RTLD_NEXT, sym_name));
    }
    LOG_IF(FATAL, !orig_ptr) << "dlsym " << sym_name << " failed";
    if (hook_func) {
        return hook_func(orig_ptr.load(), this, version);
    } else {
        return orig_ptr.load()(this, version);
    }
}

}

#else

extern "C" {

void _ZN9limestone3api11log_channel9add_entryEmSt17basic_string_viewIcSt11char_traitsIcEES5_NS0_18write_version_typeE(limestone::api::log_channel* this_ptr, limestone::api::storage_id_type storage_id, std::string_view key, std::string_view value, limestone::api::write_version_type write_version) {
    using namespace test_double::log_channel_add_entry1;
    static std::atomic<orig_type> orig_ptr{nullptr};
    if (!orig_ptr) {
        orig_ptr = reinterpret_cast<orig_type>(dlsym(RTLD_NEXT, sym_name));
    }
    LOG_IF(FATAL, !orig_ptr) << "dlsym " << sym_name << " failed";
    if (hook_func) {
        return hook_func(orig_ptr.load(), this_ptr, storage_id, key, value, write_version);
    } else {
        return orig_ptr.load()(this_ptr, storage_id, key, value, write_version);
    }
}

void _ZN9limestone3api11log_channel9add_entryEmSt17basic_string_viewIcSt11char_traitsIcEES5_NS0_18write_version_typeERKSt6vectorImSaImEE(limestone::api::log_channel* this_ptr, limestone::api::storage_id_type storage_id, std::string_view key, std::string_view value, limestone::api::write_version_type write_version, const std::vector<limestone::api::blob_id_type>& large_objects) {
    using namespace test_double::log_channel_add_entry2;
    static std::atomic<orig_type> orig_ptr{nullptr};
    if (!orig_ptr) {
        orig_ptr = reinterpret_cast<orig_type>(dlsym(RTLD_NEXT, sym_name));
    }
    LOG_IF(FATAL, !orig_ptr) << "dlsym " << sym_name << " failed";
    if (hook_func) {
        return hook_func(orig_ptr.load(), this_ptr, storage_id, key, value, write_version, large_objects);
    } else {
        return orig_ptr.load()(this_ptr, storage_id, key, value, write_version, large_objects);
    }
}

void _ZN9limestone3api9datastore33switch_available_boundary_versionENS0_18write_version_typeE(limestone::api::datastore* this_ptr, limestone::api::write_version_type version) {
    using namespace test_double::datastore_switch_available_boundary_version;
    static std::atomic<orig_type> orig_ptr{nullptr};
    if (!orig_ptr) {
        orig_ptr = reinterpret_cast<orig_type>(dlsym(RTLD_NEXT, sym_name));
    }
    LOG_IF(FATAL, !orig_ptr) << "dlsym " << sym_name << " failed";
    if (hook_func) {
        return hook_func(orig_ptr.load(), this_ptr, version);
    } else {
        return orig_ptr.load()(this_ptr, version);
    }
}

}
#endif

#undef EXTERN_C
