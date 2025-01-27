#pragma once

#include "limestone/api/write_version_type.h"
#include "limestone/api/datastore.h"

namespace test_double {

// MEMBER_FUNC_DEF
//   nickname log_channel_add_entry1
//   cppname  void limestone::api::log_channel::add_entry(limestone::api::storage_id_type storage_id, std::string_view key, std::string_view value, limestone::api::write_version_type write_version)
//   cname    _ZN9limestone3api11log_channel9add_entryEmSt17basic_string_viewIcSt11char_traitsIcEES5_NS0_18write_version_typeE
namespace log_channel_add_entry1 {
    using orig_type = void(*)(limestone::api::log_channel*, limestone::api::storage_id_type storage_id, std::string_view key, std::string_view value, limestone::api::write_version_type write_version);
    using hook_type = std::function<void(orig_type orig_func, limestone::api::log_channel*, limestone::api::storage_id_type storage_id, std::string_view key, std::string_view value, limestone::api::write_version_type write_version)>;
    extern hook_type hook_func;
    constexpr const char sym_name[] = "_ZN9limestone3api11log_channel9add_entryEmSt17basic_string_viewIcSt11char_traitsIcEES5_NS0_18write_version_typeE";
}

// MEMBER_FUNC_DEF
//   nickname log_channel_add_entry2
//   cppname  void limestone::api::log_channel::add_entry(limestone::api::storage_id_type storage_id, std::string_view key, std::string_view value, limestone::api::write_version_type write_version, const std::vector<limestone::api::blob_id_type>& large_objects)
//   cname    _ZN9limestone3api11log_channel9add_entryEmSt17basic_string_viewIcSt11char_traitsIcEES5_NS0_18write_version_typeERKSt6vectorImSaImEE
namespace log_channel_add_entry2 {
    using orig_type = void(*)(limestone::api::log_channel*, limestone::api::storage_id_type storage_id, std::string_view key, std::string_view value, limestone::api::write_version_type write_version, const std::vector<limestone::api::blob_id_type>& large_objects);
    using hook_type = std::function<void(orig_type orig_func, limestone::api::log_channel*, limestone::api::storage_id_type storage_id, std::string_view key, std::string_view value, limestone::api::write_version_type write_version, const std::vector<limestone::api::blob_id_type>& large_objects)>;
    extern hook_type hook_func;
    constexpr const char sym_name[] = "_ZN9limestone3api11log_channel9add_entryEmSt17basic_string_viewIcSt11char_traitsIcEES5_NS0_18write_version_typeERKSt6vectorImSaImEE";
}

// MEMBER_FUNC_DEF
//   nickname datastore_switch_available_boundary_version
//   cppname  void limestone::api::datastore::switch_available_boundary_version(limestone::api::write_version_type version)
//   cname    _ZN9limestone3api9datastore33switch_available_boundary_versionENS0_18write_version_typeE
namespace datastore_switch_available_boundary_version {
    using orig_type = void(*)(limestone::api::datastore*, limestone::api::write_version_type write_version);
    using hook_type = std::function<void(orig_type orig_func, limestone::api::datastore*, limestone::api::write_version_type write_version)>;
    extern hook_type hook_func;
    constexpr const char sym_name[] = "_ZN9limestone3api9datastore33switch_available_boundary_versionENS0_18write_version_typeE";
}

}
