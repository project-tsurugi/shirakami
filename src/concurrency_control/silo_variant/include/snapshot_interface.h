/**
 * @file snapshot_interface.h
 */

#include "shirakami/scheme.h"

#include "record.h"
#include "session_info.h"

namespace shirakami::snapshot_interface {

/**
 * @pre This func is called by search_key.
 * @param[in] ti
 * @param[in] storage 
 * @param[in] key
 * @param[out] ret_tuple
 * @return
 */
extern Status lookup_snapshot(session_info* ti, Storage storage, std::string_view key, Tuple** ret_tuple); // NOLINT

extern Status open_scan(session_info* ti, Storage storage, std::string_view l_key, scan_endpoint l_end, std::string_view r_key, // NOLINT
                        scan_endpoint r_end, ScanHandle &handle);

extern Status read_from_scan(session_info* ti, ScanHandle handle, Tuple** tuple); // NOLINT

extern Status read_record(session_info* ti, Record* rec_ptr, Tuple** tuple); // NOLINT

extern Status scan_key(session_info* ti, Storage storage, std::string_view l_key, scan_endpoint l_end, std::string_view r_key, // NOLINT
                       scan_endpoint r_end, std::vector<const Tuple*> &result);

} // namespace shirakami::cc_silo_variant::snapshot_interface