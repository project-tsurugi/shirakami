# Copyright 2019-2019 tsurugi project.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

if (BUILD_ONLY_WD_BENCH)
  add_subdirectory(wd)
else ()
  add_subdirectory(bcc_2b)
  add_subdirectory(bcc_3b)
  add_subdirectory(bcc_3b_st)
  add_subdirectory(map)
  add_subdirectory(rocksdb)
  add_subdirectory(ycsb)
  add_subdirectory(ycsb_mb_nc)
  add_subdirectory(ycsb_ol_bt_nc)
  add_subdirectory(ycsb_strand)
  add_subdirectory(bcc_4)
  add_subdirectory(bcc_5)
  add_subdirectory(bcc_6)
  add_subdirectory(bcc_7)
  add_subdirectory(bcc_8)
  add_subdirectory(bcc_9)
  add_subdirectory(bcc_10)
  add_subdirectory(bcc_11)
endif ()
