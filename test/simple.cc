/*
 * Copyright 2019-2019 tsurugi project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "gtest/gtest.h"

#include "kvs/interface.h"

using namespace kvs;

namespace kvs_charkey::testing {

class SimpleTest : public ::testing::Test {
};

TEST_F(SimpleTest, scan) {
    kvs::init();
    {
        std::string k("aaa");
        std::string k2("aab");
        std::string v("bbb");
        Token s{};
        Storage st{};
        kvs::enter(s);
        ASSERT_EQ(Status::OK, insert(s, st, k.data(), k.size(), v.data(), v.size()));
        ASSERT_EQ(Status::OK, commit(s));
        std::vector<Tuple*> records{};
        ASSERT_EQ(Status::OK, scan_key(s, st, k.data(), k.size(), false, k2.data(), k2.size(), false, records));
        ASSERT_EQ(1, records.size());
        ASSERT_EQ(Status::OK, commit(s));

        delete_record(s, st, k.data(), k.size());
        ASSERT_EQ(Status::OK, commit(s));

        std::vector<Tuple*> records2{};
        ASSERT_EQ(Status::OK, scan_key(s, st, k.data(), k.size(), false, k2.data(), k2.size(), false, records2));
        ASSERT_EQ(0, records2.size());
        ASSERT_EQ(Status::OK, commit(s));
        ASSERT_EQ(Status::OK, leave(s));
    }
    {
        std::string k("aaa");
        std::string k2("aab");
        std::string v("bbb");
        Token s{};
        Storage st{};
        kvs::enter(s);
        ASSERT_EQ(Status::OK, insert(s, st, k.data(), k.size(), v.data(), v.size()));
        ASSERT_EQ(Status::OK, commit(s));
        std::vector<Tuple*> records{};
        ASSERT_EQ(Status::OK,scan_key(s, st, k.data(), k.size(), false, k2.data(), k2.size(), false, records));
        ASSERT_EQ(1, records.size());
        ASSERT_EQ(Status::OK,commit(s));
        delete_record(s, st, k.data(), k.size());
        ASSERT_EQ(Status::OK,commit(s));

        std::vector<Tuple*> records2{};
        scan_key(s, st, k.data(), k.size(), false, k2.data(), k2.size(), false, records2);
        ASSERT_EQ(0, records2.size());
        ASSERT_EQ(Status::OK,commit(s));
        ASSERT_EQ(Status::OK,leave(s));
    }
}

TEST_F(SimpleTest, search) {
    kvs::init();
    {
        std::string k("aaa");
        std::string k2("aab");
        std::string v("bbb");
        Token s{};
        Storage st{};
        ASSERT_EQ(Status::OK,kvs::enter(s));
        ASSERT_EQ(Status::OK,insert(s, st, k.data(), k.size(), v.data(), v.size()));
        ASSERT_EQ(Status::OK,commit(s));
        Tuple* tuple{};
        ASSERT_EQ(Status::OK,search_key(s, st, k.data(), k.size(), &tuple));
        ASSERT_NE(nullptr, tuple);
        ASSERT_EQ(Status::OK,commit(s));
        ASSERT_EQ(Status::OK,delete_record(s, st, k.data(), k.size()));
        ASSERT_EQ(Status::OK,commit(s));
        Tuple* tuple2{};
        ASSERT_EQ(Status::ERR_NOT_FOUND, search_key(s, st, k.data(), k.size(), &tuple2));
        ASSERT_EQ(nullptr, tuple2);
        ASSERT_EQ(Status::OK, commit(s));
        ASSERT_EQ(Status::OK,leave(s));
    }
    {
        std::string k("aaa");
        std::string k2("aab");
        std::string v("bbb");
        Storage st{};
        Token s{};
        ASSERT_EQ(Status::OK,kvs::enter(s));
        ASSERT_EQ(Status::OK,insert(s, st, k.data(), k.size(), v.data(), v.size()));
        ASSERT_EQ(Status::OK,commit(s));
        Tuple* tuple{};
        ASSERT_EQ(Status::OK,search_key(s, st, k.data(), k.size(), &tuple));
        ASSERT_NE(nullptr, tuple);
        ASSERT_EQ(Status::OK,delete_record(s, st, k.data(), k.size()));
        ASSERT_EQ(Status::OK,commit(s));
        ASSERT_EQ(Status::OK,commit(s));

        Tuple* tuple2{};
        ASSERT_EQ(Status::ERR_NOT_FOUND,search_key(s, st, k.data(), k.size(), &tuple));
        ASSERT_EQ(nullptr, tuple2);
        ASSERT_EQ(Status::OK,commit(s));
        ASSERT_EQ(Status::OK,leave(s));
    }
}

TEST_F(SimpleTest, iud) {
    kvs::init();
    {
        std::string k("aaa");
        std::string k2("aab");
        std::string v("bbb");
        std::string v2("ccc");
        Storage st{};
        Token s{};
        ASSERT_EQ(Status::OK,kvs::enter(s));
        ASSERT_EQ(Status::OK,insert(s, st, k.data(), k.size(), v.data(), v.size()));
        ASSERT_EQ(Status::OK,commit(s));
        {
            Tuple* tuple{};
            ASSERT_EQ(Status::OK,search_key(s, st, k.data(), k.size(), &tuple));
            ASSERT_NE(nullptr, tuple);
            {
                std::string key{tuple->key.get(), tuple->len_key};
                std::string val{tuple->val.get(), tuple->len_val};
                ASSERT_EQ(key, k);
                ASSERT_EQ(val, v);
            }
        }
        ASSERT_EQ(Status::OK,commit(s));
        ASSERT_EQ(Status::OK,update(s, st, k.data(), k.size(), v2.data(), v2.size()));
        ASSERT_EQ(Status::OK,commit(s));
        {
            Tuple* tuple{};
            ASSERT_EQ(Status::OK,search_key(s, st, k.data(), k.size(), &tuple));
            ASSERT_NE(nullptr, tuple);
            {
                std::string key{tuple->key.get(), tuple->len_key};
                std::string val{tuple->val.get(), tuple->len_val};
                ASSERT_EQ(key, k);
                ASSERT_EQ(val, v2);
            }
        }
        ASSERT_EQ(Status::OK,commit(s));
        ASSERT_EQ(Status::OK,delete_record(s, st, k.data(), k.size()));
        ASSERT_EQ(Status::OK,commit(s));
        {
            Tuple* tuple{};
            ASSERT_EQ(Status::ERR_NOT_FOUND,search_key(s, st, k.data(), k.size(), &tuple));
            ASSERT_EQ(nullptr, tuple);
        }
        ASSERT_EQ(Status::OK,upsert(s, st, k.data(), k.size(), v.data(), v.size()));
        ASSERT_EQ(Status::OK,commit(s));
        {
            Tuple* tuple{};
            ASSERT_EQ(Status::OK,search_key(s, st, k.data(), k.size(), &tuple));
            ASSERT_NE(nullptr, tuple);
            {
                std::string key{tuple->key.get(), tuple->len_key};
                std::string val{tuple->val.get(), tuple->len_val};
                ASSERT_EQ(key, k);
                ASSERT_EQ(val, v);
            }
        }
        ASSERT_EQ(Status::OK,upsert(s, st, k.data(), k.size(), v2.data(), v2.size()));
        ASSERT_EQ(Status::OK, commit(s));
        {
            Tuple* tuple{};
            ASSERT_EQ(Status::OK,search_key(s, st, k.data(), k.size(), &tuple));
            ASSERT_NE(nullptr, tuple);
            {
                std::string key{tuple->key.get(), tuple->len_key};
                std::string val{tuple->val.get(), tuple->len_val};
                ASSERT_EQ(key, k);
                ASSERT_EQ(val, v2);
            }
        }
        ASSERT_EQ(Status::OK,delete_record(s, st, k.data(), k.size()));
        ASSERT_EQ(Status::OK,commit(s));

        Tuple* tuple2{};
        ASSERT_EQ(Status::ERR_NOT_FOUND,search_key(s, st, k.data(), k.size(), &tuple2));
        ASSERT_EQ(nullptr, tuple2);
        ASSERT_EQ(Status::OK,commit(s));
        ASSERT_EQ(Status::OK,leave(s));
    }
}

TEST_F(SimpleTest, scan_with_long_key) {
    kvs::init();
    {
        std::string k("aaa");
        std::string scankey(255, 'a');
        std::string scankey2(scankey);
        scankey2[254] = 'b';
        std::string v("bbb");
        Token s{};
        Storage st{};
        ASSERT_EQ(Status::OK,kvs::enter(s));
        ASSERT_EQ(Status::OK,insert(s, st, k.data(), k.size(), v.data(), v.size()));
        ASSERT_EQ(Status::OK,commit(s));
        std::vector<Tuple*> records{};
        ASSERT_EQ(Status::OK,scan_key(s, st, scankey.data(), scankey.size(), false, scankey2.data(), scankey2.size(), false, records));
        ASSERT_EQ(0, records.size());
        ASSERT_EQ(Status::OK,commit(s));

        Tuple* tuple{};
        ASSERT_EQ(Status::ERR_NOT_FOUND,search_key(s, st, scankey.data(), scankey.size(), &tuple));
        ASSERT_EQ(nullptr, tuple);
        ASSERT_EQ(Status::OK,commit(s));
        Tuple* tuple2{};
        ASSERT_EQ(Status::OK,search_key(s, st, k.data(), k.size(), &tuple2));
        ASSERT_NE(nullptr, tuple2);
        ASSERT_EQ(Status::OK,commit(s));
        ASSERT_EQ(Status::OK,leave(s));
    }
}
TEST_F(SimpleTest, delete_missing) {
    kvs::init();
    {
        std::string k("aaa");
        Token s{};
        Storage st{};
        kvs::enter(s);
        ASSERT_EQ(Status::OK, delete_record(s, st, k.data(), k.size()));
        ASSERT_EQ(Status::OK, commit(s));
    }
}

}  // namespace kvs_charkey::testing
