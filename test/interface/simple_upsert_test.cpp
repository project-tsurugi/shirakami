#include "kvs/interface.h"

#include <array>
#include <bitset>

#include "gtest/gtest.h"

// shirakami-impl interface library
#include "cc/silo_variant/include/scheme.h"
#include "compiler.h"
#include "tuple_local.h"

#ifdef CC_SILO_VARIANT
using namespace shirakami::cc_silo_variant;
#endif

namespace shirakami::testing {

using namespace shirakami::cc_silo_variant;

class simple_upsert : public ::testing::Test {  // NOLINT
public:
    void SetUp() override { init(); }  // NOLINT

    void TearDown() override { fin(); }
};

TEST_F(simple_upsert, upsert
) {  // NOLINT
std::string k("aaa");           // NOLINT
std::string v("aaa");           // NOLINT
std::string v2("bbb");          // NOLINT
Token s{};
Storage st{};
Tuple* tuple{};
ASSERT_EQ(Status::OK, enter(s)
);
ASSERT_EQ(Status::OK, upsert(s, st, k.data(), k.size(), v.data(), v.size())
);
ASSERT_EQ(Status::OK, commit(s)
);
ASSERT_EQ(Status::WARN_ALREADY_EXISTS,
        insert(s, st, k.data(), k.size(), v.data(), v.size())
);
ASSERT_EQ(Status::OK, commit(s)
);
ASSERT_EQ(Status::OK,
        upsert(s, st, k.data(), k.size(), v2.data(), v2.size())
);
ASSERT_EQ(Status::OK, commit(s)
);
ASSERT_EQ(Status::OK, search_key(s, st, k.data(), k.size(), &tuple)
);
ASSERT_EQ(memcmp(tuple->get_value().data(), v2.data(), v2.size()),
0);
ASSERT_EQ(Status::OK, commit(s)
);
ASSERT_EQ(Status::OK, leave(s)
);
}

TEST_F(simple_upsert, double_upsert
) {  // NOLINT
std::string k("aaa");                 // NOLINT
std::string v("bbb");                 // NOLINT
std::string v2("ccc");                // NOLINT
std::string v3("ddd");                // NOLINT
Token s{};
ASSERT_EQ(Status::OK, enter(s)
);
Storage st{};
ASSERT_EQ(Status::OK, upsert(s, st, k.data(), k.size(), v.data(), v.size())
);
ASSERT_EQ(Status::OK, commit(s)
);
Tuple* tuple{};
ASSERT_EQ(Status::OK,
        upsert(s, st, k.data(), k.size(), v2.data(), v2.size())
);
ASSERT_EQ(Status::WARN_WRITE_TO_LOCAL_WRITE,
        upsert(s, st, k.data(), k.size(), v3.data(), v3.size())
);
ASSERT_EQ(Status::WARN_READ_FROM_OWN_OPERATION,
        search_key(s, st, k.data(), k.size(), &tuple)
);
ASSERT_EQ(memcmp(tuple->get_value().data(), v3.data(), v3.size()),
0);
ASSERT_EQ(Status::OK, commit(s)
);
ASSERT_EQ(Status::OK, leave(s)
);
}

}  // namespace shirakami::testing
