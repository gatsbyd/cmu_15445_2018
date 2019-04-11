/**
 * b_plus_tree_leaf_page_test.cpp
 */

#include <cstdio>

#include "gtest/gtest.h"
#include "buffer/buffer_pool_manager.h"
#include "common/logger.h"
#include "common/config.h"
#include "page/b_plus_tree_leaf_page.h"

namespace cmudb {

TEST(BPlusLeafPageTest, test) {
    char *leaf_ptr = new char[100];
    IntComparator comparator;
    BPlusTreeLeafPage<int32_t, int32_t, IntComparator> *leaf = reinterpret_cast<BPlusTreeLeafPage<int32_t, int32_t, IntComparator> *>(leaf_ptr);
    leaf->Init(1);
    leaf->SetMaxSize(4);

    // 测试Insert(), KeyIndex()
    EXPECT_EQ(0, leaf->KeyIndex(3, comparator));

    leaf->Insert(1, 1, comparator);
    EXPECT_EQ(0, leaf->KeyIndex(0, comparator));
    EXPECT_EQ(1, leaf->KeyIndex(100, comparator));

    leaf->Insert(2, 2, comparator);
    leaf->Insert(3, 3, comparator);
    leaf->Insert(4, 4, comparator);
    EXPECT_EQ(4, leaf->GetSize());
    EXPECT_EQ(1, leaf->KeyIndex(2, comparator));
    EXPECT_EQ(3, leaf->KeyIndex(4, comparator));
    EXPECT_EQ(4, leaf->KeyIndex(100, comparator));

    // maxSize为4，最多可以容纳5个元素，测试MoveHalfTo()
    leaf->Insert(5, 5, comparator);
    char *new_leaf_ptr = new char[100];
    BPlusTreeLeafPage<int32_t, int32_t, IntComparator> *new_leaf = reinterpret_cast<BPlusTreeLeafPage<int32_t, int32_t, IntComparator> *>(new_leaf_ptr);
    new_leaf->Init(2);
    new_leaf->SetMaxSize(4);
    leaf->MoveHalfTo(new_leaf, nullptr);
    EXPECT_EQ(3, leaf->GetSize());
    EXPECT_EQ(2, new_leaf->GetSize());
    EXPECT_EQ(2, leaf->GetNextPageId());

    // 测试Lookup(), 当前leaf:[(1, 1), (2, 2), (3, 3)], new_leaf:[(4, 4), (5, 5)]
    int value;
    EXPECT_TRUE(leaf->Lookup(2, value, comparator));
    EXPECT_EQ(2, value);
    EXPECT_FALSE(leaf->Lookup(6, value, comparator));

    // 测试RemoveAndDeleteRecord()
    EXPECT_EQ(3, leaf->RemoveAndDeleteRecord(100, comparator));
    EXPECT_EQ(2, leaf->RemoveAndDeleteRecord(2, comparator));
    EXPECT_EQ(1, leaf->KeyAt(0));
    EXPECT_EQ(3, leaf->KeyAt(1));
    EXPECT_EQ(1, leaf->RemoveAndDeleteRecord(1, comparator));
    EXPECT_EQ(1, leaf->GetSize());
    EXPECT_EQ(2, new_leaf->GetSize());

    // 测试MoveAllTo(), 当前leaf:[(3, 3)], new_leaf:[(4, 4), (5, 5)]
    new_leaf->MoveAllTo(leaf, 0, nullptr);
    EXPECT_EQ(0, new_leaf->GetSize());
    EXPECT_EQ(3, leaf->GetSize());
    EXPECT_EQ(3, leaf->KeyAt(0));
    EXPECT_EQ(4, leaf->KeyAt(1));
    EXPECT_EQ(5, leaf->KeyAt(2));

    delete []leaf_ptr;
    delete []new_leaf_ptr;
}

}
