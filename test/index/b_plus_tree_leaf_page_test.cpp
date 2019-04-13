/**
 * b_plus_tree_leaf_page_test.cpp
 */

#include <cstdio>

#include "gtest/gtest.h"
#include "buffer/buffer_pool_manager.h"
#include "common/logger.h"
#include "common/config.h"
#include "page/b_plus_tree_leaf_page.h"
#include "vtable/virtual_table.h"


namespace cmudb {

void setKeyValue(int64_t k, GenericKey<8> &index_key, RID &rid) {
    index_key.SetFromInteger(k);
    int64_t value = k & 0xFFFFFFFF;
    rid.Set((int32_t)(k >> 32), value);
}

TEST(BPlusLeafPageTest, test) {
    char *leaf_ptr = new char[300];
    Schema *key_schema = ParseCreateStatement("a bigint");
    GenericComparator<8> comparator(key_schema);

    GenericKey<8> index_key;
    RID rid;

    BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>> *leaf = reinterpret_cast<BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>> *>(leaf_ptr);
    leaf->Init(1);
    leaf->SetMaxSize(4);

    // 测试Insert(), KeyIndex()
    index_key.SetFromInteger(3);
    EXPECT_EQ(0, leaf->KeyIndex(index_key, comparator));

    setKeyValue(1, index_key, rid);
    leaf->Insert(index_key, rid, comparator);
    EXPECT_EQ(0, leaf->KeyIndex(index_key, comparator));
    index_key.SetFromInteger(100);
    EXPECT_EQ(1, leaf->KeyIndex(index_key, comparator));

    setKeyValue(2, index_key, rid);
    leaf->Insert(index_key, rid, comparator);
    setKeyValue(3, index_key, rid);
    leaf->Insert(index_key, rid, comparator);
    setKeyValue(4, index_key, rid);
    leaf->Insert(index_key, rid, comparator);
    EXPECT_EQ(4, leaf->GetSize());
    index_key.SetFromInteger(2);
    EXPECT_EQ(1, leaf->KeyIndex(index_key, comparator));
    index_key.SetFromInteger(4);
    EXPECT_EQ(3, leaf->KeyIndex(index_key, comparator));
    index_key.SetFromInteger(100);
    EXPECT_EQ(4, leaf->KeyIndex(index_key, comparator));

    // maxSize为4，最多可以容纳5个元素，测试MoveHalfTo()
    setKeyValue(5, index_key, rid);
    leaf->Insert(index_key, rid, comparator);
    char *new_leaf_ptr = new char[300];
    BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>> *new_leaf = reinterpret_cast<BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>> *>(new_leaf_ptr);
    new_leaf->Init(2);
    new_leaf->SetMaxSize(4);
    leaf->MoveHalfTo(new_leaf, nullptr);
    EXPECT_EQ(3, leaf->GetSize());
    EXPECT_EQ(2, new_leaf->GetSize());
    EXPECT_EQ(2, leaf->GetNextPageId());

    // 测试Lookup(), 当前leaf:[(1, 1), (2, 2), (3, 3)], new_leaf:[(4, 4), (5, 5)]
    RID value;
    setKeyValue(2, index_key, rid);
    EXPECT_TRUE(leaf->Lookup(index_key, value, comparator));
    EXPECT_EQ(rid, value);
    index_key.SetFromInteger(6);
    EXPECT_FALSE(leaf->Lookup(index_key, value, comparator));

    // 测试RemoveAndDeleteRecord()
    index_key.SetFromInteger(100);
    EXPECT_EQ(3, leaf->RemoveAndDeleteRecord(index_key, comparator));
    index_key.SetFromInteger(2);
    EXPECT_EQ(2, leaf->RemoveAndDeleteRecord(index_key, comparator));

    index_key.SetFromInteger(1);
    EXPECT_EQ(1, leaf->RemoveAndDeleteRecord(index_key, comparator));
    EXPECT_EQ(1, leaf->GetSize());
    EXPECT_EQ(2, new_leaf->GetSize());

    // 测试MoveAllTo(), 当前leaf:[(3, 3)], new_leaf:[(4, 4), (5, 5)]
    new_leaf->MoveAllTo(leaf, 0, nullptr);
    EXPECT_EQ(0, new_leaf->GetSize());
    EXPECT_EQ(3, leaf->GetSize());

    delete []leaf_ptr;
    delete []new_leaf_ptr;
}

}
