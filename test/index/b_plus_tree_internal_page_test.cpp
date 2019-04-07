/**
 * b_plus_tree_internal_page_test.cpp
 */

#include <cstdio>

#include "gtest/gtest.h"
#include "buffer/buffer_pool_manager.h"
#include "common/logger.h"
#include "common/config.h"
#include "page/b_plus_tree_internal_page.h"
#include "vtable/virtual_table.h"

namespace cmudb {

TEST(BPlusInternalPageTest, test) {
    DiskManager *disk_manager = new DiskManager("test.db");
    BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);

    Schema *key_schema = ParseCreateStatement("a bigint");
    GenericComparator<8> comparator(key_schema);
    GenericKey<8> index_key;

    page_id_t root_page_id;
    Page *root_page = bpm->NewPage(root_page_id);
    page_id_t p0, p1, p2, p3, p4;
    bpm->NewPage(p0);
    bpm->NewPage(p1);
    bpm->NewPage(p2);
    bpm->NewPage(p3);
    bpm->NewPage(p4);

    BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>> *ip = reinterpret_cast<BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>> *>(root_page->GetData());
    ip->Init(root_page_id);
    ip->SetMaxSize(4);
    index_key.SetFromInteger(1);
    ip->PopulateNewRoot(p0, index_key, p1);
    EXPECT_EQ(2, ip->GetSize());
    EXPECT_EQ(p0, ip->ValueAt(0));
    EXPECT_EQ(p1, ip->ValueAt(1));

    // 当前数据:[<invalid, p0>, <1, p1>]，测试InsertNodeAfter()
    index_key.SetFromInteger(3);
    ip->InsertNodeAfter(p1, index_key, p3);
    index_key.SetFromInteger(2);
    ip->InsertNodeAfter(p1, index_key, p2);
    EXPECT_EQ(4, ip->GetSize());
    EXPECT_EQ(p0, ip->ValueAt(0));
    EXPECT_EQ(p1, ip->ValueAt(1));
    EXPECT_EQ(p2, ip->ValueAt(2));
    EXPECT_EQ(p3, ip->ValueAt(3));
    // 当前数据:[<invalid, p0>, <1, p1>, <2, p2>, <3, p3>]

    // 测试Lookup()
    index_key.SetFromInteger(0);
    EXPECT_EQ(p0, ip->Lookup(index_key, comparator));
    index_key.SetFromInteger(1);
    EXPECT_EQ(p1, ip->Lookup(index_key, comparator));
    index_key.SetFromInteger(20);
    EXPECT_EQ(p3, ip->Lookup(index_key, comparator));

    // 测试MoveHalfTo()，分裂后ip指向的数据:[<invalid, p0>, <1, p1>, <2, p2>]
    // new_ip指向的数据:[<3, p3>, <4, p4>]
    index_key.SetFromInteger(4);
    ip->InsertNodeAfter(p3, index_key, p4);
    page_id_t new_page_id;
    Page *new_page = bpm->NewPage(new_page_id);
    BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>> *new_ip = reinterpret_cast<BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>> *>(new_page->GetData());
    ip->MoveHalfTo(new_ip, bpm);
    EXPECT_EQ(3, ip->GetSize());
    EXPECT_EQ(2, new_ip->GetSize());
    index_key.SetFromInteger(3);
    EXPECT_EQ(index_key, new_ip->KeyAt(0));
}

}
