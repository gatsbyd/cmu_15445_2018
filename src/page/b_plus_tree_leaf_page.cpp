/**
 * b_plus_tree_leaf_page.cpp
 */

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "page/b_plus_tree_leaf_page.h"
#include "page/b_plus_tree_internal_page.h"
#include "common/logger.h"

namespace cmudb {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id) {
    SetPageType(IndexPageType::LEAF_PAGE);
    SetSize(0);
    assert(sizeof(BPlusTreeLeafPage) == 28);

    // 最后一个单独留出来分裂的时候用
    int max_size = (PAGE_SIZE - sizeof(BPlusTreeLeafPage)) / sizeof(MappingType) - 1;
    SetMaxSize(max_size);
    SetPageId(page_id);
    SetParentPageId(parent_id);
    SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const {
  return next_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {
    next_page_id_ = next_page_id;
}

/**
 * Helper method to find the first index i so that array[i].first >= key
 * NOTE: This method is only used when generating index iterator
 * 从最简单的情况分析：
 * 空数组:[]，key:3, 应该返回0,
 * 数组:[1], key:0，应该返回0,
 * 数组:[1], key:2，应该返回1.
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(
    const KeyType &key, const KeyComparator &comparator) const {
    int left = 0;
    int right = GetSize() - 1;
    int mid;
    int compareResult;
    while (left <= right) {
        mid = left + (right - left) / 2;  //(left + right) / 2;有溢出风险
        compareResult = comparator(array[mid].first, key);
        if (compareResult == 0) {
            return mid;
        } else if (compareResult < 0) {
            left = mid + 1;
        } else {
            right = mid - 1;
        }
    }
    return left;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  assert(index >= 0 && index < GetSize());
  return array[index].first;
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) const {
  // replace with your own code
  assert(index >= 0 && index < GetSize());
  return array[index];
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return  page size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key,
                                       const ValueType &value,
                                       const KeyComparator &comparator) {
    assert(GetSize() < GetMaxSize() + 1);
    int targetIndex = KeyIndex(key, comparator);

    for (int i = GetSize() - 1; i >= targetIndex; i--) {
        array[i + 1].first = array[i].first;
        array[i + 1].second = array[i].second;
    }

    array[targetIndex].first = key;
    array[targetIndex].second = value;
    IncreaseSize(1);
    return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(
    BPlusTreeLeafPage *recipient,
    __attribute__((unused)) BufferPoolManager *buffer_pool_manager) {
    assert(recipient != nullptr);
    assert(GetSize() == GetMaxSize() + 1);

    // 维护next_page_id_
    recipient->SetNextPageId(GetNextPageId());
    SetNextPageId(recipient->GetPageId());

    // 拷贝
    int lastIndex = GetSize() - 1;
    int copyStartIndex = lastIndex / 2 + 1;
    int i = 0;
    int j = copyStartIndex;
    while (j <= lastIndex) {
        recipient->array[i].first = array[j].first;
        recipient->array[i].second = array[j].second;
        i++;
        j++;
    }

    // 重新设置大小
    SetSize(copyStartIndex);
    recipient->SetSize(lastIndex - copyStartIndex + 1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyHalfFrom(MappingType *items, int size) {
    // 没用到
    assert(false);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType &value,
                                        const KeyComparator &comparator) const {
    int index = KeyIndex(key, comparator);
    if (GetSize() > 0 && index < GetSize() && comparator(key, GetItem(index).first) == 0) {
        value = GetItem(index).second;
        return true;
    }
    return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immdiately.
 * NOTE: store key&value pair continuously after deletion
 * @return   page size after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(
    const KeyType &key, const KeyComparator &comparator) {
    int index = KeyIndex(key, comparator);
    if (GetSize() > 0 && index < GetSize() && comparator(key, GetItem(index).first) == 0) {
        memmove(array + index, array + index + 1,
                static_cast<size_t>(GetSize() - 1 - index) * sizeof(MappingType));
        IncreaseSize(-1);
    }
    return GetSize();
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page, then
 * update next page id
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient,
                                           int, BufferPoolManager *) {
    assert(GetSize() + recipient->GetSize() <= GetMaxSize());
    assert(GetParentPageId() == recipient->GetParentPageId());
    assert(recipient->GetNextPageId() == GetPageId());

    recipient->CopyAllFrom(array, GetSize());
    IncreaseSize(-1 * GetSize());
    recipient->SetNextPageId(GetNextPageId());
    SetNextPageId(INVALID_PAGE_ID);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyAllFrom(MappingType *items, int size) {
    memmove(array + GetSize(), items, static_cast<size_t>(sizeof(MappingType) * size));
    IncreaseSize(size);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(
    BPlusTreeLeafPage *recipient,
    BufferPoolManager *buffer_pool_manager) {
    assert(GetParentPageId() == recipient->GetParentPageId());
    assert(recipient->GetNextPageId() == GetPageId());

    MappingType first = GetItem(0);
    recipient->CopyLastFrom(first);

    memmove(array, array + 1, static_cast<size_t>(GetSize() - 1) * sizeof(MappingType));
    IncreaseSize(-1);

    Page *page = buffer_pool_manager->FetchPage(GetParentPageId());
    if (page == nullptr) {
        throw std::bad_alloc();
    }
    BPInternalPage *parent_page = reinterpret_cast<BPInternalPage *>(page->GetData());

    parent_page->SetKeyAt(parent_page->ValueIndex(GetPageId()), GetItem(0).first);

    buffer_pool_manager->UnpinPage(GetParentPageId(), true);
    buffer_pool_manager->UnpinPage(GetPageId(), true);
    buffer_pool_manager->UnpinPage(recipient->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {
    array[GetSize()] = item;
    IncreaseSize(1);
}
/*
 * Remove the last key & value pair from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(
    BPlusTreeLeafPage *recipient, int parentIndex,
    BufferPoolManager *buffer_pool_manager) {
    assert(GetParentPageId() == recipient->GetParentPageId());
    assert(GetNextPageId() == recipient->GetPageId());

    MappingType last = GetItem(GetSize() - 1);
    IncreaseSize(-1);
    recipient->CopyFirstFrom(last, parentIndex, buffer_pool_manager);

    buffer_pool_manager->UnpinPage(GetPageId(), true);
    buffer_pool_manager->UnpinPage(recipient->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(
    const MappingType &item, int parentIndex,
    BufferPoolManager *buffer_pool_manager) {

    memmove(array + 1, array, static_cast<size_t>(GetSize() * sizeof(MappingType)));
    array[0] = item;
    IncreaseSize(1);

    Page *page = buffer_pool_manager->FetchPage(GetParentPageId());
    if (page == nullptr) {
        throw std::bad_alloc();
    }
    BPInternalPage *parent_page = reinterpret_cast<BPInternalPage *>(page->GetData());
    parent_page->SetKeyAt(parentIndex, item.first);

    buffer_pool_manager->UnpinPage(GetParentPageId(), true);
}

/*****************************************************************************
 * DEBUG
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
std::string B_PLUS_TREE_LEAF_PAGE_TYPE::ToString(bool verbose) const {
  if (GetSize() == 0) {
    return "";
  }
  std::ostringstream stream;
  if (verbose) {
    stream << "[pageId: " << GetPageId() << " parentId: " << GetParentPageId()
           << "]<" << GetSize() << "> ";
  }
  int entry = 0;
  int end = GetSize();
  bool first = true;

  while (entry < end) {
    if (first) {
      first = false;
    } else {
      stream << " ";
    }
    stream << std::dec << array[entry].first;
    if (verbose) {
      stream << "(" << array[entry].second << ")";
    }
    ++entry;
  }
  return stream.str();
}

template class BPlusTreeLeafPage<GenericKey<4>, RID,
                                       GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID,
                                       GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID,
                                       GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID,
                                       GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID,
                                       GenericComparator<64>>;


// only for test
template class BPlusTreeLeafPage<int32_t, int32_t, IntComparator>;


} // namespace cmudb
