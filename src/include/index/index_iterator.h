/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "page/b_plus_tree_leaf_page.h"

namespace cmudb {

#define INDEXITERATOR_TYPE                                                     \
  IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
public:
  // you may define your own constructor based on your member variables
  IndexIterator(B_PLUS_TREE_LEAF_PAGE_TYPE *leaf, int index, BufferPoolManager *bpm);
  ~IndexIterator();

  bool isEnd() {
    return (leaf_ == nullptr) || (index_ >= leaf_->GetSize());
  };

  // isEnd()为true的情况下调用，未定义
  const MappingType &operator*() {
    return leaf_->GetItem(index_);
  };

  // isEnd()为true的情况下调用，未定义
  IndexIterator &operator++();

private:
  // add your own private member variables here
  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_;
  int index_;
  BufferPoolManager *bmp_;
};

} // namespace cmudb
