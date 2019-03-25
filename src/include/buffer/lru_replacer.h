/**
 * lru_replacer.h
 *
 * Functionality: The buffer pool manager must maintain a LRU list to collect
 * all the pages that are unpinned and ready to be swapped. The simplest way to
 * implement LRU is a FIFO queue, but remember to dequeue or enqueue pages when
 * a page changes from unpinned to pinned, or vice-versa.
 */

#pragma once

#include <map>
#include <mutex>

#include "buffer/replacer.h"
#include "hash/extendible_hash.h"

namespace cmudb {

template <typename T> class LRUReplacer : public Replacer<T> {
public:
  // do not change public interface
  LRUReplacer();

  ~LRUReplacer();

  void Insert(const T &value);

  bool Victim(T &value);

  bool Erase(const T &value);

  size_t Size();

private:
  // add your member variables here
  struct DLinkedNode {
    DLinkedNode(T v) :value(v) {
    }

    T value;
    std::shared_ptr<DLinkedNode> pre;
    std::shared_ptr<DLinkedNode> next;
  };


  void insertAtHead(std::shared_ptr<DLinkedNode> node);
  bool erase(const T &value);

  std::shared_ptr<DLinkedNode> head;
  std::shared_ptr<DLinkedNode> tail;
  int size = 0;
  std::mutex mutex;
  std::map<T, std::shared_ptr<DLinkedNode>> index;
};

} // namespace cmudb
