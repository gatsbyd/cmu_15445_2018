/**
 * LRU implementation
 */
#include "buffer/lru_replacer.h"
#include "page/page.h"

namespace cmudb {

template <typename T> LRUReplacer<T>::LRUReplacer() {}

template <typename T> LRUReplacer<T>::~LRUReplacer() {}

template <typename T> void LRUReplacer<T>::insertAtHead(std::shared_ptr<DLinkedNode> node) {
    if (node == nullptr) {
        return;
    }
    node->pre = nullptr;
    node->next = head;

    if (head != nullptr) {
        head->pre = node;
    }
    head = node;
    if (tail == nullptr) {
        tail = node;
    }
}

/*
 * Insert value into LRU
 */
template <typename T> void LRUReplacer<T>::Insert(const T &value) {
    Erase(value);
    auto newNode = std::make_shared<DLinkedNode>(value);
    insertAtHead(newNode);
    index[value] = newNode;
    size++;
}

/* If LRU is non-empty, pop the head member from LRU to argument "value", and
 * return true. If LRU is empty, return false
 */
template <typename T> bool LRUReplacer<T>::Victim(T &value) {
    if (size == 0) {
        return false;
    }
    if (head == tail) {
        value = head->value;
        return true;
    }
    value = tail->value;
    auto discard = tail;
    discard->pre->next = nullptr;
    tail = discard->pre;
    discard->pre = nullptr;

    index.erase(value);
    size--;
    return true;
}

/*
 * Remove value from LRU. If removal is successful, return true, otherwise
 * return false
 */
template <typename T> bool LRUReplacer<T>::Erase(const T &value) {
    auto iter = index.find(value);
    if (iter == index.end()) {
        return false;
    }

    auto ptr = iter->second;
    if (ptr == head && ptr == tail) {
        head = nullptr;
        tail = nullptr;
    } else if (ptr == head) {
        ptr->next->pre = nullptr;
        head = ptr->next;
    } else if (ptr == tail) {
        ptr->pre->next = nullptr;
        tail = ptr->pre;
    } else {
        ptr->pre->next = ptr->next;
        ptr->next->pre = ptr->pre;
    }
    ptr->pre = nullptr;
    ptr->next = nullptr;

    index.erase(value);
    size--;
    return true;
}

template <typename T> size_t LRUReplacer<T>::Size() { return size; }

template class LRUReplacer<Page *>;
// test only
template class LRUReplacer<int>;

} // namespace cmudb
