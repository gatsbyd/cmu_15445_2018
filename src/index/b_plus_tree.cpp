/**
 * b_plus_tree.cpp
 */
#include <iostream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "index/b_plus_tree.h"
#include "page/header_page.h"

namespace cmudb {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(const std::string &name,
                                BufferPoolManager *buffer_pool_manager,
                                const KeyComparator &comparator,
                                page_id_t root_page_id)
    : index_name_(name), root_page_id_(root_page_id),
      buffer_pool_manager_(buffer_pool_manager), comparator_(comparator) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const {
    return root_page_id_ == INVALID_PAGE_ID;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key,
                              std::vector<ValueType> &result,
                              Transaction *transaction) {
    B_PLUS_TREE_LEAF_PAGE_TYPE *leaf = FindLeafPage(key, OperationType::GET, transaction);
    if (leaf == nullptr) {
        return false;
    }
    result.resize(1);
    auto ret = leaf->Lookup(key, result[0], comparator_);

    // 记得unpin
    if (transaction != nullptr) {
        UnLatchAndUnpinPageSet(transaction, OperationType::GET);
    } else {
        Page *page = GetPage(leaf->GetPageId(), EXCEPTION_INFO);
        page->RUnlatch();
        buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
    }
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
    return ret;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value,
                            Transaction *transaction) {
    assert(transaction != nullptr);
    root_id_mutex_.lock();
    if (IsEmpty()) {
        StartNewTree(key, value, transaction);
        root_id_mutex_.unlock();
        //LOG_DEBUG("unlock root_id_mutex");

        InsertIntoLeaf(key, value, transaction);
        return true;
    }
    root_id_mutex_.unlock();
    //LOG_DEBUG("unlock root_id_mutex");

    bool ret = InsertIntoLeaf(key, value, transaction);
    return ret;
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value, Transaction *transaction) {
    page_id_t new_page_id;
    Page *root_page = buffer_pool_manager_->NewPage(new_page_id);
    if (root_page == nullptr) {
        throw BufferPoolManagerException(EXCEPTION_INFO);
    }
    //LOG_DEBUG("start new tree with root page id=%d\n", new_page_id);
    B_PLUS_TREE_LEAF_PAGE_TYPE *root = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(root_page->GetData());
    root->Init(new_page_id, INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(new_page_id, false);

    // 更新HeaderPage，在文件的第一页记录<索引名, root_page_id>
    root_page_id_ = new_page_id;
    UpdateRootPageId(true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value,
                                    Transaction *transaction) {
    // 找到key应该插入的叶子节点
    B_PLUS_TREE_LEAF_PAGE_TYPE *leaf = FindLeafPage(key, OperationType::INSERT, transaction);
    ValueType v;
    bool isExit = leaf->Lookup(key, v, comparator_);
    if (isExit) {
        buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
        UnLatchAndUnpinPageSet(transaction, OperationType::INSERT);
        return false;
    }

    int sz = leaf->GetSize();
    if (sz < leaf->GetMaxSize()) {
        sz = leaf->Insert(key, value, comparator_);
        buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
        //LOG_DEBUG("insert %ld in page %d, size=%d, max size=%d\n", key.ToString(), leaf->GetPageId(), sz, leaf->GetMaxSize());
        assert(sz <= leaf->GetMaxSize());
    } else {
        assert(leaf->GetSize() == leaf->GetMaxSize());

        leaf->Insert(key, value, comparator_);
        // 分裂
        //LOG_DEBUG("page %d current size=%d, max size=%d, split new page\n", leaf->GetPageId(), leaf->GetSize(), leaf->GetMaxSize());
        B_PLUS_TREE_LEAF_PAGE_TYPE *new_leaf = Split(leaf);

        // 将新节点插入父节点
        InsertIntoParent(leaf, new_leaf->KeyAt(0), new_leaf, transaction);
    }

    UnLatchAndUnpinPageSet(transaction, OperationType::INSERT);
    return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 * 调用者负责unpin
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N> N *BPLUSTREE_TYPE::Split(N *node) {
    page_id_t new_page_id;
    Page *new_page = buffer_pool_manager_->NewPage(new_page_id);
    if (new_page == nullptr) {
        throw BufferPoolManagerException(EXCEPTION_INFO);
    }

    N *new_node = reinterpret_cast<N *>(new_page->GetData());
    new_node->Init(new_page_id, node->GetParentPageId());
    node->MoveHalfTo(new_node, buffer_pool_manager_);
    return new_node;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 * 负责unpin old_node, new_node
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node,
                                      const KeyType &key,
                                      BPlusTreePage *new_node,
                                      Transaction *transaction) {
    if (old_node->IsRootPage()) {
        // 创建一个新的内部节点作为根节点

        page_id_t new_page_id;
        Page *new_page = buffer_pool_manager_->NewPage(new_page_id);
        if (new_page == nullptr) {
            throw BufferPoolManagerException(EXCEPTION_INFO);
        }
        BPInternalPage *new_root = reinterpret_cast<BPInternalPage *>(new_page->GetData());
        new_root->Init(new_page_id);
        new_root->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());

        // 维护parent指针
        old_node->SetParentPageId(new_page_id);
        new_node->SetParentPageId(new_page_id);

        // 更改headerPage
        //LOG_DEBUG("create new page %d as root\n", new_page_id);
        root_page_id_ = new_page_id;
        UpdateRootPageId(false);

        // 和delete不一样，不能依靠UnLatchAndUnpinPageSet()中的IsRoot()为true来unlock，因为新的root必定不在
        // transaction的page_set_队列中
        root_id_mutex_.unlock();
        //LOG_DEBUG("unlock root_id_mutex");

        buffer_pool_manager_->UnpinPage(new_page_id, true);
        buffer_pool_manager_->UnpinPage(old_node->GetPageId(), true);
        buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
        return;
    }
    page_id_t parent_id = old_node->GetParentPageId();
    BPInternalPage *parent_page = reinterpret_cast<BPInternalPage *>(GetPage(parent_id, EXCEPTION_INFO)->GetData());

    // 维护parent指针
    new_node->SetParentPageId(parent_id);

    if (parent_page->GetSize() < parent_page->GetMaxSize()) {
        // 父节点还没满，直接插入
        //LOG_DEBUG("internal page %d, size=%d, max size=%d, is ready to insert\n", parent_id, parent_page->GetSize(), parent_page->GetMaxSize());
        int sz = parent_page->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
        //LOG_DEBUG("insert page %d, %d into internal page %d, max size=%d, %s\n", old_node->GetPageId(), new_node->GetPageId(), parent_page->GetPageId(), parent_page->GetMaxSize(), parent_page->ToString(true).c_str());
        assert(sz <= parent_page->GetMaxSize());
        buffer_pool_manager_->UnpinPage(old_node->GetPageId(), true);
        buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
    } else {
        // 父节点也需要拆分
        // LOG_DEBUG("parent_page size=%d, parent_page max size=%d\n", parent_page->GetSize(), parent_page->GetMaxSize());
        assert(parent_page->GetSize() == parent_page->GetMaxSize());
        //LOG_DEBUG("internal page %d is full, max size=%d, %s\n", parent_page->GetPageId(), parent_page->GetMaxSize(), parent_page->ToString(true).c_str());

        parent_page->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
        buffer_pool_manager_->UnpinPage(old_node->GetPageId(), true);
        buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);

        BPInternalPage *new_page = Split(parent_page);
        assert(parent_page->GetSize() < parent_page->GetMaxSize());

        InsertIntoParent(parent_page, new_page->KeyAt(0), new_page, transaction);
    }

    buffer_pool_manager_->UnpinPage(parent_id, true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
    assert(transaction != nullptr);

    B_PLUS_TREE_LEAF_PAGE_TYPE *target_page = FindLeafPage(key, OperationType::DELETE, transaction);
    if (target_page == nullptr) {
        return;
    }
    page_id_t target_id = target_page->GetPageId();
    int size_after_delete = target_page->RemoveAndDeleteRecord(key, comparator_);
    if (size_after_delete < target_page->GetMinSize()) {
        CoalesceOrRedistribute(target_page, transaction);
    } else {
        buffer_pool_manager_->UnpinPage(target_id, true);
    }

    UnLatchAndUnpinPageSet(transaction, OperationType::DELETE);
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
    assert(node->GetSize() < node->GetMinSize());

    if (node->IsRootPage()) {
        //LOG_DEBUG("adjust root page %d", node->GetPageId());
        bool ret = AdjustRoot(node, transaction);
        return ret;
    }

    decltype(node) sibling = nullptr;
    bool isLeftSibling = FindSibling(node, sibling);

    Page *page = GetPage(node->GetParentPageId(), EXCEPTION_INFO);
    BPInternalPage *parent_page = reinterpret_cast<BPInternalPage *>(page->GetData());
    int nodeIndexInParent = parent_page->ValueIndex(node->GetPageId());

    if (node->GetSize() + sibling->GetSize() <= node->GetMaxSize()) {
        Coalesce(isLeftSibling, sibling, node, parent_page, nodeIndexInParent, transaction);
        buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
        return true;
    } else {
        Redistribute(isLeftSibling, sibling, node, nodeIndexInParent);
    }
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    return false;
}

/*
 *  如果返回true，表示找到node左侧的兄弟节点，false表示找到node右侧的兄弟节点
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::FindSibling(N *node, N * &sibling) {
    Page *page = GetPage(node->GetParentPageId(), EXCEPTION_INFO);
    BPInternalPage *parent_page = reinterpret_cast<BPInternalPage *>(page->GetData());
    int index = parent_page->ValueIndex(node->GetPageId());

    int siblingIndex;
    bool isLeftSibling;
    if (index == 0) {
        siblingIndex = index + 1;
        isLeftSibling = false;
    } else {
        siblingIndex = index - 1;
        isLeftSibling = true;
    }
    Page *sibling_page = GetPage(parent_page->ValueAt(siblingIndex), EXCEPTION_INFO);
    sibling = reinterpret_cast<N *>(sibling_page->GetData());
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), false);
    return isLeftSibling;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(
    bool isLeftSibling,
    N *&neighbor_node, N *&node,
    BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *&parent,
    int index, Transaction *transaction) {
    assert(node->GetSize() + neighbor_node->GetSize() <= node->GetMaxSize());
    page_id_t nodeId = node->GetPageId();
    page_id_t neighborId = neighbor_node->GetPageId();

    if (isLeftSibling) {
        node->MoveAllTo(neighbor_node, index, buffer_pool_manager_);

        // 在父节点中删除node对应的键值对
        buffer_pool_manager_->UnpinPage(nodeId, true);
        if (!buffer_pool_manager_->DeletePage(nodeId)) {
            throw Exception("buffer_pool_manager_ delete failed, pin_count != 0");
        }
        buffer_pool_manager_->UnpinPage(neighborId, true);
        parent->Remove(index);
    } else {
        neighbor_node->MoveAllTo(node, index, buffer_pool_manager_);

        // 在父节点中删除neighbor_node对应的键值对
        buffer_pool_manager_->UnpinPage(nodeId, true);
        buffer_pool_manager_->UnpinPage(neighborId, true);
        if (!buffer_pool_manager_->DeletePage(neighborId)) {
            throw Exception("buffer_pool_manager_ delete failed, pin_count != 0");
        }
        parent->Remove(index + 1);
    }
    assert(0 == buffer_pool_manager_->GetPagePinCount(nodeId));
    assert(0 == buffer_pool_manager_->GetPagePinCount(neighborId));

    if (parent->GetSize() < parent->GetMinSize()) {
        // 父节点删除一个键值对后也不够数，递归处理
        return CoalesceOrRedistribute(parent, transaction);
    }
    return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(bool isLeftSibling,
                                  N *neighbor_node, N *node, int index) {
    if (isLeftSibling) {
        neighbor_node->MoveLastToFrontOf(node, index, buffer_pool_manager_);
    } else {
        neighbor_node->MoveFirstToEndOf(node, buffer_pool_manager_);
    }
    buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node, Transaction *transaction) {
    if (old_root_node->IsLeafPage()) {
        assert(old_root_node->GetSize() == 0);

        DeleteRootPageId();
        root_page_id_ = INVALID_PAGE_ID;

        UnLatchAndUnpinPageSet(transaction, OperationType::DELETE);

        buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(), true);
        if (!buffer_pool_manager_->DeletePage(old_root_node->GetPageId())) {
            throw Exception("buffer_pool_manager_ delete failed, pin_count != 0");
        }
        return true;
    }

    assert(old_root_node->GetSize() == 1);
    BPInternalPage *old_root = static_cast<BPInternalPage *>(old_root_node);
    root_page_id_ = old_root->ValueAt(0);
    UpdateRootPageId();
    Page *new_root_page = GetPage(root_page_id_, EXCEPTION_INFO);
    BPlusTreePage *new_root = reinterpret_cast<BPlusTreePage *>(new_root_page->GetData());
    new_root->SetParentPageId(INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(), true);

    UnLatchAndUnpinPageSet(transaction, OperationType::DELETE);
    if (!buffer_pool_manager_->DeletePage(old_root_node->GetPageId())) {
        throw Exception("buffer_pool_manager_ delete failed, pin_count != 0");
    }

    return true;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() {
    KeyType invalidKey;
    auto start_leaf = FindLeafPage(invalidKey, OperationType::GET, nullptr, true);
    return INDEXITERATOR_TYPE(start_leaf, 0, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
    auto start_leaf = FindLeafPage(key, OperationType::GET);
    int start_index = 0;
    if (start_leaf != nullptr) {
        // KeyIndex()返回的是key应该插入到的index
        int index = start_leaf->KeyIndex(key, comparator_);
        if (start_leaf->GetSize() > 0 && index < start_leaf->GetSize() && comparator_(key, start_leaf->GetItem(index).first) == 0) {
            //key在当前leaf中存在
            start_index = index;
        } else {
            //leaf中不存在key情况下，令index=start_index->GetSize()
            start_index = start_leaf->GetSize();
        }
    }
    return INDEXITERATOR_TYPE(start_leaf, start_index, buffer_pool_manager_);
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * 该函数返回后，transaction->GetPageSet()队列中保存着此次操作可能被修改的Page指针
 * 如果根节点也在其中，那么root_id_mutex_处于lock状态，因为根节点在此次操作中可能
 * 被修改，所以需要保护root_page_id_这个变量
 */
INDEX_TEMPLATE_ARGUMENTS
B_PLUS_TREE_LEAF_PAGE_TYPE *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key,
                                                         OperationType operation,
                                                         Transaction *transaction,
                                                         bool leftMost) {
    // 直接访问root_page_id_是不安全的，只有保证root_page_id_不会被修改才能通过
    // 只有当根节点是安全的情况下，才不存在修改root_page_id_的可能
    root_id_mutex_.lock();
    if (IsEmpty()) {
        root_id_mutex_.unlock();
        return nullptr;
    }

    page_id_t page_id = root_page_id_;
    auto page = GetPage(page_id, EXCEPTION_INFO);
    if (operation == OperationType::GET) {
        page->RLatch();
        //LOG_DEBUG("rlatch page %d", page_id);
    } else {
        page->WLatch();
        //LOG_DEBUG("wlatch page %d", page_id);
    }
    if (transaction != nullptr) {
        transaction->AddIntoPageSet(page);
    }
    BPlusTreePage *bp = reinterpret_cast<BPlusTreePage *>(page->GetData());

    while (!bp->IsLeafPage()) {
        BPInternalPage *internalPage = static_cast<BPInternalPage *>(bp);
        page_id_t next_page_id;
        if (leftMost) {
           next_page_id = internalPage->ValueAt(0);
        } else {
           next_page_id = internalPage->Lookup(key, comparator_);
        }
        Page *lastPage = page;
        BPlusTreePage *lastBp = bp;
        page = GetPage(next_page_id, EXCEPTION_INFO);
        if (operation == OperationType::GET) {
            page->RLatch();
            //LOG_DEBUG("rlatch page %d", next_page_id);
        } else {
            page->WLatch();
            //LOG_DEBUG("wlatch page %d", next_page_id);
        }
        bp = reinterpret_cast<BPlusTreePage *>(page->GetData());

        // 安全性检查
        if (transaction != nullptr) {
            if (operation == OperationType::GET) {
                // 释放祖先节点的锁
                UnLatchAndUnpinPageSet(transaction, operation);
                assert(transaction->GetPageSet()->size() == 0);
            } else {
                // 当前节点是safe的情况下，才释祖先父节点的锁
                if (bp->IsSafe(operation)) {
                    UnLatchAndUnpinPageSet(transaction, operation);
                }
            }
        } else {
            assert(operation == OperationType::GET);
            lastPage->RUnlatch();
            if (lastBp->IsRootPage()) {
                root_id_mutex_.unlock();
            }
            buffer_pool_manager_->UnpinPage(lastPage->GetPageId(), false);
        }

        if (transaction != nullptr) {
            transaction->AddIntoPageSet(page);
        }
        page_id = next_page_id;
    }
    return static_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(bp);
}

/*
 * 调用者负责Unpin
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::GetPage(page_id_t page_id, std::string msg) {
    auto page = buffer_pool_manager_->FetchPage(page_id);
    if (page == nullptr) {
        throw BufferPoolManagerException(msg);
    }
    return page;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(
      buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record)
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  else
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DeleteRootPageId() {
    HeaderPage *header_page = static_cast<HeaderPage *>(
      buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
    header_page->DeleteRecord(index_name_);
    buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for debug only
 * print out whole b+tree sturcture, rank by rank
 */
INDEX_TEMPLATE_ARGUMENTS
std::string BPLUSTREE_TYPE::ToString(bool verbose) {
    if (IsEmpty()) {
        return "Empty tree";
    }
    std::queue<BPlusTreePage *> todo, tmp;
    std::stringstream out;
    auto node = reinterpret_cast<BPlusTreePage *>(
        buffer_pool_manager_->FetchPage(root_page_id_));
    if (node == nullptr) {
        throw Exception(EXCEPTION_TYPE_INDEX,
                    "all page are pinned while printing");
    }
    todo.push(node);
    bool first = true;
    while (!todo.empty()) {
        node = todo.front();
    if (first) {
        first = false;
        out << "| ";
    }
    if (node->IsLeafPage()) {
        auto page = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(node);
        out << page->ToString(verbose) << "| ";
    } else {
        auto page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(node);
        out << page->ToString(verbose) << "| ";
        page->QueueUpChildren(&tmp, buffer_pool_manager_);
    }
    todo.pop();
    if (todo.empty() && !tmp.empty()) {
        todo.swap(tmp);
        out << '\n';
        first = true;
    }
    buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
  }
  return out.str();
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name,
                                    Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name,
                                    Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UnLatchAndUnpinPageSet(Transaction *transaction, OperationType op) {
    while (transaction->GetPageSet()->size() > 0) {
        Page *front = transaction->GetPageSet()->front();
        BPlusTreePage *bp = reinterpret_cast<BPlusTreePage *>(front->GetData());
        transaction->GetPageSet()->pop_front();
        if (op == OperationType::GET) {
            front->RUnlatch();
        } else {
            front->WUnlatch();
        }
        if (bp->IsRootPage()) {
            root_id_mutex_.unlock();
        }
        buffer_pool_manager_->UnpinPage(front->GetPageId(), true);
    }
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

} // namespace cmudb
