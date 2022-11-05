#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 * should have latch on the root page id before accessing
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  bool dummy_used = false;
  if (transaction == nullptr) {
    // for consistency, make sure there is always a non-empty transaction passed in
    transaction = new Transaction(0);
    dummy_used = true;
  }
  LatchRootPageId(transaction, LatchMode::READ);
  if (IsEmpty()) {
    ReleaseAllLatches(transaction, LatchMode::READ);
    if (dummy_used) {
      delete transaction;
    }
    return false;
  }
  bool found = false;
  auto [raw_leaf_page, leaf_page] = FindLeafPage(key, transaction, LatchMode::READ);
  auto left = 0;
  auto right = leaf_page->GetSize() - 1;
  while (left <= right) {
    // binary search
    auto mid = left + (right - left) / 2;
    auto comp_result = comparator_(key, leaf_page->KeyAt(mid));
    if (comp_result == 0) {
      result->push_back(leaf_page->ValueAt(mid));
      found = true;
      break;
    }
    if (comp_result < 0) {
      right = mid - 1;
    } else {
      left = mid + 1;
    }
  }
  // clear up all the latches held in this transaction
  ReleaseAllLatches(transaction, LatchMode::READ);
  if (dummy_used) {
    delete transaction;
  }
  return found;
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
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  bool dummy_used = false;
  if (transaction == nullptr) {
    // for consistency, make sure there is always a non-empty transaction passed in
    transaction = new Transaction(0);
    dummy_used = true;
  }
  bool success = InsertHelper(key, value, transaction, LatchMode::OPTIMIZE);
  if (dummy_used) {
    delete transaction;
  }
  return success;
}

/*
 * Helper for insert
 * transaction must be not null
 * when OPTIMIZE mode fails, re-call with INSERT mode
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPlusTree<KeyType, ValueType, KeyComparator>::InsertHelper(const KeyType &key, const ValueType &value,
                                                                Transaction *transaction, BPlusTree::LatchMode mode)
    -> bool {
  int dirty_height = 0;
  LatchRootPageId(transaction, mode);
  if (IsEmpty()) {
    if (mode == LatchMode::OPTIMIZE) {
      // OPTIMIZE mode fails
      ReleaseAllLatches(transaction, mode, dirty_height);  // no page is dirty
      return InsertHelper(key, value, transaction, LatchMode::INSERT);
    }
    InitBPlusTree(key, value);
    ReleaseAllLatches(transaction, mode, dirty_height);
    return true;
  }
  auto [raw_leaf_page, leaf_page] = FindLeafPage(key, transaction, mode);
  if ((1 + leaf_page->GetSize()) == leaf_page->GetMaxSize() && mode == LatchMode::OPTIMIZE) {
    // OPTIMIZE mode fails
    ReleaseAllLatches(transaction, mode, dirty_height);
    return InsertHelper(key, value, transaction, LatchMode::INSERT);
  }
  bool no_duplicate = leaf_page->Insert(key, value, comparator_);
  if (!no_duplicate) {
    ReleaseAllLatches(transaction, mode, dirty_height);
    return false;
  }
  dirty_height += 1;
  // SetPageDirty(leaf_page->GetPageId());
  if (leaf_page->GetSize() == leaf_page->GetMaxSize()) {
    // overflow, need split
    auto leaf_page_prime = CreateLeafPage();
    leaf_page->MoveLatterHalfTo(leaf_page_prime);
    leaf_page_prime->SetParentPageId(leaf_page->GetParentPageId());  // same parent by default
    const auto key_upward = leaf_page_prime->KeyAt(0);
    InsertInParent(leaf_page, leaf_page_prime, key_upward);
    buffer_pool_manager_->UnpinPage(leaf_page_prime->GetPageId(), true);
  }
  ReleaseAllLatches(transaction, mode, dirty_height);
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  bool dummy_used = false;
  if (transaction == nullptr) {
    // for consistency, make sure there is always a non-empty transaction passed in
    transaction = new Transaction(0);
    dummy_used = true;
  }
  RemoveHelper(key, transaction, LatchMode::OPTIMIZE);
  if (dummy_used) {
    delete transaction;
  }
}

/*
 * Helper for Remove
 * Start with OPTIMIZE mode
 * if fails, retry with DELETE mode
 */
INDEX_TEMPLATE_ARGUMENTS
void BPlusTree<KeyType, ValueType, KeyComparator>::RemoveHelper(const KeyType &key, Transaction *transaction,
                                                                BPlusTree::LatchMode mode) {
  int dirty_height = 0;
  LatchRootPageId(transaction, mode);
  if (IsEmpty()) {
    ReleaseAllLatches(transaction, mode, dirty_height);
    return;
  }
  auto [raw_leaf_page, leaf_page] = FindLeafPage(key, transaction, mode);
  if ((leaf_page->GetSize() - 1) < leaf_page->GetMinSize() && mode == LatchMode::OPTIMIZE) {
    auto is_root = leaf_page->IsRootPage();
    auto is_leaf = leaf_page->IsLeafPage();
    auto is_internal = leaf_page->IsInternalPage();
    auto fail_condition1 = !is_root;
    auto fail_condition2 = is_root && is_leaf && (leaf_page->GetSize() - 1) == 0;
    auto fail_condition3 = is_root && is_internal && (leaf_page->GetSize() - 1) == 1;
    if (fail_condition1 || fail_condition2 || fail_condition3) {
      ReleaseAllLatches(transaction, mode);
      return RemoveHelper(key, transaction, LatchMode::DELETE);
    }
  }
  RemoveEntry(leaf_page, key, dirty_height);
  ReleaseAllLatches(transaction, mode, dirty_height);
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
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  // no concurrency support
  if (IsEmpty()) {
    return End();
  }
  auto [raw_leaf_page, leaf_page] = FindLeafPage(KeyType());
  BUSTUB_ASSERT(leaf_page != nullptr, "leaf_page != nullptr");
  return INDEXITERATOR_TYPE(leaf_page->GetPageId(), 0, leaf_page, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  auto [raw_leaf_page, leaf_page] = FindLeafPage(key);  // no concurrency support
  auto bigger_or_equal_idx = leaf_page->FindGreaterEqualKeyPosition(key, comparator_);
  if (bigger_or_equal_idx == -1) {
    // no bigger or equal position in this leaf, need to move to next one
    auto leaf_id = leaf_page->GetPageId();
    auto next_page_id = leaf_page->GetNextPageId();
    buffer_pool_manager_->UnpinPage(leaf_id, false);
    if (next_page_id == INVALID_PAGE_ID) {
      return End();  // no viable traversal
    }
    auto base_leaf_page = FetchBPlusTreePage(next_page_id).second;
    leaf_page = ReinterpretAsLeafPage(base_leaf_page);
    bigger_or_equal_idx = 0;
  }
  return INDEXITERATOR_TYPE(leaf_page->GetPageId(), bigger_or_equal_idx, leaf_page, buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator (by default constructor)
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * need to grab root page id latch in concurrent mode
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/**
 * Create a new internal page from buffer pool manager
 * Caller should unpin this page after usage
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @return pointer to newly created internal page
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPlusTree<KeyType, ValueType, KeyComparator>::CreateInternalPage()
    -> BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> * {
  page_id_t p_id = INVALID_PAGE_ID;
  auto new_page = buffer_pool_manager_->NewPage(&p_id);
  BUSTUB_ASSERT(p_id != INVALID_PAGE_ID, "p_id != INVALID_PAGE_ID");
  auto internal_page =
      reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(new_page->GetData());
  internal_page->Init(p_id, INVALID_PAGE_ID, internal_max_size_);
  return internal_page;
}

/**
 * Create a new leaf page from buffer pool manager
 * Caller should unpin this page after usage
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @return pointer to newly created leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPlusTree<KeyType, ValueType, KeyComparator>::CreateLeafPage()
    -> BPlusTreeLeafPage<KeyType, RID, KeyComparator> * {
  page_id_t p_id = INVALID_PAGE_ID;
  auto new_page = buffer_pool_manager_->NewPage(&p_id);
  BUSTUB_ASSERT(p_id != INVALID_PAGE_ID, "p_id != INVALID_PAGE_ID");
  auto leaf_page = reinterpret_cast<BPlusTreeLeafPage<KeyType, RID, KeyComparator> *>(new_page->GetData());
  leaf_page->Init(p_id, INVALID_PAGE_ID, leaf_max_size_);
  return leaf_page;
}

/**
 * Initialize a B+ Tree from empty state, update root_page accordingly
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param key
 * @param value
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPlusTree<KeyType, ValueType, KeyComparator>::InitBPlusTree(const KeyType &key, const ValueType &value) -> void {
  /*
   * Ask for a new page from bpm (which will have pin count = 1)
   * and set this page to be B+ Leaf Page and insert first key-value pair into it
   * update the header page root_idx and unpin this page with dirty flag
   */
  auto root_leaf_page = CreateLeafPage();
  root_page_id_ = root_leaf_page->GetPageId();
  BUSTUB_ASSERT(root_leaf_page != nullptr, "root_leaf_page != nullptr");
  BUSTUB_ASSERT(root_page_id_ != INVALID_PAGE_ID, "root_page_id_ != INVALID_PAGE_ID");
  UpdateRootPageId(!header_record_created_);
  header_record_created_ = true;
  auto r = root_leaf_page->Insert(key, value, comparator_);
  BUSTUB_ASSERT(r, "BPlusTree Init Insert should be True");
  buffer_pool_manager_->UnpinPage(root_page_id_, true);  // modification made
}

/*
 * Depending on the mode (READ/INSERT/DELETE)
 * decide if this page is safe so that all previous latches could be released at this point
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPlusTree<KeyType, ValueType, KeyComparator>::IsSafePage(BPlusTreePage *page, BPlusTree::LatchMode mode) -> bool {
  if (mode == LatchMode::READ || mode == LatchMode::OPTIMIZE) {
    return true;
  }
  if (mode == LatchMode::INSERT) {
    return static_cast<bool>(1 + page->GetSize() < page->GetMaxSize());
  }
  if (mode == LatchMode::DELETE) {
    auto after_delete_size = page->GetSize() - 1;
    if (after_delete_size < page->GetMinSize()) {
      // might be unsafe
      if (page->IsRootPage()) {
        // root page get special treatment
        if (page->IsInternalPage() && after_delete_size > 1) {
          return true;
        }
        if (page->IsLeafPage() && after_delete_size > 0) {
          return true;
        }
      }
      return false;
    }
    return true;
  }
  BUSTUB_ASSERT(false, "Not supposed to hit this default return branch in IsSafePage()");
  return true;
}

/**
 * Iterate through the B+ Tree to fetch a leaf page
 * the caller should unpin the leaf page after usage
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param key
 * @param transaction
 * @return pointer to a leaf page if found, nullptr otherwise
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPlusTree<KeyType, ValueType, KeyComparator>::FindLeafPage(const KeyType &key, Transaction *transaction,
                                                                LatchMode mode)
    -> std::pair<Page *, BPlusTreeLeafPage<KeyType, RID, KeyComparator> *> {
  BUSTUB_ASSERT(root_page_id_ != INVALID_PAGE_ID, "root_page_id_ != INVALID_PAGE_ID");
  if (transaction != nullptr) {
    // root id latch should already be held in the transaction
    BUSTUB_ASSERT(transaction->GetPageSet()->front() == nullptr, "transaction->GetPageSet()->front() == nullptr");
  }
  auto [raw_curr_page, curr_page] = FetchBPlusTreePage(root_page_id_);
  decltype(raw_curr_page) raw_next_page = nullptr;
  decltype(curr_page) next_page = nullptr;
  if (transaction != nullptr) {
    // concurrency needed
    if (mode == LatchMode::READ) {
      raw_curr_page->RLatch();
    } else if (mode == LatchMode::OPTIMIZE) {
      if (curr_page->IsLeafPage()) {
        raw_curr_page->WLatch();
      } else {
        raw_curr_page->RLatch();
      }
    } else {
      // normal INSERT or DELETE
      raw_curr_page->WLatch();
    }
    if (IsSafePage(curr_page, mode)) {
      ReleaseAllLatches(transaction, mode);
    }
    transaction->AddIntoPageSet(raw_curr_page);
  }
  while (!curr_page->IsLeafPage()) {
    auto curr_page_internal = ReinterpretAsInternalPage(curr_page);
    page_id_t jump_pid = curr_page_internal->SearchPage(key, comparator_);
    BUSTUB_ASSERT(jump_pid != INVALID_PAGE_ID, "jump_pid != INVALID_PAGE_ID");
    auto next_pair = FetchBPlusTreePage(jump_pid);
    raw_next_page = next_pair.first;
    next_page = next_pair.second;
    if (transaction != nullptr) {
      // concurrency needed
      if (mode == LatchMode::READ) {
        raw_next_page->RLatch();
      } else if (mode == LatchMode::OPTIMIZE) {
        if (next_page->IsLeafPage()) {
          raw_next_page->WLatch();
        } else {
          raw_next_page->RLatch();
        }
      } else {
        // normal INSERT or DELETE
        raw_next_page->WLatch();
      }
      if (IsSafePage(next_page, mode)) {
        ReleaseAllLatches(transaction, mode);
      }
      transaction->AddIntoPageSet(raw_next_page);
    } else {
      buffer_pool_manager_->UnpinPage(curr_page->GetPageId(), false);
    }
    raw_curr_page = raw_next_page;
    curr_page = next_page;
  }
  BUSTUB_ASSERT(curr_page->IsLeafPage(), "curr_page->IsLeafPage()");
  return {raw_curr_page, ReinterpretAsLeafPage(curr_page)};
}

/*
 * Awkward SetDirty() utility
 */
INDEX_TEMPLATE_ARGUMENTS
void BPlusTree<KeyType, ValueType, KeyComparator>::SetPageDirty(page_id_t page_id) {
  buffer_pool_manager_->FetchPage(page_id);
  buffer_pool_manager_->UnpinPage(page_id, true);
}

/*
 * Recursively Insert Into the parent node
 */
INDEX_TEMPLATE_ARGUMENTS
void BPlusTree<KeyType, ValueType, KeyComparator>::InsertInParent(BPlusTreePage *left_page, BPlusTreePage *right_page,
                                                                  const KeyType &upward_key) {
  if (left_page->IsRootPage()) {
    auto new_root_page = CreateInternalPage();
    root_page_id_ = new_root_page->GetPageId();
    UpdateRootPageId(false);
    new_root_page->SetValueAt(0, left_page->GetPageId());
    new_root_page->SetKeyAt(1, upward_key);
    new_root_page->SetValueAt(1, right_page->GetPageId());
    new_root_page->IncreaseSize(1);
    BUSTUB_ASSERT(new_root_page->GetSize() == 2, "new_root_page->GetSize() == 2");
    left_page->SetParentPageId(root_page_id_);
    right_page->SetParentPageId(root_page_id_);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    return;
  }
  // upon entry from Insert(), both left and right's parent point to this parent_page, may need change
  auto [raw_parent_page, base_parent_page] = FetchBPlusTreePage(left_page->GetParentPageId());
  auto parent_page = ReinterpretAsInternalPage(base_parent_page);
  if (parent_page->GetSize() == parent_page->GetMaxSize()) {
    // follow rule that split internal node when number of values reaches max_size before insertion
    // parent page is definitely internal page, be careful of the 0-index invalid key
    auto parent_page_prime = CreateInternalPage();
    parent_page_prime->SetParentPageId(parent_page->GetParentPageId());  // same parent by default
    parent_page->MoveLatterHalfWithOneExtraTo(parent_page_prime, upward_key, right_page->GetPageId(), comparator_);
    RefreshAllParentPointer(parent_page_prime);
    const auto further_upward_key = parent_page_prime->KeyAt(0);  // actually invalid 0-indexed key
    InsertInParent(parent_page, parent_page_prime, further_upward_key);
    buffer_pool_manager_->UnpinPage(parent_page_prime->GetPageId(), true);
  } else {
    // if not split, just insert into parent's page
    parent_page->Insert(upward_key, right_page->GetPageId(), comparator_);
  }
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
}

/*
 * Driver Helper function for Remove()
 * to be called recursively with coalesce/merge sub-helper function
 */
INDEX_TEMPLATE_ARGUMENTS
void BPlusTree<KeyType, ValueType, KeyComparator>::RemoveEntry(BPlusTreePage *base_page, const KeyType &key,
                                                               int &dirty_height) {
  auto delete_success = RemoveDependingOnType(base_page, key);
  if (!delete_success) {
    // no modification made on this page
    return;
  }
  dirty_height++;
  // SetPageDirty(base_page->GetPageId());
  if (base_page->GetSize() < base_page->GetMinSize()) {
    if (base_page->IsRootPage()) {
      // root page gets special treatment
      // root's page being leaf page can violate the "half-full" property
      if (base_page->IsInternalPage()) {
        if (base_page->GetSize() == 1) {
          // left with only 1 pointer, re-root the B+ tree
          root_page_id_ = ReinterpretAsInternalPage(base_page)->ValueAt(0);
          UpdateRootPageId(false);
          auto [raw_new_root_page, new_root_page] = FetchBPlusTreePage(root_page_id_);
          new_root_page->SetParentPageId(INVALID_PAGE_ID);  // help identify self as root
          buffer_pool_manager_->UnpinPage(root_page_id_, true);
        }
      } else {
        // root's page is leaf page, only if everything is deleted
        if (base_page->GetSize() == 0) {
          root_page_id_ = INVALID_PAGE_ID;
          UpdateRootPageId(false);
        }
      }
    } else {
      // follow the order of
      // 1. redistribute from right
      // 2. redistribute from left
      // 3. merge from right
      // 4. merge from left
      auto redistribute_success = TryRedistribute(base_page, key);  // try right and then left
      if (!redistribute_success) {
        auto merge_success = TryMerge(base_page, key, dirty_height);  // must succeed
        BUSTUB_ASSERT(redistribute_success || merge_success, "redistribute_success || merge_success");
      }
    }
  }
}

/*
 * Distinguish between leaf and internal page
 * and reinterpret + remove page + return bool flag to caller
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPlusTree<KeyType, ValueType, KeyComparator>::RemoveDependingOnType(BPlusTreePage *base_page, const KeyType &key)
    -> bool {
  auto is_leaf = base_page->IsLeafPage();
  if (is_leaf) {
    return ReinterpretAsLeafPage(base_page)->RemoveKey(key, comparator_);
  }
  return ReinterpretAsInternalPage(base_page)->RemoveKey(key, comparator_);
}

/*
 * Try to redistribute from right, and then left
 * return True if redistribution is successfully, False otherwise
 * This function is self-cleaned, it will unpin any parent, sibling page
 * but will not unpin the base page, which will be done in Remove() main procedure's end
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPlusTree<KeyType, ValueType, KeyComparator>::TryRedistribute(BPlusTreePage *base_page, const KeyType &key)
    -> bool {
  BUSTUB_ASSERT(!base_page->IsRootPage(), "!base_page->IsRootPage()");
  auto parent_page_id = base_page->GetParentPageId();
  auto [raw_parent_page, base_parent_page] = FetchBPlusTreePage(parent_page_id);
  auto parent_page = ReinterpretAsInternalPage(base_parent_page);
  auto underfull_index = parent_page->SearchJumpIdx(key, comparator_);
  auto redistribute_success = false;
  if (underfull_index < parent_page->GetSize() - 1) {
    // has right sibling
    auto [sibling_raw_page, sibling_page] = FetchBPlusTreePage(parent_page->ValueAt(underfull_index + 1));
    sibling_raw_page->WLatch();  // lock sibling
    if ((sibling_page->GetSize() - 1) >= sibling_page->GetMinSize()) {
      // stealing not leading to sibling underfull
      Redistribute(base_page, sibling_page, parent_page, underfull_index, false);
      redistribute_success = true;
    }
    sibling_raw_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(), redistribute_success);
  }
  if (!redistribute_success && underfull_index > 0) {
    // has left sibling
    auto [sibling_raw_page, sibling_page] = FetchBPlusTreePage(parent_page->ValueAt(underfull_index - 1));
    sibling_raw_page->WLatch();  // lock sibling
    if ((sibling_page->GetSize() - 1) >= sibling_page->GetMinSize()) {
      // stealing not leading to sibling underfull
      Redistribute(base_page, sibling_page, parent_page, underfull_index, true);
      redistribute_success = true;
    }
    sibling_raw_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(), redistribute_success);
  }
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), redistribute_success);
  return redistribute_success;
}

/*
 * Try to merge from right, and then left
 * return True if merge is successful, False otherwise
 * This function is self-cleaned, it will unpin any parent, sibling page
 * but will not unpin the base page, which will be done in Remove() main procedure's end
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPlusTree<KeyType, ValueType, KeyComparator>::TryMerge(BPlusTreePage *base_page, const KeyType &key,
                                                            int &dirty_height) -> bool {
  BUSTUB_ASSERT(!base_page->IsRootPage(), "!base_page->IsRootPage()");
  auto parent_page_id = base_page->GetParentPageId();
  auto [raw_parent_page, base_parent_page] = FetchBPlusTreePage(parent_page_id);
  auto parent_page = ReinterpretAsInternalPage(base_parent_page);
  auto underfull_index = parent_page->SearchJumpIdx(key, comparator_);
  auto merge_success = false;
  if (underfull_index < parent_page->GetSize() - 1) {
    // has right sibling, definitely can merge in our logic flow
    auto [sibling_raw_page, sibling_page] = FetchBPlusTreePage(parent_page->ValueAt(underfull_index + 1));
    sibling_raw_page->WLatch();  // lock sibling
    Merge(base_page, sibling_page, parent_page, underfull_index, false, dirty_height);
    sibling_raw_page->WUnlatch();
    merge_success = true;
    buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(), merge_success);
  }
  if (!merge_success && underfull_index > 0) {
    // has left sibling, definitely can merge in our logic flow
    auto [sibling_raw_page, sibling_page] = FetchBPlusTreePage(parent_page->ValueAt(underfull_index - 1));
    sibling_raw_page->WLatch();  // lock sibling
    Merge(base_page, sibling_page, parent_page, underfull_index, true, dirty_height);
    sibling_raw_page->WUnlatch();
    merge_success = true;
    buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(), merge_success);
  }
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), merge_success);
  return merge_success;
}

/**
 * The helper function called in TryRedistribute()
 * @param base the page underfull
 * @param sibling sibling page, could be left or right of base page
 * @param parent parent page
 * @param base_index the base page's jump idx in parent page
 * @param true if the sibling page is on left of the base page, false if on right
 */
INDEX_TEMPLATE_ARGUMENTS
void BPlusTree<KeyType, ValueType, KeyComparator>::Redistribute(
    BPlusTreePage *base, BPlusTreePage *sibling, BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parent,
    int base_index, bool sibling_on_left) {
  if (base->IsLeafPage()) {
    auto base_leaf = ReinterpretAsLeafPage(base);
    auto sibling_leaf = ReinterpretAsLeafPage(sibling);
    if (sibling_on_left) {
      sibling_leaf->MoveLastToFrontOf(base_leaf);
      parent->SetKeyAt(base_index, base_leaf->KeyAt(0));
    } else {
      // sibling on the right
      sibling_leaf->MoveFirstToEndOf(base_leaf);
      parent->SetKeyAt(base_index + 1, sibling_leaf->KeyAt(0));
    }
  } else {
    auto base_internal = ReinterpretAsInternalPage(base);
    auto sibling_internal = ReinterpretAsInternalPage(sibling);
    if (sibling_on_left) {
      sibling_internal->MoveLastToFrontOf(base_internal);
      RefreshParentPointer(base_internal, 0);
      auto upward_key = base_internal->KeyAt(0);
      parent->SetKeyAt(base_index, upward_key);
    } else {
      // sibling on the right
      sibling_internal->MoveFirstToEndOf(base_internal);
      RefreshParentPointer(base_internal, base_internal->GetSize() - 1);
      auto parent_key = parent->KeyAt(base_index + 1);
      auto upward_key = sibling_internal->KeyAt(0);
      base_internal->SetKeyAt(base_internal->GetSize() - 1, parent_key);
      parent->SetKeyAt(base_index + 1, upward_key);
    }
  }
}

/**
 * The helper function called in TryMerge()
 * @param base the page underfull
 * @param sibling sibling page, could be left or right of base page
 * @param parent parent page
 * @param base_index the base page's jump idx in parent page
 * @param true if the sibling page is on left of the base page, false if on right
 */
INDEX_TEMPLATE_ARGUMENTS
void BPlusTree<KeyType, ValueType, KeyComparator>::Merge(
    BPlusTreePage *base, BPlusTreePage *sibling, BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parent,
    int base_index, bool sibling_on_left, int &dirty_height) {
  // also merge into left page, so that left's left page points to the correct next_page_id
  if (base->IsLeafPage()) {
    auto base_leaf = ReinterpretAsLeafPage(base);
    auto sibling_leaf = ReinterpretAsLeafPage(sibling);
    if (sibling_on_left) {
      auto key_in_between = parent->KeyAt(base_index);
      base_leaf->MoveAllTo(sibling_leaf);
      sibling_leaf->SetNextPageId(base_leaf->GetNextPageId());
      base_leaf->SetParentPageId(INVALID_PAGE_ID);  // mask off the link
      RemoveEntry(parent, key_in_between, dirty_height);
    } else {
      // sibling on the right
      auto key_in_between = parent->KeyAt(base_index + 1);
      sibling_leaf->MoveAllTo(base_leaf);
      base_leaf->SetNextPageId(sibling_leaf->GetNextPageId());
      sibling_leaf->SetParentPageId(INVALID_PAGE_ID);
      RemoveEntry(parent, key_in_between, dirty_height);
    }
  } else {
    auto base_internal = ReinterpretAsInternalPage(base);
    auto sibling_internal = ReinterpretAsInternalPage(sibling);
    if (sibling_on_left) {
      auto key_in_between = parent->KeyAt(base_index);
      auto sibling_old_size = sibling_internal->GetSize();
      base_internal->MoveAllTo(sibling_internal);
      RefreshAllParentPointer(sibling_internal);
      base_internal->SetParentPageId(INVALID_PAGE_ID);  // mask off the link
      sibling_internal->SetKeyAt(sibling_old_size, key_in_between);
      RemoveEntry(parent, key_in_between, dirty_height);
    } else {
      // sibling on the right
      auto key_in_between = parent->KeyAt(base_index + 1);
      auto base_old_size = base_internal->GetSize();
      sibling_internal->MoveAllTo(base_internal);
      RefreshAllParentPointer(base_internal);
      sibling_internal->SetParentPageId(INVALID_PAGE_ID);  // mask off the link
      base_internal->SetKeyAt(base_old_size, key_in_between);
      RemoveEntry(parent, key_in_between, dirty_height);
    }
  }
}

/*
 * Refresh the index-th children's parent pointer to myself
 */
INDEX_TEMPLATE_ARGUMENTS
void BPlusTree<KeyType, ValueType, KeyComparator>::RefreshParentPointer(
    BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *page, int index) {
  auto page_id = page->GetPageId();
  auto [raw_page, moved_page] = FetchBPlusTreePage(page->ValueAt(index));
  moved_page->SetParentPageId(page_id);
  buffer_pool_manager_->UnpinPage(moved_page->GetPageId(), true);
}

/*
 * When all children are received from other nodes
 * re-map all children's parent pointer to myself
 */
INDEX_TEMPLATE_ARGUMENTS
void BPlusTree<KeyType, ValueType, KeyComparator>::RefreshAllParentPointer(
    BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *page) {
  auto page_id = page->GetPageId();
  for (auto i = 0; i < page->GetSize(); i++) {
    auto [raw_page, moved_page] = FetchBPlusTreePage(page->ValueAt(i));
    moved_page->SetParentPageId(page_id);
    buffer_pool_manager_->UnpinPage(moved_page->GetPageId(), true);
  }
}

/*
 * In concurrent mode, need to grab latch on the root id
 * before fetching the root page
 */
INDEX_TEMPLATE_ARGUMENTS
void BPlusTree<KeyType, ValueType, KeyComparator>::LatchRootPageId(Transaction *transaction,
                                                                   BPlusTree::LatchMode mode) {
  if (mode == LatchMode::READ || mode == LatchMode::OPTIMIZE) {
    root_id_rwlatch_.RLock();
  } else {
    // normal Insert or Remove
    root_id_rwlatch_.WLock();
  }
  // nullptr as indicator for page id latch
  transaction->AddIntoPageSet(nullptr);
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
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/**
 * Fetch a page using bufferPoolManager
 * and return it in the form of <Raw Page, Base Class BPlusTreePage>
 * User could further reinterpret_cast the page based on page type
 * Caller should assure to lock the page etc after getting the page *
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page_id the page id to be fetched from buffer pool manager
 * @return pointers to rawP Page and BPlusTreePage, to be further latched / reinterpreted by caller function
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPlusTree<KeyType, ValueType, KeyComparator>::FetchBPlusTreePage(page_id_t page_id)
    -> std::pair<Page *, BPlusTreePage *> {
  Page *page = buffer_pool_manager_->FetchPage(page_id);
  BUSTUB_ASSERT(page != nullptr, "FetchBPlusTreePage(): page != nullptr");
  return {page, reinterpret_cast<BPlusTreePage *>(page->GetData())};
}

/** Cast a Base BPlusTree Page to LeafPage */
INDEX_TEMPLATE_ARGUMENTS
auto BPlusTree<KeyType, ValueType, KeyComparator>::ReinterpretAsLeafPage(BPlusTreePage *page)
    -> BPlusTreeLeafPage<KeyType, RID, KeyComparator> * {
  return reinterpret_cast<BPlusTreeLeafPage<KeyType, RID, KeyComparator> *>(page);
}

/** Cast a Base BPlusTree Page to InternalPage */
INDEX_TEMPLATE_ARGUMENTS
auto BPlusTree<KeyType, ValueType, KeyComparator>::ReinterpretAsInternalPage(BPlusTreePage *page)
    -> BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> * {
  return reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(page);
}

/** Release all the latches held in the transaction so far
 *  nullptr is treated as root_id_rwlatch_
 *  unlock according to the mode specified
 */
INDEX_TEMPLATE_ARGUMENTS
void BPlusTree<KeyType, ValueType, KeyComparator>::ReleaseAllLatches(Transaction *transaction,
                                                                     BPlusTree::LatchMode mode, int dirty_height) {
  auto page_set = transaction->GetPageSet();
  while (!page_set->empty()) {
    // release from upstream to downstream
    auto front_page = page_set->front();
    auto is_leaf =
        (front_page != nullptr) ? reinterpret_cast<BPlusTreePage *>(front_page->GetData())->IsLeafPage() : false;
    if (mode == LatchMode::READ) {
      if (front_page == nullptr) {
        root_id_rwlatch_.RUnlock();
      } else {
        front_page->RUnlatch();
      }
    } else if (mode == LatchMode::OPTIMIZE) {
      if (front_page == nullptr) {
        root_id_rwlatch_.RUnlock();
      } else {
        // in optimize mode, the last leaf page is held a Wlatch
        if (is_leaf) {
          front_page->WUnlatch();
        } else {
          front_page->RUnlatch();
        }
      }
    } else {
      // normal write mode
      if (front_page == nullptr) {
        root_id_rwlatch_.WUnlock();
      } else {
        front_page->WUnlatch();
      }
    }
    if (front_page != nullptr) {
      // the last 'dirty_height' pages should be marked as dirty
      buffer_pool_manager_->UnpinPage(front_page->GetPageId(), page_set->size() <= static_cast<size_t>(dirty_height));
    }
    page_set->pop_front();
  }
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
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
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
