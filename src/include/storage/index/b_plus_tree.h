//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/b_plus_tree.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <queue>
#include <string>
#include <utility>
#include <vector>

#include "concurrency/transaction.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/** B+ Tree Concurrency Lock Crabbing Plan
 * 1. Add another `RULatch` into B+ Tree class for the `root_page_id_` protection
 * 2. Let the `FetchBPlusPage` function return a pair of `[B+ Tree Page, Raw Page]`, so that we could easily acquire
lock and add the `Page *` into `transaction`.
+ Treat the `Page *` for `root_page_id_` as `nullptr` and add `nullptr` into `transaction` accordingly
 * 3. Add a helper function `ReleaseAllLock(transaction *, enum Mode)` to release all the latches acquired in the
`transaction`, where `mode` specifies whether to release `Rlatch` or `Wlatch`
 * 4. At the beginning of the three header function `Find()`, `Insert()`, `Remove()`, acquire the latch on the
`root_page_id_` first before fetching the root page
 * 5. In the `FindLeafPage()` function, add `transaction *` and `mode` parameters. Currently either `Read` mode in which
release parent `Rlatch` after acquiring children `Rlatch`. Another mode is `Write` mode in which we just acquire
`Wlatch` from root page all the way to the leaf page we want, populate into the `transaction *`
 * 6. In the `Insert` and `Remove` recursively helper, once deemed safe, release all the previous latches, or at the end
of `split`, `coalesce` or `merge` operation, to release all the latches
 * 7. Need to release latch on a page before calling BufferPoolManager to `Unpin` the page, otherwise this page might
already get evicted out and the pointer `Page *` is invalid
 */
#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>

/**
 * Main class providing the API for the Interactive B+ Tree.
 *
 * Implementation of simple b+ tree data structure where internal pages direct
 * the search and leaf pages contain actual data.
 * (1) We only support unique key
 * (2) support insert & remove
 * (3) The structure should shrink and grow dynamically
 * (4) Implement index iterator for range scan
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTree {
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  // control flag for latch crabbing
  enum class LatchMode { READ, INSERT, DELETE, OPTIMIZE };

  explicit BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                     int leaf_max_size = LEAF_PAGE_SIZE, int internal_max_size = INTERNAL_PAGE_SIZE);

  // Returns true if this B+ tree has no keys and values.
  auto IsEmpty() const -> bool;

  // Insert a key-value pair into this B+ tree.
  auto Insert(const KeyType &key, const ValueType &value, Transaction *transaction = nullptr) -> bool;

  // Helper for Insert
  auto InsertHelper(const KeyType &key, const ValueType &value, Transaction *transaction, LatchMode mode) -> bool;

  // Remove a key and its value from this B+ tree.
  void Remove(const KeyType &key, Transaction *transaction = nullptr);

  // Helper for Remove
  void RemoveHelper(const KeyType &key, Transaction *transaction, LatchMode mode);

  // return the value associated with a given key
  auto GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction = nullptr) -> bool;

  // return the page id of the root node
  auto GetRootPageId() -> page_id_t;

  // index iterator
  auto Begin() -> INDEXITERATOR_TYPE;
  auto Begin(const KeyType &key) -> INDEXITERATOR_TYPE;
  auto End() -> INDEXITERATOR_TYPE;

  // print the B+ tree
  void Print(BufferPoolManager *bpm);

  // draw the B+ tree
  void Draw(BufferPoolManager *bpm, const std::string &outf);

  // read data from file and insert one by one
  void InsertFromFile(const std::string &file_name, Transaction *transaction = nullptr);

  // read data from file and remove one by one
  void RemoveFromFile(const std::string &file_name, Transaction *transaction = nullptr);

 private:
  auto CreateInternalPage() -> BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *;

  auto CreateLeafPage() -> BPlusTreeLeafPage<KeyType, RID, KeyComparator> *;

  auto InitBPlusTree(const KeyType &key, const ValueType &value) -> void;

  auto IsSafePage(BPlusTreePage *page, LatchMode mode) -> bool;

  auto FindLeafPage(const KeyType &key, Transaction *transaction = nullptr, LatchMode mode = LatchMode::READ)
      -> std::pair<Page *, BPlusTreeLeafPage<KeyType, RID, KeyComparator> *>;

  void SetPageDirty(page_id_t page_id);

  void InsertInParent(BPlusTreePage *left_page, BPlusTreePage *right_page, const KeyType &upward_key);

  void RemoveEntry(BPlusTreePage *base_page, const KeyType &key, int &dirty_height);

  auto RemoveDependingOnType(BPlusTreePage *base_page, const KeyType &key) -> bool;

  auto TryRedistribute(BPlusTreePage *base_page, const KeyType &key) -> bool;

  void Redistribute(BPlusTreePage *base, BPlusTreePage *sibling,
                    BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parent, int base_index,
                    bool sibling_on_left);

  auto TryMerge(BPlusTreePage *base_page, const KeyType &key, int &dirty_height) -> bool;

  void Merge(BPlusTreePage *base, BPlusTreePage *sibling,
             BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parent, int base_index, bool sibling_on_left,
             int &dirty_height);

  void RefreshParentPointer(BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *page, int index);

  void RefreshAllParentPointer(BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *page);

  void LatchRootPageId(Transaction *transaction, LatchMode mode);

  void UpdateRootPageId(int insert_record = 0);

  auto FetchBPlusTreePage(page_id_t page_id) -> std::pair<Page *, BPlusTreePage *>;

  auto ReinterpretAsLeafPage(BPlusTreePage *page) -> BPlusTreeLeafPage<KeyType, RID, KeyComparator> *;

  auto ReinterpretAsInternalPage(BPlusTreePage *page) -> BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *;

  void ReleaseAllLatches(Transaction *transaction, LatchMode mode, int dirty_height = 0);

  /* Debug Routines for FREE!! */
  void ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const;

  void ToString(BPlusTreePage *page, BufferPoolManager *bpm) const;

  // member variable
  std::string index_name_;
  page_id_t root_page_id_;
  BufferPoolManager *buffer_pool_manager_;
  KeyComparator comparator_;
  int leaf_max_size_;
  int internal_max_size_;
  // only want to insert into header page about this index's existence once
  bool header_record_created_{false};
  // latch for the root_id
  ReaderWriterLatch root_id_rwlatch_;
};

}  // namespace bustub
