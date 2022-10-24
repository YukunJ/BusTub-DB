//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/page/b_plus_tree_internal_page.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <queue>

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

#define B_PLUS_TREE_INTERNAL_PAGE_TYPE BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>
#define INTERNAL_PAGE_HEADER_SIZE 24
#define INTERNAL_PAGE_SIZE ((BUSTUB_PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / (sizeof(MappingType)) - 1)
/**
 * Store n indexed keys and n+1 child pointers (page_id) within internal page.
 * Pointer PAGE_ID(i) points to a subtree in which all keys K satisfy:
 * K(i) <= K < K(i+1).
 * NOTE: since the number of keys does not equal to number of child pointers,
 * the first key always remains invalid. That is to say, any search/lookup
 * should ignore the first key.
 *
 * Internal page format (keys are stored in increasing order):
 *  --------------------------------------------------------------------------
 * | HEADER | KEY(1)+PAGE_ID(1) | KEY(2)+PAGE_ID(2) | ... | KEY(n)+PAGE_ID(n) |
 *  --------------------------------------------------------------------------
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeInternalPage : public BPlusTreePage {
 public:
  // must call initialize method after "create" a new node
  void Init(page_id_t page_id, page_id_t parent_id = INVALID_PAGE_ID, int max_size = INTERNAL_PAGE_SIZE);

  auto KeyAt(int index) const -> KeyType;
  void SetKeyAt(int index, const KeyType &key);
  auto ValueAt(int index) const -> ValueType;
  void SetValueAt(int index, const ValueType &value);
  auto SearchPage(const KeyType &key, KeyComparator &comparator) -> ValueType;
  auto SearchJumpIdx(const KeyType &key, KeyComparator &comparator) -> int;
  auto Insert(const KeyType &key, const ValueType &value, KeyComparator &comparator) -> bool;
  auto FindKeyPosition(const KeyType &key, KeyComparator &comparator) -> int;
  auto RemoveKey(const KeyType &key, KeyComparator &comparator) -> bool;
  void MoveAllTo(BPlusTreeInternalPage *recipient);
  void MoveLatterHalfWithOneExtraTo(BPlusTreeInternalPage *recipient, KeyType extra_key, ValueType extra_value,
                                    KeyComparator &comparator);
  void MoveFirstToEndOf(BPlusTreeInternalPage *recipient);
  void MoveLastToFrontOf(BPlusTreeInternalPage *recipient);
  auto GetMappingSize() -> size_t;
  auto GetArray() -> char *;

 private:
  void ExcavateIndex(int index);
  void FillIndex(int index);
  // Flexible array member for page data.
  MappingType array_[1];
};
}  // namespace bustub
