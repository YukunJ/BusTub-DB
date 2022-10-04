//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetPageId(page_id);
  SetNextPageId(INVALID_PAGE_ID);
  SetParentPageId(parent_id);
  SetSize(0);
  SetMaxSize(max_size);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to get/set the key associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType { return array_[index].first; }
INDEX_TEMPLATE_ARGUMENTS
void BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::SetKeyAt(int index, const KeyType &key) {
  array_[index].first = key;
}

/*
 * Helper method to get/set the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::ValueAt(int index) const -> ValueType {
  return array_[index].second;
}

INDEX_TEMPLATE_ARGUMENTS
void BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::SetValueAt(int index, const ValueType &value) {
  array_[index].second = value;
}

/*
 * Insert a key-value pair into the Leaf Page
 * return False if this key is duplicate
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::Insert(const KeyType &key, const ValueType &value,
                                                                  KeyComparator &comparator) -> bool {
  // need to maintain sorted order
  auto insert_idx = GetSize();  // initial assume at right-hand most
  auto left = 0;
  auto right = GetSize() - 1;
  while (left <= right) {
    // bindary search
    auto mid = left + (right - left) / 2;
    auto comp_res = comparator(key, KeyAt(mid));
    if (comp_res == 0) {
      return false;  // duplicate key
    }
    if (comp_res < 0) {
      insert_idx = mid;
      right = mid - 1;
    } else {
      left = mid + 1;
    }
  }
  ExcavateIndex(insert_idx);
  SetKeyAt(insert_idx, key);
  SetValueAt(insert_idx, value);
  IncreaseSize(1);
  return true;
}

/*
 * Shift all elements starting from index to right by 1 position
 * so that index end up with an empty hole for insert
 * but will not increase the size, it's up to the caller to do so
 */
INDEX_TEMPLATE_ARGUMENTS
void BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::ExcavateIndex(int index) {
  // TODO(YukunJ): Consider use std::copy_backward
  for (auto i = GetSize(); i > index; i--) {
    array_[i] = array_[i - 1];
  }
}

/*
 * Shift all elements starting from index to left by 1 position
 * essentially cover index-1, assuming it's deleted
 * but will not decrease the size, it's up to the caller to do so
 */
INDEX_TEMPLATE_ARGUMENTS
void BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::FillIndex(int index) {
  // TODO(YukunJ): Consider use std::copy
  for (auto i = index; i < GetSize(); i++) {
    array_[i - 1] = array_[i];
  }
}

/*
 * How many bytes each key-value pair takes
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::GetMappingSize() -> size_t { return sizeof(MappingType); }

/*
 * Expose a handler of the underlying data array for manipulation
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::GetArray() -> char * {
  return reinterpret_cast<char *>(&array_[0]);
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
