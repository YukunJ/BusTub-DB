//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetSize(1);  // the first entry with invalid key and negative infinity pointer
  SetMaxSize(max_size);
}

/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType { return array_[index].first; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

/*
 * Helper method to get/set the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
void BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::SetValueAt(int index, const ValueType &value) {
  array_[index].second = value;
}

/*
 * Flow through the internal page based on the key and comparator
 * and return next level page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::SearchPage(const KeyType &key, KeyComparator &comparator)
    -> ValueType {
  // find smallest i s.t. key <= curr_page[i].key
  auto bigger_or_equal_key_idx = -1;
  auto left = 1;
  auto right = GetSize() - 1;
  while (left <= right) {
    // bindary search
    auto mid = left + (right - left) / 2;
    if (comparator(key, KeyAt(mid)) <= 0) {
      bigger_or_equal_key_idx = mid;
      right = mid - 1;
    } else {
      left = mid + 1;
    }
  }

  auto jump_idx = -1;
  if (bigger_or_equal_key_idx == -1) {
    jump_idx = GetSize() - 1;
  } else {
    if (comparator(key, KeyAt(bigger_or_equal_key_idx)) == 0) {
      // equal, go to right pointer
      jump_idx = bigger_or_equal_key_idx;
    } else {
      // strict smaller, go to left pointer
      jump_idx = bigger_or_equal_key_idx - 1;
    }
  }
  return ValueAt(jump_idx);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::Insert(const KeyType &key, const ValueType &value,
                                                                      KeyComparator &comparator) -> bool {
  // need to maintain sorted order
  auto insert_idx = GetSize();  // initial assume at right-hand most
  auto left = 1;
  auto right = GetSize() - 1;
  while (left <= right) {
    // bindary search
    auto mid = left + (right - left) / 2;
    auto comp_res = comparator(key, KeyAt(mid));
    if (comp_res == 0) {
      return false;  // duplicate key
    }
    if (comp_res < 0) {
      // mid might be the place
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

INDEX_TEMPLATE_ARGUMENTS
auto BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::GetMappingSize() -> size_t {
  return sizeof(MappingType);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::GetArray() -> char * {
  return reinterpret_cast<char *>(&array_[0]);
}

INDEX_TEMPLATE_ARGUMENTS
void BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::ExcavateIndex(int index) {
  for (auto i = GetSize(); i > index; i--) {
    array_[i] = array_[i - 1];
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::FillIndex(int index) {
  for (auto i = index; i < GetSize(); i++) {
    array_[i - 1] = array_[i];
  }
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
